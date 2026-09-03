/*
Copyright 2026 The KCP Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	golog "log"
	"time"

	"github.com/go-logr/zapr"
	"github.com/spf13/pflag"
	"go.uber.org/zap"

	cacheclient "github.com/gman0/kcp-cache-syncagent/internal/client"
	kshard "github.com/gman0/kcp-cache-syncagent/internal/client/shard"
	"github.com/gman0/kcp-cache-syncagent/internal/clusters"
	"github.com/gman0/kcp-cache-syncagent/internal/controller/authoritativeshardregistry"
	resourcereplication "github.com/gman0/kcp-cache-syncagent/internal/controller/resourcereplication"
	syncagentlog "github.com/gman0/kcp-cache-syncagent/internal/log"
	"github.com/gman0/kcp-cache-syncagent/internal/peerprovider"
	"github.com/gman0/kcp-cache-syncagent/internal/sourceprovider"
	"github.com/gman0/kcp-cache-syncagent/internal/version"

	kcpcrypto "github.com/kcp-dev/apimachinery/v2/pkg/util/crypto"
	"github.com/kcp-dev/logicalcluster/v3"
	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	kcpclientset "github.com/kcp-dev/sdk/client/clientset/versioned/cluster"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	ctrlruntimelog "sigs.k8s.io/controller-runtime/pkg/log"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	multiprovider "sigs.k8s.io/multicluster-runtime/providers/multi"
)

func main() {
	ctx := context.Background()

	opts := NewOptions()
	opts.AddFlags(pflag.CommandLine)
	pflag.Parse()

	if err := opts.Validate(); err != nil {
		golog.Fatalf("Invalid command line: %v", err)
	}

	log := syncagentlog.NewFromOptions(opts.LogOptions)

	if err := opts.Complete(); err != nil {
		log.With(zap.Error(err)).Fatal("Invalid command line")
	}

	ctrlruntimelog.SetLogger(zapr.NewLogger(log.WithOptions(zap.AddCallerSkip(1))))

	if err := run(ctx, log.Sugar(), opts); err != nil {
		log.Sugar().Fatalw("kcp cache-syncagent encountered an error", zap.Error(err))
	}
}

func run(ctx context.Context, log *zap.SugaredLogger, opts *Options) error {
	v := version.NewAppVersion()
	log.Infow("Starting kcp cache-syncagent", "version", v.GitVersion, "source", opts.SourceURL)

	scheme := runtime.NewScheme()
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("registering apiextensions scheme: %w", err)
	}
	if err := kcpcorev1alpha1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("registering kcp core scheme: %w", err)
	}

	// Setup client configs.

	localRestConfig := ctrl.GetConfigOrDie()
	if opts.KubeconfigHostOverride != "" {
		localRestConfig.Host = opts.KubeconfigHostOverride
	}
	if opts.KubeconfigCAFileOverride != "" {
		if len(localRestConfig.TLSClientConfig.CAData) > 0 {
			localRestConfig.TLSClientConfig.CAData = nil
		}
		localRestConfig.TLSClientConfig.CAFile = opts.KubeconfigCAFileOverride
	}

	sourceRestConfig, err := cacheclient.BuildConfig(opts.SourceURL,
		opts.CacheClientCAFile,
		opts.CacheClientCertFile,
		opts.CacheClientKeyFile,
	)
	if err != nil {
		return fmt.Errorf("building source cache server REST config: %w", err)
	}

	// Source cluster: lightweight cache+client against the source cache-server.
	sourceCluster, err := cluster.New(sourceRestConfig, func(o *cluster.Options) {
		o.Scheme = scheme
	})
	if err != nil {
		return fmt.Errorf("creating source cluster: %w", err)
	}

	// Peer discovery provider: watches Cache objects on the source for peers.
	peerDiscoveryProvider, err := peerprovider.New(
		sourceRestConfig,
		opts.SourceURL,
		opts.InitialPeerURLs,
		opts.CacheClientCAFile,
		opts.CacheClientCertFile,
		opts.CacheClientKeyFile,
		log,
	)
	if err != nil {
		return fmt.Errorf("creating peer discovery provider: %w", err)
	}

	// Multi provider: aggregates source and peer providers under named prefixes.
	multiProv := multiprovider.New(multiprovider.Options{})
	if err := multiProv.AddProvider(clusters.SourceProviderName, sourceprovider.New(sourceCluster, log)); err != nil {
		return fmt.Errorf("adding source provider: %w", err)
	}
	if err := multiProv.AddProvider(clusters.PeersProviderName, peerDiscoveryProvider); err != nil {
		return fmt.Errorf("adding peers provider: %w", err)
	}

	// Multicluster manager: runs on local cluster for leader election and
	// routes cluster Engage calls via the multi provider.
	leaderElectionIDSuffixHash := sha256.Sum224([]byte(opts.SourceURL))
	leaderElectionIDSuffix := kcpcrypto.Base36.BytesPad(leaderElectionIDSuffixHash[:])[8:]
	mgr, err := mcmanager.New(localRestConfig, multiProv, ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: opts.MetricsAddr,
		},
		BaseContext:                   func() context.Context { return ctx },
		HealthProbeBindAddress:        opts.HealthAddr,
		LeaderElection:                opts.EnableLeaderElection,
		LeaderElectionID:              "kcp-cache-syncagent-" + leaderElectionIDSuffix,
		LeaderElectionNamespace:       opts.Namespace,
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		return fmt.Errorf("creating multicluster manager: %w", err)
	}

	// Build a kcpclientset against the source for own-name resolution
	// (requires cross-shard access via the kcp client).
	sourceKcpClient, err := kcpclientset.NewForConfig(sourceRestConfig)
	if err != nil {
		return fmt.Errorf("creating source kcp client: %w", err)
	}

	// Resolve own cache-server name from the source before the manager starts.
	log.Info("Retrieve source cache-server info")
	var ownName string
	if err := wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
		ownName, err = resolveOwnName(ctx, sourceKcpClient)
		if err != nil {
			log.Errorf("resolving own cache-server name: %w", err)
			return false, nil
		}
		return true, nil
	}); err != nil {
		log.Error(err, "failed to retrieve source cache-server info")
		return nil // don't klog.Fatal. This only happens when context is cancelled.
	}
	log.Infow("Resolved own cache-server name", "name", ownName)

	// Authoritative shard tracker: watches Shard objects on the source cluster.
	shardRegistry, err := authoritativeshardregistry.Add(mgr, ownName, log)
	if err != nil {
		return fmt.Errorf("setting up shard registry: %w", err)
	}

	// CRD manager: watches CRDs on the source, starts/stops replication
	// controllers, and receives peer Engage calls from the mc manager.
	if err := resourcereplication.Add(ctx, mgr, shardRegistry, log); err != nil {
		return fmt.Errorf("adding CRD manager: %w", err)
	}

	log.Info("Starting kcp cache-syncagent…")
	return mgr.Start(ctx)
}

// resolveOwnName reads the self-identifying Cache object from
// system:cache:server shard / system:cache cluster on the source to determine
// this syncagent's cache-server name.
func resolveOwnName(ctx context.Context, sourceClient kcpclientset.ClusterInterface) (string, error) {
	reqCtx := cacheclient.WithShardInContext(ctx, kshard.New("system:cache:server"))
	list, err := sourceClient.Cluster(logicalcluster.NewPath("system:cache")).CoreV1alpha1().Caches().List(reqCtx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("listing Cache objects in system:cache:server/system:cache: %w", err)
	}

	if len(list.Items) == 1 {
		return list.Items[0].Name, nil
	}

	return "", fmt.Errorf("could not identify own Cache object in system:cache:server/system:cache (found %d objects)", len(list.Items))
}

func loadKubeconfig(filename string) (*rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.ExplicitPath = filename
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, nil).ClientConfig()
}
