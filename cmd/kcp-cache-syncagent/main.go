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
	"github.com/gman0/kcp-cache-syncagent/internal/controller/crdmanager"
	shardtracker "github.com/gman0/kcp-cache-syncagent/internal/controller/shard"
	syncagentlog "github.com/gman0/kcp-cache-syncagent/internal/log"
	dynmanager "github.com/gman0/kcp-cache-syncagent/internal/manager"
	"github.com/gman0/kcp-cache-syncagent/internal/peer"
	"github.com/gman0/kcp-cache-syncagent/internal/version"

	kcpcrypto "github.com/kcp-dev/apimachinery/v2/pkg/util/crypto"
	"github.com/kcp-dev/logicalcluster/v3"
	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	kcpclientset "github.com/kcp-dev/sdk/client/clientset/versioned/cluster"
	kcpinformers "github.com/kcp-dev/sdk/client/informers/externalversions"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlruntimelog "sigs.k8s.io/controller-runtime/pkg/log"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
)

const resyncPeriod = 10 * time.Hour

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

	// Peer discovery provider: watches Cache objects on the source for peers.
	peerDiscoveryProvider, err := peer.New(
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

	// Main multicluster manager: runs on local cluster for leader election,
	// uses the peer discovery provider for peer coordination.
	leaderElectionIDSuffixHash := sha256.Sum224([]byte(opts.SourceURL))
	leaderElectionIDSuffix := kcpcrypto.Base36.BytesPad(leaderElectionIDSuffixHash[:])[8:]
	mgr, err := mcmanager.New(localRestConfig, peerDiscoveryProvider, ctrl.Options{
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

	// DynamicMultiClusterManager wraps the mc manager to allow controllers to
	// be started dynamically (one per CRD) and pre-seeded with known peers.
	dmcm, err := dynmanager.New(mgr)
	if err != nil {
		return fmt.Errorf("creating dynamic multicluster manager: %w", err)
	}

	// Source manager: controller-runtime manager pointed at the source
	// cache-server. CRD and Shard controllers register here.
	sourceMgr, err := ctrl.NewManager(sourceRestConfig, ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: "0", // metrics are served by the main manager
		},
	})
	if err != nil {
		return fmt.Errorf("creating source manager: %w", err)
	}

	// Add the source manager as a runnable so it starts with the main manager.
	if err := mgr.GetLocalManager().Add(sourceMgr); err != nil {
		return fmt.Errorf("adding source manager: %w", err)
	}

	// Build a kcpclientset against the source for peer discovery informers and
	// own-name resolution (requires cross-shard access via the kcp client).
	sourceClient, err := kcpclientset.NewForConfig(sourceRestConfig)
	if err != nil {
		return fmt.Errorf("creating source client: %w", err)
	}
	sourceKcpSharedInformerFactory := kcpinformers.NewSharedInformerFactoryWithOptions(
		sourceClient,
		resyncPeriod,
	)

	log.Info("Starting source informers")
	go sourceKcpSharedInformerFactory.Core().V1alpha1().Caches().Informer().RunWithContext(ctx)
	go sourceKcpSharedInformerFactory.Core().V1alpha1().Shards().Informer().RunWithContext(ctx)
	sourceKcpSharedInformerFactory.Start(ctx.Done())

	// Resolve own cache-server name from the source before the manager starts.
	ownName, err := resolveOwnName(ctx, sourceClient)
	if err != nil {
		return fmt.Errorf("resolving own cache-server name: %w", err)
	}
	log.Infow("Resolved own cache-server name", "name", ownName)

	// Authoritative shard tracker: watches Shard objects on the source.
	tracker, err := shardtracker.Add(sourceMgr, ownName, log)
	if err != nil {
		return fmt.Errorf("setting up shard tracker: %w", err)
	}

	// CRD manager: watches CRDs and starts/stops replication controllers.
	if err := crdmanager.Add(ctx, sourceMgr, dmcm, tracker, log); err != nil {
		return fmt.Errorf("adding CRD manager: %w", err)
	}

	log.Info("Starting kcp cache-syncagent…")
	return dmcm.Start(ctx)
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
