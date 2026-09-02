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
	"fmt"
	golog "log"
	"time"

	"github.com/go-logr/zapr"
	"github.com/spf13/pflag"
	"go.uber.org/zap"

	cacheclient "github.com/gman0/kcp-cache-syncagent/internal/client"
	kshard "github.com/gman0/kcp-cache-syncagent/internal/client/shard"
	"github.com/gman0/kcp-cache-syncagent/internal/controller/crdmanager"
	"github.com/gman0/kcp-cache-syncagent/internal/discovery"
	syncagentlog "github.com/gman0/kcp-cache-syncagent/internal/log"
	dynmanager "github.com/gman0/kcp-cache-syncagent/internal/manager"
	shardtracker "github.com/gman0/kcp-cache-syncagent/internal/shard"
	"github.com/gman0/kcp-cache-syncagent/internal/version"

	"github.com/kcp-dev/logicalcluster/v3"
	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	kcpclientset "github.com/kcp-dev/sdk/client/clientset/versioned/cluster"
	kcpinformers "github.com/kcp-dev/sdk/client/informers/externalversions"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	// "k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrlruntimelog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
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

	// Build the TLS-authenticated rest.Config for the source cache-server.
	sourceConfig, err := cacheclient.BuildConfig(
		opts.SourceURL,
		opts.CacheClientCAFile,
		opts.CacheClientCertFile,
		opts.CacheClientKeyFile,
	)
	if err != nil {
		return fmt.Errorf("building source config: %w", err)
	}

	// Build a shared informer against the source cache.

	sourceClient, err := kcpclientset.NewForConfig(sourceConfig)
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

	// Build the source manager's scheme.
	scheme := runtime.NewScheme()
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("registering apiextensions scheme: %w", err)
	}
	if err := kcpcorev1alpha1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("registering kcp core scheme: %w", err)
	}

	// Optional leader election via a separate Kubernetes cluster.
	leaderElectionEnabled := opts.LeaderElectionKubeconfig != ""
	var leaderElectionConfig *rest.Config
	if leaderElectionEnabled {
		leaderElectionConfig, err = loadKubeconfig(opts.LeaderElectionKubeconfig)
		if err != nil {
			return fmt.Errorf("loading leader-election kubeconfig: %w", err)
		}
	}

	// Create the source manager (connects to the source cache-server).
	sourceMgr, err := manager.New(sourceConfig, manager.Options{
		Scheme: scheme,
		BaseContext: func() context.Context {
			return ctx
		},
		Metrics:                       metricsserver.Options{BindAddress: opts.MetricsAddr},
		HealthProbeBindAddress:        opts.HealthAddr,
		LeaderElection:                leaderElectionEnabled,
		LeaderElectionID:              "kcp-cache-syncagent",
		LeaderElectionNamespace:       opts.Namespace,
		LeaderElectionConfig:          leaderElectionConfig,
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		return fmt.Errorf("creating source manager: %w", err)
	}

	// Authoritative shard tracker: watches Shard objects on the source.
	tracker := shardtracker.New(sourceConfig, ownName, log)
	if err := sourceMgr.Add(tracker); err != nil {
		return fmt.Errorf("adding shard tracker: %w", err)
	}

	// Peer discovery peerDiscoveryProvider: watches Cache objects on the source for peers.
	peerDiscoveryProvider := discovery.New(
		sourceConfig,
		opts.SourceURL,
		opts.InitialPeerURLs,
		opts.CacheClientCAFile,
		opts.CacheClientCertFile,
		opts.CacheClientKeyFile,
		log,
	)

	// DynamicMultiClusterManager wraps the source manager with multicluster
	// coordination; the provider is started automatically by mcManager.Start.
	dmcm, err := dynmanager.New(sourceMgr, peerDiscoveryProvider)
	if err != nil {
		return fmt.Errorf("creating dynamic multicluster manager: %w", err)
	}

	// CRD manager: watches CRDs and starts/stops replication controllers.
	crdMgr := crdmanager.New(sourceConfig, dmcm, tracker, log)
	if err := sourceMgr.Add(crdMgr); err != nil {
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
