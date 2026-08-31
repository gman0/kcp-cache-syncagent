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
	"flag"
	"fmt"
	golog "log"
	"slices"

	"github.com/go-logr/zapr"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	reconcilerlog "k8c.io/reconciler/pkg/log"

	cachectrl "github.com/gman0/kcp-cache-syncagent/internal/controller/cache"
	"github.com/gman0/kcp-cache-syncagent/internal/kubeconfig"
	syncagentlog "github.com/gman0/kcp-cache-syncagent/internal/log"
	"github.com/gman0/kcp-cache-syncagent/internal/version"

	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrlruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	ctrlruntimelog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var availableControllers = sets.New("apiexport", "apiresourceschema", "sync")

func main() {
	ctx := context.Background()

	opts := NewOptions()
	opts.AddFlags(pflag.CommandLine)

	// ctrl-runtime will have added its --kubeconfig to Go's flag set
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	if err := opts.Validate(); err != nil {
		golog.Fatalf("Invalid command line: %v", err)
	}

	log := syncagentlog.NewFromOptions(opts.LogOptions)

	if err := opts.Complete(); err != nil {
		log.With(zap.Error(err)).Fatal("Invalid command line")
	}

	sugar := log.Sugar()

	// set the logger used by sigs.k8s.io/controller-runtime
	ctrlruntimelog.SetLogger(zapr.NewLogger(log.WithOptions(zap.AddCallerSkip(1))))
	reconcilerlog.SetLogger(sugar)

	if err := run(ctx, sugar, opts); err != nil {
		sugar.Fatalw("Sync Agent has encountered an error", zap.Error(err))
	}
}

func run(ctx context.Context, log *zap.SugaredLogger, opts *Options) error {
	v := version.NewAppVersion()
	hello := log.With(
		"version", v.GitVersion,
		"name", opts.AgentName,
	)

	hello.Info("Salut, I'm the kcp Sync Agent - for cache")

	// create the ctrl-runtime manager
	mgr, err := setupLocalManager(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to setup local manager: %w", err)
	}

	kcpRootShardKubeconfig, err := loadKubeconfig(opts.KcpRootShardKubeconfig)
	if err != nil {
		return fmt.Errorf("failed to load kcp root shard kubeconfig: %w", err)
	}
	if err := kubeconfig.Validate(kcpRootShardKubeconfig); err != nil {
		return fmt.Errorf("failed to check kcp root shard kubeconfig: %w", err)
	}

	rootShardCluster, err := setupKcpRootShardCluster(kcpRootShardKubeconfig)
	if err != nil {
		return fmt.Errorf("failed to initialize root shard kcp cluster: %w", err)
	}

	startController := func(name string, creator func() error) error {
		if slices.Contains(opts.DisabledControllers, name) {
			log.Infof("Not starting %s controller because it is disabled.", name)
			return nil
		}

		if err := creator(); err != nil {
			return fmt.Errorf("failed to add %s controller: %w", name, err)
		}

		return nil
	}

	if err := startController("cache", func() error {
		return cachectrl.Add(mgr, rootShardCluster, log, 4, opts.AgentName)
	}); err != nil {
		return err
	}

	log.Info("Starting kcp Sync Agent…")

	return mgr.Start(ctx)
}

func setupLocalManager(ctx context.Context, opts *Options) (manager.Manager, error) {
	scheme := runtime.NewScheme()
	restConfig := ctrlruntime.GetConfigOrDie()

	if opts.KubeconfigHostOverride != "" {
		restConfig.Host = opts.KubeconfigHostOverride
	}

	if opts.KubeconfigCAFileOverride != "" {
		// override the caData if it exists.
		if len(restConfig.TLSClientConfig.CAData) > 0 {
			restConfig.TLSClientConfig.CAData = nil
		}
		restConfig.TLSClientConfig.CAFile = opts.KubeconfigCAFileOverride
	}

	mgr, err := manager.New(restConfig, manager.Options{
		Scheme: scheme,
		BaseContext: func() context.Context {
			return ctx
		},
		Metrics:                 metricsserver.Options{BindAddress: opts.MetricsAddr},
		LeaderElection:          opts.EnableLeaderElection,
		LeaderElectionID:        "cachesyncagent." + opts.AgentName,
		LeaderElectionNamespace: opts.Namespace,
		HealthProbeBindAddress:  opts.HealthAddr,
	})
	if err != nil {
		return nil, err
	}

	if err := corev1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to register local scheme %s: %w", corev1.SchemeGroupVersion, err)
	}

	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to register local scheme %s: %w", apiextensionsv1.SchemeGroupVersion, err)
	}

	if err := kcpcorev1alpha1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to register local scheme %s: %w", kcpcorev1alpha1.SchemeGroupVersion, err)
	}

	return mgr, nil
}

func setupKcpRootShardCluster(config *rest.Config) (cluster.Cluster, error) {
	scheme := runtime.NewScheme()

	if err := kcpcorev1alpha1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to register scheme %s: %w", kcpcorev1alpha1.SchemeGroupVersion, err)
	}

	return cluster.New(config, func(o *cluster.Options) {
		o.Scheme = scheme
		o.Cache = cache.Options{
			Scheme: scheme,
		}
	})
}

func loadKubeconfig(filename string) (*rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.ExplicitPath = filename

	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, nil).ClientConfig()
}
