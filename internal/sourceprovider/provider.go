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

// Package sourceprovider provides a multicluster.Provider for the source
// cache-server, engaging it as a single named cluster.
package sourceprovider

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/gman0/kcp-cache-syncagent/internal/clusters"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

var _ multicluster.Provider = (*Provider)(nil)
var _ multicluster.ProviderRunnable = (*Provider)(nil)

// Provider implements multicluster.Provider and multicluster.ProviderRunnable
// for the single source cache-server cluster.
type Provider struct {
	cl  cluster.Cluster
	log *zap.SugaredLogger
}

// New returns a Provider that engages cl as the source cache-server cluster.
// The caller is responsible for creating the cluster.Cluster; this provider
// starts its cache.
func New(cl cluster.Cluster, log *zap.SugaredLogger) *Provider {
	return &Provider{
		cl:  cl,
		log: log.Named("source-provider"),
	}
}

// Start implements multicluster.ProviderRunnable. It starts the source
// cluster's cache in a goroutine and then engages it with the mc manager.
func (p *Provider) Start(ctx context.Context, aware multicluster.Aware) error {
	go func() {
		if err := p.cl.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			p.log.Warnw("Source cluster cache exited unexpectedly", zap.Error(err))
		}
	}()

	p.log.Infow("Engaging source cache-server", "name", clusters.SourceClusterName)
	if err := aware.Engage(ctx, multicluster.ClusterName(clusters.SourceClusterName), p.cl); err != nil {
		return fmt.Errorf("engaging source cluster: %w", err)
	}

	<-ctx.Done()
	return nil
}

// Get implements multicluster.Provider.
func (p *Provider) Get(_ context.Context, name multicluster.ClusterName) (cluster.Cluster, error) {
	if name == multicluster.ClusterName(clusters.SourceClusterName) {
		return p.cl, nil
	}
	return nil, multicluster.ErrClusterNotFound
}

// IndexField implements multicluster.Provider.
func (p *Provider) IndexField(ctx context.Context, obj client.Object, field string, fn client.IndexerFunc) error {
	return p.cl.GetFieldIndexer().IndexField(ctx, obj, field, fn)
}
