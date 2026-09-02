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

// Package discovery provides the peer cache-server discovery provider.
package discovery

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	cacheclient "github.com/gman0/kcp-cache-syncagent/internal/client"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

const (
	// systemCacheServerShard is the virtual shard that holds the cache-server's
	// own Cache object.
	systemCacheServerShard = "system:cache:server"

	// systemCacheCluster is the logical cluster that holds Cache objects.
	systemCacheCluster = "system:cache"

	// systemShardCluster is the logical cluster that holds Shard objects.
	systemShardCluster = "system:shard"

	// cacheSelfAnnotation is the annotation value on the source's own Cache
	// object.
	cacheSelfAnnotation = ".self"

	// cacheAnnotationKey marks a Cache object with its owning cache-server name.
	cacheAnnotationKey = "kcp.io/cache"

	// clusterAnnotationKey holds the logical cluster of an object.
	clusterAnnotationKey = "kcp.io/cluster"
)

var cacheGVR = schema.GroupVersionResource{
	Group:    "core.kcp.io",
	Version:  "v1alpha1",
	Resource: "caches",
}

// Provider implements multicluster.Provider and multicluster.ProviderRunnable.
// It discovers peer cache-servers by watching Cache objects on the source.
type Provider struct {
	sourceConfig    *rest.Config
	sourceURL       string
	initialPeerURLs []string
	caFile          string
	certFile        string
	keyFile         string
	log             *zap.SugaredLogger

	mu    sync.RWMutex
	peers map[multicluster.ClusterName]*peerEntry
}

type peerEntry struct {
	cl     cluster.Cluster
	cancel context.CancelFunc
}

// New creates a Provider.
func New(
	sourceConfig *rest.Config,
	sourceURL string,
	initialPeerURLs []string,
	caFile, certFile, keyFile string,
	log *zap.SugaredLogger,
) *Provider {
	return &Provider{
		sourceConfig:    sourceConfig,
		sourceURL:       sourceURL,
		initialPeerURLs: initialPeerURLs,
		caFile:          caFile,
		certFile:        certFile,
		keyFile:         keyFile,
		log:             log.Named("peer-discovery"),
		peers:           make(map[multicluster.ClusterName]*peerEntry),
	}
}

// Start implements multicluster.ProviderRunnable.
func (p *Provider) Start(ctx context.Context, aware multicluster.Aware) error {
	// Seed from initial peer URLs before starting the ongoing watch.
	for _, url := range p.initialPeerURLs {
		if err := p.seedFromPeer(ctx, aware, url); err != nil {
			p.log.Warnw("Failed to seed from initial peer", "url", url, zap.Error(err))
		}
	}

	// Watch Cache objects on the source for ongoing peer discovery.
	for {
		if err := p.watchCacheObjects(ctx, aware); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			p.log.Warnw("Cache watch loop failed, retrying", zap.Error(err))
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
			}
		} else {
			return nil
		}
	}
}

// seedFromPeer does an initial cross-shard LIST of Cache objects on peerURL to
// discover peers that are already in the mesh.
func (p *Provider) seedFromPeer(ctx context.Context, aware multicluster.Aware, peerURL string) error {
	peerConfig, err := cacheclient.BuildConfig(peerURL, p.caFile, p.certFile, p.keyFile)
	if err != nil {
		return fmt.Errorf("building peer config for %s: %w", peerURL, err)
	}

	dynClient, err := dynamic.NewForConfig(peerConfig)
	if err != nil {
		return fmt.Errorf("creating dynamic client for %s: %w", peerURL, err)
	}

	list, err := dynClient.Resource(cacheGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing Cache objects on %s: %w", peerURL, err)
	}

	for i := range list.Items {
		if err := p.engageCacheObject(ctx, aware, &list.Items[i]); err != nil {
			p.log.Warnw("Failed to engage cache-server from seed", "url", peerURL, zap.Error(err))
		}
	}
	return nil
}

// watchCacheObjects watches Cache objects on the source and calls Engage for
// each new peer found.
func (p *Provider) watchCacheObjects(ctx context.Context, aware multicluster.Aware) error {
	dynClient, err := dynamic.NewForConfig(p.sourceConfig)
	if err != nil {
		return fmt.Errorf("creating dynamic client: %w", err)
	}

	list, err := dynClient.Resource(cacheGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing Cache objects: %w", err)
	}

	for i := range list.Items {
		if err := p.engageCacheObject(ctx, aware, &list.Items[i]); err != nil {
			p.log.Warnw("Failed to engage cache-server from list", zap.Error(err))
		}
	}

	watcher, err := dynClient.Resource(cacheGVR).Watch(ctx, metav1.ListOptions{
		ResourceVersion: list.ResourceVersion,
	})
	if err != nil {
		return fmt.Errorf("watching Cache objects: %w", err)
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return fmt.Errorf("watch channel closed")
			}
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			switch event.Type {
			case watch.Added, watch.Modified:
				if err := p.engageCacheObject(ctx, aware, obj); err != nil {
					p.log.Warnw("Failed to engage cache-server", zap.Error(err))
				}
			case watch.Deleted:
				p.disengageCacheObject(obj)
			}
		}
	}
}

// engageCacheObject processes a Cache object: if it represents a peer
// cache-server (not our own source), creates a cluster for it and calls Engage.
func (p *Provider) engageCacheObject(ctx context.Context, aware multicluster.Aware, obj *unstructured.Unstructured) error {
	baseURL, found, err := unstructured.NestedString(obj.Object, "spec", "baseURL")
	if err != nil || !found || baseURL == "" {
		return nil
	}

	// Skip the source's own Cache object.
	if baseURL == p.sourceURL {
		return nil
	}

	// Cache object name is the cache-server's name.
	peerName := multicluster.ClusterName(obj.GetName())
	if peerName == "" {
		return nil
	}

	p.mu.Lock()
	_, already := p.peers[peerName]
	p.mu.Unlock()
	if already {
		return nil
	}

	peerConfig, err := cacheclient.BuildConfig(baseURL, p.caFile, p.certFile, p.keyFile)
	if err != nil {
		return fmt.Errorf("building config for peer %q: %w", peerName, err)
	}

	peerCluster, err := cluster.New(peerConfig)
	if err != nil {
		return fmt.Errorf("creating cluster for peer %q: %w", peerName, err)
	}

	peerCtx, cancel := context.WithCancel(ctx)

	p.mu.Lock()
	// Double-check under lock.
	if _, already := p.peers[peerName]; already {
		p.mu.Unlock()
		cancel()
		return nil
	}
	p.peers[peerName] = &peerEntry{cl: peerCluster, cancel: cancel}
	p.mu.Unlock()

	p.log.Infow("Engaging peer cache-server", "peer", peerName, "url", baseURL)

	if err := aware.Engage(peerCtx, peerName, peerCluster); err != nil {
		cancel()
		p.mu.Lock()
		delete(p.peers, peerName)
		p.mu.Unlock()
		return fmt.Errorf("engaging peer %q: %w", peerName, err)
	}

	return nil
}

// disengageCacheObject cancels the context for a peer whose Cache object was deleted.
func (p *Provider) disengageCacheObject(obj *unstructured.Unstructured) {
	peerName := multicluster.ClusterName(obj.GetName())
	if peerName == "" {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if entry, ok := p.peers[peerName]; ok {
		p.log.Infow("Disengaging peer cache-server", "peer", peerName)
		entry.cancel()
		delete(p.peers, peerName)
	}
}

// Get implements multicluster.Provider.
func (p *Provider) Get(_ context.Context, name multicluster.ClusterName) (cluster.Cluster, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if entry, ok := p.peers[name]; ok {
		return entry.cl, nil
	}
	return nil, multicluster.ErrClusterNotFound
}

// IndexField implements multicluster.Provider.
func (p *Provider) IndexField(ctx context.Context, obj client.Object, field string, fn client.IndexerFunc) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, entry := range p.peers {
		if err := entry.cl.GetFieldIndexer().IndexField(ctx, obj, field, fn); err != nil {
			return err
		}
	}
	return nil
}
