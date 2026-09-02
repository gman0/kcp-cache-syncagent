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

// Package peer provides the peer cache-server machinery.
package peer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"

	cacheclient "github.com/gman0/kcp-cache-syncagent/internal/client"

	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

var _ multicluster.Provider = &Provider{}
var _ multicluster.ProviderRunnable = &Provider{}

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
	scheme          *runtime.Scheme

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
) (*Provider, error) {
	scheme := runtime.NewScheme()
	if err := kcpcorev1alpha1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("registering kcp core scheme: %w", err)
	}

	return &Provider{
		sourceConfig:    sourceConfig,
		sourceURL:       sourceURL,
		initialPeerURLs: initialPeerURLs,
		caFile:          caFile,
		certFile:        certFile,
		keyFile:         keyFile,
		log:             log.Named("peer-discovery"),
		scheme:          scheme,
		peers:           make(map[multicluster.ClusterName]*peerEntry),
	}, nil
}

// Start implements multicluster.ProviderRunnable.
func (p *Provider) Start(ctx context.Context, aware multicluster.Aware) error {
	// Seed from initial peer URLs before starting the ongoing watch.
	for _, url := range p.initialPeerURLs {
		if err := p.seedFromPeer(ctx, aware, url); err != nil {
			p.log.Warnw("Failed to seed from initial peer", "url", url, zap.Error(err))
		}
	}

	// Build a controller-runtime cache for watching Cache objects on the source.
	sourceCache, err := ctrlcache.New(p.sourceConfig, ctrlcache.Options{
		Scheme: p.scheme,
	})
	if err != nil {
		return fmt.Errorf("creating source cache: %w", err)
	}

	informer, err := sourceCache.GetInformer(ctx, &kcpcorev1alpha1.Cache{}, ctrlcache.BlockUntilSynced(false))
	if err != nil {
		return fmt.Errorf("getting Cache informer: %w", err)
	}

	handler, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			cacheObj, ok := obj.(*kcpcorev1alpha1.Cache)
			if !ok {
				return
			}
			if err := p.engageCacheObject(ctx, aware, cacheObj); err != nil {
				p.log.Warnw("Failed to engage peer", zap.Error(err))
			}
		},
		UpdateFunc: func(_, newObj any) {
			cacheObj, ok := newObj.(*kcpcorev1alpha1.Cache)
			if !ok {
				return
			}
			if err := p.engageCacheObject(ctx, aware, cacheObj); err != nil {
				p.log.Warnw("Failed to engage peer", zap.Error(err))
			}
		},
		DeleteFunc: func(obj any) {
			cacheObj, ok := obj.(*kcpcorev1alpha1.Cache)
			if !ok {
				tombstone, ok := obj.(toolscache.DeletedFinalStateUnknown)
				if !ok {
					p.log.Warnw("Couldn't get object from tombstone", "obj", obj)
					return
				}
				cacheObj, ok = tombstone.Obj.(*kcpcorev1alpha1.Cache)
				if !ok {
					p.log.Warnw("Tombstone contained unexpected object type", "obj", tombstone.Obj)
					return
				}
			}
			p.disengageCacheObject(cacheObj)
		},
	})
	if err != nil {
		return fmt.Errorf("adding event handler: %w", err)
	}
	defer func() {
		if err := informer.RemoveEventHandler(handler); err != nil {
			p.log.Warnw("Failed to remove event handler", zap.Error(err))
		}
	}()

	return sourceCache.Start(ctx)
}

// seedFromPeer lists Cache objects on peerURL and engages each discovered peer.
func (p *Provider) seedFromPeer(ctx context.Context, aware multicluster.Aware, peerURL string) error {
	peerConfig, err := cacheclient.BuildConfig(peerURL, p.caFile, p.certFile, p.keyFile)
	if err != nil {
		return fmt.Errorf("building peer config for %s: %w", peerURL, err)
	}

	cl, err := client.New(peerConfig, client.Options{Scheme: p.scheme})
	if err != nil {
		return fmt.Errorf("creating client for %s: %w", peerURL, err)
	}

	var list kcpcorev1alpha1.CacheList
	if err := cl.List(ctx, &list); err != nil {
		return fmt.Errorf("listing Cache objects on %s: %w", peerURL, err)
	}

	for i := range list.Items {
		if err := p.engageCacheObject(ctx, aware, &list.Items[i]); err != nil {
			p.log.Warnw("Failed to engage cache-server from seed", "url", peerURL, zap.Error(err))
		}
	}
	return nil
}

// engageCacheObject processes a Cache object: if it's a new peer (not our own
// source), creates a cluster for it and calls Engage.
func (p *Provider) engageCacheObject(ctx context.Context, aware multicluster.Aware, obj *kcpcorev1alpha1.Cache) error {
	baseURL := obj.Spec.BaseURL
	if baseURL == "" || baseURL == p.sourceURL {
		return nil
	}

	peerName := multicluster.ClusterName(obj.GetName())
	if peerName == "" {
		return nil
	}

	// Fast path: already engaged.
	p.mu.RLock()
	_, already := p.peers[peerName]
	p.mu.RUnlock()
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
	// Double-check under write lock.
	if _, already := p.peers[peerName]; already {
		p.mu.Unlock()
		cancel()
		return nil
	}
	p.peers[peerName] = &peerEntry{cl: peerCluster, cancel: cancel}
	p.mu.Unlock()

	// Start the peer cluster's cache in the background so that MultiClusterWatch
	// sources can call GetInformer and WaitForCacheSync when Engage is called.
	go func() {
		if err := peerCluster.Start(peerCtx); err != nil && !errors.Is(err, context.Canceled) {
			p.log.Warnw("Peer cluster exited unexpectedly", "peer", peerName, zap.Error(err))
		}
	}()

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
func (p *Provider) disengageCacheObject(obj *kcpcorev1alpha1.Cache) {
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
