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

// Package manager provides the DynamicMultiClusterManager, adapted from
// github.com/kcp-dev/api-syncagent/internal/kcp/multicluster.go.
package manager

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"

	ctrlmanager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

// DynamicMultiClusterManager wraps a multicluster-aware manager and allows
// controllers to be started and pre-seeded with engaged clusters at any time.
//
// It is goroutine-safe.
type DynamicMultiClusterManager struct {
	manager mcmanager.Manager

	runnablesLock sync.RWMutex
	runnables     map[string]mcmanager.Runnable

	tracker *clusterTracker
}

// New wraps sourceManager with the given multicluster provider to form a
// DynamicMultiClusterManager. sourceManager must be a controller-runtime
// manager already configured with a suitable scheme and rest.Config.
func New(sourceManager ctrlmanager.Manager, provider multicluster.Provider) (*DynamicMultiClusterManager, error) {
	mcMgr, err := mcmanager.WithMultiCluster(sourceManager, provider)
	if err != nil {
		return nil, fmt.Errorf("creating multicluster manager: %w", err)
	}

	d := &DynamicMultiClusterManager{
		manager:   mcMgr,
		runnables: make(map[string]mcmanager.Runnable),
	}

	tracker := newClusterTracker(d)
	d.tracker = tracker

	if err := mcMgr.Add(tracker); err != nil {
		return nil, fmt.Errorf("adding cluster tracker: %w", err)
	}

	return d, nil
}

// Start starts the underlying multicluster manager (including the provider if
// it implements ProviderRunnable) and blocks until ctx is cancelled.
func (d *DynamicMultiClusterManager) Start(ctx context.Context) error {
	return d.manager.Start(ctx)
}

// StartController registers ctrl, starts it in a goroutine with the given ctx,
// and pre-seeds it with all currently-engaged clusters so it doesn't miss any
// that were discovered before it was registered.
func (d *DynamicMultiClusterManager) StartController(ctx context.Context, log *zap.SugaredLogger, ctrl mcmanager.Runnable) error {
	key := uuid.New().String()

	d.runnablesLock.Lock()
	d.runnables[key] = ctrl
	d.runnablesLock.Unlock()

	go func() {
		log.Debugw("Starting replication controller", "key", key)
		if err := ctrl.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Errorw("Replication controller exited unexpectedly", zap.Error(err), "key", key)
		}
		log.Debugw("Replication controller stopped", "key", key)

		d.runnablesLock.Lock()
		delete(d.runnables, key)
		d.runnablesLock.Unlock()
	}()

	if err := d.tracker.preSeed(ctrl); err != nil {
		return fmt.Errorf("pre-seeding controller: %w", err)
	}

	return nil
}

// engagedCluster pairs a cluster with the context tied to its lifetime.
type engagedCluster struct {
	ctx context.Context
	cl  cluster.Cluster
}

// clusterTracker is a no-op Runnable that records every Engage call so that
// controllers registered later via StartController can be pre-seeded.
type clusterTracker struct {
	d       *DynamicMultiClusterManager
	lock    sync.RWMutex
	engaged map[multicluster.ClusterName]engagedCluster
}

func newClusterTracker(d *DynamicMultiClusterManager) *clusterTracker {
	return &clusterTracker{
		d:       d,
		engaged: make(map[multicluster.ClusterName]engagedCluster),
	}
}

func (t *clusterTracker) Start(_ context.Context) error {
	return nil
}

func (t *clusterTracker) Engage(ctx context.Context, name multicluster.ClusterName, cl cluster.Cluster) error {
	t.lock.Lock()
	if _, exists := t.engaged[name]; !exists {
		t.engaged[name] = engagedCluster{ctx: ctx, cl: cl}
		go func() {
			<-ctx.Done()
			t.lock.Lock()
			delete(t.engaged, name)
			t.lock.Unlock()
		}()
	}
	t.lock.Unlock()

	t.d.runnablesLock.RLock()
	defer t.d.runnablesLock.RUnlock()

	for _, ctrl := range t.d.runnables {
		if err := ctrl.Engage(ctx, name, cl); err != nil {
			return err
		}
	}
	return nil
}

func (t *clusterTracker) preSeed(ctrl mcmanager.Runnable) error {
	t.lock.RLock()
	defer t.lock.RUnlock()

	for name, ec := range t.engaged {
		if err := ctrl.Engage(ec.ctx, name, ec.cl); err != nil {
			return fmt.Errorf("engaging cluster %q: %w", name, err)
		}
	}
	return nil
}
