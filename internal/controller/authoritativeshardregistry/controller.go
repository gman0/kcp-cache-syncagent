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

// Package shard provides the authoritative-shard tracker.
package authoritativeshardregistry

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	kshard "github.com/gman0/kcp-cache-syncagent/internal/client/shard"
	"github.com/gman0/kcp-cache-syncagent/internal/clusters"

	"github.com/kcp-dev/logicalcluster/v3"
	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"
)

const (
	// cacheAnnotationKey is the annotation set on Shard objects by kcp to record
	// which cache-server the shard is directly connected to.
	cacheAnnotationKey = "kcp.io/cache"

	// systemShardCluster is the logical cluster that holds the authoritative
	// Shard objects we care about.
	systemShardCluster = "system:shard"
)

// Registry watches Shard objects on the source cache-server and maintains the
// set of shards authoritative for our cache-server instance (i.e. those whose
// kcp.io/cache annotation equals ownName).
type Registry struct {
	ownName string
	log     *zap.SugaredLogger
	mcMgr   mcmanager.Manager

	mu       sync.RWMutex
	shardSet map[kshard.Name]struct{}
}

// Add registers the shard registry controller with mgr and returns the Registry.
// The controller watches Shard objects only on the source cache-server cluster.
func Add(mgr mcmanager.Manager, ownName string, log *zap.SugaredLogger) (*Registry, error) {
	t := &Registry{
		ownName:  ownName,
		log:      log.Named("shard-registry"),
		mcMgr:    mgr,
		shardSet: make(map[kshard.Name]struct{}),
	}

	// Only reconcile Shard objects from the system:shard logical cluster;
	// objects from other clusters are not authoritative and should be ignored.
	inSystemShard := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return logicalcluster.From(e.Object) == systemShardCluster
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return logicalcluster.From(e.ObjectNew) == systemShardCluster
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return logicalcluster.From(e.Object) == systemShardCluster
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return logicalcluster.From(e.Object) == systemShardCluster
		},
	}

	if err := mcbuilder.ControllerManagedBy(mgr).
		Named("shard-tracker").
		For(&kcpcorev1alpha1.Shard{},
			mcbuilder.WithPredicates(inSystemShard),
			mcbuilder.WithClusterFilter(clusters.IsSource),
		).
		Complete(t); err != nil {
		return nil, err
	}

	return t, nil
}

// Reconcile implements reconcile.TypedReconciler[mcreconcile.Request].
func (t *Registry) Reconcile(ctx context.Context, req mcreconcile.Request) (reconcile.Result, error) {
	sourceCl, err := t.mcMgr.GetCluster(ctx, req.ClusterName)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("getting cluster %q: %w", req.ClusterName, err)
	}

	shard := &kcpcorev1alpha1.Shard{}
	if err := sourceCl.GetClient().Get(ctx, req.NamespacedName, shard); err != nil {
		if apierrors.IsNotFound(err) {
			t.remove(kshard.New(req.Name))
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	if shard.GetAnnotations()[cacheAnnotationKey] == t.ownName {
		t.add(kshard.New(shard.Name))
	} else {
		t.remove(kshard.New(shard.Name))
	}

	return reconcile.Result{}, nil
}

func (t *Registry) add(name kshard.Name) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.shardSet[name]; !exists {
		t.log.Infow("Shard became authoritative", "shard", name)
		t.shardSet[name] = struct{}{}
	}
}

func (t *Registry) remove(name kshard.Name) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.shardSet[name]; exists {
		t.log.Infow("Shard no longer authoritative", "shard", name)
		delete(t.shardSet, name)
	}
}

// IsAuthoritative returns true if the named shard is in the authoritative set.
func (t *Registry) IsAuthoritative(name kshard.Name) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.shardSet[name]
	return ok
}
