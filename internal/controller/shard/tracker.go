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
package shard

import (
	"context"
	"sync"

	"go.uber.org/zap"

	kshard "github.com/gman0/kcp-cache-syncagent/internal/client/shard"

	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// cacheAnnotationKey is the annotation set on Shard objects by kcp to record
	// which cache-server the shard is directly connected to.
	cacheAnnotationKey = "kcp.io/cache"

	// clusterAnnotationKey is the logical-cluster annotation used by kcp.
	clusterAnnotationKey = "kcp.io/cluster"

	// systemShardCluster is the logical cluster that holds the authoritative
	// Shard objects we care about.
	systemShardCluster = "system:shard"
)

// Tracker watches Shard objects on the source cache-server and maintains the
// set of shards authoritative for our cache-server instance (i.e. those whose
// kcp.io/cache annotation equals ownName).
type Tracker struct {
	client  ctrlclient.Client
	ownName string
	log     *zap.SugaredLogger

	mu       sync.RWMutex
	shardSet map[kshard.Name]struct{}
}

// Add registers the shard-tracker controller with mgr and returns the Tracker.
// mgr must be connected to the source cache-server.
func Add(mgr manager.Manager, ownName string, log *zap.SugaredLogger) (*Tracker, error) {
	t := &Tracker{
		client:   mgr.GetClient(),
		ownName:  ownName,
		log:      log.Named("shard-tracker"),
		shardSet: make(map[kshard.Name]struct{}),
	}

	// Only reconcile Shard objects from the system:shard logical cluster;
	// objects from other clusters are not authoritative and should be ignored.
	inSystemShard := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return e.Object.GetAnnotations()[clusterAnnotationKey] == systemShardCluster
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectNew.GetAnnotations()[clusterAnnotationKey] == systemShardCluster
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return e.Object.GetAnnotations()[clusterAnnotationKey] == systemShardCluster
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return e.Object.GetAnnotations()[clusterAnnotationKey] == systemShardCluster
		},
	}

	if err := builder.ControllerManagedBy(mgr).
		Named("shard-tracker").
		For(&kcpcorev1alpha1.Shard{}, builder.WithPredicates(inSystemShard)).
		Complete(t); err != nil {
		return nil, err
	}

	return t, nil
}

// Reconcile implements reconcile.Reconciler.
func (t *Tracker) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	shard := &kcpcorev1alpha1.Shard{}
	if err := t.client.Get(ctx, req.NamespacedName, shard); err != nil {
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

func (t *Tracker) add(name kshard.Name) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.shardSet[name]; !exists {
		t.log.Infow("Shard became authoritative", "shard", name)
		t.shardSet[name] = struct{}{}
	}
}

func (t *Tracker) remove(name kshard.Name) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.shardSet[name]; exists {
		t.log.Infow("Shard no longer authoritative", "shard", name)
		delete(t.shardSet, name)
	}
}

// IsAuthoritative returns true if the named shard is in the authoritative set.
func (t *Tracker) IsAuthoritative(name kshard.Name) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.shardSet[name]
	return ok
}
