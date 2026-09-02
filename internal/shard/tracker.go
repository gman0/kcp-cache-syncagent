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
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	kshard "github.com/gman0/kcp-cache-syncagent/internal/client/shard"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

const (
	// cacheAnnotationKey is the annotation set on Shard objects by kcp to record
	// which cache-server the shard is directly connected to.
	cacheAnnotationKey = "kcp.io/cache"

	// clusterAnnotationKey is the logical-cluster annotation used by kcp.
	clusterAnnotationKey = "kcp.io/cluster"

	// systemShardCluster is the logical cluster that holds Shard objects.
	systemShardCluster = "system:shard"
)

var shardGVR = schema.GroupVersionResource{
	Group:   "core.kcp.io",
	Version: "v1alpha1",
	Resource: "shards",
}

// Tracker watches Shard objects on the source cache-server (via wildcard
// shard + cluster) and maintains the set of shards that are authoritative for
// our cache-server instance (i.e., their kcp.io/cache annotation equals
// ownName).
type Tracker struct {
	sourceConfig *rest.Config
	ownName      string
	log          *zap.SugaredLogger

	mu       sync.RWMutex
	shardSet map[kshard.Name]struct{}
}

// New creates a Tracker that treats shards annotated with ownName as authoritative.
func New(sourceConfig *rest.Config, ownName string, log *zap.SugaredLogger) *Tracker {
	return &Tracker{
		sourceConfig: sourceConfig,
		ownName:      ownName,
		log:          log.Named("shard-tracker"),
		shardSet:     make(map[kshard.Name]struct{}),
	}
}

// Start implements manager.Runnable. It runs until ctx is cancelled.
func (t *Tracker) Start(ctx context.Context) error {
	dynClient, err := dynamic.NewForConfig(t.sourceConfig)
	if err != nil {
		return fmt.Errorf("creating dynamic client: %w", err)
	}

	for {
		if err := t.run(ctx, dynClient); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			t.log.Warnw("Shard watch loop failed, retrying", zap.Error(err))
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

func (t *Tracker) run(ctx context.Context, dynClient dynamic.Interface) error {
	list, err := dynClient.Resource(shardGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing shards: %w", err)
	}

	// Process existing shards.
	for i := range list.Items {
		t.applyObject(&list.Items[i], false)
	}
	t.log.Infow("Initial shard sync complete", "authoritative", t.count())

	watcher, err := dynClient.Resource(shardGVR).Watch(ctx, metav1.ListOptions{
		ResourceVersion: list.ResourceVersion,
	})
	if err != nil {
		return fmt.Errorf("watching shards: %w", err)
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
				t.applyObject(obj, false)
			case watch.Deleted:
				t.applyObject(obj, true)
			}
		}
	}
}

// applyObject updates the authoritative set based on a Shard object event.
func (t *Tracker) applyObject(obj *unstructured.Unstructured, deleted bool) {
	ann := obj.GetAnnotations()

	// Only track Shard objects in the system:shard logical cluster.
	if ann[clusterAnnotationKey] != systemShardCluster {
		return
	}

	// The Shard object's name IS the shard name.
	shardName := kshard.New(obj.GetName())

	t.mu.Lock()
	defer t.mu.Unlock()

	if !deleted && ann[cacheAnnotationKey] == t.ownName {
		if _, exists := t.shardSet[shardName]; !exists {
			t.log.Infow("Shard became authoritative", "shard", shardName)
			t.shardSet[shardName] = struct{}{}
		}
	} else {
		if _, exists := t.shardSet[shardName]; exists {
			t.log.Infow("Shard no longer authoritative", "shard", shardName)
			delete(t.shardSet, shardName)
		}
	}
}

// IsAuthoritative returns true if the named shard is in the authoritative set.
func (t *Tracker) IsAuthoritative(name kshard.Name) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.shardSet[name]
	return ok
}

func (t *Tracker) count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.shardSet)
}
