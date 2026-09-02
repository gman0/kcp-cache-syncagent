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

// Package crdmanager watches CRDs on the source cache-server and starts/stops
// a ReplicationController per resource type via the DynamicMultiClusterManager.
package crdmanager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/gman0/kcp-cache-syncagent/internal/controller/replication"
	dynmanager "github.com/gman0/kcp-cache-syncagent/internal/manager"
	"github.com/gman0/kcp-cache-syncagent/internal/shard"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

var crdGVR = schema.GroupVersionResource{
	Group:    "apiextensions.k8s.io",
	Version:  "v1",
	Resource: "customresourcedefinitions",
}

// Controller watches CRDs on the source and starts/stops a replication
// controller per resource type.
type Controller struct {
	sourceConfig *rest.Config
	dmcm         *dynmanager.DynamicMultiClusterManager
	tracker      *shard.Tracker
	log          *zap.SugaredLogger

	mu sync.Mutex
	// crdCount tracks how many shards report each CRD (by group/resource).
	crdCount map[string]int
	// crdCancels holds the cancel func for each active replication controller.
	crdCancels map[string]context.CancelFunc
}

// New creates a CRDManager.
func New(
	sourceConfig *rest.Config,
	dmcm *dynmanager.DynamicMultiClusterManager,
	tracker *shard.Tracker,
	log *zap.SugaredLogger,
) *Controller {
	return &Controller{
		sourceConfig: sourceConfig,
		dmcm:         dmcm,
		tracker:      tracker,
		log:          log.Named("crd-manager"),
		crdCount:     make(map[string]int),
		crdCancels:   make(map[string]context.CancelFunc),
	}
}

// Start implements manager.Runnable. It watches CRDs until ctx is cancelled.
func (c *Controller) Start(ctx context.Context) error {
	dynClient, err := dynamic.NewForConfig(c.sourceConfig)
	if err != nil {
		return fmt.Errorf("creating dynamic client: %w", err)
	}

	for {
		if err := c.run(ctx, dynClient); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.log.Warnw("CRD watch loop failed, retrying", zap.Error(err))
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

func (c *Controller) run(ctx context.Context, dynClient dynamic.Interface) error {
	list, err := dynClient.Resource(crdGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing CRDs: %w", err)
	}

	for i := range list.Items {
		c.handleAdd(ctx, &list.Items[i])
	}

	watcher, err := dynClient.Resource(crdGVR).Watch(ctx, metav1.ListOptions{
		ResourceVersion: list.GetResourceVersion(),
	})
	if err != nil {
		return fmt.Errorf("watching CRDs: %w", err)
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
			case watch.Added:
				c.handleAdd(ctx, obj)
			case watch.Deleted:
				c.handleDelete(obj)
			}
		}
	}
}

// crdKey returns a stable key for a CRD by group+resource (shared across shards).
func crdKey(obj *unstructured.Unstructured) string {
	group, _, _ := unstructured.NestedString(obj.Object, "spec", "group")
	resource, _, _ := unstructured.NestedString(obj.Object, "spec", "names", "plural")
	return group + "/" + resource
}

func (c *Controller) handleAdd(ctx context.Context, obj *unstructured.Unstructured) {
	key := crdKey(obj)
	if key == "/" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.crdCount[key]++
	if c.crdCount[key] > 1 {
		// Already started for this CRD (appeared from another shard).
		return
	}

	gvr, err := gvrFromCRD(obj)
	if err != nil {
		c.log.Warnw("Could not determine GVR for CRD", "crd", obj.GetName(), zap.Error(err))
		return
	}

	ctrl := replication.New(gvr, c.sourceConfig, c.tracker, c.log)

	ctrlCtx, cancel := context.WithCancel(ctx)
	c.crdCancels[key] = cancel

	c.log.Infow("Starting replication controller", "gvr", gvr)
	if err := c.dmcm.StartController(ctrlCtx, c.log, ctrl); err != nil {
		c.log.Errorw("Failed to start replication controller", "gvr", gvr, zap.Error(err))
		cancel()
		delete(c.crdCancels, key)
	}
}

func (c *Controller) handleDelete(obj *unstructured.Unstructured) {
	key := crdKey(obj)
	if key == "/" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.crdCount[key]--
	if c.crdCount[key] > 0 {
		// Still present on other shards.
		return
	}
	delete(c.crdCount, key)

	if cancel, ok := c.crdCancels[key]; ok {
		gvr, _ := gvrFromCRD(obj)
		c.log.Infow("Stopping replication controller", "gvr", gvr)
		cancel()
		delete(c.crdCancels, key)
	}
}

func gvrFromCRD(obj *unstructured.Unstructured) (schema.GroupVersionResource, error) {
	group, found, err := unstructured.NestedString(obj.Object, "spec", "group")
	if err != nil || !found || group == "" {
		return schema.GroupVersionResource{}, fmt.Errorf("spec.group missing")
	}

	resource, found, err := unstructured.NestedString(obj.Object, "spec", "names", "plural")
	if err != nil || !found || resource == "" {
		return schema.GroupVersionResource{}, fmt.Errorf("spec.names.plural missing")
	}

	versions, found, err := unstructured.NestedSlice(obj.Object, "spec", "versions")
	if err != nil || !found || len(versions) == 0 {
		return schema.GroupVersionResource{}, fmt.Errorf("spec.versions missing or empty")
	}

	// Prefer the storage version; fall back to the first entry.
	version := ""
	for _, v := range versions {
		vm, ok := v.(map[string]interface{})
		if !ok {
			continue
		}
		if storage, _, _ := unstructured.NestedBool(vm, "storage"); storage {
			version, _, _ = unstructured.NestedString(vm, "name")
			break
		}
	}
	if version == "" {
		if vm, ok := versions[0].(map[string]interface{}); ok {
			version, _, _ = unstructured.NestedString(vm, "name")
		}
	}
	if version == "" {
		return schema.GroupVersionResource{}, fmt.Errorf("could not determine storage version")
	}

	return schema.GroupVersionResource{Group: group, Version: version, Resource: resource}, nil
}
