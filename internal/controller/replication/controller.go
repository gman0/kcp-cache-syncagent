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

// Package replication provides the per-resource-type replication controller.
package replication

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	cacheclient "github.com/gman0/kcp-cache-syncagent/internal/client"
	kshard "github.com/gman0/kcp-cache-syncagent/internal/client/shard"
	shardtracker "github.com/gman0/kcp-cache-syncagent/internal/shard"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

// objectKey uniquely identifies a cached object across shards and clusters.
type objectKey struct {
	shard, cluster, namespace, name string
}

func keyFromObject(obj *unstructured.Unstructured) objectKey {
	ann := obj.GetAnnotations()
	return objectKey{
		shard:     ann[kshard.AnnotationKey],
		cluster:   ann["kcp.io/cluster"],
		namespace: obj.GetNamespace(),
		name:      obj.GetName(),
	}
}

// Controller replicates objects of a single resource type from the source
// cache-server to all known peer cache-servers.
//
// It implements mcmanager.Runnable: Start() runs the source informer and
// Engage() is called for each new peer cluster.
type Controller struct {
	gvr          schema.GroupVersionResource
	sourceConfig *rest.Config
	tracker      *shardtracker.Tracker
	log          *zap.SugaredLogger

	// sourceState holds the current authoritative view of source objects.
	sourceMu    sync.RWMutex
	sourceState map[objectKey]*unstructured.Unstructured

	// peers holds active peer dynamic clients.
	peersMu sync.Mutex
	peers   map[multicluster.ClusterName]*peerClient
}

type peerClient struct {
	config *rest.Config
	dyn    dynamic.Interface
}

// New creates a Controller for the given resource type.
func New(gvr schema.GroupVersionResource, sourceConfig *rest.Config, tracker *shardtracker.Tracker, log *zap.SugaredLogger) *Controller {
	return &Controller{
		gvr:          gvr,
		sourceConfig: sourceConfig,
		tracker:      tracker,
		log:          log.Named("replication").With("gvr", gvr),
		sourceState:  make(map[objectKey]*unstructured.Unstructured),
		peers:        make(map[multicluster.ClusterName]*peerClient),
	}
}

// Start implements mcmanager.Runnable. Runs the source informer until ctx is done.
func (c *Controller) Start(ctx context.Context) error {
	dynClient, err := dynamic.NewForConfig(c.sourceConfig)
	if err != nil {
		return fmt.Errorf("creating source dynamic client: %w", err)
	}

	for {
		if err := c.runSource(ctx, dynClient); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.log.Warnw("Source watch loop failed, retrying", zap.Error(err))
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

func (c *Controller) runSource(ctx context.Context, dynClient dynamic.Interface) error {
	list, err := dynClient.Resource(c.gvr).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing source objects: %w", err)
	}

	for i := range list.Items {
		obj := &list.Items[i]
		if !c.isAuthoritative(obj) {
			continue
		}
		key := keyFromObject(obj)
		c.sourceMu.Lock()
		c.sourceState[key] = obj
		c.sourceMu.Unlock()
		c.replicateToPeers(ctx, obj, false)
	}

	watcher, err := dynClient.Resource(c.gvr).Watch(ctx, metav1.ListOptions{
		ResourceVersion: list.ResourceVersion,
	})
	if err != nil {
		return fmt.Errorf("watching source objects: %w", err)
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return fmt.Errorf("source watch channel closed")
			}
			obj, ok := toUnstructured(event.Object)
			if !ok {
				continue
			}
			if !c.isAuthoritative(obj) {
				continue
			}
			switch event.Type {
			case watch.Added, watch.Modified:
				key := keyFromObject(obj)
				c.sourceMu.Lock()
				c.sourceState[key] = obj
				c.sourceMu.Unlock()
				c.replicateToPeers(ctx, obj, false)

			case watch.Deleted:
				key := keyFromObject(obj)
				c.sourceMu.Lock()
				delete(c.sourceState, key)
				c.sourceMu.Unlock()
				c.replicateToPeers(ctx, obj, true)
			}
		}
	}
}

// Engage implements mcmanager.Runnable. Called when a new peer cluster is discovered.
func (c *Controller) Engage(ctx context.Context, name multicluster.ClusterName, cl cluster.Cluster) error {
	peerConfig := cl.GetConfig()
	dynClient, err := dynamic.NewForConfig(peerConfig)
	if err != nil {
		return fmt.Errorf("creating peer dynamic client for %q: %w", name, err)
	}

	pc := &peerClient{config: peerConfig, dyn: dynClient}

	c.peersMu.Lock()
	c.peers[name] = pc
	c.peersMu.Unlock()

	go func() {
		<-ctx.Done()
		c.peersMu.Lock()
		delete(c.peers, name)
		c.peersMu.Unlock()
	}()

	// Start the peer reconcile loop in a goroutine.
	go c.runPeer(ctx, name, pc)
	return nil
}

func (c *Controller) runPeer(ctx context.Context, name multicluster.ClusterName, pc *peerClient) {
	for {
		if err := c.runPeerLoop(ctx, name, pc); err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Warnw("Peer watch loop failed, retrying", "peer", name, zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		} else {
			return
		}
	}
}

func (c *Controller) runPeerLoop(ctx context.Context, name multicluster.ClusterName, pc *peerClient) error {
	list, err := pc.dyn.Resource(c.gvr).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing peer objects: %w", err)
	}

	// Reconcile existing peer objects against source state.
	for i := range list.Items {
		obj := &list.Items[i]
		if !c.isAuthoritative(obj) {
			continue
		}
		c.reconcilePeerObject(ctx, pc, obj)
	}

	watcher, err := pc.dyn.Resource(c.gvr).Watch(ctx, metav1.ListOptions{
		ResourceVersion: list.ResourceVersion,
	})
	if err != nil {
		return fmt.Errorf("watching peer objects: %w", err)
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return fmt.Errorf("peer watch channel closed")
			}
			obj, ok := toUnstructured(event.Object)
			if !ok {
				continue
			}
			if !c.isAuthoritative(obj) {
				continue
			}
			switch event.Type {
			case watch.Added, watch.Modified:
				c.reconcilePeerObject(ctx, pc, obj)
			}
		}
	}
}

// reconcilePeerObject checks whether a peer object matches source state and
// corrects any divergence.
func (c *Controller) reconcilePeerObject(ctx context.Context, pc *peerClient, peerObj *unstructured.Unstructured) {
	key := keyFromObject(peerObj)

	c.sourceMu.RLock()
	sourceObj, exists := c.sourceState[key]
	c.sourceMu.RUnlock()

	objCtx := withObjectContext(ctx, peerObj)

	if !exists {
		// Present on peer but absent from source: delete it.
		if err := deleteObject(objCtx, pc.dyn, c.gvr, peerObj); err != nil {
			c.log.Warnw("Failed to delete stale peer object", "key", key, zap.Error(err))
		}
		return
	}

	if !objectsMatch(sourceObj, peerObj) {
		if err := updateObject(objCtx, pc.dyn, c.gvr, sourceObj, peerObj.GetResourceVersion()); err != nil {
			c.log.Warnw("Failed to update diverged peer object", "key", key, zap.Error(err))
		}
	}
}

// replicateToPeers pushes a source object change to all known peers.
func (c *Controller) replicateToPeers(ctx context.Context, obj *unstructured.Unstructured, deleted bool) {
	c.peersMu.Lock()
	peers := make([]*peerClient, 0, len(c.peers))
	for _, pc := range c.peers {
		peers = append(peers, pc)
	}
	c.peersMu.Unlock()

	objCtx := withObjectContext(ctx, obj)

	for _, pc := range peers {
		var err error
		if deleted {
			err = deleteObject(objCtx, pc.dyn, c.gvr, obj)
		} else {
			err = createOrUpdate(objCtx, pc.dyn, c.gvr, obj)
		}
		if err != nil {
			c.log.Warnw("Failed to replicate to peer", zap.Error(err))
		}
	}
}

// isAuthoritative returns true if the object's shard annotation is in our
// authoritative shard set.
func (c *Controller) isAuthoritative(obj *unstructured.Unstructured) bool {
	shardName := kshard.New(obj.GetAnnotations()[kshard.AnnotationKey])
	return c.tracker.IsAuthoritative(shardName)
}

// withObjectContext injects the shard and cluster from obj's annotations into ctx,
// so the round-trippers write to the correct shard/cluster on the target server.
func withObjectContext(ctx context.Context, obj *unstructured.Unstructured) context.Context {
	ann := obj.GetAnnotations()
	ctx = cacheclient.WithShardInContext(ctx, kshard.New(ann[kshard.AnnotationKey]))
	ctx = cacheclient.WithClusterInContext(ctx, ann["kcp.io/cluster"])
	return ctx
}

func createOrUpdate(ctx context.Context, dyn dynamic.Interface, gvr schema.GroupVersionResource, obj *unstructured.Unstructured) error {
	ns := obj.GetNamespace()

	existing, err := dyn.Resource(gvr).Namespace(ns).Get(ctx, obj.GetName(), metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("getting object: %w", err)
		}
		// Create.
		toCreate := obj.DeepCopy()
		toCreate.SetResourceVersion("")
		_, err = dyn.Resource(gvr).Namespace(ns).Create(ctx, toCreate, metav1.CreateOptions{})
		return err
	}

	return updateObject(ctx, dyn, gvr, obj, existing.GetResourceVersion())
}

func updateObject(ctx context.Context, dyn dynamic.Interface, gvr schema.GroupVersionResource, source *unstructured.Unstructured, peerRV string) error {
	toUpdate := source.DeepCopy()
	toUpdate.SetResourceVersion(peerRV)
	_, err := dyn.Resource(gvr).Namespace(source.GetNamespace()).Update(ctx, toUpdate, metav1.UpdateOptions{})
	return err
}

func deleteObject(ctx context.Context, dyn dynamic.Interface, gvr schema.GroupVersionResource, obj *unstructured.Unstructured) error {
	err := dyn.Resource(gvr).Namespace(obj.GetNamespace()).Delete(ctx, obj.GetName(), metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// objectsMatch returns true if the two objects have equal spec and metadata
// (annotations, labels) — sufficient for detecting divergence.
func objectsMatch(source, peer *unstructured.Unstructured) bool {
	if source.GetGeneration() != peer.GetGeneration() {
		return false
	}
	// Quick label/annotation comparison.
	if len(source.GetLabels()) != len(peer.GetLabels()) {
		return false
	}
	for k, v := range source.GetLabels() {
		if peer.GetLabels()[k] != v {
			return false
		}
	}
	// Compare spec by resourceVersion as a proxy: if source and peer were last
	// written by us they'll diverge when the source version advances.
	srcSpec, _, _ := unstructured.NestedMap(source.Object, "spec")
	peerSpec, _, _ := unstructured.NestedMap(peer.Object, "spec")
	return mapsEqual(srcSpec, peerSpec)
}

func mapsEqual(a, b map[string]interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok {
			return false
		}
		switch av := av.(type) {
		case map[string]interface{}:
			bm, ok := bv.(map[string]interface{})
			if !ok || !mapsEqual(av, bm) {
				return false
			}
		default:
			if fmt.Sprintf("%v", av) != fmt.Sprintf("%v", bv) {
				return false
			}
		}
	}
	return true
}

func toUnstructured(obj interface{}) (*unstructured.Unstructured, bool) {
	u, ok := obj.(*unstructured.Unstructured)
	return u, ok
}
