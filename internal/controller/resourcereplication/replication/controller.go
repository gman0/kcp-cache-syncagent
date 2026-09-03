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

	"github.com/go-logr/zapr"
	"go.uber.org/zap"

	cacheclient "github.com/gman0/kcp-cache-syncagent/internal/client"
	kshard "github.com/gman0/kcp-cache-syncagent/internal/client/shard"
	"github.com/gman0/kcp-cache-syncagent/internal/controller/authoritativeshardregistry"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	mccontroller "sigs.k8s.io/multicluster-runtime/pkg/controller"
	mchandler "sigs.k8s.io/multicluster-runtime/pkg/handler"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"
	mcsource "sigs.k8s.io/multicluster-runtime/pkg/source"
)

// Reconciler replicates objects of a single resource type from the source
// cache-server to the peer cache-server identified by req.ClusterName.
type Reconciler struct {
	sourceClient  ctrlclient.Client
	mcMgr         mcmanager.Manager
	shardRegistry *authoritativeshardregistry.Registry
	gvk           schema.GroupVersionKind
	log           *zap.SugaredLogger
}

// Create builds a new unmanaged replication controller. The caller
// (crdmanager) is responsible for pre-seeding it with known peers and starting it.
//
// getPeers returns the current set of engaged peer cluster names and is called
// inside the source-watch event mapper to fan out one reconcile request per peer.
func Create(
	ctx context.Context,
	sourceClient ctrlclient.Client,
	sourceCache ctrlcache.Cache,
	mcMgr mcmanager.Manager,
	gvr schema.GroupVersionResource,
	gvk schema.GroupVersionKind,
	shardRegistry *authoritativeshardregistry.Registry,
	getPeers func() []multicluster.ClusterName,
	log *zap.SugaredLogger,
) (mccontroller.Controller, error) {
	log = log.Named("replication").With("gvr", gvr)

	r := &Reconciler{
		sourceClient:  sourceClient,
		mcMgr:         mcMgr,
		shardRegistry: shardRegistry,
		gvk:           gvk,
		log:           log,
	}

	sourceDummy := &unstructured.Unstructured{}
	sourceDummy.SetGroupVersionKind(gvk)

	peerDummy := &unstructured.Unstructured{}
	peerDummy.SetGroupVersionKind(gvk)

	c, err := mccontroller.NewUnmanaged(
		fmt.Sprintf("replication-%s.%s", gvr.Resource, gvr.Group),
		mcMgr,
		mccontroller.Options{
			Reconciler:         r,
			SkipNameValidation: ptr.To(true),
			Logger:             zapr.NewLogger(log.Desugar()),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("creating unmanaged controller: %w", err)
	}

	// Watch each peer cluster to detect and correct drift.
	if err := c.MultiClusterWatch(mcsource.TypedKind(peerDummy,
		mchandler.TypedEnqueueRequestForObject[*unstructured.Unstructured](),
	)); err != nil {
		return nil, fmt.Errorf("setting up peer watch: %w", err)
	}

	// Watch the source cache and fan out one reconcile request per engaged peer.
	authoritativeShard := predicate.NewTypedPredicateFuncs(func(u *unstructured.Unstructured) bool {
		return shardRegistry.IsAuthoritative(kshard.New(u.GetAnnotations()[kshard.AnnotationKey]))
	})
	enqueueAllPeers := handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, obj *unstructured.Unstructured) []mcreconcile.Request {
		peers := getPeers()
		reqs := make([]mcreconcile.Request, 0, len(peers))
		for _, name := range peers {
			reqs = append(reqs, mcreconcile.Request{
				ClusterName: name,
				Request:     reconcile.Request{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}},
			})
		}
		return reqs
	})
	if err := c.Watch(source.TypedKind(sourceCache, sourceDummy, enqueueAllPeers, authoritativeShard)); err != nil {
		return nil, fmt.Errorf("setting up source watch: %w", err)
	}

	return c, nil
}

func (r *Reconciler) Reconcile(ctx context.Context, req mcreconcile.Request) (reconcile.Result, error) {
	peerCluster, err := r.mcMgr.GetCluster(ctx, req.ClusterName)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("getting peer cluster %q: %w", req.ClusterName, err)
	}
	peerClient := peerCluster.GetClient()

	// Fetch source state from the in-memory cache (no shard routing needed).
	sourceObj := &unstructured.Unstructured{}
	sourceObj.SetGroupVersionKind(r.gvk)
	sourceErr := r.sourceClient.Get(ctx, req.NamespacedName, sourceObj)
	sourceNotFound := apierrors.IsNotFound(sourceErr)
	if sourceErr != nil && !sourceNotFound {
		return reconcile.Result{}, sourceErr
	}

	// Only replicate objects whose shard is authoritative for this agent.
	if !sourceNotFound && !r.isAuthoritative(sourceObj) {
		return reconcile.Result{}, nil
	}

	// Inject the shard into context for correct URL routing on the peer.
	// Cluster routing is not needed: the cache-server derives the cluster from
	// the object's annotations server-side, not from the URL.
	peerCtx := ctx
	if !sourceNotFound {
		ann := sourceObj.GetAnnotations()
		peerCtx = cacheclient.WithShardInContext(ctx, kshard.New(ann[kshard.AnnotationKey]))
	}

	// Fetch peer state.
	peerObj := &unstructured.Unstructured{}
	peerObj.SetGroupVersionKind(r.gvk)
	peerErr := peerClient.Get(peerCtx, req.NamespacedName, peerObj)
	peerNotFound := apierrors.IsNotFound(peerErr)
	if peerErr != nil && !peerNotFound {
		return reconcile.Result{}, peerErr
	}

	if sourceNotFound {
		if !peerNotFound {
			// Derive shard routing from the peer object's own annotations.
			ann := peerObj.GetAnnotations()
			deleteCtx := cacheclient.WithShardInContext(ctx, kshard.New(ann[kshard.AnnotationKey]))
			return reconcile.Result{}, ctrlclient.IgnoreNotFound(peerClient.Delete(deleteCtx, peerObj))
		}
		return reconcile.Result{}, nil
	}

	if peerNotFound {
		toCreate := sourceObj.DeepCopy()
		toCreate.SetResourceVersion("")
		return reconcile.Result{}, peerClient.Create(peerCtx, toCreate)
	}

	if !objectsMatch(sourceObj, peerObj) {
		toUpdate := sourceObj.DeepCopy()
		toUpdate.SetResourceVersion(peerObj.GetResourceVersion())
		return reconcile.Result{}, peerClient.Update(peerCtx, toUpdate)
	}

	return reconcile.Result{}, nil
}

func (r *Reconciler) isAuthoritative(obj *unstructured.Unstructured) bool {
	return r.shardRegistry.IsAuthoritative(kshard.New(obj.GetAnnotations()[kshard.AnnotationKey]))
}

// objectsMatch returns true if source and peer have equivalent spec and labels.
func objectsMatch(source, peer *unstructured.Unstructured) bool {
	if source.GetGeneration() != peer.GetGeneration() {
		return false
	}
	srcLabels, peerLabels := source.GetLabels(), peer.GetLabels()
	if len(srcLabels) != len(peerLabels) {
		return false
	}
	for k, v := range srcLabels {
		if peerLabels[k] != v {
			return false
		}
	}
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
