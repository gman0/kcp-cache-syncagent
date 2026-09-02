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
	shardtracker "github.com/gman0/kcp-cache-syncagent/internal/controller/shard"
	dynmanager "github.com/gman0/kcp-cache-syncagent/internal/manager"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	mccontroller "sigs.k8s.io/multicluster-runtime/pkg/controller"
	mchandler "sigs.k8s.io/multicluster-runtime/pkg/handler"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"
	mcsource "sigs.k8s.io/multicluster-runtime/pkg/source"
)

// Reconciler replicates objects of a single resource type from the source
// cache-server to the peer cache-server identified by req.ClusterName.
type Reconciler struct {
	sourceClient  ctrlclient.Client
	remoteManager mcmanager.Manager
	tracker       *shardtracker.Tracker
	gvk           schema.GroupVersionKind
	log           *zap.SugaredLogger
}

// Create creates a new unmanaged replication controller. The caller is
// responsible for starting it (via DynamicMultiClusterManager.StartController).
func Create(
	ctx context.Context,
	sourceManager manager.Manager,
	remoteManager mcmanager.Manager,
	gvr schema.GroupVersionResource,
	gvk schema.GroupVersionKind,
	tracker *shardtracker.Tracker,
	dmcm *dynmanager.DynamicMultiClusterManager,
	log *zap.SugaredLogger,
) (mccontroller.Controller, error) {
	log = log.Named("replication").With("gvr", gvr)

	r := &Reconciler{
		sourceClient:  sourceManager.GetClient(),
		remoteManager: remoteManager,
		tracker:       tracker,
		gvk:           gvk,
		log:           log,
	}

	sourceDummy := &unstructured.Unstructured{}
	sourceDummy.SetGroupVersionKind(gvk)

	peerDummy := &unstructured.Unstructured{}
	peerDummy.SetGroupVersionKind(gvk)

	c, err := mccontroller.NewUnmanaged(
		fmt.Sprintf("replication-%s.%s", gvr.Resource, gvr.Group),
		remoteManager,
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

	// Watch the source cache and fan out one reconcile request per engaged peer
	// so that source changes are propagated to all peers.
	authoritativeShard := predicate.NewTypedPredicateFuncs(func(u *unstructured.Unstructured) bool {
		return tracker.IsAuthoritative(kshard.New(u.GetAnnotations()[kshard.AnnotationKey]))
	})
	enqueueAllPeers := handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, obj *unstructured.Unstructured) []mcreconcile.Request {
		peers := dmcm.EngagedClusters()
		reqs := make([]mcreconcile.Request, 0, len(peers))
		for _, name := range peers {
			reqs = append(reqs, mcreconcile.Request{
				ClusterName: name,
				Request:     reconcile.Request{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}},
			})
		}
		return reqs
	})
	if err := c.Watch(source.TypedKind(sourceManager.GetCache(), sourceDummy, enqueueAllPeers, authoritativeShard)); err != nil {
		return nil, fmt.Errorf("setting up source watch: %w", err)
	}

	return c, nil
}

func (r *Reconciler) Reconcile(ctx context.Context, req mcreconcile.Request) (reconcile.Result, error) {
	peerCluster, err := r.remoteManager.GetCluster(ctx, req.ClusterName)
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

	// Inject shard+cluster into context for correct URL routing on the peer.
	peerCtx := ctx
	if !sourceNotFound {
		ann := sourceObj.GetAnnotations()
		peerCtx = cacheclient.WithShardInContext(ctx, kshard.New(ann[kshard.AnnotationKey]))
		peerCtx = cacheclient.WithClusterInContext(peerCtx, ann["kcp.io/cluster"])
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
			deleteCtx = cacheclient.WithClusterInContext(deleteCtx, ann["kcp.io/cluster"])
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
	return r.tracker.IsAuthoritative(kshard.New(obj.GetAnnotations()[kshard.AnnotationKey]))
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
