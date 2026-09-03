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
// a replication controller per resource type.
package resourcereplication

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/gman0/kcp-cache-syncagent/internal/controller/authoritativeshardregistry"
	"github.com/gman0/kcp-cache-syncagent/internal/controller/resourcereplication/replication"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	mccontroller "sigs.k8s.io/multicluster-runtime/pkg/controller"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

// syncEntry holds the lifecycle state for a running replication controller.
type syncEntry struct {
	cancel context.CancelCauseFunc
	ctrl   mccontroller.Controller
}

// clusterEntry records an engaged peer cluster.
type clusterEntry struct {
	ctx context.Context
	cl  cluster.Cluster
}

// Reconciler watches CRDs on the source and manages one replication controller
// per CRD. It also implements mcmanager.Runnable so the mc manager forwards
// peer Engage calls to all running replication controllers.
type Reconciler struct {
	// ctx is the root application context; replication controllers derive from it.
	ctx           context.Context
	mcMgr         mcmanager.Manager
	localMgr      ctrl.Manager
	shardRegistry *authoritativeshardregistry.Registry
	log           *zap.SugaredLogger

	clustersLock sync.RWMutex
	clusters     map[multicluster.ClusterName]clusterEntry

	syncLock    sync.RWMutex
	syncEntries map[string]*syncEntry
}

// Add registers the CRD-watcher controller with mcMgr (source cluster only) and
// the Reconciler as an mcmanager.Runnable so it receives peer Engage calls.
func Add(
	ctx context.Context,
	mcMgr mcmanager.Manager,
	shardRegistry *authoritativeshardregistry.Registry,
	log *zap.SugaredLogger,
) error {
	r := &Reconciler{
		ctx:           ctx,
		mcMgr:         mcMgr,
		localMgr:      mcMgr.GetLocalManager(),
		shardRegistry: shardRegistry,
		log:           log.Named("crd-manager"),
		clusters:      make(map[multicluster.ClusterName]clusterEntry),
		syncEntries:   make(map[string]*syncEntry),
	}

	if err := ctrl.NewControllerManagedBy(r.localMgr).
		Named("crd-manager").
		For(&apiextensionsv1.CustomResourceDefinition{}).
		Complete(r); err != nil {
		return err
	}

	return mcMgr.Add(r)
}

// Start implements mcmanager.Runnable — NOP since the CRD watch is managed by
// the builder-registered controller above.
func (r *Reconciler) Start(_ context.Context) error { return nil }

// Engage implements mcmanager.Runnable. The mc manager calls this when a peer
// cluster is engaged. We track it and forward to all running replication controllers.
func (r *Reconciler) Engage(ctx context.Context, name multicluster.ClusterName, cl cluster.Cluster) error {
	r.clustersLock.Lock()
	r.clusters[name] = clusterEntry{ctx: ctx, cl: cl}
	r.clustersLock.Unlock()

	go func() {
		<-ctx.Done()
		r.clustersLock.Lock()
		delete(r.clusters, name)
		r.clustersLock.Unlock()
	}()

	r.syncLock.RLock()
	defer r.syncLock.RUnlock()

	for key, e := range r.syncEntries {
		if err := e.ctrl.Engage(ctx, name, cl); err != nil {
			r.log.Warnw("Failed to engage replication controller with new peer",
				"peer", name, "crd", key, zap.Error(err))
		}
	}

	return nil
}

// engagedClusterNames returns the names of all currently engaged peer clusters.
// Used as the getPeers closure in replication controllers.
func (r *Reconciler) engagedClusterNames() []multicluster.ClusterName {
	r.clustersLock.RLock()
	defer r.clustersLock.RUnlock()
	names := make([]multicluster.ClusterName, 0, len(r.clusters))
	for name := range r.clusters {
		names = append(names, name)
	}
	return names
}

func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := r.localMgr.GetClient().Get(ctx, req.NamespacedName, crd); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, r.cleanupController(req.Name)
		}
		return reconcile.Result{}, err
	}

	if crd.DeletionTimestamp != nil {
		return reconcile.Result{}, r.cleanupController(req.Name)
	}

	return reconcile.Result{}, r.ensureReplicationController(crd)
}

func (r *Reconciler) ensureReplicationController(crd *apiextensionsv1.CustomResourceDefinition) error {
	key := crd.Name

	r.syncLock.Lock()
	defer r.syncLock.Unlock()

	if _, exists := r.syncEntries[key]; exists {
		return nil
	}

	gvr, gvk, err := gvrAndGVKFromCRD(crd)
	if err != nil {
		return fmt.Errorf("determining GVR/GVK for CRD %q: %w", crd.Name, err)
	}

	ctrlCtx, ctrlCancel := context.WithCancelCause(r.ctx)

	ctrl, err := replication.Create(
		ctrlCtx,
		r.localMgr.GetClient(),
		r.localMgr.GetCache(),
		r.mcMgr,
		gvr,
		gvk,
		r.shardRegistry,
		r.engagedClusterNames,
		r.log,
	)
	if err != nil {
		ctrlCancel(fmt.Errorf("creating replication controller: %w", err))
		return fmt.Errorf("creating replication controller for CRD %q: %w", crd.Name, err)
	}

	// Pre-seed the new controller with all already-engaged peer clusters.
	r.clustersLock.RLock()
	for name, e := range r.clusters {
		if err := ctrl.Engage(e.ctx, name, e.cl); err != nil {
			r.log.Warnw("Failed to pre-seed replication controller", "peer", name, "crd", key, zap.Error(err))
		}
	}
	r.clustersLock.RUnlock()

	entry := &syncEntry{cancel: ctrlCancel, ctrl: ctrl}
	r.syncEntries[key] = entry

	r.log.Infow("Starting replication controller", "crd", crd.Name, "gvr", gvr)
	go func() {
		if err := ctrl.Start(ctrlCtx); err != nil && !errors.Is(err, context.Canceled) {
			r.log.Errorw("Replication controller exited unexpectedly", "crd", key, zap.Error(err))
		}
		r.syncLock.Lock()
		if r.syncEntries[key] == entry {
			delete(r.syncEntries, key)
		}
		r.syncLock.Unlock()
	}()

	return nil
}

func (r *Reconciler) cleanupController(crdName string) error {
	r.syncLock.Lock()
	defer r.syncLock.Unlock()

	if entry, ok := r.syncEntries[crdName]; ok {
		r.log.Infow("Stopping replication controller", "crd", crdName)
		entry.cancel(errors.New("CRD deleted"))
		delete(r.syncEntries, crdName)
	}

	return nil
}

func gvrAndGVKFromCRD(crd *apiextensionsv1.CustomResourceDefinition) (schema.GroupVersionResource, schema.GroupVersionKind, error) {
	group := crd.Spec.Group
	resource := crd.Spec.Names.Plural
	kind := crd.Spec.Names.Kind

	version := ""
	for _, v := range crd.Spec.Versions {
		if v.Storage {
			version = v.Name
			break
		}
	}
	if version == "" && len(crd.Spec.Versions) > 0 {
		version = crd.Spec.Versions[0].Name
	}
	if version == "" {
		return schema.GroupVersionResource{}, schema.GroupVersionKind{}, fmt.Errorf("no version found in CRD %q", crd.Name)
	}

	gvr := schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
	gvk := schema.GroupVersionKind{Group: group, Version: version, Kind: kind}
	return gvr, gvk, nil
}
