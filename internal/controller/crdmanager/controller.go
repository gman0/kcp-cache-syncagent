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
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/gman0/kcp-cache-syncagent/internal/controller/replication"
	shardtracker "github.com/gman0/kcp-cache-syncagent/internal/controller/shard"
	dynmanager "github.com/gman0/kcp-cache-syncagent/internal/manager"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Reconciler watches CRDs and starts/stops a replication controller per type.
type Reconciler struct {
	// ctx is the root application context; replication controllers are tied to
	// derived contexts so they shut down when the app shuts down.
	ctx           context.Context
	client        ctrlclient.Client
	sourceManager manager.Manager
	dmcm          *dynmanager.DynamicMultiClusterManager
	tracker       *shardtracker.Tracker
	log           *zap.SugaredLogger

	syncCancelsLock sync.RWMutex
	syncCancels     map[string]context.CancelCauseFunc
}

// Add creates the CRD-manager controller and registers it with sourceMgr.
func Add(
	ctx context.Context,
	sourceMgr manager.Manager,
	dmcm *dynmanager.DynamicMultiClusterManager,
	tracker *shardtracker.Tracker,
	log *zap.SugaredLogger,
) error {
	r := &Reconciler{
		ctx:           ctx,
		client:        sourceMgr.GetClient(),
		sourceManager: sourceMgr,
		dmcm:          dmcm,
		tracker:       tracker,
		log:           log.Named("crd-manager"),
		syncCancels:   make(map[string]context.CancelCauseFunc),
	}

	_, err := builder.ControllerManagedBy(sourceMgr).
		Named("crd-manager").
		For(&apiextensionsv1.CustomResourceDefinition{}).
		Build(r)
	return err
}

func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := r.client.Get(ctx, req.NamespacedName, crd); err != nil {
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

	r.syncCancelsLock.RLock()
	_, exists := r.syncCancels[key]
	r.syncCancelsLock.RUnlock()
	if exists {
		return nil
	}

	gvr, gvk, err := gvrAndGVKFromCRD(crd)
	if err != nil {
		return fmt.Errorf("determining GVR/GVK for CRD %q: %w", crd.Name, err)
	}

	ctrlCtx, ctrlCancel := context.WithCancelCause(r.ctx)

	ctrl, err := replication.Create(
		ctrlCtx,
		r.sourceManager,
		r.dmcm.GetManager(),
		gvr,
		gvk,
		r.tracker,
		r.dmcm,
		r.log,
	)
	if err != nil {
		ctrlCancel(fmt.Errorf("creating replication controller: %w", err))
		return fmt.Errorf("creating replication controller for CRD %q: %w", crd.Name, err)
	}

	r.syncCancelsLock.Lock()
	r.syncCancels[key] = ctrlCancel
	r.syncCancelsLock.Unlock()

	r.log.Infow("Starting replication controller", "crd", crd.Name, "gvr", gvr)
	if err := r.dmcm.StartController(ctrlCtx, r.log.With("crd", crd.Name), ctrl); err != nil {
		ctrlCancel(fmt.Errorf("starting replication controller: %w", err))
		r.syncCancelsLock.Lock()
		delete(r.syncCancels, key)
		r.syncCancelsLock.Unlock()
		return fmt.Errorf("starting replication controller for CRD %q: %w", crd.Name, err)
	}

	return nil
}

func (r *Reconciler) cleanupController(crdName string) error {
	r.syncCancelsLock.Lock()
	defer r.syncCancelsLock.Unlock()

	if cancel, ok := r.syncCancels[crdName]; ok {
		r.log.Infow("Stopping replication controller", "crd", crdName)
		cancel(errors.New("CRD deleted"))
		delete(r.syncCancels, crdName)
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
