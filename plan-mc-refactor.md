# Plan: peerprovider MCR refactor

Replace the standalone `ctrlcache.New(p.sourceConfig)` in `peerprovider.Start()` with the local
manager's shared cache.  The goal is to eliminate the separate controller-runtime cache in the
peerprovider and let the MCR local manager own the Cache informer lifecycle.

## Background

`peerprovider.Provider.Start()` currently builds its own cache to watch `core.kcp.io/v1alpha1/Cache`
objects on the source:

```go
sourceCache, err := ctrlcache.New(p.sourceConfig, ctrlcache.Options{Scheme: p.scheme, Mapper: ...})
informer, _ := sourceCache.GetInformer(ctx, &kcpcorev1alpha1.Cache{}, ...)
// ... add event handlers ...
return sourceCache.Start(ctx)          // blocks until ctx cancelled
```

The quick-fix in the previous step patched the startup crash by providing a static `Mapper`, but the
standalone cache is still redundant.  The MCR local manager already has a running cache with the same
`kcpcorev1alpha1` scheme, and peerprovider is started as a `ProviderRunnable` by the MCR manager —
so the local cache is guaranteed to be running when `Start()` is called.

## Changes

### 1. `internal/peerprovider/provider.go`

**a. Get the local manager from `aware`**

`Start(ctx, aware)` receives the MCR manager as `aware`.  Obtain the local manager via a local
interface assertion — avoids importing the multicluster-runtime manager package in the peerprovider:

```go
type localManagerGetter interface {
    GetLocalManager() ctrl.Manager
}
```

At the top of `Start()`:

```go
localMgrGetter, ok := aware.(localManagerGetter)
if !ok {
    return fmt.Errorf("aware does not expose local manager (unexpected provider type)")
}
localMgr := localMgrGetter.GetLocalManager()
```

**b. Replace `ctrlcache.New` + `sourceCache.Start` with `localMgr.GetCache()`**

Remove:
```go
cacheMapper := meta.NewDefaultRESTMapper(nil)
cacheMapper.Add(...)
sourceCache, err := ctrlcache.New(p.sourceConfig, ctrlcache.Options{...})
if err != nil { return ... }
informer, err := sourceCache.GetInformer(ctx, &kcpcorev1alpha1.Cache{}, ctrlcache.BlockUntilSynced(false))
// ...
return sourceCache.Start(ctx)
```

Replace with:
```go
informer, err := localMgr.GetCache().GetInformer(ctx, &kcpcorev1alpha1.Cache{}, ctrlcache.BlockUntilSynced(false))
if err != nil {
    return fmt.Errorf("getting Cache informer: %w", err)
}
// ... existing AddEventHandler / defer RemoveEventHandler ...
<-ctx.Done()
return nil
```

The local manager's cache is started by the MCR framework before runnables are started, so
`GetInformer` is safe to call here.  The `defer RemoveEventHandler` block stays unchanged.

**c. Remove unused struct fields and constructor arguments**

- `sourceConfig *rest.Config` — only used for `ctrlcache.New`.  Remove from `Provider` struct and
  from `New()` signature.
- `scheme *runtime.Scheme` — currently used both for `ctrlcache.New` (going away) and for
  `client.New(peerConfig, client.Options{Scheme: p.scheme})` in `seedFromPeer`.

  **Decision point**: keep `scheme` for the peer `client.New` (minimal change), or drop it and use
  `localMgr.GetScheme()` instead (cleaner, but requires passing `localMgr` into `seedFromPeer`).
  Recommended: keep `scheme` for now; it costs nothing and keeps `seedFromPeer` self-contained.

**d. Remove unused imports**

- `ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"` — remove if no longer used (check whether
  `ctrlcache.BlockUntilSynced` is still referenced; if so, keep the import but drop `ctrlcache.New`).
- `"k8s.io/apimachinery/pkg/api/meta"` — remove (added for the static `cacheMapper` quick-fix).
- `"k8s.io/client-go/rest"` — remove if `sourceConfig` field is dropped and `rest.Config` no longer
  appears in the file.

Add:
- `ctrl "sigs.k8s.io/controller-runtime"` — for `ctrl.Manager` in the interface.

### 2. `cmd/kcp-cache-syncagent/main.go`

**a. Add `Cache` to the static `MapperProvider`**

After the refactor the local manager's cache will watch Cache objects, so the static mapper must
include it:

```go
ctrlOpts.MapperProvider = func(_ *rest.Config, _ *http.Client) (meta.RESTMapper, error) {
    m := meta.NewDefaultRESTMapper(nil)
    m.Add(kcpcorev1alpha1.SchemeGroupVersion.WithKind("Shard"), meta.RESTScopeRoot)
    m.Add(kcpcorev1alpha1.SchemeGroupVersion.WithKind("Cache"), meta.RESTScopeRoot)  // add this
    m.Add(apiextensionsv1.SchemeGroupVersion.WithKind("CustomResourceDefinition"), meta.RESTScopeRoot)
    return m, nil
}
```

**b. Remove `sourceRestConfig` from `peerprovider.New()` call**

Once `sourceConfig` is removed from the `Provider` struct and `New()` signature:

```go
peerProvider, err := peerprovider.New(
    // sourceRestConfig,   <-- remove
    opts.SourceURL,
    opts.InitialPeerURLs,
    opts.CacheClientCAFile,
    opts.CacheClientCertFile,
    opts.CacheClientKeyFile,
    log,
)
```

If nothing else in `main.go` uses `sourceRestConfig` after passing it to `mcmanager.New`, verify
whether the import of `"k8s.io/client-go/rest"` (added for `MapperProvider`) still has other
consumers.  It does (`_ *rest.Config` in the `MapperProvider` closure), so keep it.

## What does NOT change

- `seedFromPeer` — uses `client.New(peerConfig, ...)` against a *peer* cache-server URL.  Stays
  as-is.
- `engageCacheObject` — uses `cluster.New(peerConfig)` for a peer cluster.  Stays as-is.
- `disengageCacheObject` — unchanged.
- The event handler logic (Add/Update/Delete funcs) — unchanged.
- The initial-peer seeding loop — unchanged.

## Open question

The replication controller (`internal/controller/resourcereplication/replication/controller.go`)
calls `source.TypedKind(sourceCache, unstructuredDummy, ...)` where `sourceCache` is
`localMgr.GetCache()`.  The `unstructuredDummy` carries a dynamic GVK (whatever CRD is being
replicated).  The static `MapperProvider` won't cover these GVKs.

This is a separate issue from the peerprovider refactor and is not in scope here, but it will need
a follow-up: either extend the static mapper lazily via `meta.FirstHitRESTMapper` chaining a dynamic
mapper, or fix cache-server discovery so the dynamic mapper can work.
