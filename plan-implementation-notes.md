# Implementation notes

Cross-reference with plan-data-sync-mechanism.md (variant A).

## Relationship to kcp-api-syncagent

kcp-api-syncagent solves a structurally similar problem: watch a dynamic set of "clusters"
(kcp virtual workspaces discovered via APIExportEndpointSlice) and for each of a dynamic set
of resource types (driven by PublishedResources), run a sync controller. It uses
`sigs.k8s.io/multicluster-runtime` + `github.com/kcp-dev/multicluster-provider` to wire this
together. The same framework fits the cache-syncagent well, with different inputs driving the
two dynamic dimensions:

| Dimension            | api-syncagent                        | cache-syncagent                       |
|----------------------|--------------------------------------|---------------------------------------|
| Cluster discovery    | APIExportEndpointSlice endpoints     | Cache objects on source (spec.baseURL)|
| Resource type set    | PublishedResources (user-defined)    | All CRDs present on source            |
| Client construction  | kubeconfig from EndpointSlice        | URL + `--cache-client-*` flags        |
| Sync direction       | kcp workspace → service cluster      | source → all peer cache-servers       |
| Shard awareness      | `/clusters/*` URL suffix (kcp)       | shard-in-URL round-tripper            |

## What can be reused or closely adapted

- **DynamicMultiClusterManager (DMCM)** (`internal/kcp/multicluster.go`): the pattern of
  wrapping a multicluster-runtime manager with dynamic `StartController`/stop lifecycle is
  directly reusable. Controllers started via DMCM are pre-seeded with all currently-engaged
  clusters and receive future `Engage()` calls as new clusters arrive. Copy + adapt into
  `internal/manager/`; no shared library for now.

- **Dependencies to add**: `sigs.k8s.io/multicluster-runtime v0.24.1`,
  `github.com/kcp-dev/multicluster-provider v0.8.0`, and
  `github.com/kcp-dev/multicluster-provider/client v0.8.0` — same versions as api-syncagent,
  same controller-runtime series, no conflicts expected.

- **multicluster-runtime `mccontroller.NewUnmanaged()` + `MultiClusterWatch()`**: the pattern
  for per-resource-type controllers that become cluster-aware via `Engage()` calls rather than
  static registration.

- **crdmanager controller pattern** (`internal/controller/syncmanager` in api-syncagent):
  watches a set of objects (PublishedResources there, CRDs here), starts/stops a Replication
  controller per object via DMCM. Structure is reusable; the trigger resource and the sync
  logic differ.

- **Overall project structure**: main.go / options.go / internal/ layout, logging, metrics,
  leader election wiring.

## What is new / does not exist in api-syncagent

### 1. Peer discovery (`internal/peer`)

api-syncagent uses `multicluster-provider/apiexport.New()` which watches APIExportEndpointSlice.
We need a new `multicluster.Provider` implementation that:

- On start: for each `--initial-peer-urls` entry, cross-shard LIST Cache objects on that peer
  to seed the initial cluster set. This is a read from the peer (a different server than the
  source), not redundant with the source WATCH's own initial LIST. It is specifically needed
  when the source is brand-new and has no shard data yet — the source WATCH's initial LIST
  would return nothing in that case.
- Ongoing: cross-shard WATCH Cache objects on the source (wildcard shard `*`, cluster
  `system:shard`). When a new Cache object appears (excluding own, identified by `spec.baseURL
  == --source-url`), call `Engage(ctx, cacheName, cluster.Cluster)` on the manager's runnables.
- Cluster client: constructed from `spec.baseURL` + shared TLS creds (`--cache-client-ca-file/cert-file/key-file`),
  wrapped with the shard-in-URL round-tripper for all operations.
- When a Cache object disappears: signal cluster disengagement (context cancellation).

### 2. TLS client builder

No equivalent in api-syncagent (which uses kubeconfigs). New package needed:

- Build `rest.Config` from a bare URL + `--cache-client-ca-file`, `--cache-client-cert-file`, `--cache-client-key-file`.
- Wrap with `WithShardNameFromContextRoundTripper` — both source and peer cache-servers use the
  same implementation and always expect the shard in the URL, so the round-tripper is needed for
  all operations (reads and writes alike). The shard value in context (`*` for wildcard watches,
  specific name for targeted writes) determines what appears in the URL.
- Used by both the peer discovery component (peer clients) and the source client (`--source-url`).

### 3. Source client and identity resolution

The syncagent itself needs a client for the source cache-server:

- Built from `--source-url` + TLS (same builder as above, round-tripper included).
- On startup: read Cache object from source's `system:cache:server/system:cache` → own
  cache-server name.
- Own name used to filter authoritative shards throughout.

### 4. Authoritative shard tracker

No equivalent in api-syncagent. New component:

- Cross-shard WATCH Shard objects on source (wildcard `*`, cluster `system:shard`).
- Maintains a thread-safe set of authoritative shard names: those whose Shard object carries
  `kcp.io/cache == own-name`.
- Read by sync controllers to filter events client-side.

### 5. CRD manager (`internal/controller/crdmanager`)

api-syncagent's syncmanager watches PublishedResources (user-defined CRs) and checks that the
resource is bound on all endpoints before starting a sync controller. The cache-syncagent
variant:

- Watches CRDs on the source.
- For each CRD: start a Replication controller via DMCM (no binding check needed — if the CRD
  exists on source, it exists).
- When a CRD is removed: stop the corresponding Replication controller.

### 6. Replication controller (`internal/controller/replication`)

api-syncagent's sync controller is bidirectional (spec kcp→local, status local→kcp) and has
projection/mutation/related resources. The cache-syncagent Replication controller is simpler:

- **Source informer**: wildcard shard `*`, wildcard cluster `*` on source. Filter events to
  authoritative shards. On ADD/UPDATE/DELETE: replicate to all peers (write via shard-in-URL
  client, retry with backoff).
- **Peer informer**: one per peer (fed via `Engage()` from the peer discovery component). Wildcard
  shard `*`, wildcard cluster `*` on that peer. Filter to authoritative shards. On event:
  compare object against source; DELETE/UPDATE/CREATE on peer as needed to correct divergence.
- No projection, mutation, or status-only sync — objects are replicated as-is.
- The source client is injected at construction time (shared across all controllers of the same
  resource type); the peer client arrives via `Engage()` per cluster.

### 7. Shard decommissioning cleanup tool

TODO: not in scope. Shard decommissioning workflow is not ready on the kcp side in general.
Revisit when kcp catches up. See `plan-data-sync-mechanism.md` for Option 1 (dedicated cleanup
tool) and Option 2 (autonomous GC on cache-servers) as candidate approaches.

## Proposed package structure

```
internal/
├── client/
│   └── builder.go          Build rest.Config from URL+TLS; shard-in-URL round-tripper
├── peer/
│   └── discovery.go        Peer discovery: multicluster.Provider driven by Cache objects
├── shard/
│   └── tracker.go          Authoritative shard set (cross-shard Shard object watch)
├── controller/
│   ├── crdmanager/         CRD-driven lifecycle: starts/stops a Replication controller per type
│   └── replication/        Replication controller: source + peer informers, reconcile logic
└── ...existing (log, metrics, version, kubeconfig, options)...
```

