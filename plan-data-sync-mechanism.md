# Data sync mechanism - variant A

## Current state

* The cache-server implements on top of apiextensions-apiserver (see @kcp/vendor).
* There MAY BE many shards in the kcp installation.
* There MAY BE many cache-servers in the kcp installation.
* Each shard gets a kubeconfig for one cache-server (it starts with `kcp start ... --cache-kubeconfig=<kubeconfig file>`).
* Each cache-server creates a special, self-identifying Cache object (see @kcp/staging/src/github.com/kcp-dev/sdk/apis/core/v1alpha1/cache_types.go) in its `system:cache:server` shard & `system:cache` cluster, with special `kcp.io/cache: .self` annotation. Note: `system:cache:server` is a virtual shard — a cache-local etcd prefix convention with no corresponding Shard object. It exists purely to give system-level cache resources a distinct, unambiguous prefix in etcd.
* Each shard pulls Cache object(s) from its associated cache-server into its `system:shard` cluster (see the reconciler in @kcp/pkg/reconciler/core/cache). This makes Cache objects part of real shard data, stored under the shard's own etcd prefix.
* Each shard pushes a set of resources into its cache-server, into `<Storage prefix> / <Group> / <Resource> / [ <If CR-based, then this segment contains Identity (if APIBinding) or "customresources" (if local CRD)> / ] <Shard> / <Cluster> / [ <Namespace> / ] <Name>`
  * Observe the `<Shard>` segment. This is resolved from:
    * Client side:
      * 1. Context is decorated with Shard name: See `WithShardInContext` in @kcp/pkg/cache/client/context.go
      * 2. The replication reconcilers trigger cache client calls to create/update/delete an object on the cache-server: Details not important, but just for a reference: @kcp/pkg/reconciler/cache/replication
      * 3. Round-tripper extracts the shard name from context into URL (`http.Request.URL.Path`), right before making the request: See `WithShardNameFromContextRoundTripper` in @kcp/pkg/cache/client/round_tripper.go
    * Server side:
      * 1. A handler in the apiextensions-apiserver handler chain extracts the shard name from the URL on the incoming request and stores it in the context: See `WithShardScope` in @kcp/pkg/cache/server/handler.go
      * 2. The apiextensions-apiserver extracts the shard name in request's context: `ShardFrom` in @kcp/vendor/k8s.io/apiserver/pkg/endpoints/request/context_shard_kcp.go
      * 3. The apiserver machinery constructs the etcd key for the object such that it always includes shard and cluster names.

* Each cache-server stores exactly one Cache object in its local `system:cache:server` shard & `system:cache` cluster — its own. Other cache-servers' Cache objects do not land here; they propagate across the mesh via shard data (see desired state).
* Assume that each cache-server contains Shard objects ( @kcp/staging/src/github.com/kcp-dev/sdk/apis/core/v1alpha1/shard_types.go ) in their respective `<shard name>` shard & `system:shard` cluster. Listing across `*` (wildcard) shards in `system:shard` cluster would then return all shards currently known to the cache-server. Each such Shard object in the cache-server is annotated with `kcp.io/cache` that contains the name of the cache-server these shards are originally connected to.

## Desired state

### Topology

One cache-syncagent is deployed per cache-server. Each syncagent is responsible for replicating data from its **source** cache-server into all other known cache-servers (**peers**).

A cache-server holds the **authoritative** copy of data for a shard if that shard is directly connected to it — i.e. the shard's Shard object carries `kcp.io/cache` equal to that cache-server's name. The sync topology is star-shaped: shards directly connected to cache-server A are synced into B, C, D by A's syncagent; shards connected to B are synced into A, C, D by B's syncagent; and so on. There are no write conflicts because each shard is authoritative on exactly one cache-server.

### Configuration

The cache-syncagent accepts the following flags:

* `--cache-client-ca-file` — CA certificate used to verify cache-server serving certificates.
* `--cache-client-cert-file` — client certificate used to authenticate to cache-servers.
* `--cache-client-key-file` — key for the client certificate.
* `--source-url` — the URL of the cache-server that this syncagent watches and replicates data from.
* `--initial-peer-urls` — comma-separated list of peer cache-server URLs used to bootstrap the peer mesh before peer Cache objects are discovered via source data. Optional; not needed in a single-cache-server installation.

### Identity

On startup, the syncagent reads the Cache object from the source's `system:cache:server` shard & `system:cache` cluster. That location holds exactly one object — the cache-server's own — so no annotation is needed to identify it. The object's name becomes the syncagent's own cache-server name, used throughout to filter authoritative shards.

### Peer discovery

Peer connectivity is bootstrapped and maintained as follows:

**Bootstrap (one-time):** For each URL in `--initial-peer-urls`, the syncagent performs a cross-shard LIST of Cache objects on that peer (wildcard shard `*`, `system:shard` cluster). This seeds the initial peer list. Because Cache objects from other cache-servers propagate through shard data (see below), a single initial peer URL is sufficient to discover the full existing mesh transitively.

**Ongoing discovery:** The syncagent maintains a cross-shard watch of Cache objects on its source. When a new Cache object appears, the syncagent reads `spec.baseURL`, constructs a client using the `--cache-client-*` credentials, and begins syncing to that peer.

**Propagation:** The cache-server writes its Cache object into `system:cache:server/system:cache`. Each connected shard pulls that object into its `system:shard` cluster during bootstrap (existing behaviour, see `@kcp/pkg/reconciler/core/cache`). Because shard data is synced across all peers by each respective syncagent, Cache objects propagate through the mesh without a dedicated reconciler — they travel as ordinary shard data.

Example — cache-c joins an existing two-server mesh (cache-a, cache-b):

1. cache-c starts with `--initial-peer-urls=cache-a`.
2. cache-c's syncagent seeds its peer list from cache-a via cross-shard LIST — discovers cache-b.
3. cache-c's syncagent begins syncing its authoritative shard data (including each shard's cache-c Cache object copy) to both cache-a and cache-b.
4. cache-a's and cache-b's syncagents see cache-c's Cache object appear in received shard data → discover cache-c → start syncing their own data to cache-c.
5. The mesh is fully connected.

### Authoritative shard determination

The syncagent watches Shard objects on its source across all shards (wildcard `*`) in the `system:shard` cluster. A shard is **authoritative** for this syncagent if its Shard object carries `kcp.io/cache` equal to the syncagent's own name. Only data from authoritative shards is synced to peers.

### Watch and reconciliation mechanism

The set of resource types available to each shard changes dynamically over time. The syncagent maintains two complementary sets of informers, both driven by CRD discovery on the source.

**Resource type discovery:** The syncagent watches CRD objects on the source. Each new CRD triggers creation of a source informer and a peer informer per known peer for that resource type. A removed CRD triggers teardown of the corresponding informers.

**Source informers:** One informer per resource type, watching across wildcard `*` shards and clusters on the source. Each informer uses standard list-watch semantics (resourceVersion, bookmark events, GONE (410) re-list). Events are filtered client-side to authoritative shards. On any ADD, UPDATE, or DELETE event, the syncagent replicates the change to all known peers.

**Peer informers:** One informer per resource type per peer, watching across wildcard `*` shards and clusters on that peer. Events are filtered client-side to authoritative shards. On any event, the syncagent compares the peer's object against the source and corrects any divergence:

* Present on peer, absent from source → DELETE from peer
* Present on peer, differs from source → UPDATE peer to match source
* Absent from peer, present on source → CREATE on peer

On startup and on new peer discovery, the peer informer performs its initial LIST, generating synthetic ADDED events for all existing objects on that peer. This serves as the initial reconciliation automatically — no separate explicit step is needed, and ongoing drift is corrected event-by-event.

**Known limitation:** Total informer count is `M × (1 + P)` where M = resource types and P = peers. This is a known, accepted limitation to be revisited if scaling constraints are hit in practice.

### Write path to peers

When syncing an object to a peer, the syncagent writes via the peer's Kubernetes API using the same shard-in-URL mechanism as the shard replication reconcilers:

* A client is constructed from the peer's `spec.baseURL` and the `--cache-client-ca-file`, `--cache-client-cert-file`, `--cache-client-key-file` credentials.
* The shard name is injected into the request URL via `WithShardNameFromContextRoundTripper`, producing the same `<Storage prefix> / <Group> / <Resource> / ... / <Shard> / <Cluster> / ...` etcd key structure on the target.
* Create, update, and delete operations are issued via the standard Kubernetes API.

Because each shard is authoritative on exactly one cache-server, no write conflicts between syncagents can occur for the same key on a given peer.

### Bootstrap sequence

1. Connect to the source cache-server using `--source-url` and the `--cache-client-*` flags.
2. Read the Cache object from source's `system:cache:server/system:cache` → derive own cache-server name.
3. For each `--initial-peer-urls` entry: cross-shard LIST all Cache objects → seed initial peer list; build a client per discovered peer.
4. Start a cross-shard watch of Cache objects on source → ongoing peer discovery; for each newly appearing Cache object whose `spec.baseURL` differs from `--source-url`, build a client and begin syncing to that peer.
5. Start a cross-shard watch of Shard objects on source → maintain the authoritative shard list (those with `kcp.io/cache == own-name`).
6. Watch CRDs on source → for each resource type, start a source informer and a peer informer per known peer (see Watch and reconciliation mechanism); reconcile on events from either side.
7. As new peers are discovered (step 4) or new authoritative shards appear (step 5), start the corresponding peer informers; their initial LIST serves as the initial reconciliation automatically.

### Deletion propagation

#### Active shard objects

DELETE events from source informers are propagated to each peer immediately, retrying with backoff until each peer confirms. DELETE events arising from peer informers — object present on peer but absent from source — are handled by the same retry-until-confirm path.

No periodic re-listing is required: the informer LIST-on-start and GONE (410) re-list semantics ensure the syncagent never permanently loses track of source state after a restart, and peer informers continuously surface and correct any drift.

Eventual consistency is the correctness guarantee — there is no hard bound on propagation delay.

#### Shard decommissioning

When a shard is removed, it leaves no authoritative footprint — the syncagent stops tracking it, the source watch for that shard stops, and the peer watch filters it out (not authoritative). Stale data for that shard accumulates on all peers indefinitely. Two options for cleanup:

**Option 1: Dedicated cleanup tool**
An operator-run tool (e.g. a subcommand of the syncagent binary) that, given a shard name, connects to all known peers and deletes all objects for that shard across all resource types. Explicit, auditable, and does not depend on the syncagent being running.

**Option 2: Autonomous GC on cache-servers (follow-up)**
Cache-servers detect that a shard no longer exists in the mesh — no Shard object in any connected shard's data — and garbage-collect its data autonomously. Fully automatic and requires no operator action, but needs changes to the cache-server itself. Deferred to a follow-up.

Both options can coexist: the dedicated tool as the near-term operational escape hatch, autonomous GC as the long-term solution.

**Current decision**: neither is implemented in the initial version. Shard decommissioning is not yet in scope — the kcp-side workflow for shard removal is not ready. Revisit when kcp catches up.
