# Data sync mechanism

Current state:

* The cache-server implements on top of apiextensions-apiserver (see @kcp/vendor).
* There MAY BE many shards in the kcp installation.
* There MAY BE many cache-servers in the kcp installation.
* Each shard gets a kubeconfig for one cache-server (it starts with `kcp start ... --cache-kubeconfig=<kubeconfig file>`).
* Each cache-server creates a special, self-identifying Cache object (see @kcp/staging/src/github.com/kcp-dev/sdk/apis/core/v1alpha1/cache_types.go) in its `system:cache:server` shard & `system:cache` cluster, with special `kcp.io/cache: .self` annotation
* Each shard pulls Cache object(s) from its associated cache-server into `system:shard` cluster (see the reconciler in @kcp/pkg/reconciler/core/cache)
* Each shard pushes a set of resources into its cache-server, into `<Storage prefix> / <Group> / <Resource> / [ <If CR-based, then this segment contains Identity (if APIBinding) or "customresources" (if local CRD)> / ] <Shard> / <Cluster> / [ <Namespace> / ] <Name>`
  * Observe the `<Shard>` segment. This is resolved from:
    * Client side:
      * 1. Context is decorated with Shard name: See `WithShardInContext` in @kcp/pkg/cache/client/context.go
      * 2. The replication reconcilers trigger cache client calls to create/update/delete an object on the cache-server: Details not important, but just for a reference: @kcp/pkg/reconciler/cache/replication
      * 2. Round-tripper extracts the shard name from context into URL (`http.Request.URL.Path`), right before making the request: See `WithShardNameFromContextRoundTripper` in @kcp/pkg/cache/client/round_tripper.go
    * Server side:
      * 1. A handler in the apiextensions-apiserver handler chain extracts the shard name from the URL on the incoming request and stores it in the context: See `WithShardScope` in @kcp/pkg/cache/server/handler.go
      * 2. The apiextensions-apiserver extracts the shard name in request's context: `ShardFrom` in @kcp/vendor/k8s.io/apiserver/pkg/endpoints/request/context_shard_kcp.go
      * 3. The apiserver machinery constructs the etcd key for the object such that it always includes shard and cluster names.

* Assume that in each cache-server, there may be multiple Cache objects on their local `system:cache:server` shard & `system:cache` cluster, identifying the different cache-servers in the whole kcp installation (with exactly one of them identifying _this_ cache-server, based on the `kcp.io/cache` annotation).
* Assume that each cache-server contains Shard objects ( @kcp/staging/src/github.com/kcp-dev/sdk/apis/core/v1alpha1/shard_types.go ) in their respective `<shard name>` shard & `system:shard` cluster. Listing across `*` (wildcard) shards in `system:shard` cluster would then return all shards currently known to the cache-server. Each such Shard object in the cache-server is annotated with `kcp.io/cache` that contains the name of the cache-server these shards are originally connected to.

Desired state:

* There needs to be one cache-syncagent per cache-server.
* The cache-syncagent needs to sync-out authoritative data from its source cache-server into all other known cache-servers. A cache-server has authoritative copy of data, if the shard this data belongs to is directly connected to this cache-server (i.e. star-shaped graph - e.g. shards directly replicated into cache-server `A` are pushed to cache-servers `B`, `C`, `D`; shards directly connected to cache-server `B` are pushed to `A`, `C`, `D`).
* Assume the cache-syncagent has `--tls-ca`, `--tls-cert`, `--tls-key`, `--source-url` and `--initial-member-peer-urls` flags. All cache-servers are expected to use the same CA, certificate, and key. The kubeconfigs are then constructed with this TLS data and server URL. The `--source-url` is the URL of the cache-server from which this cache-syncagent is watching the data, writing it into the rest of the cache-servers (peers).
* Assume there can be many shards connected to a cache-server, and each shard can have many resources, and the sets of resources across these shards may be distinct. The set of resources available to a shard is changing dynamically over time. That means that creating informers on per-shard-per-resource basis may be ineffective and expensive. If needed, the cache-syncagent may connect directly to the etcd (for the cache-server pointed to by `--source-url`), if there are opportunities for more efficient watches.
