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

// Package client contains round-trippers for shard-and-cluster-aware cache-server access.
// Adapted from github.com/kcp-dev/kcp/pkg/cache/client.
package client

import (
	"net/http"
	"regexp"
	"strings"

	kshard "github.com/gman0/kcp-cache-syncagent/internal/client/shard"

	"k8s.io/client-go/rest"
)

var (
	shardSegmentRegex   = regexp.MustCompile(`shards/([^/]+)/.+`)
	clusterSegmentRegex = regexp.MustCompile(`clusters/([^/]+)/.+`)
)

// WithShardNameFromContextRoundTripper wraps cfg so that every request reads
// the shard name from its context and injects it into the URL path.
func WithShardNameFromContextRoundTripper(cfg *rest.Config) {
	cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return &shardRoundTripper{delegate: rt}
	})
}

type shardRoundTripper struct{ delegate http.RoundTripper }

func (r *shardRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if s := ShardFromContext(req.Context()); !s.Empty() {
		req = req.Clone(req.Context())
		req.URL.Path = shardInPath(req.URL.Path, s)
		req.URL.RawPath = shardInPath(req.URL.RawPath, s)
	}
	return r.delegate.RoundTrip(req)
}

// shardInPath rewrites orig so that /shards/<s>/ appears at the front.
func shardInPath(orig string, s kshard.Name) string {
	if strings.HasPrefix(orig, s.Path()) {
		return orig
	}
	if strings.HasPrefix(orig, "/shards") {
		if m := shardSegmentRegex.FindStringSubmatch(orig); len(m) >= 2 {
			return strings.Replace(orig, kshard.New(m[1]).Path(), s.Path(), 1)
		}
		p := s.Path()
		if len(orig) > 0 && orig[len(orig)-1] == '/' {
			p += "/"
		}
		return p
	}
	p := s.Path()
	if len(orig) > 0 && orig[0] != '/' {
		p += "/"
	}
	return p + orig
}

// WithDefaultShardRoundTripper wraps cfg so that requests without a shard in
// context receive defaultShard before the URL is modified.
func WithDefaultShardRoundTripper(cfg *rest.Config, defaultShard kshard.Name) {
	cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return &defaultShardRoundTripper{delegate: rt, shard: defaultShard}
	})
}

type defaultShardRoundTripper struct {
	delegate http.RoundTripper
	shard    kshard.Name
}

func (r *defaultShardRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if ShardFromContext(req.Context()).Empty() {
		req = req.WithContext(WithShardInContext(req.Context(), r.shard))
	}
	return r.delegate.RoundTrip(req)
}

// WithClusterNameFromContextRoundTripper wraps cfg so that every request reads
// the cluster name from its context and injects it into the URL path.
func WithClusterNameFromContextRoundTripper(cfg *rest.Config) {
	cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return &clusterRoundTripper{delegate: rt}
	})
}

type clusterRoundTripper struct{ delegate http.RoundTripper }

func (r *clusterRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if c := ClusterFromContext(req.Context()); c != "" {
		req = req.Clone(req.Context())
		req.URL.Path = clusterInPath(req.URL.Path, c)
		req.URL.RawPath = clusterInPath(req.URL.RawPath, c)
	}
	return r.delegate.RoundTrip(req)
}

// clusterInPath rewrites orig so that /clusters/<cluster>/ appears before the
// API path segment (after any existing /shards/<shard>/ prefix, which is
// injected by the inner ShardRoundTripper that runs after us in the chain).
func clusterInPath(orig, cluster string) string {
	prefix := "/clusters/" + cluster
	if strings.HasPrefix(orig, prefix+"/") || orig == prefix {
		return orig
	}
	if strings.HasPrefix(orig, "/clusters/") {
		if m := clusterSegmentRegex.FindStringSubmatch(orig); len(m) >= 2 {
			return strings.Replace(orig, "/clusters/"+m[1], prefix, 1)
		}
		p := prefix
		if len(orig) > 0 && orig[len(orig)-1] == '/' {
			p += "/"
		}
		return p
	}
	if len(orig) > 0 && orig[0] != '/' {
		return prefix + "/" + orig
	}
	return prefix + orig
}

// WithDefaultClusterRoundTripper wraps cfg so that requests without a cluster
// in context receive defaultCluster before the URL is modified.
func WithDefaultClusterRoundTripper(cfg *rest.Config, defaultCluster string) {
	cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return &defaultClusterRoundTripper{delegate: rt, cluster: defaultCluster}
	})
}

type defaultClusterRoundTripper struct {
	delegate http.RoundTripper
	cluster  string
}

func (r *defaultClusterRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if ClusterFromContext(req.Context()) == "" {
		req = req.WithContext(WithClusterInContext(req.Context(), r.cluster))
	}
	return r.delegate.RoundTrip(req)
}
