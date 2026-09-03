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

// Package client contains round-trippers for shard-aware cache-server access.
// Adapted from github.com/kcp-dev/kcp/pkg/cache/client.
package client

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"

	kshard "github.com/gman0/kcp-cache-syncagent/internal/client/shard"

	"k8s.io/client-go/rest"
)

var shardSegmentRegex = regexp.MustCompile(`shards/([^/]+)/.+`)

// WithShardNameFromContextRoundTripper wraps cfg so that every request reads
// the shard name from its context and injects it into the URL path.
func WithShardNameFromContextRoundTripper(cfg *rest.Config) {
	cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return &shardRoundTripper{delegate: rt}
	})
}

type shardRoundTripper struct{ delegate http.RoundTripper }

func (r *shardRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	s := ShardFromContext(req.Context())
	fmt.Printf("### kcp-cache-syncagent/internal/client/round_tripper.go shardRoundTripper shard=%q path-before=%q\n", s, req.URL.String())
	if !s.Empty() {
		req = req.Clone(req.Context())
		req.URL.Path = shardInPath(req.URL.Path, s)
		req.URL.RawPath = shardInPath(req.URL.RawPath, s)
		fmt.Printf("### kcp-cache-syncagent/internal/client/round_tripper.go shardRoundTripper path-after=%q\n", req.URL.String())
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
