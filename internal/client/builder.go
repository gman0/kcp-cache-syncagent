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

package client

import (
	"fmt"
	"os"

	kshard "github.com/gman0/kcp-cache-syncagent/internal/client/shard"

	"k8s.io/client-go/rest"
)

// BuildConfig creates a *rest.Config for direct mTLS access to a cache-server.
// The config is pre-wrapped with the four-layer round-tripper chain so that
// bare clients perform wildcard shard+cluster requests by default.
// Callers override the defaults by setting shard/cluster in the request context
// via WithShardInContext / WithClusterInContext.
//
// Round-tripper execution order at runtime:
//
//	DefaultCluster → DefaultShard → ClusterRT → ShardRT → transport
//
// Wildcard reads: no context override → defaults propagate → URL becomes
//
//	/shards/*/clusters/*/...
//
// Targeted writes: caller sets shard+cluster in context → overrides defaults.
func BuildConfig(serverURL, caFile, certFile, keyFile string) (*rest.Config, error) {
	caData, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("reading CA file: %w", err)
	}
	certData, err := os.ReadFile(certFile)
	if err != nil {
		return nil, fmt.Errorf("reading cert file: %w", err)
	}
	keyData, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("reading key file: %w", err)
	}

	cfg := &rest.Config{
		Host: serverURL,
		TLSClientConfig: rest.TLSClientConfig{
			CAData:   caData,
			CertData: certData,
			KeyData:  keyData,
		},
	}

	// Apply round-tripper chain innermost first (each cfg.Wrap call adds an
	// outer layer over the previous chain).
	WithShardNameFromContextRoundTripper(cfg)
	WithClusterNameFromContextRoundTripper(cfg)
	WithDefaultShardRoundTripper(cfg, kshard.Wildcard)
	WithDefaultClusterRoundTripper(cfg, "*")

	return cfg, nil
}
