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

// BuildKcpConfig creates a *rest.Config for direct mTLS access to a cache-server.
// The config is pre-wrapped with shard round-trippers that default to the wildcard
// shard; callers can override per-request via WithShardInContext.
// Intended for use with kcp-aware clients.
func BuildKcpConfig(serverURL, caFile, certFile, keyFile string) (*rest.Config, error) {
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
	WithDefaultShardRoundTripper(cfg, kshard.Wildcard)

	return cfg, nil
}
