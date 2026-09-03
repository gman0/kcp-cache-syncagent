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

// Package clusters defines the cluster name conventions used with the
// multi provider and provides filter helpers for controllers.
package clusters

import (
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

const (
	// SourceProviderName is the name under which the source cache-server provider
	// is registered with the multi provider.
	SourceProviderName = "source"

	// PeersProviderName is the name under which the peer discovery provider is
	// registered with the multi provider.
	PeersProviderName = "peers"

	// separator must match multi.Options{}.Separator (default "#").
	separator = "#"

	// SourceClusterName is the cluster name the source provider passes to
	// aware.Engage (without the multi-provider prefix). Combined with the
	// provider prefix it gives SourceCluster.
	SourceClusterName = "cache-server"

	// SourceCluster is the cluster name of the source cache-server as seen by
	// the multicluster manager after multi-provider name prefixing:
	// "<SourceProviderName>#<SourceClusterName>".
	SourceCluster = multicluster.ClusterName(SourceProviderName + separator + SourceClusterName)
)

// IsSource returns true for the single source cache-server cluster.
func IsSource(name multicluster.ClusterName, _ cluster.Cluster) bool {
	return strings.HasPrefix(string(name), SourceProviderName+separator)
}

// IsPeer returns true for peer cache-server clusters.
func IsPeer(name multicluster.ClusterName, _ cluster.Cluster) bool {
	return strings.HasPrefix(string(name), PeersProviderName+separator)
}
