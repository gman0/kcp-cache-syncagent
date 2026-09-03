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

package main

import (
	"errors"
	"slices"

	"github.com/spf13/pflag"

	"github.com/gman0/kcp-cache-syncagent/internal/log"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

// Options holds all command-line options for the cache-syncagent.
type Options struct {
	// CacheClientCAFile is the CA certificate used to verify cache-server
	// serving certificates.
	CacheClientCAFile string

	// CacheClientCertFile is the client certificate used to authenticate
	// to cache-servers.
	CacheClientCertFile string

	// CacheClientKeyFile is the private key for the client certificate.
	CacheClientKeyFile string

	// SourceURL is the URL of the cache-server this syncagent replicates from.
	SourceURL string

	// InitialPeerURLs is a comma-separated list of peer cache-server URLs used
	// to bootstrap the peer mesh before peer Cache objects are discovered via
	// the source.
	InitialPeerURLs []string

	// Whether or not to perform leader election (requires permissions to
	// manage coordination/v1 leases)
	EnableLeaderElection bool

	// Namespace is the Kubernetes namespace used for leader-election lease
	// objects (only meaningful when LeaderElectionKubeconfig is set).
	Namespace string

	LogOptions log.Options

	MetricsAddr string
	HealthAddr  string
}

func NewOptions() *Options {
	return &Options{
		LogOptions:  log.NewDefaultOptions(),
		MetricsAddr: "127.0.0.1:8085",
		HealthAddr:  "0",
	}
}

func (o *Options) AddFlags(flags *pflag.FlagSet) {
	o.LogOptions.AddPFlags(flags)

	flags.StringVar(&o.CacheClientCAFile, "cache-client-ca-file", o.CacheClientCAFile,
		"CA certificate file used to verify cache-server serving certificates")
	flags.StringVar(&o.CacheClientCertFile, "cache-client-cert-file", o.CacheClientCertFile,
		"client certificate file used to authenticate to cache-servers")
	flags.StringVar(&o.CacheClientKeyFile, "cache-client-key-file", o.CacheClientKeyFile,
		"private key file for the client certificate")
	flags.StringVar(&o.SourceURL, "source-url", o.SourceURL,
		"URL of the source cache-server to replicate from")
	flags.StringSliceVar(&o.InitialPeerURLs, "initial-peer-urls", o.InitialPeerURLs,
		"comma-separated list of peer cache-server URLs used to bootstrap the peer mesh (optional)")
	flags.BoolVar(&o.EnableLeaderElection, "enable-leader-election", o.EnableLeaderElection,
		"whether to perform leader election")
	flags.StringVar(&o.Namespace, "namespace", o.Namespace,
		"Kubernetes namespace for leader-election leases (only used with --leader-election-kubeconfig)")
	flags.StringVar(&o.MetricsAddr, "metrics-address", o.MetricsAddr,
		"host:port for Prometheus metrics via /metrics (HTTP)")
	flags.StringVar(&o.HealthAddr, "health-address", o.HealthAddr,
		"host:port for health probes via /readyz and /healthz (HTTP)")
}

func (o *Options) Validate() error {
	var errs []error

	if err := o.LogOptions.Validate(); err != nil {
		errs = append(errs, err)
	}
	if o.CacheClientCAFile == "" {
		errs = append(errs, errors.New("--cache-client-ca-file is required"))
	}
	if o.CacheClientCertFile == "" {
		errs = append(errs, errors.New("--cache-client-cert-file is required"))
	}
	if o.CacheClientKeyFile == "" {
		errs = append(errs, errors.New("--cache-client-key-file is required"))
	}
	if o.SourceURL == "" {
		errs = append(errs, errors.New("--source-url is required"))
	}
	if o.EnableLeaderElection && o.Namespace == "" {
		errs = append(errs, errors.New("--namespace is required when --leader-election-kubeconfig is set"))
	}

	return utilerrors.NewAggregate(errs)
}

func (o *Options) Complete() error {
	// Remove the source URL from the initial peer list if present.
	o.InitialPeerURLs = slices.DeleteFunc(o.InitialPeerURLs, func(s string) bool {
		return s == o.SourceURL
	})
	return nil
}
