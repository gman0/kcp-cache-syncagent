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
	"fmt"

	"github.com/spf13/pflag"

	"github.com/gman0/kcp-cache-syncagent/internal/log"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
)

type Options struct {
	KcpCacheTLSCa  string
	KcpCacheTLSKey string
	KcpCacheName   string

	KcpRootShardKubeconfig string

	// NB: Not actually defined here, as ctrl-runtime registers its
	// own --kubeconfig flag that is required to make its GetConfigOrDie()
	// work.
	// KubeconfigFile string

	// Namespace is the namespace that the Sync Agent runs in.
	Namespace string

	// Whether or not to perform leader election (requires permissions to
	// manage coordination/v1 leases)
	EnableLeaderElection bool

	// AgentName can be used to give this Sync Agent instance a custom name. This name is used
	// for the Sync Agent resource inside kcp. This value must not be changed after a Sync Agent
	// has registered for the first time in kcp.
	// If not given, defaults to "<service ref>-syncagent".
	AgentName string

	KubeconfigHostOverride   string
	KubeconfigCAFileOverride string

	LogOptions log.Options

	MetricsAddr string
	HealthAddr  string

	DisabledControllers []string

	// EnableServerSideApply switches the sync controller to use Kubernetes
	// Server-Side Apply when writing synchronized objects to the local
	// cluster. This preserves fields owned by other field managers (e.g.
	// Crossplane writing spec.resourceRef onto a claim) and hopefully avoids
	// accidentally creating duplicate composite resources.
	EnableServerSideApply bool
}

func NewOptions() *Options {
	return &Options{
		LogOptions:            log.NewDefaultOptions(),
		MetricsAddr:           "127.0.0.1:8085",
		EnableServerSideApply: false,
	}
}

func (o *Options) AddFlags(flags *pflag.FlagSet) {
	o.LogOptions.AddPFlags(flags)

	flags.StringVar(&o.KcpRootShardKubeconfig, "kcp-root-shard-kubeconfig", o.KcpRootShardKubeconfig, "root shard kubeconfig")
	flags.StringVar(&o.Namespace, "namespace", o.Namespace, "Kubernetes namespace the Sync Agent is running in")
	flags.StringVar(&o.AgentName, "agent-name", o.AgentName, "name of this Sync Agent, must not be changed after the first run, can be left blank to auto-generate a name")
	flags.BoolVar(&o.EnableLeaderElection, "enable-leader-election", o.EnableLeaderElection, "whether to perform leader election")
	flags.StringVar(&o.KubeconfigHostOverride, "kubeconfig-host-override", o.KubeconfigHostOverride, "override the host configured in the local kubeconfig")
	flags.StringVar(&o.KubeconfigCAFileOverride, "kubeconfig-ca-file-override", o.KubeconfigCAFileOverride, "override the server CA file configured in the local kubeconfig")
	flags.StringVar(&o.MetricsAddr, "metrics-address", o.MetricsAddr, "host and port to serve Prometheus metrics via /metrics (HTTP)")
	flags.StringVar(&o.HealthAddr, "health-address", o.HealthAddr, "host and port to serve probes via /readyz and /healthz (HTTP)")
	flags.StringSliceVar(&o.DisabledControllers, "disabled-controllers", o.DisabledControllers, fmt.Sprintf("comma-separated list of controllers (out of %v) to disable (can be given multiple times)", sets.List(availableControllers)))
	flags.BoolVar(&o.EnableServerSideApply, "enable-server-side-apply", o.EnableServerSideApply, "use Kubernetes Server-Side Apply when writing synchronized objects to the local cluster (recommended; preserves fields owned by other controllers such as Crossplane's claim binder)")
}

func (o *Options) Validate() error {
	errs := []error{}

	if err := o.LogOptions.Validate(); err != nil {
		errs = append(errs, err)
	}

	if len(o.AgentName) > 0 {
		if e := validation.IsDNS1035Label(o.AgentName); len(e) > 0 {
			errs = append(errs, fmt.Errorf("--agent-name is invalid: %v", e))
		}
	}

	if len(o.KcpRootShardKubeconfig) == 0 {
		errs = append(errs, errors.New("--kcp-root-shard-kubeconfig is required"))
	}

	disabled := sets.New(o.DisabledControllers...)
	unknown := disabled.Difference(availableControllers)

	if unknown.Len() > 0 {
		errs = append(errs, fmt.Errorf("unknown controller(s) %v, mut be any of %v", sets.List(unknown), sets.List(availableControllers)))
	}

	return utilerrors.NewAggregate(errs)
}

func (o *Options) Complete() error {
	errs := []error{}

	return utilerrors.NewAggregate(errs)
}
