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

package options

import (
	"fmt"

	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/util/sets"
)

type ControllerRunOptions struct {
	MetricsAddr             string
	HealthAddr              string
	EnableLeaderElection    bool
	LeaderElectionWorkspace string
	LeaderElectionNamespace string
	WorkerCount             int

	EnabledControllers  sets.Set[string]
	DisabledControllers sets.Set[string]
}

// portIndex is a unique number per cmd/, in order for them
// to be able to run in parallel during development without
// having ports clash.
func NewDefaultOptions(portIndex int) ControllerRunOptions {
	return ControllerRunOptions{
		EnableLeaderElection:    true,
		LeaderElectionWorkspace: "root",
		LeaderElectionNamespace: "default",
		MetricsAddr:             fmt.Sprintf("127.0.0.1:8%d85", portIndex),
		HealthAddr:              fmt.Sprintf("127.0.0.1:8%d86", portIndex),
		WorkerCount:             4,

		EnabledControllers:  sets.New[string](),
		DisabledControllers: sets.New[string](),
	}
}

func (opts *ControllerRunOptions) AddPFlags(flags *pflag.FlagSet) {
	flags.BoolVar(&opts.EnableLeaderElection, "enable-leader-election", opts.EnableLeaderElection, "Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
	flags.StringVar(&opts.LeaderElectionWorkspace, "leader-election-workspace", opts.LeaderElectionWorkspace, "Leader election workspace. This will be ignored if the kubeconfig already points to a specific workspace.")
	flags.StringVar(&opts.LeaderElectionNamespace, "leader-election-namespace", opts.LeaderElectionNamespace, "Leader election namespace. In-cluster discovery will be attempted in such case.")
	flags.StringVar(&opts.MetricsAddr, "metrics-listen-address", opts.MetricsAddr, "The address on which /metrics is served.")
	flags.StringVar(&opts.HealthAddr, "health-listen-address", opts.HealthAddr, "The address on which the health endpoints /readyz and /healthz are served.")
	flags.IntVar(&opts.WorkerCount, "worker-count", opts.WorkerCount, "Number of workers which process in parallel.")

	flags.Var(SetFlag(&opts.EnabledControllers), "enable-controllers", "Comma-separated list of controllers to enable (cannot be combined with --disable-controllers).")
	flags.Var(SetFlag(&opts.DisabledControllers), "disable-controllers", "Comma-separated list of controllers to disable (cannot be combined with --enable-controllers).")
}

func (opts *ControllerRunOptions) EffectiveControllers(allControllerNames sets.Set[string]) sets.Set[string] {
	if opts.EnabledControllers.Len() > 0 {
		return opts.EnabledControllers
	}

	if opts.DisabledControllers.Len() > 0 {
		return allControllerNames.Difference(opts.DisabledControllers)
	}

	return allControllerNames
}
