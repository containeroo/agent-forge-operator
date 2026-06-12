/*
Copyright 2026.

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

package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
)

const (
	poolMetricNamespaceLabel = "namespace"
	poolMetricPoolLabel      = "pool"
	poolMetricStateLabel     = "state"
)

var (
	vmOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agent_forge_vsphere_vm_operations_total",
		Help: "Total vSphere VM operations attempted by the operator.",
	}, []string{"operation", "result"})

	isoOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agent_forge_iso_operations_total",
		Help: "Total InfraEnv ISO cache operations attempted by the operator.",
	}, []string{"operation", "result"})

	poolCapacityGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "agent_forge_pool_capacity",
		Help: "Observed VsphereAgentPool capacity by pool and state.",
	}, []string{poolMetricNamespaceLabel, poolMetricPoolLabel, poolMetricStateLabel})
)

func init() {
	metrics.Registry.MustRegister(vmOperationsTotal, isoOperationsTotal, poolCapacityGauge)
}

func recordVMOperation(operation string, err error) {
	vmOperationsTotal.WithLabelValues(operation, metricResult(err)).Inc()
}

func recordISOOperation(operation string, err error) {
	isoOperationsTotal.WithLabelValues(operation, metricResult(err)).Inc()
}

func recordPoolCapacityMetrics(pool *agentforgev1alpha1.VsphereAgentPool, plan PoolPlan) {
	labels := prometheus.Labels{
		poolMetricNamespaceLabel: pool.Namespace,
		poolMetricPoolLabel:      pool.Name,
	}
	for state, value := range map[string]int32{
		"desired":                plan.DesiredReplicas,
		"agent_machines":         plan.AgentMachines,
		"waiting_agent_machines": plan.WaitingAgentMachines,
		"unready_agent_machines": plan.UnreadyAgentMachines,
		"without_agent":          plan.AgentMachinesWithoutAgent,
		"matching_agents":        plan.MatchingAgents,
		"bound_agents":           plan.BoundAgents,
		"available_agents":       plan.AvailableAgents,
		"pending_owned_vms":      plan.PendingOwnedVMs,
		"demand_deficit":         plan.DemandDeficit,
	} {
		labelsWithState := prometheus.Labels{
			poolMetricNamespaceLabel: labels[poolMetricNamespaceLabel],
			poolMetricPoolLabel:      labels[poolMetricPoolLabel],
			poolMetricStateLabel:     state,
		}
		poolCapacityGauge.With(labelsWithState).Set(float64(value))
	}
}

func deletePoolCapacityMetrics(pool *agentforgev1alpha1.VsphereAgentPool) {
	poolCapacityGauge.DeletePartialMatch(prometheus.Labels{
		poolMetricNamespaceLabel: pool.Namespace,
		poolMetricPoolLabel:      pool.Name,
	})
}

func metricResult(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}
