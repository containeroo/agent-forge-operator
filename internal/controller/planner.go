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
	"fmt"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
)

const (
	actionCreateVM    = "CreateVM"
	actionDeleteVM    = "DeleteVM"
	actionDeleteAgent = "DeleteAgent"
	actionPatchAgent  = "PatchAgent"
	actionNoop        = "Noop"

	deletePolicyOwnedOnly = "OwnedOnly"
	deletePolicyRetain    = "Retain"
	phaseAvailable        = "Available"
	phaseBound            = "Bound"
	phaseOrphaned         = "Orphaned"
	phaseProvisioning     = "Provisioning"
	phaseReleased         = "Released"
	defaultAgentRole      = "worker"

	conditionReady              = "Ready"
	conditionDryRun             = "DryRun"
	conditionAgentMachineDemand = "AgentMachineDemandFound"
	conditionInfraEnvAvailable  = "InfraEnvAvailable"
	conditionCapacitySatisfied  = "CapacitySatisfied"
	conditionVsphereReady       = "VsphereReady"
	conditionISOReady           = "ISOReady"
)

// AgentInfo is the small subset of an Assisted Installer Agent needed for
// bridge capacity planning.
type AgentInfo struct {
	Name              string
	Bound             bool
	MachineName       string
	Approved          bool
	SpecRole          string
	RoleLabel         string
	Hostname          string
	InventoryHostname string
	MAC               string
	BIOSUUID          string
}

// PoolSnapshot is the observed cluster state used by the pure planner.
type PoolSnapshot struct {
	AgentMachines        int32
	WaitingAgentMachines int32
	UnreadyAgentMachines int32
	MatchingAgents       []AgentInfo
	OwnedVMs             []agentforgev1alpha1.OwnedVMStatus
}

// PoolPlan is the reconcile plan derived from a snapshot and CR spec.
type PoolPlan struct {
	AgentMachines        int32
	WaitingAgentMachines int32
	UnreadyAgentMachines int32
	DesiredReplicas      int32
	MatchingAgents       int32
	BoundAgents          int32
	AvailableAgents      int32
	PendingOwnedVMs      int32
	VMsToCreate          int32
	VMsToDelete          []agentforgev1alpha1.OwnedVMStatus
	AgentsToDelete       []AgentInfo
	AgentsToPatch        []AgentInfo
	Actions              []agentforgev1alpha1.PlannedActionStatus
}

func buildPlan(pool *agentforgev1alpha1.VsphereAgentPool, snapshot PoolSnapshot) PoolPlan {
	bufferAgents := pool.Spec.Scaling.BufferAgents
	if bufferAgents < 0 {
		bufferAgents = 0
	}
	maxProvisioning := pool.Spec.Scaling.MaxProvisioning
	if maxProvisioning <= 0 {
		maxProvisioning = 3
	}
	deletePolicy := pool.Spec.Scaling.DeletePolicy
	if deletePolicy == "" {
		deletePolicy = deletePolicyOwnedOnly
	}

	var boundAgents int32
	var availableAgents int32
	for _, agent := range snapshot.MatchingAgents {
		if agent.Bound {
			boundAgents++
			continue
		}
		availableAgents++
	}

	matchingAgents := int32(len(snapshot.MatchingAgents)) //nolint:gosec // Kubernetes object counts fit in int32 here.
	pendingOwnedVMs := countPendingOwnedVMs(snapshot.OwnedVMs)
	desiredReplicas := snapshot.AgentMachines + bufferAgents
	deficit := snapshot.WaitingAgentMachines + bufferAgents - availableAgents - pendingOwnedVMs
	if deficit < 0 {
		deficit = 0
	}
	vmsToCreate := minInt32(deficit, maxProvisioning)

	var actions []agentforgev1alpha1.PlannedActionStatus
	for i := int32(0); i < vmsToCreate; i++ {
		actions = append(actions, agentforgev1alpha1.PlannedActionStatus{
			Type:   actionCreateVM,
			Reason: fmt.Sprintf("%d AgentMachines are waiting for suitable Agents plus %d buffer Agents, with %d available Agents and %d owned provisioning VMs", snapshot.WaitingAgentMachines, bufferAgents, availableAgents, pendingOwnedVMs),
			DryRun: pool.Spec.DryRun,
		})
	}

	var agentsToPatch []AgentInfo
	for _, agent := range snapshot.MatchingAgents {
		if agent.Bound {
			continue
		}
		if agent.Approved && agent.SpecRole == pool.Spec.Agent.Role && agent.RoleLabel == pool.Spec.Agent.Role && agent.Hostname != "" {
			continue
		}
		agentsToPatch = append(agentsToPatch, agent)
		actions = append(actions, agentforgev1alpha1.PlannedActionStatus{
			Type:   actionPatchAgent,
			Name:   agent.Name,
			Reason: "Candidate Agent is not approved, named, or assigned to the requested role",
			DryRun: pool.Spec.DryRun,
		})
	}

	vmsToDelete, agentsToDelete := deletedMachineTargets(snapshot.OwnedVMs, snapshot.MatchingAgents, deletePolicy)
	if snapshot.WaitingAgentMachines == 0 && snapshot.UnreadyAgentMachines == 0 {
		surplusAvailable := availableAgents - bufferAgents
		if surplusAvailable > 0 {
			surplusVMs, surplusAgents := surplusAvailableDeletionTargets(snapshot.OwnedVMs, snapshot.MatchingAgents, vmsToDelete, deletePolicy, surplusAvailable)
			vmsToDelete = append(vmsToDelete, surplusVMs...)
			agentsToDelete = append(agentsToDelete, surplusAgents...)
		}
		vmsToDelete = append(vmsToDelete, orphanedDeletionTargets(snapshot.OwnedVMs, vmsToDelete, deletePolicy)...)
	}
	for _, vm := range vmsToDelete {
		reason := "CAPI Machine has been deleted and deletePolicy is OwnedOnly"
		switch vm.Phase {
		case phaseAvailable:
			reason = "Owned available Agent exceeds current AgentMachine demand and buffer"
		case phaseOrphaned:
			reason = "Owned VM no longer has a matching Agent and is eligible for cleanup"
		}
		actions = append(actions, agentforgev1alpha1.PlannedActionStatus{
			Type:   actionDeleteVM,
			Name:   vm.Name,
			Reason: reason,
			DryRun: pool.Spec.DryRun,
		})
	}
	agentDeleteReasons := agentDeletionReasonsByName(vmsToDelete)
	for _, agent := range agentsToDelete {
		reason := agentDeleteReasons[agent.Name]
		if reason == "" {
			reason = "CAPI Machine has been deleted and deletePolicy is OwnedOnly"
		}
		actions = append(actions, agentforgev1alpha1.PlannedActionStatus{
			Type:   actionDeleteAgent,
			Name:   agent.Name,
			Reason: reason,
			DryRun: pool.Spec.DryRun,
		})
	}

	if len(actions) == 0 {
		actions = append(actions, agentforgev1alpha1.PlannedActionStatus{
			Type:   actionNoop,
			Reason: "Agent capacity satisfies current AgentMachine demand",
			DryRun: pool.Spec.DryRun,
		})
	}

	return PoolPlan{
		AgentMachines:        snapshot.AgentMachines,
		WaitingAgentMachines: snapshot.WaitingAgentMachines,
		UnreadyAgentMachines: snapshot.UnreadyAgentMachines,
		DesiredReplicas:      desiredReplicas,
		MatchingAgents:       matchingAgents,
		BoundAgents:          boundAgents,
		AvailableAgents:      availableAgents,
		PendingOwnedVMs:      pendingOwnedVMs,
		VMsToCreate:          vmsToCreate,
		VMsToDelete:          vmsToDelete,
		AgentsToDelete:       agentsToDelete,
		AgentsToPatch:        agentsToPatch,
		Actions:              actions,
	}
}

func minInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func countPendingOwnedVMs(vms []agentforgev1alpha1.OwnedVMStatus) int32 {
	var count int32
	for _, vm := range vms {
		if vm.Name == "" || vm.Phase != phaseProvisioning {
			continue
		}
		count++
	}
	return count
}

func deletedMachineTargets(vms []agentforgev1alpha1.OwnedVMStatus, agents []AgentInfo, deletePolicy string) ([]agentforgev1alpha1.OwnedVMStatus, []AgentInfo) {
	if deletePolicy != deletePolicyOwnedOnly {
		return nil, nil
	}
	agentsByName := map[string]AgentInfo{}
	for _, agent := range agents {
		agentsByName[agent.Name] = agent
	}

	var selectedVMs []agentforgev1alpha1.OwnedVMStatus
	var selectedAgents []AgentInfo
	for _, vm := range vms {
		if vm.Name == "" || vm.Phase != phaseReleased || vm.Reason != "MachineDeleted" {
			continue
		}
		selectedVMs = append(selectedVMs, vm)
		if vm.AgentRef == nil || vm.AgentRef.Name == "" {
			continue
		}
		agent, ok := agentsByName[vm.AgentRef.Name]
		if !ok {
			continue
		}
		selectedAgents = append(selectedAgents, agent)
	}
	return selectedVMs, selectedAgents
}

func surplusAvailableDeletionTargets(vms []agentforgev1alpha1.OwnedVMStatus, agents []AgentInfo, alreadySelected []agentforgev1alpha1.OwnedVMStatus, deletePolicy string, surplus int32) ([]agentforgev1alpha1.OwnedVMStatus, []AgentInfo) {
	if deletePolicy != deletePolicyOwnedOnly || surplus <= 0 {
		return nil, nil
	}
	selected := selectedVMNames(alreadySelected)
	unboundAgentsByName := map[string]AgentInfo{}
	for _, agent := range agents {
		if agent.Name == "" || agent.Bound {
			continue
		}
		unboundAgentsByName[agent.Name] = agent
	}

	var selectedVMs []agentforgev1alpha1.OwnedVMStatus
	var selectedAgents []AgentInfo
	for _, vm := range vms {
		if int32(len(selectedVMs)) >= surplus { //nolint:gosec // slice length is bounded by observed Kubernetes objects.
			break
		}
		if vm.Name == "" || vm.Phase != phaseAvailable {
			continue
		}
		if _, exists := selected[vm.Name]; exists {
			continue
		}
		agentName := ownedVMAgentName(vm)
		if agentName == "" {
			continue
		}
		agent, ok := unboundAgentsByName[agentName]
		if !ok {
			continue
		}
		selectedVMs = append(selectedVMs, vm)
		selectedAgents = append(selectedAgents, agent)
	}
	return selectedVMs, selectedAgents
}

func agentDeletionReasonsByName(vms []agentforgev1alpha1.OwnedVMStatus) map[string]string {
	reasons := map[string]string{}
	for _, vm := range vms {
		agentName := ownedVMAgentName(vm)
		if agentName == "" {
			continue
		}
		switch vm.Phase {
		case phaseAvailable:
			reasons[agentName] = "Owned available Agent exceeds current AgentMachine demand and buffer"
		default:
			reasons[agentName] = "CAPI Machine has been deleted and deletePolicy is OwnedOnly"
		}
	}
	return reasons
}

func orphanedDeletionTargets(vms, alreadySelected []agentforgev1alpha1.OwnedVMStatus, deletePolicy string) []agentforgev1alpha1.OwnedVMStatus {
	if deletePolicy != deletePolicyOwnedOnly {
		return nil
	}
	selected := selectedVMNames(alreadySelected)

	var targets []agentforgev1alpha1.OwnedVMStatus
	for _, vm := range vms {
		if vm.Name == "" || vm.Phase != phaseOrphaned {
			continue
		}
		if _, exists := selected[vm.Name]; exists {
			continue
		}
		targets = append(targets, vm)
	}
	return targets
}

func selectedVMNames(vms []agentforgev1alpha1.OwnedVMStatus) map[string]struct{} {
	selected := map[string]struct{}{}
	for _, vm := range vms {
		if vm.Name != "" {
			selected[vm.Name] = struct{}{}
		}
	}
	return selected
}
