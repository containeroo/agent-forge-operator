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

	conditionReady             = "Ready"
	conditionDryRun            = "DryRun"
	conditionMachineSetFound   = "MachineSetFound"
	conditionInfraEnvAvailable = "InfraEnvAvailable"
	conditionCapacitySatisfied = "CapacitySatisfied"
	conditionVsphereReady      = "VsphereReady"
	conditionISOReady          = "ISOReady"
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
	MachineSetReplicas int32
	MatchingAgents     []AgentInfo
	OwnedVMs           []agentforgev1alpha1.OwnedVMStatus
}

// PoolPlan is the reconcile plan derived from a snapshot and CR spec.
type PoolPlan struct {
	MachineSetReplicas int32
	DesiredReplicas    int32
	MatchingAgents     int32
	BoundAgents        int32
	AvailableAgents    int32
	PendingOwnedVMs    int32
	VMsToCreate        int32
	VMsToDelete        []agentforgev1alpha1.OwnedVMStatus
	AgentsToDelete     []AgentInfo
	AgentsToPatch      []AgentInfo
	Actions            []agentforgev1alpha1.PlannedActionStatus
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
	desiredReplicas := snapshot.MachineSetReplicas + bufferAgents
	deficit := desiredReplicas - matchingAgents - pendingOwnedVMs
	if deficit < 0 {
		deficit = 0
	}
	vmsToCreate := minInt32(deficit, maxProvisioning)

	var actions []agentforgev1alpha1.PlannedActionStatus
	for i := int32(0); i < vmsToCreate; i++ {
		actions = append(actions, agentforgev1alpha1.PlannedActionStatus{
			Type:   actionCreateVM,
			Reason: fmt.Sprintf("MachineSet requires %d replicas plus %d buffer Agents, but only %d matching Agents and %d owned provisioning VMs exist", snapshot.MachineSetReplicas, bufferAgents, matchingAgents, pendingOwnedVMs),
			DryRun: pool.Spec.DryRun,
		})
	}

	excess := matchingAgents - desiredReplicas
	vmsToDelete, agentsToDelete := deletionTargets(snapshot.OwnedVMs, snapshot.MatchingAgents, excess, deletePolicy)
	pendingSurplus := matchingAgents + pendingOwnedVMs - desiredReplicas
	if pendingSurplus < 0 {
		pendingSurplus = 0
	}
	vmsToDelete = append(vmsToDelete, surplusProvisioningDeletionTargets(snapshot.OwnedVMs, vmsToDelete, pendingSurplus, deletePolicy)...)
	vmsToDelete = append(vmsToDelete, orphanedDeletionTargets(snapshot.OwnedVMs, vmsToDelete, deletePolicy)...)
	for _, vm := range vmsToDelete {
		reason := fmt.Sprintf("There are %d more matching Agents than desired and deletePolicy is OwnedOnly", excess)
		switch vm.Phase {
		case phaseOrphaned:
			reason = "Owned VM no longer has a matching Agent after the discovery grace period"
		case phaseProvisioning:
			reason = "Owned provisioning VM is no longer needed because matching Agents already satisfy demand"
		}
		actions = append(actions, agentforgev1alpha1.PlannedActionStatus{
			Type:   actionDeleteVM,
			Name:   vm.Name,
			Reason: reason,
			DryRun: pool.Spec.DryRun,
		})
	}
	for _, agent := range agentsToDelete {
		actions = append(actions, agentforgev1alpha1.PlannedActionStatus{
			Type:   actionDeleteAgent,
			Name:   agent.Name,
			Reason: fmt.Sprintf("There are %d more matching Agents than desired and deletePolicy is OwnedOnly", excess),
			DryRun: pool.Spec.DryRun,
		})
	}

	agentsMarkedForDelete := map[string]struct{}{}
	for _, agent := range agentsToDelete {
		agentsMarkedForDelete[agent.Name] = struct{}{}
	}
	var agentsToPatch []AgentInfo
	for _, agent := range snapshot.MatchingAgents {
		if agent.Bound {
			continue
		}
		if _, deleting := agentsMarkedForDelete[agent.Name]; deleting {
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

	if len(actions) == 0 {
		actions = append(actions, agentforgev1alpha1.PlannedActionStatus{
			Type:   actionNoop,
			Reason: "Matching Agent capacity satisfies MachineSet demand",
			DryRun: pool.Spec.DryRun,
		})
	}

	return PoolPlan{
		MachineSetReplicas: snapshot.MachineSetReplicas,
		DesiredReplicas:    desiredReplicas,
		MatchingAgents:     matchingAgents,
		BoundAgents:        boundAgents,
		AvailableAgents:    availableAgents,
		PendingOwnedVMs:    pendingOwnedVMs,
		VMsToCreate:        vmsToCreate,
		VMsToDelete:        vmsToDelete,
		AgentsToDelete:     agentsToDelete,
		AgentsToPatch:      agentsToPatch,
		Actions:            actions,
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

func deletionTargets(vms []agentforgev1alpha1.OwnedVMStatus, agents []AgentInfo, excess int32, deletePolicy string) ([]agentforgev1alpha1.OwnedVMStatus, []AgentInfo) {
	if excess <= 0 || deletePolicy != deletePolicyOwnedOnly {
		return nil, nil
	}

	unboundAgents := map[string]AgentInfo{}
	for _, agent := range agents {
		if !agent.Bound {
			unboundAgents[agent.Name] = agent
		}
	}

	var released []agentforgev1alpha1.OwnedVMStatus
	var available []agentforgev1alpha1.OwnedVMStatus
	var provisioning []agentforgev1alpha1.OwnedVMStatus
	for _, vm := range vms {
		if vm.Name == "" || vm.Phase == phaseBound {
			continue
		}
		if vm.AgentRef == nil || vm.AgentRef.Name == "" {
			if vm.Phase == phaseProvisioning {
				provisioning = append(provisioning, vm)
			}
			continue
		}
		if _, ok := unboundAgents[vm.AgentRef.Name]; !ok {
			continue
		}
		if vm.Phase == phaseReleased {
			released = append(released, vm)
			continue
		}
		available = append(available, vm)
	}

	ordered := append(released, available...)
	ordered = append(ordered, provisioning...)
	selectedVMs := make([]agentforgev1alpha1.OwnedVMStatus, 0, minInt(excess, int32(len(ordered))))
	selectedAgents := make([]AgentInfo, 0, minInt(excess, int32(len(ordered))))
	selectedAgentNames := map[string]struct{}{}
	for _, vm := range ordered {
		if int32(len(selectedVMs)) >= excess { //nolint:gosec // bounded by slice length.
			break
		}
		selectedVMs = append(selectedVMs, vm)
		if vm.AgentRef == nil || vm.AgentRef.Name == "" {
			continue
		}
		agent, ok := unboundAgents[vm.AgentRef.Name]
		if !ok {
			continue
		}
		if _, exists := selectedAgentNames[agent.Name]; exists {
			continue
		}
		selectedAgents = append(selectedAgents, agent)
		selectedAgentNames[agent.Name] = struct{}{}
	}
	return selectedVMs, selectedAgents
}

func surplusProvisioningDeletionTargets(vms, alreadySelected []agentforgev1alpha1.OwnedVMStatus, surplus int32, deletePolicy string) []agentforgev1alpha1.OwnedVMStatus {
	if surplus <= 0 || deletePolicy != deletePolicyOwnedOnly {
		return nil
	}
	selected := selectedVMNames(alreadySelected)

	var targets []agentforgev1alpha1.OwnedVMStatus
	for _, vm := range vms {
		if int32(len(targets)) >= surplus { //nolint:gosec // bounded by slice length.
			break
		}
		if vm.Name == "" || vm.Phase != phaseProvisioning {
			continue
		}
		if vm.AgentRef != nil && vm.AgentRef.Name != "" {
			continue
		}
		if _, exists := selected[vm.Name]; exists {
			continue
		}
		targets = append(targets, vm)
	}
	return targets
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

func minInt(a int32, b int32) int {
	if a < b {
		return int(a)
	}
	return int(b)
}
