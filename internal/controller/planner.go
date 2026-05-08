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
	actionCreateVM   = "CreateVM"
	actionDeleteVM   = "DeleteVM"
	actionPatchAgent = "PatchAgent"
	actionNoop       = "Noop"

	deletePolicyOwnedOnly = "OwnedOnly"
	deletePolicyRetain    = "Retain"
	phaseAvailable        = "Available"
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
	Name      string
	Bound     bool
	Approved  bool
	RoleLabel string
	Hostname  string
	MAC       string
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

	for _, agent := range snapshot.MatchingAgents {
		if agent.Bound {
			continue
		}
		if agent.Approved && agent.RoleLabel == pool.Spec.Agent.Role {
			continue
		}
		actions = append(actions, agentforgev1alpha1.PlannedActionStatus{
			Type:   actionPatchAgent,
			Name:   agent.Name,
			Reason: "Matching Agent is not approved or does not have the requested role label",
			DryRun: pool.Spec.DryRun,
		})
	}

	excess := matchingAgents - desiredReplicas
	var vmsToDelete []agentforgev1alpha1.OwnedVMStatus
	if excess > 0 && deletePolicy == deletePolicyOwnedOnly {
		for _, vm := range snapshot.OwnedVMs {
			if int32(len(vmsToDelete)) >= excess { //nolint:gosec // bounded by slice length.
				break
			}
			if vm.Phase == "Bound" {
				continue
			}
			vmsToDelete = append(vmsToDelete, vm)
			actions = append(actions, agentforgev1alpha1.PlannedActionStatus{
				Type:   actionDeleteVM,
				Name:   vm.Name,
				Reason: fmt.Sprintf("There are %d more matching Agents than desired and deletePolicy is OwnedOnly", excess),
				DryRun: pool.Spec.DryRun,
			})
		}
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
		if vm.Name == "" || vm.Phase == "Bound" {
			continue
		}
		count++
	}
	return count
}
