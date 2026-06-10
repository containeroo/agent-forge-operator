//nolint:goconst
package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
)

const (
	testAgent1      = "a-1"
	testAgent2      = "a-2"
	testAgent3      = "a-3"
	testCustomerKey = "customer"
	testCustomer    = "example"
	testFreeVM      = "free-vm"
	testWorkerRole  = defaultAgentRole
	testVMAvailable = phaseAvailable
)

func TestBuildPlanCreatesVMsForMachineDeficit(t *testing.T) {
	pool := testPool()

	plan := buildPlan(pool, PoolSnapshot{
		AgentMachines:        8,
		WaitingAgentMachines: 5,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent2, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent3, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
		},
	})

	if plan.DesiredReplicas != 8 {
		t.Fatalf("desired replicas = %d, want 8", plan.DesiredReplicas)
	}
	if plan.VMsToCreate != 5 {
		t.Fatalf("VMsToCreate = %d, want full deficit 5", plan.VMsToCreate)
	}
	if len(plan.Actions) != 5 {
		t.Fatalf("actions = %d, want 5", len(plan.Actions))
	}
	for _, action := range plan.Actions {
		if action.Type != actionCreateVM {
			t.Fatalf("action type = %s, want %s", action.Type, actionCreateVM)
		}
	}
}

func TestBuildPlanCountsOwnedProvisioningVMsAsPendingCapacity(t *testing.T) {
	pool := testPool()

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 2,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent2, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent3, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "pending-vm-1", Phase: phaseProvisioning},
			{Name: "pending-vm-2", Phase: phaseProvisioning},
		},
	})

	if plan.VMsToCreate != 0 {
		t.Fatalf("VMsToCreate = %d, want 0 because owned provisioning VMs already cover the deficit", plan.VMsToCreate)
	}
	if plan.PendingOwnedVMs != 2 {
		t.Fatalf("PendingOwnedVMs = %d, want 2", plan.PendingOwnedVMs)
	}
	if len(plan.Actions) != 1 || plan.Actions[0].Type != actionNoop {
		t.Fatalf("actions = %#v, want one Noop", plan.Actions)
	}
}

func TestBuildPlanCreatesOnlyRemainingDeficitAfterOwnedProvisioningVMs(t *testing.T) {
	pool := testPool()

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 4,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent2, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent3, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "pending-vm-1", Phase: phaseProvisioning},
			{Name: "pending-vm-2", Phase: phaseProvisioning},
			{Name: "bound-vm", Phase: phaseBound},
		},
	})

	if plan.VMsToCreate != 2 {
		t.Fatalf("VMsToCreate = %d, want remaining deficit 2", plan.VMsToCreate)
	}
}

func TestBuildPlanDoesNotPatchUnownedAgentsWithDemand(t *testing.T) {
	pool := testPool()

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 1,
		MatchingAgents: []AgentInfo{
			{Name: "agent-1", Bound: false, Approved: false, RoleLabel: ""},
		},
	})

	if len(plan.Actions) != 1 {
		t.Fatalf("actions = %d, want 1", len(plan.Actions))
	}
	if plan.Actions[0].Type != actionNoop {
		t.Fatalf("action type = %s, want %s", plan.Actions[0].Type, actionNoop)
	}
	if len(plan.AgentsToPatch) != 0 {
		t.Fatalf("AgentsToPatch = %#v, want none for unowned Agent", plan.AgentsToPatch)
	}
}

func TestBuildPlanDoesNotPatchUnownedAgentsWithoutDemand(t *testing.T) {
	pool := testPool()

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 0,
		MatchingAgents: []AgentInfo{
			{Name: "agent-1", Bound: false, Approved: false, RoleLabel: ""},
		},
	})

	if len(plan.AgentsToPatch) != 0 {
		t.Fatalf("AgentsToPatch = %#v, want none for unowned Agent without demand", plan.AgentsToPatch)
	}
	if len(plan.Actions) != 1 || plan.Actions[0].Type != actionNoop {
		t.Fatalf("actions = %#v, want one Noop", plan.Actions)
	}
}

func TestBuildPlanPatchesAgentAssociatedWithOwnedVMWithoutDemand(t *testing.T) {
	pool := testPool()

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 0,
		MatchingAgents: []AgentInfo{
			{Name: "agent-1", Bound: false, Approved: false, RoleLabel: "", MAC: "00-50-56-b2-a1-9f"},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "owned-vm", Phase: phaseProvisioning, MACAddress: "00-50-56-b2-a1-9f"},
		},
	})

	if len(plan.AgentsToPatch) != 1 || plan.AgentsToPatch[0].Name != "agent-1" {
		t.Fatalf("AgentsToPatch = %#v, want owned Agent patched", plan.AgentsToPatch)
	}
}

func TestBuildPlanDeletesOrphanedOwnedVMsWithoutExcessAgents(t *testing.T) {
	pool := testPool()

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 0,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent2, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent3, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "bound-vm-1", Phase: phaseBound, AgentRef: testAgentRef(testAgent1)},
			{Name: "orphaned-vm-1", Phase: phaseOrphaned},
			{Name: "orphaned-vm-2", Phase: phaseOrphaned},
		},
	})

	if plan.PendingOwnedVMs != 0 {
		t.Fatalf("PendingOwnedVMs = %d, want 0 for orphaned VMs", plan.PendingOwnedVMs)
	}
	if plan.VMsToCreate != 0 {
		t.Fatalf("VMsToCreate = %d, want 0 because matching Agents satisfy demand", plan.VMsToCreate)
	}
	if len(plan.VMsToDelete) != 2 {
		t.Fatalf("VMsToDelete = %d, want 2 orphaned VMs", len(plan.VMsToDelete))
	}
	if plan.VMsToDelete[0].Name != "orphaned-vm-1" || plan.VMsToDelete[1].Name != "orphaned-vm-2" {
		t.Fatalf("VMsToDelete = %#v, want orphaned VMs", plan.VMsToDelete)
	}
	if len(plan.Actions) != 2 || plan.Actions[0].Type != actionDeleteVM || plan.Actions[1].Type != actionDeleteVM {
		t.Fatalf("actions = %#v, want two DeleteVM actions", plan.Actions)
	}
}

func TestBuildPlanDoesNotDeleteSurplusProvisioningVMs(t *testing.T) {
	pool := testPool()

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 0,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent2, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent3, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "pending-vm-1", Phase: phaseProvisioning},
			{Name: "pending-vm-2", Phase: phaseProvisioning},
		},
	})

	if plan.VMsToCreate != 0 {
		t.Fatalf("VMsToCreate = %d, want 0 because matching Agents satisfy demand", plan.VMsToCreate)
	}
	if plan.PendingOwnedVMs != 2 {
		t.Fatalf("PendingOwnedVMs = %d, want 2 before applying cleanup", plan.PendingOwnedVMs)
	}
	if len(plan.VMsToDelete) != 0 {
		t.Fatalf("VMsToDelete = %#v, want no provisioning VM deletes without Machine deletion", plan.VMsToDelete)
	}
}

func TestBuildPlanDeletesSurplusAvailableOwnedAgents(t *testing.T) {
	pool := testPool()

	plan := buildPlan(pool, PoolSnapshot{
		AgentMachines:        3,
		WaitingAgentMachines: 0,
		UnreadyAgentMachines: 0,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent1},
			{Name: testAgent2, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent2},
			{Name: testAgent3, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent3},
			{Name: "extra-agent", Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: "extra-vm"},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "extra-vm", Phase: phaseAvailable, AgentRef: testAgentRef("extra-agent")},
		},
	})

	if plan.DesiredReplicas != 3 {
		t.Fatalf("desired replicas = %d, want 3 AgentMachines", plan.DesiredReplicas)
	}
	if len(plan.VMsToDelete) != 1 || plan.VMsToDelete[0].Name != "extra-vm" {
		t.Fatalf("VMsToDelete = %#v, want extra-vm", plan.VMsToDelete)
	}
	if len(plan.AgentsToDelete) != 1 || plan.AgentsToDelete[0].Name != "extra-agent" {
		t.Fatalf("AgentsToDelete = %#v, want extra-agent", plan.AgentsToDelete)
	}
}

func TestBuildPlanDoesNotDeleteSurplusAvailableAgentsWhileAgentMachinesNeedAgents(t *testing.T) {
	pool := testPool()

	plan := buildPlan(pool, PoolSnapshot{
		AgentMachines:             4,
		WaitingAgentMachines:      0,
		UnreadyAgentMachines:      1,
		AgentMachinesWithoutAgent: 1,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent1},
			{Name: testAgent2, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent2},
			{Name: testAgent3, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent3},
			{Name: "candidate-agent", Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: "candidate-vm"},
			{Name: "extra-agent", Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: "extra-vm"},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "candidate-vm", Phase: phaseAvailable, AgentRef: testAgentRef("candidate-agent")},
			{Name: "extra-vm", Phase: phaseAvailable, AgentRef: testAgentRef("extra-agent")},
		},
	})

	if len(plan.VMsToDelete) != 0 || len(plan.AgentsToDelete) != 0 {
		t.Fatalf("delete targets = VMs %#v Agents %#v, want no cleanup while AgentMachines still need Agents", plan.VMsToDelete, plan.AgentsToDelete)
	}
}

func TestBuildPlanDoesNotDeleteUnboundAgentsDuringScaleUp(t *testing.T) {
	pool := testPool()

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 4,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent1},
			{Name: testAgent2, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent2},
			{Name: testAgent3, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent3},
			{Name: "new-agent-1", Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: "new-agent-1"},
			{Name: "new-agent-2", Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: "new-agent-2"},
			{Name: "new-agent-3", Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: "new-agent-3"},
			{Name: "new-agent-4", Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: "new-agent-4"},
			{Name: "new-agent-5", Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: "new-agent-5"},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "new-agent-1", Phase: phaseAvailable, AgentRef: testAgentRef("new-agent-1")},
			{Name: "new-agent-2", Phase: phaseAvailable, AgentRef: testAgentRef("new-agent-2")},
			{Name: "new-agent-3", Phase: phaseAvailable, AgentRef: testAgentRef("new-agent-3")},
			{Name: "new-agent-4", Phase: phaseAvailable, AgentRef: testAgentRef("new-agent-4")},
			{Name: "new-agent-5", Phase: phaseAvailable, AgentRef: testAgentRef("new-agent-5")},
		},
	})

	if len(plan.VMsToDelete) != 0 || len(plan.AgentsToDelete) != 0 {
		t.Fatalf("delete targets = VMs %#v Agents %#v, want no cleanup while Machine is still scaling up", plan.VMsToDelete, plan.AgentsToDelete)
	}
}

func TestBuildPlanDoesNotDeleteOrphanedVMsDuringScaleUp(t *testing.T) {
	pool := testPool()

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 3,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent1},
			{Name: testAgent2, Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent2},
			{Name: testAgent3, Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent3},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "orphaned-vm", Phase: phaseOrphaned},
		},
	})

	if len(plan.VMsToDelete) != 0 {
		t.Fatalf("VMsToDelete = %#v, want no orphan cleanup while Machine is still scaling up", plan.VMsToDelete)
	}
}

func TestBuildPlanDeletesVMsForDeletedMachines(t *testing.T) {
	pool := testPool()

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 0,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent1},
			{Name: testAgent2, Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testFreeVM},
			{Name: testAgent3, Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "bound-vm", Phase: phaseBound},
			{Name: testFreeVM, Phase: phaseReleased, Reason: reasonMachineDeleted, AgentRef: testAgentRef(testAgent2)},
		},
	})

	if len(plan.VMsToDelete) != 1 {
		t.Fatalf("VMsToDelete = %d, want 1", len(plan.VMsToDelete))
	}
	if plan.VMsToDelete[0].Name != testFreeVM {
		t.Fatalf("deleted VM = %s, want %s", plan.VMsToDelete[0].Name, testFreeVM)
	}
	if len(plan.AgentsToDelete) != 1 || plan.AgentsToDelete[0].Name != testAgent2 {
		t.Fatalf("AgentsToDelete = %#v, want Agent paired with deleted VM", plan.AgentsToDelete)
	}
}

func TestBuildPlanRetainsCleanupTargetsWhenCleanupPolicyRetain(t *testing.T) {
	pool := testPool()
	pool.Spec.CleanupPolicy = agentforgev1alpha1.CleanupPolicyRetain

	plan := buildPlan(pool, PoolSnapshot{
		AgentMachines:        1,
		WaitingAgentMachines: 0,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent1},
			{Name: testAgent2, Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testFreeVM},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: testFreeVM, Phase: phaseAvailable, AgentRef: testAgentRef(testAgent2)},
			{Name: "orphaned-vm", Phase: phaseOrphaned},
			{Name: "released-vm", Phase: phaseReleased, Reason: reasonMachineDeleted, AgentRef: testAgentRef(testAgent2)},
		},
	})

	if len(plan.VMsToDelete) != 0 || len(plan.AgentsToDelete) != 0 {
		t.Fatalf("delete targets = VMs %#v Agents %#v, want retained cleanup targets", plan.VMsToDelete, plan.AgentsToDelete)
	}
	if len(plan.Actions) != 1 || plan.Actions[0].Type != actionNoop {
		t.Fatalf("actions = %#v, want Noop when cleanup targets are retained", plan.Actions)
	}
}

func TestBuildPlanDeletesMultipleVMsForDeletedMachines(t *testing.T) {
	pool := testPool()

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 0,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent1},
			{Name: testAgent2, Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent2},
			{Name: testAgent3, Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent3},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: testAgent2, Phase: phaseReleased, Reason: reasonMachineDeleted, AgentRef: testAgentRef(testAgent2)},
			{Name: testAgent3, Phase: phaseReleased, Reason: reasonMachineDeleted, AgentRef: testAgentRef(testAgent3)},
		},
	})

	if len(plan.AgentsToDelete) != 2 {
		t.Fatalf("AgentsToDelete = %d, want 2", len(plan.AgentsToDelete))
	}
	if plan.AgentsToDelete[0].Name != testAgent2 || plan.AgentsToDelete[1].Name != testAgent3 {
		t.Fatalf("AgentsToDelete = %#v, want unbound excess agents", plan.AgentsToDelete)
	}
	if len(plan.VMsToDelete) != 2 {
		t.Fatalf("VMsToDelete = %d, want 2 paired VMs", len(plan.VMsToDelete))
	}
	if len(plan.Actions) != 4 {
		t.Fatalf("actions = %#v, want two VM deletes and two Agent deletes", plan.Actions)
	}
}

func TestBuildPlanDoesNotDeleteReleasedVMsDuringScaleUp(t *testing.T) {
	pool := testPool()

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 2,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent1},
			{Name: testAgent2, Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent2},
			{Name: testAgent3, Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent3},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: testAgent2, Phase: phaseAvailable, AgentRef: testAgentRef(testAgent2)},
			{Name: testAgent3, Phase: phaseReleased, AgentRef: testAgentRef(testAgent3)},
		},
	})

	if len(plan.VMsToDelete) != 0 || len(plan.AgentsToDelete) != 0 {
		t.Fatalf("delete targets = VMs %#v Agents %#v, want no cleanup while Machine is still scaling up", plan.VMsToDelete, plan.AgentsToDelete)
	}
}

func testPool() *agentforgev1alpha1.VsphereAgentPool {
	return &agentforgev1alpha1.VsphereAgentPool{
		Spec: agentforgev1alpha1.VsphereAgentPoolSpec{
			Agent: agentforgev1alpha1.AgentBindingSpec{
				Role: testWorkerRole,
				Labels: map[string]string{
					testCustomerKey: testCustomer,
				},
			},
		},
	}
}

func testAgentRef(name string) *corev1.ObjectReference {
	return &corev1.ObjectReference{Name: name}
}
