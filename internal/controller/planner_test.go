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
	pool := testPool(true)
	pool.Spec.Scaling.MaxProvisioning = 2

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
	if plan.VMsToCreate != 2 {
		t.Fatalf("VMsToCreate = %d, want throttle-limited 2", plan.VMsToCreate)
	}
	if len(plan.Actions) != 2 {
		t.Fatalf("actions = %d, want 2", len(plan.Actions))
	}
	for _, action := range plan.Actions {
		if action.Type != actionCreateVM {
			t.Fatalf("action type = %s, want %s", action.Type, actionCreateVM)
		}
		if !action.DryRun {
			t.Fatal("dry-run action was not marked DryRun=true")
		}
	}
}

func TestBuildPlanIncludesBufferAgents(t *testing.T) {
	pool := testPool(true)
	pool.Spec.Scaling.BufferAgents = 1
	pool.Spec.Scaling.MaxProvisioning = 3

	plan := buildPlan(pool, PoolSnapshot{
		AgentMachines:        3,
		WaitingAgentMachines: 0,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent1},
			{Name: testAgent2, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent2},
			{Name: testAgent3, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent3},
		},
	})

	if plan.DesiredReplicas != 4 {
		t.Fatalf("desired replicas = %d, want 4", plan.DesiredReplicas)
	}
	if plan.VMsToCreate != 1 {
		t.Fatalf("VMsToCreate = %d, want 1 buffer VM", plan.VMsToCreate)
	}
}

func TestBuildPlanCountsOwnedProvisioningVMsAsPendingCapacity(t *testing.T) {
	pool := testPool(false)
	pool.Spec.Scaling.MaxProvisioning = 3

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 2,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent2, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent3, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "pending-vm-1", Phase: "Provisioning"},
			{Name: "pending-vm-2", Phase: "Provisioning"},
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

func TestBuildPlanDeletesOrphanedOwnedVMsWithoutExcessAgents(t *testing.T) {
	pool := testPool(false)
	pool.Spec.Scaling.DeletePolicy = deletePolicyOwnedOnly

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
	pool := testPool(false)
	pool.Spec.Scaling.DeletePolicy = deletePolicyOwnedOnly

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
	pool := testPool(false)
	pool.Spec.Scaling.DeletePolicy = deletePolicyOwnedOnly

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
	pool := testPool(false)
	pool.Spec.Scaling.DeletePolicy = deletePolicyOwnedOnly

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

func TestBuildPlanDeletesSurplusAvailableAgentsWhileAssignedAgentMachinesAreBinding(t *testing.T) {
	pool := testPool(false)
	pool.Spec.Scaling.DeletePolicy = deletePolicyOwnedOnly

	plan := buildPlan(pool, PoolSnapshot{
		AgentMachines:             6,
		WaitingAgentMachines:      0,
		UnreadyAgentMachines:      3,
		AgentMachinesWithoutAgent: 0,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent1},
			{Name: testAgent2, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent2},
			{Name: testAgent3, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent3},
			{Name: "binding-agent-1", Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: "binding-vm-1"},
			{Name: "binding-agent-2", Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: "binding-vm-2"},
			{Name: "binding-agent-3", Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: "binding-vm-3"},
			{Name: "extra-agent-1", Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: "extra-vm-1"},
			{Name: "extra-agent-2", Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: "extra-vm-2"},
			{Name: "extra-agent-3", Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: "extra-vm-3"},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "extra-vm-1", Phase: phaseAvailable, AgentRef: testAgentRef("extra-agent-1")},
			{Name: "extra-vm-2", Phase: phaseAvailable, AgentRef: testAgentRef("extra-agent-2")},
			{Name: "extra-vm-3", Phase: phaseAvailable, AgentRef: testAgentRef("extra-agent-3")},
		},
	})

	if len(plan.VMsToDelete) != 3 || len(plan.AgentsToDelete) != 3 {
		t.Fatalf("delete targets = VMs %#v Agents %#v, want three surplus unbound Agents deleted", plan.VMsToDelete, plan.AgentsToDelete)
	}
}

func TestBuildPlanKeepsConfiguredBufferAgents(t *testing.T) {
	pool := testPool(false)
	pool.Spec.Scaling.DeletePolicy = deletePolicyOwnedOnly
	pool.Spec.Scaling.BufferAgents = 1

	plan := buildPlan(pool, PoolSnapshot{
		AgentMachines:        3,
		WaitingAgentMachines: 0,
		UnreadyAgentMachines: 0,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent1},
			{Name: testAgent2, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent2},
			{Name: testAgent3, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent3},
			{Name: "buffer-agent", Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: "buffer-vm"},
			{Name: "extra-agent", Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: "extra-vm"},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "buffer-vm", Phase: phaseAvailable, AgentRef: testAgentRef("buffer-agent")},
			{Name: "extra-vm", Phase: phaseAvailable, AgentRef: testAgentRef("extra-agent")},
		},
	})

	if len(plan.VMsToDelete) != 1 {
		t.Fatalf("VMsToDelete = %#v, want exactly one surplus VM after retaining one buffer", plan.VMsToDelete)
	}
}

func TestBuildPlanDoesNotDeleteProvisioningVMsWhilePatchingNewAgents(t *testing.T) {
	pool := testPool(false)
	pool.Spec.Scaling.DeletePolicy = deletePolicyOwnedOnly

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 4,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent1},
			{Name: testAgent2, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent2},
			{Name: testAgent3, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent3},
			{Name: "new-agent-1", Bound: false, Approved: false, SpecRole: "", RoleLabel: testWorkerRole},
			{Name: "new-agent-2", Bound: false, Approved: false, SpecRole: "", RoleLabel: testWorkerRole},
			{Name: "new-agent-3", Bound: false, Approved: false, SpecRole: "", RoleLabel: testWorkerRole},
			{Name: "new-agent-4", Bound: false, Approved: false, SpecRole: "", RoleLabel: testWorkerRole},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "pending-vm-1", Phase: phaseProvisioning},
			{Name: "pending-vm-2", Phase: phaseProvisioning},
			{Name: "pending-vm-3", Phase: phaseProvisioning},
			{Name: "pending-vm-4", Phase: phaseProvisioning},
		},
	})

	if len(plan.AgentsToPatch) != 4 {
		t.Fatalf("AgentsToPatch = %d, want 4 new Agents", len(plan.AgentsToPatch))
	}
	if len(plan.VMsToDelete) != 0 {
		t.Fatalf("VMsToDelete = %#v, want no VM deletes while new Agents still need their VMs", plan.VMsToDelete)
	}
}

func TestBuildPlanDoesNotDeleteUnboundAgentsDuringScaleUp(t *testing.T) {
	pool := testPool(false)
	pool.Spec.Scaling.DeletePolicy = deletePolicyOwnedOnly

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
	pool := testPool(false)
	pool.Spec.Scaling.DeletePolicy = deletePolicyOwnedOnly

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

func TestBuildPlanCreatesOnlyRemainingDeficitAfterOwnedProvisioningVMs(t *testing.T) {
	pool := testPool(false)
	pool.Spec.Scaling.MaxProvisioning = 3

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 4,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent2, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent3, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "pending-vm-1", Phase: "Provisioning"},
			{Name: "pending-vm-2", Phase: "Provisioning"},
			{Name: "bound-vm", Phase: "Bound"},
		},
	})

	if plan.VMsToCreate != 2 {
		t.Fatalf("VMsToCreate = %d, want remaining deficit 2", plan.VMsToCreate)
	}
}

func TestBuildPlanPatchesUnapprovedAgents(t *testing.T) {
	pool := testPool(true)

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 1,
		MatchingAgents: []AgentInfo{
			{Name: "agent-1", Bound: false, Approved: false, RoleLabel: ""},
		},
	})

	if len(plan.Actions) != 1 {
		t.Fatalf("actions = %d, want 1", len(plan.Actions))
	}
	if plan.Actions[0].Type != actionPatchAgent {
		t.Fatalf("action type = %s, want %s", plan.Actions[0].Type, actionPatchAgent)
	}
	if plan.Actions[0].Name != "agent-1" {
		t.Fatalf("action name = %s, want agent-1", plan.Actions[0].Name)
	}
}

func TestBuildPlanDeletesVMsForDeletedMachines(t *testing.T) {
	pool := testPool(false)
	pool.Spec.Scaling.DeletePolicy = deletePolicyOwnedOnly

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 0,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent2, Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent3, Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "bound-vm", Phase: "Bound"},
			{Name: testFreeVM, Phase: phaseReleased, Reason: "MachineDeleted", AgentRef: testAgentRef(testAgent2)},
		},
	})

	if len(plan.VMsToDelete) != 1 {
		t.Fatalf("VMsToDelete = %d, want 1", len(plan.VMsToDelete))
	}
	if plan.VMsToDelete[0].Name != testFreeVM {
		t.Fatalf("deleted VM = %s, want %s", plan.VMsToDelete[0].Name, testFreeVM)
	}
	if plan.Actions[0].DryRun {
		t.Fatal("non-dry-run plan marked delete action as dry-run")
	}
	if len(plan.AgentsToDelete) != 1 || plan.AgentsToDelete[0].Name != testAgent2 {
		t.Fatalf("AgentsToDelete = %#v, want Agent paired with deleted VM", plan.AgentsToDelete)
	}
}

func TestBuildPlanDeletesMultipleVMsForDeletedMachines(t *testing.T) {
	pool := testPool(false)
	pool.Spec.Scaling.DeletePolicy = deletePolicyOwnedOnly

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 0,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent1},
			{Name: testAgent2, Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent2},
			{Name: testAgent3, Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole, Hostname: testAgent3},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: testAgent2, Phase: phaseReleased, Reason: "MachineDeleted", AgentRef: testAgentRef(testAgent2)},
			{Name: testAgent3, Phase: phaseReleased, Reason: "MachineDeleted", AgentRef: testAgentRef(testAgent3)},
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
	pool := testPool(false)
	pool.Spec.Scaling.DeletePolicy = deletePolicyOwnedOnly

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

func TestBuildPlanRetainPolicyDoesNotDelete(t *testing.T) {
	pool := testPool(false)
	pool.Spec.Scaling.DeletePolicy = deletePolicyRetain

	plan := buildPlan(pool, PoolSnapshot{
		WaitingAgentMachines: 1,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent2, Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{{Name: testFreeVM, Phase: testVMAvailable}},
	})

	if len(plan.VMsToDelete) != 0 {
		t.Fatalf("VMsToDelete = %d, want 0 with Retain policy", len(plan.VMsToDelete))
	}
}

func testPool(dryRun bool) *agentforgev1alpha1.VsphereAgentPool {
	return &agentforgev1alpha1.VsphereAgentPool{
		Spec: agentforgev1alpha1.VsphereAgentPoolSpec{
			DryRun: dryRun,
			Agent: agentforgev1alpha1.AgentBindingSpec{
				Role: testWorkerRole,
				Labels: map[string]string{
					testCustomerKey: testCustomer,
				},
			},
			Scaling: agentforgev1alpha1.ScalingPolicySpec{
				MaxProvisioning: 3,
				DeletePolicy:    deletePolicyOwnedOnly,
			},
		},
	}
}

func testAgentRef(name string) *corev1.ObjectReference {
	return &corev1.ObjectReference{Name: name}
}
