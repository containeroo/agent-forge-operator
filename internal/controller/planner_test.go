package controller

import (
	"testing"

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

func TestBuildPlanCreatesVMsForMachineSetDeficit(t *testing.T) {
	pool := testPool(true)
	pool.Spec.Scaling.MaxProvisioning = 2

	plan := buildPlan(pool, PoolSnapshot{
		MachineSetReplicas: 5,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent2, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent3, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
		},
	})

	if plan.DesiredReplicas != 5 {
		t.Fatalf("desired replicas = %d, want 5", plan.DesiredReplicas)
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
		MachineSetReplicas: 3,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent2, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent3, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
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
		MachineSetReplicas: 5,
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

func TestBuildPlanCreatesOnlyRemainingDeficitAfterOwnedProvisioningVMs(t *testing.T) {
	pool := testPool(false)
	pool.Spec.Scaling.MaxProvisioning = 3

	plan := buildPlan(pool, PoolSnapshot{
		MachineSetReplicas: 7,
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
		MachineSetReplicas: 1,
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

func TestBuildPlanDeletesOnlyOwnedUnboundVMs(t *testing.T) {
	pool := testPool(false)
	pool.Spec.Scaling.DeletePolicy = deletePolicyOwnedOnly

	plan := buildPlan(pool, PoolSnapshot{
		MachineSetReplicas: 1,
		MatchingAgents: []AgentInfo{
			{Name: testAgent1, Bound: true, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent2, Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
			{Name: testAgent3, Bound: false, Approved: true, SpecRole: testWorkerRole, RoleLabel: testWorkerRole},
		},
		OwnedVMs: []agentforgev1alpha1.OwnedVMStatus{
			{Name: "bound-vm", Phase: "Bound"},
			{Name: testFreeVM, Phase: testVMAvailable},
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
}

func TestBuildPlanRetainPolicyDoesNotDelete(t *testing.T) {
	pool := testPool(false)
	pool.Spec.Scaling.DeletePolicy = deletePolicyRetain

	plan := buildPlan(pool, PoolSnapshot{
		MachineSetReplicas: 1,
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
