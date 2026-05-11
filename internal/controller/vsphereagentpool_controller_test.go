package controller

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
)

const (
	testNamespace             = "demo"
	testHostedCluster         = "demo"
	testNodePool              = "demo-worker"
	testControlPlaneNamespace = "demo-demo"
	testInfraEnvName          = "demo"
	testAPIVersionKey         = "apiVersion"
	testKindKey               = "kind"
)

var agentHostnamePattern = regexp.MustCompile(`^demo-worker-[a-z0-9]{4}$`)

func TestReconcileDryRunPlansWithoutCallingProvider(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	am1 := testReadyAgentMachine(testControlPlaneNamespace, "agent-1-machine", "demo/demo-worker")
	am2 := testReadyAgentMachine(testControlPlaneNamespace, "agent-2-machine", "demo/demo-worker")
	am3 := testReadyAgentMachine(testControlPlaneNamespace, "agent-3-machine", "demo/demo-worker")
	am4 := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	agent1 := testAgent(testNamespace, "agent-1", true, true)
	agent2 := testAgent(testNamespace, "agent-2", true, true)
	agent3 := testAgent(testNamespace, "agent-3", true, true)
	machine1 := testMachine(testControlPlaneNamespace, "agent-1-machine", "demo/demo-worker", false)
	machine2 := testMachine(testControlPlaneNamespace, "agent-2-machine", "demo/demo-worker", false)
	machine3 := testMachine(testControlPlaneNamespace, "agent-3-machine", "demo/demo-worker", false)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am1, am2, am3, am4, machine1, machine2, machine3, infraEnv, agent1, agent2, agent3).
		WithStatusSubresource(pool).
		Build()

	providerCalled := false
	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			providerCalled = true
			return &fakeVMProvider{}, nil
		},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if providerCalled {
		t.Fatal("dry-run reconcile called vSphere provider")
	}

	var updated agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.DesiredReplicas != 4 {
		t.Fatalf("desired replicas = %d, want 4", updated.Status.DesiredReplicas)
	}
	if updated.Status.AgentMachines != 4 {
		t.Fatalf("agentMachines = %d, want 4", updated.Status.AgentMachines)
	}
	if updated.Status.UnreadyAgentMachines != 1 {
		t.Fatalf("unreadyAgentMachines = %d, want 1", updated.Status.UnreadyAgentMachines)
	}
	if updated.Status.AgentMachinesWithoutAgent != 1 {
		t.Fatalf("agentMachinesWithoutAgent = %d, want 1", updated.Status.AgentMachinesWithoutAgent)
	}
	if updated.Status.MatchingAgents != 3 {
		t.Fatalf("matching agents = %d, want 3", updated.Status.MatchingAgents)
	}
	if len(updated.Status.PlannedActions) != 1 {
		t.Fatalf("planned actions = %d, want 1", len(updated.Status.PlannedActions))
	}
	if updated.Status.PlannedActions[0].Type != actionCreateVM {
		t.Fatalf("first action = %s, want %s", updated.Status.PlannedActions[0].Type, actionCreateVM)
	}
}

func TestReconcileReportsAgentMachineDemandCondition(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, infraEnv).
		WithStatusSubresource(pool).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var updated agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updated); err != nil {
		t.Fatal(err)
	}
	condition := findCondition(updated.Status.Conditions, conditionAgentMachineDemand)
	if condition == nil {
		t.Fatal("AgentMachineDemandFound condition was not set")
	}
	if condition.Status != metav1.ConditionTrue {
		t.Fatalf("AgentMachineDemandFound status = %s, want True", condition.Status)
	}
}

func TestReconcileApplyFailureRequeuesWithoutReturningError(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.Spec.DryRun = false
	am := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "vsphere-credentials"},
		Data: map[string][]byte{
			"server":   []byte("vcenter.example.invalid"),
			"username": []byte("user"),
			"password": []byte("pass"),
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, infraEnv, secret).
		WithStatusSubresource(pool).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return failingVMProvider{}, nil
		},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if result.RequeueAfter != 30*time.Second {
		t.Fatalf("requeueAfter = %s, want 30s", result.RequeueAfter)
	}

	var updated agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updated); err != nil {
		t.Fatal(err)
	}
	condition := findCondition(updated.Status.Conditions, conditionReady)
	if condition == nil {
		t.Fatal("Ready condition was not set")
	}
	if condition.Status != metav1.ConditionFalse {
		t.Fatalf("Ready status = %s, want False", condition.Status)
	}
}

func TestReconcileEnsuresISOOnceForMultipleCreates(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.Spec.DryRun = false
	pool.Spec.Scaling.MaxProvisioning = 2
	am := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")
	am2 := testAgentMachine(testControlPlaneNamespace, testNodePool+"-2", "demo/demo-worker")
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	agent1 := testAgent(testNamespace, "agent-1", true, true)
	agent2 := testAgent(testNamespace, "agent-2", true, true)
	agent3 := testAgent(testNamespace, "agent-3", true, true)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "vsphere-credentials"},
		Data: map[string][]byte{
			"server":   []byte("vcenter.example.invalid"),
			"username": []byte("user"),
			"password": []byte("pass"),
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, am2, infraEnv, agent1, agent2, agent3, secret).
		WithStatusSubresource(pool).
		Build()

	provider := &fakeVMProvider{isoPath: "agent-forge/demo/demo-worker/abc.iso"}
	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return provider, nil
		},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if provider.ensureISOCalls != 1 {
		t.Fatalf("EnsureISO calls = %d, want 1", provider.ensureISOCalls)
	}
	if provider.createVMCalls != 2 {
		t.Fatalf("CreateVM calls = %d, want 2", provider.createVMCalls)
	}
	for _, path := range provider.createISOPaths {
		if path != provider.isoPath {
			t.Fatalf("CreateVM ISO path = %s, want %s", path, provider.isoPath)
		}
	}

	var updated agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.ISO.Path != provider.isoPath {
		t.Fatalf("status ISO path = %s, want %s", updated.Status.ISO.Path, provider.isoPath)
	}
	if updated.Status.ISO.SHA256 != "abc" {
		t.Fatalf("status ISO sha = %s, want abc", updated.Status.ISO.SHA256)
	}
	condition := findCondition(updated.Status.Conditions, conditionISOReady)
	if condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("ISOReady condition = %#v, want True", condition)
	}
}

func TestReconcilePatchesCandidateAgentFromInfraEnv(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.Spec.DryRun = false
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{
		newOwnedVMStatus("demo-worker-ab12"),
	}
	am := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	agent := testCandidateAgent(testNamespace, "abcdef12-3456-7890-abcd-ef1234567890")

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, infraEnv, agent).
		WithStatusSubresource(pool).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var updated unstructured.Unstructured
	updated.SetGroupVersionKind(agentGVK)
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: agent.GetName()}, &updated); err != nil {
		t.Fatal(err)
	}
	approved, _, _ := unstructured.NestedBool(updated.Object, "spec", "approved")
	if !approved {
		t.Fatal("candidate Agent was not approved")
	}
	role, _, _ := unstructured.NestedString(updated.Object, "spec", "role")
	if role != testWorkerRole {
		t.Fatalf("spec.role = %q, want %q", role, testWorkerRole)
	}
	hostname, _, _ := unstructured.NestedString(updated.Object, "spec", "hostname")
	if hostname != "demo-worker-ab12" {
		t.Fatalf("spec.hostname = %q, want matching owned VM name", hostname)
	}
	labels := updated.GetLabels()
	if labels[roleLabelKey] != testWorkerRole {
		t.Fatalf("role label = %q, want %q", labels[roleLabelKey], testWorkerRole)
	}
	if labels[testCustomerKey] != testCustomer {
		t.Fatalf("customer label = %q, want %q", labels[testCustomerKey], testCustomer)
	}

	var updatedPool agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updatedPool); err != nil {
		t.Fatal(err)
	}
	if len(updatedPool.Status.OwnedVMs) != 1 {
		t.Fatalf("ownedVMs = %d, want 1", len(updatedPool.Status.OwnedVMs))
	}
	vm := updatedPool.Status.OwnedVMs[0]
	if vm.Name != "demo-worker-ab12" || vm.Phase != phaseAvailable || vm.AgentRef == nil || vm.AgentRef.Name != agent.GetName() {
		t.Fatalf("owned VM status = %#v, want available VM linked to candidate Agent", vm)
	}
}

func TestReconcileRefreshesOwnedVMBoundStatus(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{
		newOwnedVMStatus("demo-worker-ab12"),
	}
	am := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")
	machine := testMachine(testControlPlaneNamespace, "demo-worker-ab12-machine", "demo/demo-worker", false)
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	agent := testAgent(testNamespace, "demo-worker-ab12", true, true)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, machine, infraEnv, agent).
		WithStatusSubresource(pool).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var updated agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updated); err != nil {
		t.Fatal(err)
	}
	if len(updated.Status.OwnedVMs) != 1 {
		t.Fatalf("ownedVMs = %d, want 1", len(updated.Status.OwnedVMs))
	}
	vm := updated.Status.OwnedVMs[0]
	if vm.Phase != "Bound" || vm.Reason != "AgentBound" {
		t.Fatalf("owned VM phase/reason = %s/%s, want Bound/AgentBound", vm.Phase, vm.Reason)
	}
	if vm.AgentRef == nil || vm.AgentRef.Name != agent.GetName() {
		t.Fatalf("agentRef = %#v, want bound Agent ref", vm.AgentRef)
	}
	if vm.MachineRef == nil || vm.MachineRef.Name != agent.GetName()+"-machine" || vm.MachineRef.Namespace != testControlPlaneNamespace {
		t.Fatalf("machineRef = %#v, want AgentMachine ref in control plane namespace", vm.MachineRef)
	}
}

func TestRefreshOwnedVMStatusesMarksExpiredUndiscoveredVMOrphaned(t *testing.T) {
	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{
		{
			Name:               "demo-worker-old1",
			Phase:              phaseProvisioning,
			Reason:             "AgentNotDiscovered",
			LastTransitionTime: metav1.NewTime(time.Now().Add(-orphanedOwnedVMGracePeriod - time.Minute)),
		},
		{
			Name:               "demo-worker-new1",
			Phase:              phaseProvisioning,
			Reason:             "AgentNotDiscovered",
			LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Minute)),
		},
	}

	vms := refreshOwnedVMStatuses(pool, nil, nil)

	if len(vms) != 2 {
		t.Fatalf("ownedVMs = %d, want 2", len(vms))
	}
	if vms[0].Phase != phaseOrphaned || vms[0].Reason != "AgentDiscoveryExpired" {
		t.Fatalf("expired VM phase/reason = %s/%s, want Orphaned/AgentDiscoveryExpired", vms[0].Phase, vms[0].Reason)
	}
	if vms[1].Phase != phaseProvisioning || vms[1].Reason != "AgentNotDiscovered" {
		t.Fatalf("recent VM phase/reason = %s/%s, want Provisioning/AgentNotDiscovered", vms[1].Phase, vms[1].Reason)
	}
}

func TestRefreshOwnedVMStatusesMarksMissingDiscoveredAgentOrphaned(t *testing.T) {
	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{
		{
			Name:               "demo-worker-gone",
			Phase:              phaseAvailable,
			Reason:             "AgentAvailable",
			AgentRef:           testAgentRef("missing-agent"),
			LastTransitionTime: metav1.Now(),
		},
	}

	vms := refreshOwnedVMStatuses(pool, nil, nil)

	if len(vms) != 1 {
		t.Fatalf("ownedVMs = %d, want 1", len(vms))
	}
	if vms[0].Phase != phaseOrphaned || vms[0].Reason != "AgentMissing" {
		t.Fatalf("VM phase/reason = %s/%s, want Orphaned/AgentMissing", vms[0].Phase, vms[0].Reason)
	}
	if vms[0].AgentRef != nil {
		t.Fatalf("agentRef = %#v, want nil after Agent disappeared", vms[0].AgentRef)
	}
}

func TestRefreshOwnedVMStatusesRequiresObservedMachineDeletingBeforeMachineDeleted(t *testing.T) {
	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{
		{
			Name:       "demo-worker-bound",
			Phase:      phaseBound,
			Reason:     "AgentBound",
			AgentRef:   &corev1.ObjectReference{Name: "bound-agent"},
			MachineRef: testMachineRef("missing-machine"),
		},
		{
			Name:       "demo-worker-deleting",
			Phase:      phaseReleased,
			Reason:     "MachineDeleting",
			AgentRef:   &corev1.ObjectReference{Name: "deleting-agent"},
			MachineRef: testMachineRef("deleted-machine"),
		},
	}

	vms := refreshOwnedVMStatuses(pool, []AgentInfo{
		{Name: "bound-agent", Bound: true, MachineName: "missing-machine", Hostname: "demo-worker-bound"},
		{Name: "deleting-agent", Bound: true, MachineName: "deleted-machine", Hostname: "demo-worker-deleting"},
	}, nil)

	if vms[0].Phase != phaseBound || vms[0].Reason != "AgentBound" {
		t.Fatalf("missing unobserved Machine phase/reason = %s/%s, want Bound/AgentBound", vms[0].Phase, vms[0].Reason)
	}
	if vms[1].Phase != phaseReleased || vms[1].Reason != "MachineDeleted" {
		t.Fatalf("deleted Machine phase/reason = %s/%s, want Released/MachineDeleted", vms[1].Phase, vms[1].Reason)
	}
}

func TestReconcileDoesNotDeleteProvisioningOwnedVMsWithoutDeletedMachine(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.Spec.DryRun = false
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{
		{Name: "demo-worker-one1", Phase: phaseBound, AgentRef: testAgentRef("demo-worker-one1")},
		{Name: "demo-worker-two2", Phase: phaseBound, AgentRef: testAgentRef("demo-worker-two2")},
		{Name: "demo-worker-thr3", Phase: phaseBound, AgentRef: testAgentRef("demo-worker-thr3")},
		{Name: "demo-worker-old1", Phase: phaseProvisioning, Reason: "AgentNotDiscovered", LastTransitionTime: metav1.Now()},
		{Name: "demo-worker-old2", Phase: phaseProvisioning, Reason: "AgentNotDiscovered", LastTransitionTime: metav1.Now()},
	}
	am := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "vsphere-credentials"}}
	agent1 := testAgent(testNamespace, "demo-worker-one1", true, true)
	agent2 := testAgent(testNamespace, "demo-worker-two2", true, true)
	agent3 := testAgent(testNamespace, "demo-worker-thr3", true, true)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, infraEnv, secret, agent1, agent2, agent3).
		WithStatusSubresource(pool).
		Build()

	provider := &fakeVMProvider{}
	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return provider, nil
		},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	if provider.deleteVMCalls != 0 {
		t.Fatalf("DeleteVM calls = %d, want 0 without deleted Machines", provider.deleteVMCalls)
	}

	var updated agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updated); err != nil {
		t.Fatal(err)
	}
	if len(updated.Status.OwnedVMs) != 5 {
		t.Fatalf("ownedVMs = %d, want bound and provisioning VMs retained", len(updated.Status.OwnedVMs))
	}
	condition := findCondition(updated.Status.Conditions, conditionCapacitySatisfied)
	if condition == nil {
		t.Fatal("CapacitySatisfied condition missing")
	}
	if condition.Message != "agentMachines=1 waitingAgentMachines=1 unreadyAgentMachines=1 agentMachinesWithoutAgent=1 matchingAgents=3 pendingOwnedVMs=2 boundAgents=3 availableAgents=0" {
		t.Fatalf("CapacitySatisfied message = %q, want retained pending VMs", condition.Message)
	}
}

func TestReconcileMarksReturnedAgentReleased(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{
		{
			Name:     "demo-worker-ab12",
			Phase:    phaseBound,
			Reason:   "AgentBound",
			AgentRef: &corev1.ObjectReference{Name: "demo-worker-ab12"},
		},
	}
	am := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	agent := testAgent(testNamespace, "demo-worker-ab12", false, true)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, infraEnv, agent).
		WithStatusSubresource(pool).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var updated agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updated); err != nil {
		t.Fatal(err)
	}
	if len(updated.Status.OwnedVMs) != 1 {
		t.Fatalf("ownedVMs = %d, want 1", len(updated.Status.OwnedVMs))
	}
	vm := updated.Status.OwnedVMs[0]
	if vm.Phase != phaseReleased || vm.Reason != "AgentReleased" {
		t.Fatalf("owned VM phase/reason = %s/%s, want Released/AgentReleased", vm.Phase, vm.Reason)
	}
}

func TestReconcileAdoptsExistingBoundAgentAsOwnedVM(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	am := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")
	machine := testMachine(testControlPlaneNamespace, "demo-worker-ab12-machine", "demo/demo-worker", false)
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	agent := testAgent(testNamespace, "demo-worker-ab12", true, true)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, machine, infraEnv, agent).
		WithStatusSubresource(pool).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var updated agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updated); err != nil {
		t.Fatal(err)
	}
	if len(updated.Status.OwnedVMs) != 1 {
		t.Fatalf("ownedVMs = %d, want adopted existing VM", len(updated.Status.OwnedVMs))
	}
	vm := updated.Status.OwnedVMs[0]
	if vm.Name != "demo-worker-ab12" || vm.Phase != "Bound" || vm.AgentRef == nil || vm.AgentRef.Name != agent.GetName() {
		t.Fatalf("owned VM status = %#v, want adopted bound Agent", vm)
	}
}

func TestNormalizeVMwareSerialUUID(t *testing.T) {
	uuid := normalizeVMwareSerialUUID("VMware-42 32 97 c6 d7 2e 28 bb-b2 79 12 09 c2 9a b7 2b")
	if uuid != "423297c6-d72e-28bb-b279-1209c29ab72b" {
		t.Fatalf("uuid = %q, want normalized VMware BIOS UUID", uuid)
	}
}

func TestReconcileAdoptsInventoryHostnameForCandidateAgent(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.Spec.DryRun = false
	am := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	agent := testCandidateAgent(testNamespace, "candidate-agent")
	setAgentInventoryHostname(t, agent, "demo-worker-c3p0")

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, infraEnv, agent).
		WithStatusSubresource(pool).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var updatedAgent unstructured.Unstructured
	updatedAgent.SetGroupVersionKind(agentGVK)
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: agent.GetName()}, &updatedAgent); err != nil {
		t.Fatal(err)
	}
	hostname, _, _ := unstructured.NestedString(updatedAgent.Object, "spec", "hostname")
	if hostname != "demo-worker-c3p0" {
		t.Fatalf("spec.hostname = %q, want adopted inventory hostname", hostname)
	}

	var updatedPool agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updatedPool); err != nil {
		t.Fatal(err)
	}
	if len(updatedPool.Status.OwnedVMs) != 1 {
		t.Fatalf("ownedVMs = %d, want adopted VM", len(updatedPool.Status.OwnedVMs))
	}
	vm := updatedPool.Status.OwnedVMs[0]
	if vm.Name != "demo-worker-c3p0" || vm.Phase != phaseAvailable || vm.AgentRef == nil || vm.AgentRef.Name != agent.GetName() {
		t.Fatalf("owned VM status = %#v, want available adopted VM linked to Agent", vm)
	}
}

func TestRequestsForAgentMachineChangeFindsMatchingPool(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	otherPool := reconcileTestPool()
	otherPool.Name = "other-worker"
	otherPool.Spec.NodePoolRef.Name = "other-worker"
	am := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, otherPool).
		WithIndex(&agentforgev1alpha1.VsphereAgentPool{}, vsphereAgentPoolControlPlaneNamespaceIndex, controlPlaneNamespaceIndexFunc).
		Build()

	reconciler := &VsphereAgentPoolReconciler{Client: k8sClient}
	reqs := reconciler.requestsForControlPlaneObjectChange(ctx, am)
	if len(reqs) != 1 {
		t.Fatalf("requests = %#v, want one request", reqs)
	}
	if reqs[0].NamespacedName != (types.NamespacedName{Namespace: testNamespace, Name: testNodePool}) {
		t.Fatalf("request = %s, want demo/demo-worker", reqs[0].NamespacedName)
	}
}

func TestRequestsForAgentChangeFindsMatchingPool(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	otherPool := reconcileTestPool()
	otherPool.Name = "other-worker"
	otherPool.Spec.InfraEnvRef.Name = "other-infraenv"
	agent := testCandidateAgent(testNamespace, "candidate-agent")

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, otherPool).
		Build()

	reconciler := &VsphereAgentPoolReconciler{Client: k8sClient}
	reqs := reconciler.requestsForAgentChange(ctx, agent)
	if len(reqs) != 1 {
		t.Fatalf("requests = %#v, want one request", reqs)
	}
	if reqs[0].NamespacedName != (types.NamespacedName{Namespace: testNamespace, Name: testNodePool}) {
		t.Fatalf("request = %s, want demo/demo-worker", reqs[0].NamespacedName)
	}
}

func TestReconcileKeepsUnboundAgentsWithoutDeletedMachine(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.Spec.DryRun = false
	am := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	boundAgent := testAgent(testNamespace, "bound-agent", true, true)
	excessAgent1 := testAgent(testNamespace, "excess-agent-1", false, true)
	excessAgent2 := testAgent(testNamespace, "excess-agent-2", false, true)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "vsphere-credentials"},
		Data: map[string][]byte{
			"server":   []byte("vcenter.example.invalid"),
			"username": []byte("user"),
			"password": []byte("pass"),
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, infraEnv, boundAgent, excessAgent1, excessAgent2, secret).
		WithStatusSubresource(pool).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return &fakeVMProvider{}, nil
		},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	for _, name := range []string{excessAgent1.GetName(), excessAgent2.GetName()} {
		var retained unstructured.Unstructured
		retained.SetGroupVersionKind(agentGVK)
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: name}, &retained); err != nil {
			t.Fatalf("unbound Agent %s should be retained without deleted Machine: %v", name, err)
		}
	}
	var retained unstructured.Unstructured
	retained.SetGroupVersionKind(agentGVK)
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: boundAgent.GetName()}, &retained); err != nil {
		t.Fatalf("bound Agent should be retained: %v", err)
	}
}

func TestISOCacheDueDetectsStableURLIntervalAndForceRefresh(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	pool := reconcileTestPool()
	pool.Spec.ISO.CheckInterval.Duration = 10 * time.Minute
	pool.Status.ISO = agentforgev1alpha1.ISOCacheStatus{
		URL:               "https://example.invalid/discovery.iso",
		Path:              "agent-forge/demo/demo-worker/abc.iso",
		SHA256:            "abc",
		CheckedAt:         metav1.NewTime(now.Add(-5 * time.Minute)),
		ForceRefreshToken: "old",
	}

	if isoCacheDue(pool, pool.Status.ISO.URL, "", now) {
		t.Fatal("cache was due before check interval elapsed")
	}
	if !isoCacheDue(pool, pool.Status.ISO.URL, "new", now) {
		t.Fatal("cache was not due for a new force refresh token")
	}
	if !isoCacheDue(pool, pool.Status.ISO.URL, "", now.Add(-5*time.Minute).Add(10*time.Minute)) {
		t.Fatal("cache was not due once check interval elapsed")
	}
	pool.Spec.ISO.PathPrefix = "agent-forge/demo/other"
	if !isoCacheDue(pool, pool.Status.ISO.URL, "", now) {
		t.Fatal("cache was not due after path prefix changed")
	}
}

func TestISOHistoryRetainsNewestAndPrunesStalePaths(t *testing.T) {
	history := []agentforgev1alpha1.ISOCacheHistoryEntry{
		{Path: "cache/old-1.iso", SHA256: "old-1"},
		{Path: "cache/old-2.iso", SHA256: "old-2"},
		{Path: "cache/old-3.iso", SHA256: "old-3"},
	}
	current := agentforgev1alpha1.ISOCacheHistoryEntry{Path: "cache/new.iso", SHA256: "new"}

	updated := updatedISOHistory(history, current, 2)
	if len(updated) != 2 {
		t.Fatalf("history length = %d, want 2", len(updated))
	}
	if updated[0].Path != "cache/new.iso" || updated[1].Path != "cache/old-1.iso" {
		t.Fatalf("history = %#v, want current plus newest old entry", updated)
	}

	stale := staleISOPaths(history, "cache/old-1.iso", "cache/new.iso", 2)
	if len(stale) != 2 || stale[0] != "cache/old-2.iso" || stale[1] != "cache/old-3.iso" {
		t.Fatalf("stale paths = %#v, want old-2 and old-3", stale)
	}
}

type fakeVMProvider struct {
	ensureISOCalls int
	createVMCalls  int
	deleteVMCalls  int
	createISOPaths []string
	deletedVMNames []string
	isoPath        string
}

func (p *fakeVMProvider) EnsureISO(context.Context, *agentforgev1alpha1.VsphereAgentPool, ISOEnsureRequest) (ISOEnsureResult, error) {
	p.ensureISOCalls++
	if p.isoPath == "" {
		p.isoPath = "agent-forge/demo/demo-worker/abc.iso"
	}
	return ISOEnsureResult{Path: p.isoPath, SHA256: "abc", SizeBytes: 3, Uploaded: true}, nil
}

func (p *fakeVMProvider) CreateVM(_ context.Context, pool *agentforgev1alpha1.VsphereAgentPool, req VMCreateRequest) (agentforgev1alpha1.OwnedVMStatus, error) {
	p.createVMCalls++
	p.createISOPaths = append(p.createISOPaths, req.ISOPath)
	return newOwnedVMStatus(desiredAgentHostname(pool)), nil
}

func (p *fakeVMProvider) DeleteVM(_ context.Context, _ *agentforgev1alpha1.VsphereAgentPool, vm agentforgev1alpha1.OwnedVMStatus) error {
	p.deleteVMCalls++
	p.deletedVMNames = append(p.deletedVMNames, vm.Name)
	return nil
}

func (*fakeVMProvider) DeleteISO(context.Context, *agentforgev1alpha1.VsphereAgentPool, string) error {
	return nil
}

type failingVMProvider struct{}

func (failingVMProvider) EnsureISO(context.Context, *agentforgev1alpha1.VsphereAgentPool, ISOEnsureRequest) (ISOEnsureResult, error) {
	return ISOEnsureResult{Path: "agent-forge/demo/demo-worker/abc.iso", SHA256: "abc", SizeBytes: 3, Uploaded: true}, nil
}

func (failingVMProvider) CreateVM(context.Context, *agentforgev1alpha1.VsphereAgentPool, VMCreateRequest) (agentforgev1alpha1.OwnedVMStatus, error) {
	return agentforgev1alpha1.OwnedVMStatus{}, fmt.Errorf("provider failed")
}

func (failingVMProvider) DeleteVM(context.Context, *agentforgev1alpha1.VsphereAgentPool, agentforgev1alpha1.OwnedVMStatus) error {
	return nil
}

func (failingVMProvider) DeleteISO(context.Context, *agentforgev1alpha1.VsphereAgentPool, string) error {
	return nil
}

func reconcileTestPool() *agentforgev1alpha1.VsphereAgentPool {
	return &agentforgev1alpha1.VsphereAgentPool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  testNamespace,
			Name:       testNodePool,
			Generation: 1,
			Finalizers: []string{finalizerName},
		},
		Spec: agentforgev1alpha1.VsphereAgentPoolSpec{
			HostedClusterRef:      agentforgev1alpha1.LocalObjectReference{Name: testHostedCluster},
			NodePoolRef:           agentforgev1alpha1.LocalObjectReference{Name: testNodePool},
			InfraEnvRef:           agentforgev1alpha1.LocalObjectReference{Name: testInfraEnvName},
			ControlPlaneNamespace: testControlPlaneNamespace,
			DryRun:                true,
			VSphere: agentforgev1alpha1.VspherePlacementSpec{
				CredentialsSecretRef: agentforgev1alpha1.SecretReference{Name: "vsphere-credentials"},
				Datacenter:           "dc1",
				DatastoreCluster:     "dsc",
				ISODatastore:         "iso",
				ResourcePool:         "cluster/Resources",
				Network:              "VM Network",
			},
			Template: agentforgev1alpha1.VMTemplateSpec{
				NamePrefix: testNodePool,
				NumCPUs:    4,
				MemoryMiB:  16384,
				DiskGiB:    100,
			},
			Agent: agentforgev1alpha1.AgentBindingSpec{
				Role: testWorkerRole,
				Labels: map[string]string{
					testCustomerKey: testCustomer,
				},
			},
			Scaling: agentforgev1alpha1.ScalingPolicySpec{
				MaxProvisioning: 2,
				DeletePolicy:    deletePolicyOwnedOnly,
			},
		},
	}
}

func testAgentMachine(namespace, name, nodePool string) *unstructured.Unstructured {
	return testAgentMachineWithReadyCondition(namespace, name, nodePool, metav1.ConditionFalse, "NoSuitableAgents")
}

func testReadyAgentMachine(namespace, name, nodePool string) *unstructured.Unstructured {
	return testAgentMachineWithReadyCondition(namespace, name, nodePool, metav1.ConditionTrue, "")
}

func testAgentMachineWithReadyCondition(namespace, name, nodePool string, status metav1.ConditionStatus, reason string) *unstructured.Unstructured {
	condition := map[string]any{"type": conditionReady, "status": string(status)}
	if reason != "" {
		condition["reason"] = reason
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		testAPIVersionKey: "capi-provider.agent-install.openshift.io/v1beta1",
		testKindKey:       "AgentMachine",
		"status": map[string]any{
			"conditions": []any{
				condition,
			},
		},
	}}
	obj.SetGroupVersionKind(agentMachineGVK)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	obj.SetAnnotations(map[string]string{nodePoolAnnotation: nodePool})
	return obj
}

func testMachine(namespace, name, nodePool string, deleting bool) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		testAPIVersionKey: "cluster.x-k8s.io/v1beta1",
		testKindKey:       "Machine",
		"status": map[string]any{
			"phase": "Running",
		},
	}}
	if deleting {
		obj.Object["status"] = map[string]any{"phase": "Deleting"}
	}
	obj.SetGroupVersionKind(machineGVK)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	obj.SetAnnotations(map[string]string{nodePoolAnnotation: nodePool})
	return obj
}

func testInfraEnv(namespace, name, isoURL string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		testAPIVersionKey: "agent-install.openshift.io/v1beta1",
		testKindKey:       "InfraEnv",
		"status": map[string]any{
			"isoDownloadURL": isoURL,
		},
	}}
	obj.SetGroupVersionKind(infraEnvGVK)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	return obj
}

func testAgent(namespace, name string, bound, approved bool) *unstructured.Unstructured {
	labels := map[string]string{
		"infraenvs.agent-install.openshift.io": testInfraEnvName,
		testCustomerKey:                        testCustomer,
		roleLabelKey:                           testWorkerRole,
	}
	if bound {
		labels[agentMachineRefKey] = name + "-machine"
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		testAPIVersionKey: "agent-install.openshift.io/v1beta1",
		testKindKey:       "Agent",
		"spec": map[string]any{
			"approved": approved,
			"hostname": name,
			"role":     testWorkerRole,
		},
	}}
	obj.SetGroupVersionKind(agentGVK)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	obj.SetLabels(labels)
	return obj
}

func testCandidateAgent(namespace, name string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		testAPIVersionKey: "agent-install.openshift.io/v1beta1",
		testKindKey:       "Agent",
		"spec": map[string]any{
			"approved": false,
			"role":     "",
		},
	}}
	obj.SetGroupVersionKind(agentGVK)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	obj.SetLabels(map[string]string{
		"infraenvs.agent-install.openshift.io": testInfraEnvName,
		testCustomerKey:                        testCustomer,
	})
	return obj
}

func setAgentInventoryHostname(t *testing.T, agent *unstructured.Unstructured, hostname string) {
	t.Helper()
	if err := unstructured.SetNestedField(agent.Object, hostname, "status", "inventory", "hostname"); err != nil {
		t.Fatal(err)
	}
}

func findCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

func testMachineRef(name string) *corev1.ObjectReference {
	return &corev1.ObjectReference{Name: name}
}

func controlPlaneNamespaceIndexFunc(o client.Object) []string {
	pool := o.(*agentforgev1alpha1.VsphereAgentPool)
	if pool.Spec.ControlPlaneNamespace == "" {
		return nil
	}
	return []string{pool.Spec.ControlPlaneNamespace}
}
