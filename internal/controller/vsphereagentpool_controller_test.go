//nolint:goconst,unparam
package controller

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"

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
	testAdoptedVM             = "demo-worker-adopted"
)

var agentHostnamePattern = regexp.MustCompile(`^demo-worker-[a-z0-9]{4}$`)

func TestReconcilePlansWithoutCallingProvider(t *testing.T) {
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
		Recorder: events.NewFakeRecorder(10),
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
		t.Fatal("pool reconcile called vSphere provider")
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
		Recorder: events.NewFakeRecorder(10),
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

func TestReconcileMarksReadyFalseWhenInfraEnvUnavailable(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.Status.Conditions = []metav1.Condition{{
		Type:               conditionReady,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: pool.Generation,
		Reason:             "PreviouslyReady",
		Message:            "stale condition",
	}}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool).
		WithStatusSubresource(pool).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: events.NewFakeRecorder(10),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var updated agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updated); err != nil {
		t.Fatal(err)
	}
	ready := findCondition(updated.Status.Conditions, conditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "InfraEnvUnavailable" {
		t.Fatalf("Ready condition = %#v, want InfraEnvUnavailable False", ready)
	}
	infraEnv := findCondition(updated.Status.Conditions, conditionInfraEnvAvailable)
	if infraEnv == nil || infraEnv.Status != metav1.ConditionFalse {
		t.Fatalf("InfraEnvAvailable condition = %#v, want False", infraEnv)
	}
}

func TestPoolDeleteWaitsForVsphereAgentFinalizers(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	agent := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  testNamespace,
			Name:       "demo-worker-agent",
			Finalizers: []string{vsphereAgentFinalizerName},
			Labels: map[string]string{
				vsphereAgentPoolNameLabel: testNodePool,
			},
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: testNodePool},
		},
		Status: agentforgev1alpha1.VsphereAgentStatus{
			VM: newOwnedVMStatus("demo-worker-agent"),
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, agent).
		WithStatusSubresource(pool, agent).
		Build()

	provider := &fakeVMProvider{}
	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: events.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return provider, nil
		},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if result.RequeueAfter != 10*time.Second {
		t.Fatalf("requeueAfter = %s, want 10s while child finalizer runs", result.RequeueAfter)
	}
	if provider.deleteVMCalls != 0 {
		t.Fatalf("DeleteVM calls = %d, want child VsphereAgent to delete VM", provider.deleteVMCalls)
	}

	var updatedPool agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updatedPool); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&updatedPool, finalizerName) {
		t.Fatalf("pool finalizers = %#v, want pool finalizer retained until child is gone", updatedPool.Finalizers)
	}
	var deletingAgent agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: agent.Name}, &deletingAgent); err != nil {
		t.Fatal(err)
	}
	if deletingAgent.GetDeletionTimestamp() == nil {
		t.Fatal("child VsphereAgent was not marked for deletion")
	}
}

func TestPoolDeleteCleansUpLegacyStatusVMsAfterChildrenGone(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{newOwnedVMStatus("legacy-vm")}
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
		WithObjects(pool, secret).
		WithStatusSubresource(pool).
		Build()

	provider := &fakeVMProvider{}
	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: events.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return provider, nil
		},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if provider.deleteVMCalls != 1 || provider.deletedVMNames[0] != "legacy-vm" {
		t.Fatalf("deleted VMs = %#v, want legacy-vm", provider.deletedVMNames)
	}
	var updatedPool agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updatedPool); err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&updatedPool, finalizerName) {
		t.Fatalf("pool finalizers = %#v, want pool finalizer removed after legacy VM cleanup", updatedPool.Finalizers)
	}
}

func TestPoolDeleteRetainsLegacyStatusVMsWhenCleanupPolicyRetain(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.Spec.CleanupPolicy = agentforgev1alpha1.CleanupPolicyRetain
	pool.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{newOwnedVMStatus("legacy-vm")}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool).
		WithStatusSubresource(pool).
		Build()

	provider := &fakeVMProvider{}
	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: events.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return provider, nil
		},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if provider.deleteVMCalls != 0 {
		t.Fatalf("DeleteVM calls = %d, want retained VM", provider.deleteVMCalls)
	}
	var updatedPool agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updatedPool); err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&updatedPool, finalizerName) {
		t.Fatalf("pool finalizers = %#v, want pool finalizer removed after retaining legacy VM", updatedPool.Finalizers)
	}
}

func TestPoolStatusUpdatePreservesISOStatus(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	current := reconcileTestPool()
	current.Status.ISO = agentforgev1alpha1.ISOCacheStatus{
		URL:    "https://example.invalid/discovery.iso",
		Path:   "agent-forge/demo/demo-worker/abc.iso",
		SHA256: "abc",
	}
	stale := current.DeepCopy()
	stale.Status.ISO = agentforgev1alpha1.ISOCacheStatus{}
	setPlanConditions(stale, PoolPlan{AgentMachines: 1}, "")

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(current).
		WithStatusSubresource(current).
		Build()
	reconciler := &VsphereAgentPoolReconciler{Client: k8sClient}

	if err := reconciler.updateStatus(ctx, stale, PoolPlan{AgentMachines: 1, DesiredReplicas: 1}); err != nil {
		t.Fatalf("updateStatus returned error: %v", err)
	}

	var updated agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.ISO.Path != current.Status.ISO.Path || updated.Status.ISO.SHA256 != current.Status.ISO.SHA256 {
		t.Fatalf("ISO status = %#v, want preserved current ISO status", updated.Status.ISO)
	}
}

func TestPoolPatchFinalizerPreservesForeignFinalizers(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.Finalizers = []string{finalizerName, "example.com/other-finalizer"}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool).
		Build()
	reconciler := &VsphereAgentPoolReconciler{Client: k8sClient}

	desired := pool.DeepCopy()
	controllerutil.RemoveFinalizer(desired, finalizerName)
	if err := reconciler.patchFinalizer(ctx, desired); err != nil {
		t.Fatalf("patchFinalizer returned error: %v", err)
	}

	var updated agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updated); err != nil {
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&updated, finalizerName) {
		t.Fatalf("finalizers = %#v, want managed finalizer removed", updated.Finalizers)
	}
	if !controllerutil.ContainsFinalizer(&updated, "example.com/other-finalizer") {
		t.Fatalf("finalizers = %#v, want foreign finalizer preserved", updated.Finalizers)
	}
}

func TestVsphereAgentReconcileRetainsVMWhenCleanupPolicyRetain(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.Spec.CleanupPolicy = agentforgev1alpha1.CleanupPolicyRetain
	agent := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         testNamespace,
			Name:              "demo-worker-retained",
			Finalizers:        []string{vsphereAgentFinalizerName},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
			Labels: map[string]string{
				vsphereAgentPoolNameLabel: testNodePool,
			},
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: testNodePool},
		},
		Status: agentforgev1alpha1.VsphereAgentStatus{
			VM: newOwnedVMStatus("demo-worker-retained"),
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, agent).
		WithStatusSubresource(pool, agent).
		Build()

	provider := &fakeVMProvider{}
	reconciler := &VsphereAgentReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: events.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return provider, nil
		},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: agent.Name}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if provider.deleteVMCalls != 0 {
		t.Fatalf("DeleteVM calls = %d, want retained VM", provider.deleteVMCalls)
	}
	var updated agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: agent.Name}, &updated); err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&updated, vsphereAgentFinalizerName) {
		t.Fatalf("finalizers = %#v, want finalizer removed without VM deletion", updated.Finalizers)
	}
}

func TestReconcileCreatesVsphereAgentInsteadOfCallingProvider(t *testing.T) {
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

	providerCalled := false
	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: events.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			providerCalled = true
			return failingVMProvider{}, nil
		},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if providerCalled {
		t.Fatal("pool reconcile called vSphere provider")
	}
	if result.RequeueAfter != time.Minute {
		t.Fatalf("requeueAfter = %s, want 1m", result.RequeueAfter)
	}

	var vsphereAgents agentforgev1alpha1.VsphereAgentList
	if err := k8sClient.List(ctx, &vsphereAgents, client.InNamespace(testNamespace), client.MatchingLabels{vsphereAgentPoolNameLabel: testNodePool}); err != nil {
		t.Fatal(err)
	}
	if len(vsphereAgents.Items) != 0 {
		t.Fatalf("VsphereAgents = %d, want 0 because AgentMachine controller owns demand creation", len(vsphereAgents.Items))
	}
}

func TestReconcileCreatesVsphereAgentsForMultipleCreates(t *testing.T) {
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
		Recorder: events.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return provider, nil
		},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if provider.ensureISOCalls != 0 {
		t.Fatalf("EnsureISO calls = %d, want 0 from pool reconcile", provider.ensureISOCalls)
	}
	if provider.createVMCalls != 0 {
		t.Fatalf("CreateVM calls = %d, want 0 from pool reconcile", provider.createVMCalls)
	}

	var vsphereAgents agentforgev1alpha1.VsphereAgentList
	if err := k8sClient.List(ctx, &vsphereAgents, client.InNamespace(testNamespace), client.MatchingLabels{vsphereAgentPoolNameLabel: testNodePool}); err != nil {
		t.Fatal(err)
	}
	var adoptedAgents int
	for _, agent := range vsphereAgents.Items {
		if agent.Labels[vsphereAgentCreatedForLabel] == vsphereAgentCreatedForAdopted {
			adoptedAgents++
		}
	}
	if adoptedAgents != 3 {
		t.Fatalf("adopted VsphereAgents = %d, want 3", adoptedAgents)
	}
}

func TestAgentMachineReconcileCreatesVsphereAgentForNoSuitableAgents(t *testing.T) {
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
	am.SetUID(types.UID("agent-machine-uid"))

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am).
		WithIndex(&agentforgev1alpha1.VsphereAgentPool{}, vsphereAgentPoolControlPlaneNamespaceIndex, controlPlaneNamespaceIndexFunc).
		Build()

	reconciler := &AgentMachineReconciler{Client: k8sClient, Scheme: scheme}
	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testControlPlaneNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var vsphereAgents agentforgev1alpha1.VsphereAgentList
	if err := k8sClient.List(ctx, &vsphereAgents, client.InNamespace(testNamespace), client.MatchingLabels{vsphereAgentPoolNameLabel: testNodePool}); err != nil {
		t.Fatal(err)
	}
	if len(vsphereAgents.Items) != 1 {
		t.Fatalf("VsphereAgents = %d, want 1", len(vsphereAgents.Items))
	}
	created := vsphereAgents.Items[0]
	if created.Spec.PoolRef.Name != testNodePool {
		t.Fatalf("poolRef = %q, want %q", created.Spec.PoolRef.Name, testNodePool)
	}
	if !agentHostnamePattern.MatchString(created.Name) {
		t.Fatalf("VsphereAgent name = %q, want VM-style name with pool prefix and 4-character suffix", created.Name)
	}

	_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testControlPlaneNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("second reconcile returned error: %v", err)
	}
	if err := k8sClient.List(ctx, &vsphereAgents, client.InNamespace(testNamespace), client.MatchingLabels{vsphereAgentPoolNameLabel: testNodePool}); err != nil {
		t.Fatal(err)
	}
	if len(vsphereAgents.Items) != 1 {
		t.Fatalf("VsphereAgents after second reconcile = %d, want 1", len(vsphereAgents.Items))
	}
}

func TestReconcileAdoptsExistingMatchingAgents(t *testing.T) {
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
	agent := testAgent(testNamespace, testAdoptedVM, false, true)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, infraEnv, agent).
		WithStatusSubresource(pool).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: events.NewFakeRecorder(10),
	}
	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var adopted agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testAdoptedVM}, &adopted); err != nil {
		t.Fatal(err)
	}
	if adopted.Spec.PoolRef.Name != testNodePool {
		t.Fatalf("poolRef = %q, want %q", adopted.Spec.PoolRef.Name, testNodePool)
	}
	if adopted.Labels[vsphereAgentCreatedForLabel] != vsphereAgentCreatedForAdopted {
		t.Fatalf("created-for label = %q, want adopted", adopted.Labels[vsphereAgentCreatedForLabel])
	}
	if adopted.Status.VM.Name != testAdoptedVM || adopted.Status.VM.AgentRef == nil || adopted.Status.VM.AgentRef.Name != testAdoptedVM {
		t.Fatalf("adopted VM status = %#v, want VM linked to existing Agent", adopted.Status.VM)
	}
}

func TestReconcileCorrectsAdoptedVsphereAgentStatus(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	adopted := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testAdoptedVM,
			Labels: map[string]string{
				vsphereAgentPoolNameLabel:   testNodePool,
				vsphereAgentCreatedForLabel: vsphereAgentCreatedForAdopted,
			},
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: testNodePool},
		},
		Status: agentforgev1alpha1.VsphereAgentStatus{
			VM: agentforgev1alpha1.OwnedVMStatus{
				Name:       "wrong-new-vm",
				Phase:      phaseProvisioning,
				Reason:     reasonVMCreateRequested,
				BIOSUUID:   "wrong-bios",
				MACAddress: "00-50-56-aa-bb-cc",
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, adopted).
		WithStatusSubresource(adopted).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: events.NewFakeRecorder(10),
	}
	err := reconciler.adoptMatchingAgents(ctx, pool, []AgentInfo{{
		Name:     testAdoptedVM,
		Hostname: testAdoptedVM,
		MAC:      "00-50-56-dd-ee-ff",
		BIOSUUID: "adopted-bios",
	}})
	if err != nil {
		t.Fatalf("adoptMatchingAgents returned error: %v", err)
	}

	var updated agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testAdoptedVM}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.VM.Name != testAdoptedVM || updated.Status.VM.AgentRef == nil || updated.Status.VM.AgentRef.Name != testAdoptedVM {
		t.Fatalf("adopted VM status = %#v, want corrected existing Agent VM", updated.Status.VM)
	}
	if updated.Status.VM.BIOSUUID != "adopted-bios" || updated.Status.VM.MACAddress != "00-50-56-dd-ee-ff" {
		t.Fatalf("adopted VM identity = %#v, want corrected identity", updated.Status.VM)
	}
}

func TestAdoptMatchingAgentsDoesNotOverwriteConflictingDemandVMByHostname(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	demandAgent := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testAdoptedVM,
			Labels: map[string]string{
				vsphereAgentPoolNameLabel:   testNodePool,
				vsphereAgentCreatedForLabel: vsphereAgentCreatedForDemand,
			},
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: testNodePool},
		},
		Status: agentforgev1alpha1.VsphereAgentStatus{
			VM: agentforgev1alpha1.OwnedVMStatus{
				Name:       testAdoptedVM,
				Phase:      phaseProvisioning,
				Reason:     reasonVMCreateRequested,
				BIOSUUID:   "existing-vm-bios",
				MACAddress: "00-50-56-aa-bb-cc",
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, demandAgent).
		WithStatusSubresource(demandAgent).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: events.NewFakeRecorder(10),
	}
	err := reconciler.adoptMatchingAgents(ctx, pool, []AgentInfo{{
		Name:     "assisted-agent",
		Bound:    true,
		Hostname: testAdoptedVM,
		MAC:      "00-50-56-dd-ee-ff",
		BIOSUUID: "different-agent-bios",
	}})
	if err != nil {
		t.Fatalf("adoptMatchingAgents returned error: %v", err)
	}

	var updated agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testAdoptedVM}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.VM.BIOSUUID != "existing-vm-bios" || updated.Status.VM.AgentRef != nil {
		t.Fatalf("VM status = %#v, want conflicting demand VM status left unchanged", updated.Status.VM)
	}
}

func TestReconcileReusesExistingVsphereAgentForDiscoveredVM(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	demandAgent := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      "demo-worker-legacy",
			Labels: map[string]string{
				vsphereAgentPoolNameLabel:   testNodePool,
				vsphereAgentCreatedForLabel: vsphereAgentCreatedForDemand,
			},
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: testNodePool},
		},
		Status: agentforgev1alpha1.VsphereAgentStatus{
			VM: agentforgev1alpha1.OwnedVMStatus{
				Name:       testAdoptedVM,
				Phase:      phaseProvisioning,
				Reason:     reasonVMCreateRequested,
				BIOSUUID:   "vm-bios",
				MACAddress: "00-50-56-aa-bb-cc",
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, demandAgent).
		WithStatusSubresource(demandAgent).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: events.NewFakeRecorder(10),
	}
	err := reconciler.adoptMatchingAgents(ctx, pool, []AgentInfo{{
		Name:     "assisted-agent",
		Bound:    true,
		Hostname: testAdoptedVM,
		MAC:      "00-50-56-aa-bb-cc",
		BIOSUUID: "vm-bios",
	}})
	if err != nil {
		t.Fatalf("adoptMatchingAgents returned error: %v", err)
	}

	var vsphereAgents agentforgev1alpha1.VsphereAgentList
	if err := k8sClient.List(ctx, &vsphereAgents, client.InNamespace(testNamespace), client.MatchingLabels{vsphereAgentPoolNameLabel: testNodePool}); err != nil {
		t.Fatal(err)
	}
	if len(vsphereAgents.Items) != 1 {
		t.Fatalf("VsphereAgents = %d, want existing object reused without duplicate", len(vsphereAgents.Items))
	}
	var updated agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "demo-worker-legacy"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.VM.Phase != phaseBound || updated.Status.VM.AgentRef == nil || updated.Status.VM.AgentRef.Name != "assisted-agent" {
		t.Fatalf("VM status = %#v, want existing object updated to bound discovered Agent", updated.Status.VM)
	}
}

func TestReconcileDeletesDuplicateVsphereAgentForDiscoveredVM(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	namedAgent := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testAdoptedVM,
			Labels: map[string]string{
				vsphereAgentPoolNameLabel:   testNodePool,
				vsphereAgentCreatedForLabel: vsphereAgentCreatedForAdopted,
			},
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: testNodePool},
		},
		Status: agentforgev1alpha1.VsphereAgentStatus{
			VM: agentforgev1alpha1.OwnedVMStatus{
				Name:       testAdoptedVM,
				Phase:      phaseBound,
				Reason:     "AgentBound",
				BIOSUUID:   "vm-bios",
				MACAddress: "00-50-56-aa-bb-cc",
				AgentRef:   agentObjectReference(pool, "assisted-agent"),
			},
		},
	}
	duplicateAgent := namedAgent.DeepCopy()
	duplicateAgent.Name = "demo-worker-legacy"
	duplicateAgent.Labels = map[string]string{
		vsphereAgentPoolNameLabel:   testNodePool,
		vsphereAgentCreatedForLabel: vsphereAgentCreatedForDemand,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, namedAgent, duplicateAgent).
		WithStatusSubresource(namedAgent, duplicateAgent).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: events.NewFakeRecorder(10),
	}
	err := reconciler.adoptMatchingAgents(ctx, pool, []AgentInfo{{
		Name:     "assisted-agent",
		Bound:    true,
		Hostname: testAdoptedVM,
		MAC:      "00-50-56-aa-bb-cc",
		BIOSUUID: "vm-bios",
	}})
	if err != nil {
		t.Fatalf("adoptMatchingAgents returned error: %v", err)
	}

	var vsphereAgents agentforgev1alpha1.VsphereAgentList
	if err := k8sClient.List(ctx, &vsphereAgents, client.InNamespace(testNamespace), client.MatchingLabels{vsphereAgentPoolNameLabel: testNodePool}); err != nil {
		t.Fatal(err)
	}
	if len(vsphereAgents.Items) != 1 || vsphereAgents.Items[0].Name != testAdoptedVM {
		t.Fatalf("VsphereAgents = %#v, want only named VM object retained", vsphereAgents.Items)
	}
}

func TestVsphereAgentReconcileCreatesVM(t *testing.T) {
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
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "vsphere-credentials"},
		Data: map[string][]byte{
			"server":   []byte("vcenter.example.invalid"),
			"username": []byte("user"),
			"password": []byte("pass"),
		},
	}
	vsphereAgent := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  testNamespace,
			Name:       "demo-worker-agent",
			Finalizers: []string{vsphereAgentFinalizerName},
			Labels: map[string]string{
				vsphereAgentPoolNameLabel: testNodePool,
			},
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: testNodePool},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, infraEnv, secret, vsphereAgent).
		WithStatusSubresource(pool, vsphereAgent).
		Build()

	provider := &fakeVMProvider{isoPath: "agent-forge/demo/demo-worker/abc.iso"}
	reconciler := &VsphereAgentReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: events.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return provider, nil
		},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: "demo-worker-agent"}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if provider.ensureISOCalls != 1 {
		t.Fatalf("EnsureISO calls = %d, want 1", provider.ensureISOCalls)
	}
	if provider.createVMCalls != 1 {
		t.Fatalf("CreateVM calls = %d, want 1", provider.createVMCalls)
	}

	var updated agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "demo-worker-agent"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.VM.Name != "demo-worker-agent" || updated.Status.VM.Phase != phaseProvisioning {
		t.Fatalf("VM status = %#v, want created provisioning VM named after VsphereAgent", updated.Status.VM)
	}
}

func TestVsphereAgentReconcileSkipsVMDeleteForDuplicate(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "vsphere-credentials"},
		Data: map[string][]byte{
			"server":   []byte("vcenter.example.invalid"),
			"username": []byte("user"),
			"password": []byte("pass"),
		},
	}
	duplicate := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         testNamespace,
			Name:              "demo-worker-legacy",
			Finalizers:        []string{vsphereAgentFinalizerName},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
			Labels: map[string]string{
				vsphereAgentPoolNameLabel: testNodePool,
			},
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: testNodePool},
		},
		Status: agentforgev1alpha1.VsphereAgentStatus{
			VM: newOwnedVMStatus(testAdoptedVM),
		},
	}
	named := duplicate.DeepCopy()
	named.Name = testAdoptedVM
	named.DeletionTimestamp = nil
	named.Finalizers = nil

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, secret, duplicate, named).
		WithStatusSubresource(duplicate, named).
		Build()

	provider := &fakeVMProvider{}
	reconciler := &VsphereAgentReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: events.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return provider, nil
		},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: "demo-worker-legacy"}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if provider.deleteVMCalls != 0 {
		t.Fatalf("DeleteVM calls = %d, want 0 for duplicate VsphereAgent", provider.deleteVMCalls)
	}
	var updated agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "demo-worker-legacy"}, &updated); err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&updated, vsphereAgentFinalizerName) {
		t.Fatalf("finalizers = %#v, want duplicate finalizer removed without VM deletion", updated.Finalizers)
	}
}

func TestVsphereAgentReconcileWaitsForAdoptedStatus(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	vsphereAgent := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  testNamespace,
			Name:       testAdoptedVM,
			Finalizers: []string{vsphereAgentFinalizerName},
			Labels: map[string]string{
				vsphereAgentPoolNameLabel:   testNodePool,
				vsphereAgentCreatedForLabel: vsphereAgentCreatedForAdopted,
			},
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: testNodePool},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, vsphereAgent).
		WithStatusSubresource(vsphereAgent).
		Build()

	provider := &fakeVMProvider{}
	reconciler := &VsphereAgentReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: events.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return provider, nil
		},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testAdoptedVM}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if result.RequeueAfter != 10*time.Second {
		t.Fatalf("requeueAfter = %s, want 10s", result.RequeueAfter)
	}
	if provider.createVMCalls != 0 {
		t.Fatalf("CreateVM calls = %d, want 0 for adopted VsphereAgent without status", provider.createVMCalls)
	}

	var updated agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testAdoptedVM}, &updated); err != nil {
		t.Fatal(err)
	}
	ready := findCondition(updated.Status.Conditions, conditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "AdoptionPending" {
		t.Fatalf("Ready condition = %#v, want AdoptionPending False", ready)
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
		Recorder: events.NewFakeRecorder(10),
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
		Recorder: events.NewFakeRecorder(10),
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

func TestRefreshOwnedVMStatusesPreservesDeletingMachineRefUntilMachineGone(t *testing.T) {
	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{
		{
			Name:       "demo-worker-deleting",
			Phase:      phaseReleased,
			Reason:     "MachineDeleting",
			AgentRef:   &corev1.ObjectReference{Name: "deleting-agent"},
			MachineRef: testMachineRef("deleting-machine"),
		},
	}

	vms := refreshOwnedVMStatuses(pool, []AgentInfo{
		{Name: "deleting-agent", Bound: false, Hostname: "demo-worker-deleting"},
	}, []MachineInfo{{Name: "deleting-machine", Deleting: true}})

	if vms[0].MachineRef == nil || vms[0].MachineRef.Name != "deleting-machine" {
		t.Fatalf("machineRef = %#v, want previous deleting Machine ref retained", vms[0].MachineRef)
	}
	if vms[0].Phase != phaseReleased || vms[0].Reason != "MachineDeleting" {
		t.Fatalf("VM phase/reason = %s/%s, want Released/MachineDeleting", vms[0].Phase, vms[0].Reason)
	}
}

func TestRefreshOwnedVMStatusesMatchesAgentsByBIOSUUIDBeforeHostname(t *testing.T) {
	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{
		{
			Name:       "demo-worker-real",
			Phase:      phaseProvisioning,
			Reason:     reasonVMCreateRequested,
			BIOSUUID:   "423297c6-d72e-28bb-b279-1209c29ab72b",
			MACAddress: "00-50-56-aa-bb-cc",
		},
	}

	vms := refreshOwnedVMStatuses(pool, []AgentInfo{
		{
			Name:     "agent-1",
			Bound:    false,
			Hostname: "demo-worker-wrong",
			BIOSUUID: "423297c6-d72e-28bb-b279-1209c29ab72b",
			MAC:      "00-50-56-aa-bb-cc",
		},
	}, nil)

	if len(vms) != 1 {
		t.Fatalf("ownedVMs = %d, want 1", len(vms))
	}
	if vms[0].Name != "demo-worker-real" || vms[0].AgentRef == nil || vms[0].AgentRef.Name != "agent-1" {
		t.Fatalf("owned VM = %#v, want real VM matched to agent by BIOS UUID", vms[0])
	}
}

func TestRefreshOwnedVMStatusesDoesNotMatchHostnameWhenIdentityConflicts(t *testing.T) {
	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{
		{
			Name:       "demo-worker-host",
			Phase:      phaseProvisioning,
			Reason:     reasonVMCreateRequested,
			BIOSUUID:   "existing-vm-bios",
			MACAddress: "00-50-56-aa-bb-cc",
		},
	}

	vms := refreshOwnedVMStatuses(pool, []AgentInfo{
		{
			Name:     "assisted-agent",
			Bound:    true,
			Hostname: "demo-worker-host",
			BIOSUUID: "different-agent-bios",
			MAC:      "00-50-56-dd-ee-ff",
		},
	}, nil)

	if len(vms) != 1 {
		t.Fatalf("ownedVMs = %d, want original VM only", len(vms))
	}
	if vms[0].AgentRef != nil || vms[0].Phase != phaseProvisioning || vms[0].Reason != reasonAgentNotDiscovered {
		t.Fatalf("conflicting VM status = %#v, want no Agent match by hostname", vms[0])
	}
}

func TestAssignedAgentHostnamesUsesVMIdentityBeforeFreeSlot(t *testing.T) {
	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{
		{Name: "demo-worker-first", Phase: phaseProvisioning, BIOSUUID: "11111111-1111-1111-1111-111111111111"},
		{Name: "demo-worker-match", Phase: phaseProvisioning, BIOSUUID: "22222222-2222-2222-2222-222222222222"},
	}

	hostnames := assignedAgentHostnames(pool, []AgentInfo{{
		Name:     "agent-1",
		BIOSUUID: "22222222-2222-2222-2222-222222222222",
	}})

	if hostnames["agent-1"] != "demo-worker-match" {
		t.Fatalf("assigned hostname = %q, want identity-matched VM name", hostnames["agent-1"])
	}
}

func TestRefreshOwnedVMStatusesRecoversDeletingVMWithLostMachineRef(t *testing.T) {
	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{
		{
			Name:     "demo-worker-deleting",
			Phase:    phaseReleased,
			Reason:   "MachineDeleting",
			AgentRef: &corev1.ObjectReference{Name: "deleting-agent"},
		},
	}

	vms := refreshOwnedVMStatuses(pool, []AgentInfo{
		{Name: "deleting-agent", Bound: false, Hostname: "demo-worker-deleting"},
	}, nil)

	if vms[0].Phase != phaseReleased || vms[0].Reason != "MachineDeleted" {
		t.Fatalf("VM phase/reason = %s/%s, want Released/MachineDeleted", vms[0].Phase, vms[0].Reason)
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
		Recorder: events.NewFakeRecorder(10),
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
		Recorder: events.NewFakeRecorder(10),
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
		Recorder: events.NewFakeRecorder(10),
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
		Recorder: events.NewFakeRecorder(10),
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

func TestListMatchingAgentsSkipsForeignClusterDeployment(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	agent := testAgent(testNamespace, "foreign-agent", false, true)
	if err := unstructured.SetNestedField(agent.Object, "other-cluster", "spec", "clusterDeploymentName", "name"); err != nil {
		t.Fatal(err)
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(agent).
		Build()

	reconciler := &VsphereAgentPoolReconciler{Client: k8sClient}
	agents, err := reconciler.listMatchingAgents(ctx, pool)
	if err != nil {
		t.Fatalf("listMatchingAgents returned error: %v", err)
	}
	if len(agents) != 0 {
		t.Fatalf("matching agents = %#v, want foreign ClusterDeployment Agent ignored", agents)
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

func TestAgentChangePredicateWatchesInventoryIdentity(t *testing.T) {
	oldAgent := testCandidateAgent(testNamespace, "candidate-agent")
	newAgent := oldAgent.DeepCopy()
	setAgentInventoryHostname(t, newAgent, "demo-worker-c3p0")

	if !agentChangePredicate().Update(event.UpdateEvent{ObjectOld: oldAgent, ObjectNew: newAgent}) {
		t.Fatal("Agent inventory hostname change did not trigger reconcile")
	}
}

func TestAgentMachineChangePredicateWatchesAssignment(t *testing.T) {
	oldAgentMachine := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")
	newAgentMachine := oldAgentMachine.DeepCopy()
	if err := unstructured.SetNestedField(newAgentMachine.Object, "agent://agent-1", "spec", "providerID"); err != nil {
		t.Fatal(err)
	}

	if !agentMachineChangePredicate().Update(event.UpdateEvent{ObjectOld: oldAgentMachine, ObjectNew: newAgentMachine}) {
		t.Fatal("AgentMachine providerID assignment change did not trigger reconcile")
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
		Recorder: events.NewFakeRecorder(10),
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
	name := req.Name
	if name == "" {
		name = desiredAgentHostname(pool)
	}
	return newOwnedVMStatus(name), nil
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
