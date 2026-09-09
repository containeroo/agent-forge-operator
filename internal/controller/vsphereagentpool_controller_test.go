//nolint:goconst,unparam
package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

func TestReconcileReportsDemandAndCapacity(t *testing.T) {
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

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
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
	if updated.Status.PlannedActions[0].Type != actionDemandDeficit {
		t.Fatalf("first action = %s, want %s", updated.Status.PlannedActions[0].Type, actionDemandDeficit)
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
	condition := meta.FindStatusCondition(updated.Status.Conditions, conditionAgentMachineDemand)
	if condition == nil {
		t.Fatal("AgentMachineDemandFound condition was not set")
		return
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
	ready := meta.FindStatusCondition(updated.Status.Conditions, conditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "InfraEnvUnavailable" {
		t.Fatalf("Ready condition = %#v, want InfraEnvUnavailable False", ready)
	}
	infraEnv := meta.FindStatusCondition(updated.Status.Conditions, conditionInfraEnvAvailable)
	if infraEnv == nil || infraEnv.Status != metav1.ConditionFalse {
		t.Fatalf("InfraEnvAvailable condition = %#v, want False", infraEnv)
	}
}

func TestInfraEnvAvailableWaitsForCurrentImage(t *testing.T) {
	tests := []struct {
		name            string
		isoURL          string
		imageVersion    string
		conditionStatus string
		conditionReason string
		conditionMsg    string
		wantAvailable   bool
		wantMessage     string
	}{
		{
			name:            "unsupported image version",
			imageVersion:    "4.22",
			conditionStatus: string(corev1.ConditionFalse),
			conditionReason: "ImageCreationError",
			conditionMsg:    "No OS image for Openshift version 5.0 and architecture x86_64",
			wantMessage:     "InfraEnv ImageCreated condition is False (ImageCreationError): No OS image for Openshift version 5.0 and architecture x86_64",
		},
		{
			name:            "stale ISO version",
			isoURL:          "https://example.invalid/byapikey/token/4.21/x86_64/minimal.iso",
			imageVersion:    "4.22",
			conditionStatus: string(corev1.ConditionTrue),
			conditionReason: "InfraEnvAvailable",
			wantMessage:     "InfraEnv discovery image version 4.21 does not match spec.osImageVersion 4.22",
		},
		{
			name:            "current ISO version",
			isoURL:          "https://example.invalid/byapikey/token/4.22/x86_64/minimal.iso",
			imageVersion:    "4.22",
			conditionStatus: string(corev1.ConditionTrue),
			conditionReason: "InfraEnvAvailable",
			wantAvailable:   true,
			wantMessage:     "InfraEnv exposes discovery ISO",
		},
		{
			name:            "legacy unversioned URL",
			isoURL:          "https://example.invalid/downloads/image",
			imageVersion:    "4.22",
			conditionStatus: string(corev1.ConditionTrue),
			conditionReason: "InfraEnvAvailable",
			wantAvailable:   true,
			wantMessage:     "InfraEnv exposes discovery ISO",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			infraEnv := testInfraEnv(testNamespace, testInfraEnvName, tt.isoURL)
			infraEnv.Object["spec"] = map[string]any{"osImageVersion": tt.imageVersion}
			infraEnv.Object["status"].(map[string]any)["conditions"] = []any{map[string]any{
				"type":    "ImageCreated",
				"status":  tt.conditionStatus,
				"reason":  tt.conditionReason,
				"message": tt.conditionMsg,
			}}
			k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(infraEnv).Build()

			available, gotURL, message := infraEnvAvailable(context.Background(), k8sClient, reconcileTestPool())
			if available != tt.wantAvailable {
				t.Fatalf("available = %t, want %t (message %q)", available, tt.wantAvailable, message)
			}
			if message != tt.wantMessage {
				t.Fatalf("message = %q, want %q", message, tt.wantMessage)
			}
			if tt.wantAvailable && gotURL != tt.isoURL {
				t.Fatalf("URL = %q, want %q", gotURL, tt.isoURL)
			}
			if !tt.wantAvailable && gotURL != "" {
				t.Fatalf("URL = %q, want empty while waiting", gotURL)
			}
		})
	}
}

func TestInfraEnvAvailableUsesBootArtifactVersionForOpaqueISOURL(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/downloads/image")
	infraEnv.Object["spec"] = map[string]any{"osImageVersion": "4.22"}
	infraEnv.Object["status"].(map[string]any)["bootArtifacts"] = map[string]any{
		"rootfs": "https://example.invalid/boot-artifacts/rootfs?arch=x86_64&version=4.21",
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(infraEnv).Build()

	available, _, message := infraEnvAvailable(context.Background(), k8sClient, reconcileTestPool())
	if available {
		t.Fatal("InfraEnv reported available with stale boot artifacts")
	}
	if !strings.Contains(message, "version 4.21 does not match spec.osImageVersion 4.22") {
		t.Fatalf("message = %q, want version mismatch", message)
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
			Namespace: testNamespace,
			Name:      "demo-worker-agent",

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

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Recorder: events.NewFakeRecorder(10),
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if result.RequeueAfter != 10*time.Second {
		t.Fatalf("requeueAfter = %s, want 10s while child finalizer runs", result.RequeueAfter)
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

func TestPoolDeleteRemovesFinalizerAfterChildrenGone(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{newOwnedVMStatus("legacy-vm")}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool).
		WithStatusSubresource(pool).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Recorder: events.NewFakeRecorder(10),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	var updatedPool agentforgev1alpha1.VsphereAgentPool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testNodePool}, &updatedPool); err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&updatedPool, finalizerName) {
		t.Fatalf("pool finalizers = %#v, want pool finalizer removed after child cleanup is complete", updatedPool.Finalizers)
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

func TestPoolStatusUpdatePreservesISOReadyCondition(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	current := reconcileTestPool()
	meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
		Type:               conditionISOReady,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: current.Generation,
		Reason:             "ISOReady",
		Message:            "cached",
	})
	stale := current.DeepCopy()
	stale.Status.Conditions = nil
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
	isoReady := meta.FindStatusCondition(updated.Status.Conditions, conditionISOReady)
	if isoReady == nil || isoReady.Status != metav1.ConditionTrue || isoReady.Reason != "ISOReady" {
		t.Fatalf("ISOReady condition = %#v, want preserved True condition", isoReady)
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

func TestPoolReconcileLeavesDemandCreationToAgentMachineController(t *testing.T) {
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

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, infraEnv).
		WithStatusSubresource(pool).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Recorder: events.NewFakeRecorder(10),
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
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

func TestReconcileDoesNotAdoptMatchingAgents(t *testing.T) {
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

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, am2, infraEnv, agent1, agent2, agent3).
		WithStatusSubresource(pool).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Recorder: events.NewFakeRecorder(10),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var vsphereAgents agentforgev1alpha1.VsphereAgentList
	if err := k8sClient.List(ctx, &vsphereAgents, client.InNamespace(testNamespace), client.MatchingLabels{vsphereAgentPoolNameLabel: testNodePool}); err != nil {
		t.Fatal(err)
	}
	if len(vsphereAgents.Items) != 0 {
		t.Fatalf("VsphereAgents = %d, want 0 because existing Agents are managed through existing VsphereAgents only", len(vsphereAgents.Items))
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
	agentMachineName := testNodePool + "-8phl4"
	am := testAgentMachine(testControlPlaneNamespace, agentMachineName, "demo/demo-worker")
	am.SetUID(types.UID("agent-machine-uid"))

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am).
		WithIndex(&agentforgev1alpha1.VsphereAgentPool{}, vsphereAgentPoolControlPlaneNamespaceIndex, controlPlaneNamespaceIndexFunc).
		Build()

	reconciler := &AgentMachineReconciler{Client: k8sClient, Scheme: scheme}
	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testControlPlaneNamespace, Name: agentMachineName}})
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
	if created.Name != agentMachineName {
		t.Fatalf("VsphereAgent name = %q, want AgentMachine name %q", created.Name, agentMachineName)
	}
	if created.Labels[agentMachineSelectionLabel] != agentMachineName {
		t.Fatalf("VsphereAgent AgentMachine label = %q, want %q", created.Labels[agentMachineSelectionLabel], agentMachineName)
	}
	if created.Annotations[vsphereAgentVMNameAnnotation] != agentMachineName {
		t.Fatalf("VsphereAgent VM name annotation = %q, want %q", created.Annotations[vsphereAgentVMNameAnnotation], agentMachineName)
	}
	var updatedAgentMachine unstructured.Unstructured
	updatedAgentMachine.SetGroupVersionKind(agentMachineGVK)
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testControlPlaneNamespace, Name: agentMachineName}, &updatedAgentMachine); err != nil {
		t.Fatal(err)
	}
	matchLabels, _, err := unstructured.NestedStringMap(updatedAgentMachine.Object, "spec", "agentLabelSelector", "matchLabels")
	if err != nil {
		t.Fatal(err)
	}
	if matchLabels[agentMachineSelectionLabel] != agentMachineName {
		t.Fatalf("AgentMachine selector label = %q, want %q", matchLabels[agentMachineSelectionLabel], agentMachineName)
	}

	_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testControlPlaneNamespace, Name: agentMachineName}})
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

func TestAgentMachineReconcileCreatesAlternateVsphereAgentWhenNameConflicts(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	agentMachineName := testNodePool + "-8phl4"
	am := testAgentMachine(testControlPlaneNamespace, agentMachineName, "demo/demo-worker")
	conflicting := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      agentMachineName,
			Labels: map[string]string{
				vsphereAgentPoolNameLabel:   "other-worker",
				vsphereAgentCreatedForLabel: vsphereAgentCreatedForDemand,
				agentMachineSelectionLabel:  agentMachineName,
			},
			Annotations: map[string]string{
				vsphereAgentVMNameAnnotation: agentMachineName,
			},
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: "other-worker"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, conflicting).
		WithIndex(&agentforgev1alpha1.VsphereAgentPool{}, vsphereAgentPoolControlPlaneNamespaceIndex, controlPlaneNamespaceIndexFunc).
		Build()

	reconciler := &AgentMachineReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testControlPlaneNamespace, Name: agentMachineName}}); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var vsphereAgents agentforgev1alpha1.VsphereAgentList
	if err := k8sClient.List(ctx, &vsphereAgents, client.InNamespace(testNamespace)); err != nil {
		t.Fatal(err)
	}
	var created *agentforgev1alpha1.VsphereAgent
	for i := range vsphereAgents.Items {
		if vsphereAgents.Items[i].Spec.PoolRef.Name == testNodePool {
			created = &vsphereAgents.Items[i]
			break
		}
	}
	if created == nil {
		t.Fatalf("VsphereAgents = %#v, want alternate VsphereAgent for pool %q", vsphereAgents.Items, testNodePool)
	}
	if created.Name == agentMachineName {
		t.Fatalf("created VsphereAgent reused conflicting name %q", agentMachineName)
	}
	if created.Labels[agentMachineSelectionLabel] != agentMachineName {
		t.Fatalf("AgentMachine label = %q, want %q", created.Labels[agentMachineSelectionLabel], agentMachineName)
	}
	if created.Annotations[vsphereAgentVMNameAnnotation] != agentMachineName {
		t.Fatalf("VM name annotation = %q, want %q", created.Annotations[vsphereAgentVMNameAnnotation], agentMachineName)
	}
}

func TestAgentMachineReconcileFailsWhenMultiplePoolsMatch(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	duplicatePool := reconcileTestPool()
	duplicatePool.Name = "duplicate-worker"
	agentMachineName := testNodePool + "-8phl4"
	am := testAgentMachine(testControlPlaneNamespace, agentMachineName, "demo/demo-worker")

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, duplicatePool, am).
		WithIndex(&agentforgev1alpha1.VsphereAgentPool{}, vsphereAgentPoolControlPlaneNamespaceIndex, controlPlaneNamespaceIndexFunc).
		Build()

	reconciler := &AgentMachineReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testControlPlaneNamespace, Name: agentMachineName}}); err == nil {
		t.Fatal("reconcile returned nil error, want ambiguous pool match error")
	}

	var vsphereAgents agentforgev1alpha1.VsphereAgentList
	if err := k8sClient.List(ctx, &vsphereAgents, client.InNamespace(testNamespace)); err != nil {
		t.Fatal(err)
	}
	if len(vsphereAgents.Items) != 0 {
		t.Fatalf("VsphereAgents = %#v, want none for ambiguous pool match", vsphereAgents.Items)
	}
}

func TestAgentMachineReconcileSkipsDeletingPool(t *testing.T) {
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
	agentMachineName := testNodePool + "-8phl4"
	am := testAgentMachine(testControlPlaneNamespace, agentMachineName, "demo/demo-worker")

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am).
		WithIndex(&agentforgev1alpha1.VsphereAgentPool{}, vsphereAgentPoolControlPlaneNamespaceIndex, controlPlaneNamespaceIndexFunc).
		Build()

	reconciler := &AgentMachineReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testControlPlaneNamespace, Name: agentMachineName}}); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var vsphereAgents agentforgev1alpha1.VsphereAgentList
	if err := k8sClient.List(ctx, &vsphereAgents, client.InNamespace(testNamespace), client.MatchingLabels{vsphereAgentPoolNameLabel: testNodePool}); err != nil {
		t.Fatal(err)
	}
	if len(vsphereAgents.Items) != 0 {
		t.Fatalf("VsphereAgents = %d, want none while pool is deleting", len(vsphereAgents.Items))
	}
}

func TestListVsphereAgentsForPoolUsesSpecPoolRefWhenLabelMissing(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	agent := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      "demo-worker-unlabeled",
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: testNodePool},
		},
	}
	other := agent.DeepCopy()
	other.Name = "other-worker-unlabeled"
	other.Spec.PoolRef.Name = "other-worker"

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, agent, other).
		Build()

	reconciler := &VsphereAgentPoolReconciler{Client: k8sClient}
	agents, err := reconciler.listVsphereAgentsForPool(ctx, pool)
	if err != nil {
		t.Fatalf("listVsphereAgentsForPool returned error: %v", err)
	}
	if len(agents.Items) != 1 || agents.Items[0].Name != agent.Name {
		t.Fatalf("VsphereAgents = %#v, want only spec.poolRef match", agents.Items)
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
			Namespace: testNamespace,
			Name:      "demo-worker-agent",
			UID:       types.UID("demo-worker-agent-uid"),

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
	wantRequest := VMCreateRequest{Name: vsphereAgent.Name, ISOPath: provider.isoPath, OwnerUID: string(vsphereAgent.UID)}
	if got := provider.createRequests[0]; got != wantRequest {
		t.Fatalf("CreateVM request = %#v, want %#v", got, wantRequest)
	}

	var updated agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "demo-worker-agent"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.VM.Name != "demo-worker-agent" || updated.Status.VM.Phase != phaseProvisioning {
		t.Fatalf("VM status = %#v, want created provisioning VM named after VsphereAgent", updated.Status.VM)
	}
}

func TestVsphereAgentReconcileUsesAnnotatedVMName(t *testing.T) {
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
			Name:       "demo-worker-8phl4-demo-worker-a1b2c3d4e5",
			Finalizers: []string{vsphereAgentFinalizerName},
			Labels: map[string]string{
				vsphereAgentPoolNameLabel:   testNodePool,
				vsphereAgentCreatedForLabel: vsphereAgentCreatedForDemand,
			},
			Annotations: map[string]string{
				vsphereAgentVMNameAnnotation: "demo-worker-8phl4",
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
		Recorder: events.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return provider, nil
		},
	}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: vsphereAgent.Name}}); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if len(provider.createVMNames) != 1 || provider.createVMNames[0] != "demo-worker-8phl4" {
		t.Fatalf("CreateVM names = %#v, want annotated VM name", provider.createVMNames)
	}

	var updated agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: vsphereAgent.Name}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.VM.Name != "demo-worker-8phl4" {
		t.Fatalf("VM status name = %q, want annotated VM name", updated.Status.VM.Name)
	}
}

func TestVsphereAgentReconcileSyncsVMStatusFromPool(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{{
		Name: "demo-worker-agent",

		Phase:      phaseBound,
		Reason:     "AgentBound",
		BIOSUUID:   "42324d21-274c-03a8-b6fe-f9e8edb55e33",
		MACAddress: "00-50-56-b2-db-c8",
		AgentRef:   agentObjectReference(pool, "214d3242-4c27-a803-b6fe-f9e8edb55e33"),
		MachineRef: machineObjectReference(pool, "demo-worker-agent"),
	}}
	vsphereAgent := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      "demo-worker-agent",

			Finalizers: []string{vsphereAgentFinalizerName},
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: testNodePool},
		},
		Status: agentforgev1alpha1.VsphereAgentStatus{
			VM: agentforgev1alpha1.OwnedVMStatus{
				Name: "demo-worker-agent",

				Phase:      phaseProvisioning,
				Reason:     "CreateRequested",
				BIOSUUID:   "42324d21-274c-03a8-b6fe-f9e8edb55e33",
				MACAddress: "00-50-56-b2-db-c8",
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, vsphereAgent).
		WithStatusSubresource(pool, vsphereAgent).
		Build()

	reconciler := &VsphereAgentReconciler{
		Client:   k8sClient,
		Recorder: events.NewFakeRecorder(10),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: "demo-worker-agent"}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var updated agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "demo-worker-agent"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.VM.Phase != phaseBound || updated.Status.VM.Reason != "AgentBound" {
		t.Fatalf("VM status = %s/%s, want %s/%s", updated.Status.VM.Phase, updated.Status.VM.Reason, phaseBound, "AgentBound")
	}
	if updated.Status.VM.AgentRef == nil || updated.Status.VM.AgentRef.Name != "214d3242-4c27-a803-b6fe-f9e8edb55e33" {
		t.Fatalf("AgentRef = %#v, want synced AgentRef", updated.Status.VM.AgentRef)
	}
	if updated.Status.VM.MachineRef == nil || updated.Status.VM.MachineRef.Name != "demo-worker-agent" {
		t.Fatalf("MachineRef = %#v, want synced MachineRef", updated.Status.VM.MachineRef)
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

func TestVsphereAgentReconcileSkipsVMDeleteForAdoptedDuplicateByIdentity(t *testing.T) {
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
	staleAdopted := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         testNamespace,
			Name:              "demo-worker-stale",
			Finalizers:        []string{vsphereAgentFinalizerName},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
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
				Name:     "demo-worker-stale",
				BIOSUUID: "shared-bios",
			},
		},
	}
	demandAgent := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      "demo-worker-current",
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
				Name:     "demo-worker-current",
				BIOSUUID: "shared-bios",
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, secret, staleAdopted, demandAgent).
		WithStatusSubresource(staleAdopted, demandAgent).
		Build()

	provider := &fakeVMProvider{}
	reconciler := &VsphereAgentReconciler{
		Client:   k8sClient,
		Recorder: events.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return provider, nil
		},
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: staleAdopted.Name}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if provider.deleteVMCalls != 0 {
		t.Fatalf("DeleteVM calls = %d, want 0 for adopted duplicate with shared identity", provider.deleteVMCalls)
	}
}

func TestVsphereAgentReconcileInitializesAdoptedStatus(t *testing.T) {
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
	}
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
		WithObjects(pool, secret, vsphereAgent).
		WithStatusSubresource(vsphereAgent).
		Build()

	provider := &fakeVMProvider{}
	reconciler := &VsphereAgentReconciler{
		Client:   k8sClient,
		Recorder: events.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return provider, nil
		},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testAdoptedVM}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if result.RequeueAfter != time.Minute {
		t.Fatalf("requeueAfter = %s, want 1m", result.RequeueAfter)
	}
	if provider.createVMCalls != 0 {
		t.Fatalf("CreateVM calls = %d, want 0 for adopted VsphereAgent without status", provider.createVMCalls)
	}
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testAdoptedVM}}); err != nil {
		t.Fatalf("second reconcile returned error: %v", err)
	}

	var updated agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: testAdoptedVM}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.VM.Name != testAdoptedVM || updated.Status.VM.Reason != reasonVMAdopted {
		t.Fatalf("VM status = %#v, want adopted VM status", updated.Status.VM)
	}
	ready := meta.FindStatusCondition(updated.Status.Conditions, conditionReady)
	if ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason != reasonVMAdopted {
		t.Fatalf("Ready condition = %#v, want VMAdopted True", ready)
	}
}

func TestVsphereAgentDeleteDropsFinalizerWhenPoolMissing(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	vsphereAgent := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         testNamespace,
			Name:              "demo-worker-orphaned",
			Finalizers:        []string{vsphereAgentFinalizerName},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: testNodePool},
		},
		Status: agentforgev1alpha1.VsphereAgentStatus{
			VM: newOwnedVMStatus("demo-worker-orphaned"),
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(vsphereAgent).
		WithStatusSubresource(vsphereAgent).
		Build()

	reconciler := &VsphereAgentReconciler{
		Client:   k8sClient,
		Recorder: events.NewFakeRecorder(10),
	}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: vsphereAgent.Name}}); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var updated agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: vsphereAgent.Name}, &updated); err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(&updated, vsphereAgentFinalizerName) {
		t.Fatalf("finalizers = %#v, want VsphereAgent finalizer removed", updated.Finalizers)
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
	pool.Spec.Agent.Labels[roleLabelKey] = "user-supplied-role"
	pool.Spec.Agent.Labels[agentMachineSelectionLabel] = "user-supplied-selection"
	pool.Spec.Agent.Labels[agentMachineRefKey] = "user-supplied-binding"
	ownedVM := agentforgev1alpha1.OwnedVMStatus{
		Name:       "demo-worker-ab12",
		Phase:      phaseProvisioning,
		MACAddress: "00-50-56-b2-a1-9f",
	}
	vsphereAgent := testVsphereAgentForVM(pool, ownedVM)
	am := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	agent := testCandidateAgent(testNamespace, "abcdef12-3456-7890-abcd-ef1234567890")
	setAgentPrimaryMAC(t, agent, "00:50:56:b2:a1:9f")

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, infraEnv, agent, vsphereAgent).
		WithStatusSubresource(pool, vsphereAgent).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
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
	if labels[agentMachineSelectionLabel] != "demo-worker-ab12" {
		t.Fatalf("agent machine selector label = %q, want demo-worker-ab12", labels[agentMachineSelectionLabel])
	}
	if labels[agentMachineRefKey] != "" {
		t.Fatalf("controller wrote reserved Agent binding label %q", labels[agentMachineRefKey])
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

func TestReconcilePatchesCandidateAgentWithPoolDiscriminatorLabel(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.Spec.Agent.Labels[poolLabelKey] = "worker-32c128g"
	ownedVM := agentforgev1alpha1.OwnedVMStatus{
		Name:       "demo-worker-32c128g-ab12",
		Phase:      phaseProvisioning,
		MACAddress: "00-50-56-b2-a1-9f",
	}
	vsphereAgent := testVsphereAgentForVM(pool, ownedVM)
	am := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	agent := testCandidateAgent(testNamespace, "abcdef12-3456-7890-abcd-ef1234567890")
	setAgentPrimaryMAC(t, agent, "00:50:56:b2:a1:9f")

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, infraEnv, agent, vsphereAgent).
		WithStatusSubresource(pool, vsphereAgent).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
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
	labels := updated.GetLabels()
	if labels[roleLabelKey] != testWorkerRole {
		t.Fatalf("role label = %q, want %q", labels[roleLabelKey], testWorkerRole)
	}
	if labels[poolLabelKey] != "worker-32c128g" {
		t.Fatalf("pool label = %q, want worker-32c128g", labels[poolLabelKey])
	}
	if labels[agentMachineSelectionLabel] != "demo-worker-32c128g-ab12" {
		t.Fatalf("agent machine selector label = %q, want demo-worker-32c128g-ab12", labels[agentMachineSelectionLabel])
	}
	hostname, _, _ := unstructured.NestedString(updated.Object, "spec", "hostname")
	if hostname != "demo-worker-32c128g-ab12" {
		t.Fatalf("spec.hostname = %q, want matching owned VM name", hostname)
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
	am := testAgentMachine(testControlPlaneNamespace, "demo-worker-ab12-machine", "demo/demo-worker")
	machine := testMachine(testControlPlaneNamespace, "demo-worker-ab12-machine", "demo/demo-worker", false)
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	agent := testAgent(testNamespace, "demo-worker-ab12", true, true)
	am.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "cluster.x-k8s.io/v1beta1", Kind: "Machine", Name: machine.GetName()}})

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, machine, infraEnv, agent).
		WithStatusSubresource(pool).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
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
		t.Fatalf("machineRef = %#v, want Machine ref in control plane namespace", vm.MachineRef)
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

func TestReconcilePreservesOwnedVMTransitionState(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{{
		Name:               "demo-worker-old1",
		Phase:              phaseProvisioning,
		Reason:             reasonAgentNotDiscovered,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-orphanedOwnedVMGracePeriod - time.Minute)),
	}}
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	am := testAgentMachine(testControlPlaneNamespace, "demo-worker-new1", "demo/demo-worker")
	vsphereAgent := testVsphereAgentForVM(pool, agentforgev1alpha1.OwnedVMStatus{
		Name:               "demo-worker-old1",
		Phase:              phaseProvisioning,
		Reason:             reasonVMCreateRequested,
		LastTransitionTime: metav1.Now(),
	})

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, infraEnv, am, vsphereAgent).
		WithStatusSubresource(pool, vsphereAgent).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Recorder: events.NewFakeRecorder(10),
	}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}}); err != nil {
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
	if vm.Phase != phaseOrphaned || vm.Reason != "AgentDiscoveryExpired" {
		t.Fatalf("owned VM phase/reason = %s/%s, want Orphaned/AgentDiscoveryExpired", vm.Phase, vm.Reason)
	}
}

func TestReconcilePreservesMachineDeletingStateForCleanup(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{{
		Name:       "demo-worker-gone",
		Phase:      phaseReleased,
		Reason:     reasonMachineDeleting,
		AgentRef:   testAgentRef("demo-worker-gone"),
		MachineRef: testMachineRef("demo-worker-gone-machine"),
	}}
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	agent := testAgent(testNamespace, "demo-worker-gone", false, true)
	vsphereAgent := testVsphereAgentForVM(pool, agentforgev1alpha1.OwnedVMStatus{
		Name:               "demo-worker-gone",
		Phase:              phaseProvisioning,
		Reason:             reasonVMCreateRequested,
		LastTransitionTime: metav1.Now(),
	})
	vsphereAgent.Finalizers = []string{vsphereAgentFinalizerName}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, infraEnv, agent, vsphereAgent).
		WithStatusSubresource(pool, vsphereAgent).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Recorder: events.NewFakeRecorder(10),
	}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testNodePool}}); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	var deletingAgent agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: vsphereAgent.Name}, &deletingAgent); err != nil {
		t.Fatal(err)
	}
	if deletingAgent.GetDeletionTimestamp() == nil {
		t.Fatal("VsphereAgent was not marked for deletion after remembered MachineDeleting transitioned to MachineDeleted")
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
	if vms[0].AgentRef == nil || vms[0].AgentRef.Name != "missing-agent" {
		t.Fatalf("agentRef = %#v, want original identity retained for deletion checks", vms[0].AgentRef)
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

func TestRefreshOwnedVMStatusesDoesNotChooseAmbiguousAgentIdentity(t *testing.T) {
	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{{
		Name:               "worker-vm",
		BIOSUUID:           "duplicate-bios",
		Phase:              phaseProvisioning,
		Reason:             reasonAgentNotDiscovered,
		LastTransitionTime: metav1.Now(),
	}}
	agents := []AgentInfo{
		{Name: "agent-a", Hostname: "worker-a", BIOSUUID: "duplicate-bios"},
		{Name: "agent-b", Hostname: "worker-b", BIOSUUID: "duplicate-bios"},
	}

	vms := refreshOwnedVMStatuses(pool, agents, nil)
	if len(vms) != 3 {
		t.Fatalf("owned VMs = %#v, want original VM plus independently observed Agents", vms)
	}
	if vms[0].AgentRef != nil {
		t.Fatalf("ambiguous identity selected AgentRef %#v, want no arbitrary match", vms[0].AgentRef)
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
		Hostname: "demo-worker-stale",
		BIOSUUID: "22222222-2222-2222-2222-222222222222",
	}})

	if hostnames["agent-1"] != "demo-worker-match" {
		t.Fatalf("assigned hostname = %q, want identity-matched VM name", hostnames["agent-1"])
	}
}

func TestAssignedAgentHostnamesPrefersObservedHostnameBeforeFreeSlot(t *testing.T) {
	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{
		{Name: "demo-worker-first", Phase: phaseProvisioning},
		{Name: "demo-worker-observed", Phase: phaseProvisioning},
	}

	hostnames := assignedAgentHostnames(pool, []AgentInfo{{
		Name:     "agent-1",
		Hostname: "demo-worker-observed",
	}})

	if hostnames["agent-1"] != "demo-worker-observed" {
		t.Fatalf("assigned hostname = %q, want observed VM name", hostnames["agent-1"])
	}
}

func TestAssignedAgentHostnamesUsesAgentRefBeforeFreeSlot(t *testing.T) {
	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{
		{Name: "demo-worker-first", Phase: phaseProvisioning},
		{Name: "demo-worker-match", Phase: phaseAvailable, AgentRef: testAgentRef("agent-1")},
	}

	hostnames := assignedAgentHostnames(pool, []AgentInfo{{
		Name: "agent-1",
	}})

	if hostnames["agent-1"] != "demo-worker-match" {
		t.Fatalf("assigned hostname = %q, want AgentRef-matched VM name", hostnames["agent-1"])
	}
}

func TestAssignedAgentHostnamesDoesNotUseAmbiguousFreeVM(t *testing.T) {
	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{
		{Name: "demo-worker-first", Phase: phaseProvisioning},
		{Name: "demo-worker-second", Phase: phaseProvisioning},
	}

	hostnames := assignedAgentHostnames(pool, []AgentInfo{
		{Name: "agent-1", InventoryHostname: "00-50-56-aa-bb-01"},
		{Name: "agent-2", InventoryHostname: "00-50-56-aa-bb-02"},
	})

	if len(hostnames) != 0 {
		t.Fatalf("assigned hostnames = %#v, want no assignment without identity, AgentRef, or matching hostname", hostnames)
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
	ownedVMs := []agentforgev1alpha1.OwnedVMStatus{
		{Name: "demo-worker-one1", Phase: phaseBound, AgentRef: testAgentRef("demo-worker-one1")},
		{Name: "demo-worker-two2", Phase: phaseBound, AgentRef: testAgentRef("demo-worker-two2")},
		{Name: "demo-worker-thr3", Phase: phaseBound, AgentRef: testAgentRef("demo-worker-thr3")},
		{Name: "demo-worker-old1", Phase: phaseProvisioning, Reason: "AgentNotDiscovered", LastTransitionTime: metav1.Now()},
		{Name: "demo-worker-old2", Phase: phaseProvisioning, Reason: "AgentNotDiscovered", LastTransitionTime: metav1.Now()},
	}
	vsphereAgent1 := testVsphereAgentForVM(pool, ownedVMs[0])
	vsphereAgent2 := testVsphereAgentForVM(pool, ownedVMs[1])
	vsphereAgent3 := testVsphereAgentForVM(pool, ownedVMs[2])
	vsphereAgent4 := testVsphereAgentForVM(pool, ownedVMs[3])
	vsphereAgent5 := testVsphereAgentForVM(pool, ownedVMs[4])
	am := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	agent1 := testAgent(testNamespace, "demo-worker-one1", true, true)
	agent2 := testAgent(testNamespace, "demo-worker-two2", true, true)
	agent3 := testAgent(testNamespace, "demo-worker-thr3", true, true)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, infraEnv, agent1, agent2, agent3, vsphereAgent1, vsphereAgent2, vsphereAgent3, vsphereAgent4, vsphereAgent5).
		WithStatusSubresource(pool, vsphereAgent1, vsphereAgent2, vsphereAgent3, vsphereAgent4, vsphereAgent5).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
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
	if len(updated.Status.OwnedVMs) != 5 {
		t.Fatalf("ownedVMs = %d, want bound and provisioning VMs retained", len(updated.Status.OwnedVMs))
	}
	condition := meta.FindStatusCondition(updated.Status.Conditions, conditionCapacitySatisfied)
	if condition == nil {
		t.Fatal("CapacitySatisfied condition missing")
		return
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
	ownedVM := agentforgev1alpha1.OwnedVMStatus{
		Name:     "demo-worker-ab12",
		Phase:    phaseBound,
		Reason:   "AgentBound",
		AgentRef: &corev1.ObjectReference{Name: "demo-worker-ab12"},
	}
	vsphereAgent := testVsphereAgentForVM(pool, ownedVM)
	am := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	agent := testAgent(testNamespace, "demo-worker-ab12", false, true)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, infraEnv, agent, vsphereAgent).
		WithStatusSubresource(pool, vsphereAgent).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
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
	if vm.Name != "demo-worker-ab12" || vm.Source != vmSourceDiscoveredAgent || vm.Phase != "Bound" || vm.AgentRef == nil || vm.AgentRef.Name != agent.GetName() {
		t.Fatalf("owned VM status = %#v, want adopted bound Agent", vm)
	}
}

func TestNormalizeVMwareSerialUUID(t *testing.T) {
	uuid := normalizeVMwareSerialUUID("VMware-42 32 97 c6 d7 2e 28 bb-b2 79 12 09 c2 9a b7 2b")
	if uuid != "423297c6-d72e-28bb-b279-1209c29ab72b" {
		t.Fatalf("uuid = %q, want normalized VMware BIOS UUID", uuid)
	}
}

func TestNormalizeVMwareSerialUUIDRejectsNonHexSerial(t *testing.T) {
	if got := normalizeVMwareSerialUUID("VMware-zz zz zz zz zz zz zz zz-zz zz zz zz zz zz zz zz"); got != "" {
		t.Fatalf("normalized invalid UUID = %q, want empty", got)
	}
}

func TestAgentCandidateLabelsExcludeControllerManagedKeys(t *testing.T) {
	pool := reconcileTestPool()
	pool.Spec.Agent.Labels[roleLabelKey] = "stale-role"
	pool.Spec.Agent.Labels[poolLabelKey] = "pool-a"
	pool.Spec.Agent.Labels[agentMachineSelectionLabel] = "stale-machine"
	pool.Spec.Agent.Labels[agentMachineRefKey] = "bound-machine"

	labels := agentCandidateLabels(pool)
	for _, key := range []string{roleLabelKey, poolLabelKey, agentMachineSelectionLabel, agentMachineRefKey} {
		if _, found := labels[key]; found {
			t.Fatalf("controller-managed key %q leaked into candidate selector %#v", key, labels)
		}
	}
	if labels[testCustomerKey] != testCustomer {
		t.Fatalf("candidate selector = %#v, want user discriminator retained", labels)
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

func TestListMatchingAgentsRequiresConfiguredPoolLabel(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	defaultPool := reconcileTestPool()
	customPool := reconcileTestPool()
	customPool.Name = "demo-worker-32c128g"
	customPool.Spec.NodePoolRef.Name = "demo-worker-32c128g"
	customPool.Spec.Agent.Labels[poolLabelKey] = "worker-32c128g"

	defaultAgent := testAgent(testNamespace, "default-worker-agent", true, true)
	customAgent := testAgent(testNamespace, "custom-worker-agent", true, true)
	customLabels := customAgent.GetLabels()
	customLabels[poolLabelKey] = "worker-32c128g"
	customAgent.SetLabels(customLabels)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(defaultAgent, customAgent).
		Build()

	reconciler := &VsphereAgentPoolReconciler{Client: k8sClient}

	customAgents, err := reconciler.listMatchingAgents(ctx, customPool)
	if err != nil {
		t.Fatalf("listMatchingAgents returned error for custom pool: %v", err)
	}
	if len(customAgents) != 1 || customAgents[0].Name != customAgent.GetName() {
		t.Fatalf("custom pool matching agents = %#v, want only custom pool Agent", customAgents)
	}

	defaultAgents, err := reconciler.listMatchingAgents(ctx, defaultPool)
	if err != nil {
		t.Fatalf("listMatchingAgents returned error for default pool: %v", err)
	}
	if len(defaultAgents) != 1 || defaultAgents[0].Name != defaultAgent.GetName() {
		t.Fatalf("default pool matching agents = %#v, want only unlabeled default pool Agent", defaultAgents)
	}
}

func TestReconcileDoesNotRecordAgentClaimedByOtherPool(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	otherPool := reconcileTestPool()
	otherPool.Name = "other-worker"
	otherPool.Spec.NodePoolRef.Name = "other-worker"
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	otherAgent := testAgent(testNamespace, "other-agent", false, true)
	otherVsphereAgent := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      "other-agent",
			Labels: map[string]string{
				vsphereAgentPoolNameLabel: otherPool.Name,
			},
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: otherPool.Name},
		},
		Status: agentforgev1alpha1.VsphereAgentStatus{
			VM: agentforgev1alpha1.OwnedVMStatus{
				Name:     "other-agent",
				Phase:    phaseAvailable,
				Reason:   "AgentAvailable",
				AgentRef: agentObjectReference(otherPool, "other-agent"),
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, otherPool, infraEnv, otherAgent, otherVsphereAgent).
		WithStatusSubresource(pool, otherPool, otherVsphereAgent).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
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
	if updated.Status.MatchingAgents != 0 {
		t.Fatalf("matching agents = %d, want other pool Agent ignored", updated.Status.MatchingAgents)
	}
	if len(updated.Status.OwnedVMs) != 0 {
		t.Fatalf("owned VMs = %#v, want other pool Agent absent from status", updated.Status.OwnedVMs)
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
	if len(reqs) != 2 {
		t.Fatalf("requests = %#v, want namespace-local pools", reqs)
	}
	got := map[types.NamespacedName]struct{}{}
	for _, req := range reqs {
		got[req.NamespacedName] = struct{}{}
	}
	for _, want := range []types.NamespacedName{
		{Namespace: testNamespace, Name: testNodePool},
		{Namespace: testNamespace, Name: "other-worker"},
	} {
		if _, ok := got[want]; !ok {
			t.Fatalf("requests = %#v, missing %s", reqs, want)
		}
	}
}

func TestRequestsForInfraEnvChangeFindsReferencingPools(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	otherPool := reconcileTestPool()
	otherPool.Name = "other-worker"
	otherPool.Spec.InfraEnvRef.Name = "other-infraenv"
	otherNamespacePool := reconcileTestPool()
	otherNamespacePool.Namespace = "other-namespace"
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, otherPool, otherNamespacePool).
		WithIndex(&agentforgev1alpha1.VsphereAgentPool{}, vsphereAgentPoolInfraEnvNameIndex, infraEnvNameIndexFunc).
		Build()

	reconciler := &VsphereAgentPoolReconciler{Client: k8sClient}
	reqs := reconciler.requestsForInfraEnvChange(ctx, infraEnv)
	if len(reqs) != 1 {
		t.Fatalf("requests = %#v, want one request", reqs)
	}
	if reqs[0].NamespacedName != (types.NamespacedName{Namespace: testNamespace, Name: testNodePool}) {
		t.Fatalf("request = %s, want demo/demo-worker", reqs[0].NamespacedName)
	}
}

func TestAgentChangePredicate(t *testing.T) {
	for _, tt := range []struct {
		name  string
		path  []string
		value any
		want  bool
	}{
		{name: "unchanged"},
		{name: "unrelated status", path: []string{"status", "debugInfo"}, value: "changed"},
		{name: "hostname", path: []string{"status", "inventory", "hostname"}, value: "worker-1", want: true},
		{name: "BIOS UUID", path: []string{"status", "inventory", "systemVendor", "serialNumber"}, value: "VMware-423297c6d72e28bbb2791209c29ab72b", want: true},
		{name: "MAC", path: []string{"status", "inventory", "interfaces"}, value: []any{map[string]any{"macAddress": "00:50:56:aa:bb:cc"}}, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			oldAgent := testCandidateAgent(testNamespace, "candidate-agent")
			newAgent := oldAgent.DeepCopy()
			if len(tt.path) > 0 {
				if err := unstructured.SetNestedField(newAgent.Object, tt.value, tt.path...); err != nil {
					t.Fatal(err)
				}
			}
			if got := agentChangePredicate().Update(event.UpdateEvent{ObjectOld: oldAgent, ObjectNew: newAgent}); got != tt.want {
				t.Fatalf("Update = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestAgentMachineChangePredicate(t *testing.T) {
	for _, tt := range []struct {
		name  string
		path  []string
		value string
		want  bool
	}{
		{name: "unchanged"},
		{name: "unrelated status", path: []string{"status", "debugInfo"}, value: "changed"},
		{name: "provider ID", path: []string{"spec", "providerID"}, value: "agent://agent-1", want: true},
		{name: "agent reference", path: []string{"status", "agentRef", "name"}, value: "agent-1", want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			oldMachine := testAgentMachine(testControlPlaneNamespace, testNodePool, "demo/demo-worker")
			newMachine := oldMachine.DeepCopy()
			if len(tt.path) > 0 {
				if err := unstructured.SetNestedField(newMachine.Object, tt.value, tt.path...); err != nil {
					t.Fatal(err)
				}
			}
			if got := agentMachineChangePredicate().Update(event.UpdateEvent{ObjectOld: oldMachine, ObjectNew: newMachine}); got != tt.want {
				t.Fatalf("Update = %t, want %t", got, tt.want)
			}
		})
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

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, am, infraEnv, boundAgent, excessAgent1, excessAgent2).
		WithStatusSubresource(pool).
		Build()

	reconciler := &VsphereAgentPoolReconciler{
		Client:   k8sClient,
		Recorder: events.NewFakeRecorder(10),
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

	pool.Status.ISO.URLHash = downloadURLHash(pool.Status.ISO.URL)
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

func TestEnsureISOCacheDoesNotPersistDownloadCredentials(t *testing.T) {
	pool := reconcileTestPool()
	provider := &fakeVMProvider{isoPath: "agent-forge/demo/demo-worker/abc.iso"}
	reconciler := &VsphereAgentReconciler{Recorder: events.NewFakeRecorder(10)}
	rawURL := "https://user:password@example.invalid/discovery.iso?token=super-secret#fragment"

	if _, err := reconciler.ensureISOCache(context.Background(), pool, provider, rawURL); err != nil {
		t.Fatalf("ensureISOCache returned error: %v", err)
	}
	if got, want := pool.Status.ISO.URL, "https://example.invalid/discovery.iso"; got != want {
		t.Fatalf("stored ISO URL = %q, want %q", got, want)
	}
	if isoCacheDue(pool, rawURL, "", time.Now()) {
		t.Fatal("redacted URL did not match the same credentialed download URL")
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

func TestISOHistoryRetainsSameDigestAtDifferentPath(t *testing.T) {
	history := []agentforgev1alpha1.ISOCacheHistoryEntry{
		{Path: "cache-old/abc.iso", SHA256: "abc"},
	}
	current := agentforgev1alpha1.ISOCacheHistoryEntry{Path: "cache-new/abc.iso", SHA256: "abc"}

	updated := updatedISOHistory(history, current, 2)
	if len(updated) != 2 {
		t.Fatalf("history length = %d, want current and previous path with same digest", len(updated))
	}
	if updated[0].Path != "cache-new/abc.iso" || updated[1].Path != "cache-old/abc.iso" {
		t.Fatalf("history = %#v, want both same-digest paths retained by path", updated)
	}

	stale := staleISOPaths(history, "cache-old/abc.iso", "cache-new/abc.iso", 1)
	if len(stale) != 1 || stale[0] != "cache-old/abc.iso" {
		t.Fatalf("stale paths = %#v, want previous path pruned when retain=1", stale)
	}
}

func TestVsphereAgentPoolReferenceOverridesStaleLabel(t *testing.T) {
	agent := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{vsphereAgentPoolNameLabel: "stale-pool"}},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: "current-pool"},
		},
	}

	if !vsphereAgentBelongsToPool(agent, "current-pool") {
		t.Fatal("authoritative spec.poolRef did not match current pool")
	}
	if vsphereAgentBelongsToPool(agent, "stale-pool") {
		t.Fatal("stale compatibility label made VsphereAgent belong to a second pool")
	}
}

func TestEnsureISOCacheRetriesFailedPrunes(t *testing.T) {
	pool := reconcileTestPool()
	pool.Spec.ISO.RetainVersions = 1
	pool.Status.ISO = agentforgev1alpha1.ISOCacheStatus{
		URL:        "https://example.invalid/old.iso",
		Path:       "cache/old-1.iso",
		SHA256:     "old-1",
		SizeBytes:  5,
		CheckedAt:  metav1.NewTime(time.Now().Add(-time.Hour)),
		UploadedAt: metav1.NewTime(time.Now().Add(-2 * time.Hour)),
		History: []agentforgev1alpha1.ISOCacheHistoryEntry{
			{Path: "cache/old-1.iso", SHA256: "old-1"},
			{Path: "cache/old-2.iso", SHA256: "old-2"},
		},
	}
	provider := &fakeVMProvider{
		isoPath:      "cache/current.iso",
		deleteISOErr: fmt.Errorf("datastore busy"),
	}
	reconciler := &VsphereAgentReconciler{Recorder: events.NewFakeRecorder(10)}

	if _, err := reconciler.ensureISOCache(context.Background(), pool, provider, "https://example.invalid/current.iso"); err != nil {
		t.Fatalf("ensureISOCache returned error: %v", err)
	}
	if len(provider.deletedISOPaths) != 2 {
		t.Fatalf("DeleteISO calls = %#v, want both stale paths", provider.deletedISOPaths)
	}
	if len(pool.Status.ISO.History) != 3 {
		t.Fatalf("history = %#v, want retained current entry and two failed prunes", pool.Status.ISO.History)
	}

	provider.deleteISOErr = nil
	provider.deletedISOPaths = nil
	pool.Status.ISO.CheckedAt = metav1.NewTime(time.Now().Add(-time.Hour))
	if _, err := reconciler.ensureISOCache(context.Background(), pool, provider, "https://example.invalid/current.iso"); err != nil {
		t.Fatalf("second ensureISOCache returned error: %v", err)
	}
	if len(provider.deletedISOPaths) != 2 {
		t.Fatalf("retried DeleteISO calls = %#v, want both previously failed paths", provider.deletedISOPaths)
	}
	if len(pool.Status.ISO.History) != 1 || pool.Status.ISO.History[0].Path != "cache/current.iso" {
		t.Fatalf("history after successful retry = %#v, want only retained current ISO", pool.Status.ISO.History)
	}
}

func TestVsphereAgentReconcileRecreatesMissingManagedVM(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "vsphere-credentials"}}
	agent := testVsphereAgentForVM(pool, newOwnedVMStatus("missing-worker"))
	agent.Name = "missing-worker"
	agent.UID = types.UID("missing-worker-uid")
	agent.Finalizers = []string{vsphereAgentFinalizerName}
	agent.Labels[vsphereAgentCreatedForLabel] = vsphereAgentCreatedForDemand

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, secret, agent, testInfraEnv(pool.Namespace, pool.Spec.InfraEnvRef.Name, "https://example.invalid/discovery.iso")).
		WithStatusSubresource(pool, agent).
		Build()
	provider := &fakeVMProvider{vmStatusErr: fmt.Errorf("%w: missing-worker", errVMNotFound)}
	reconciler := &VsphereAgentReconciler{
		Client:   k8sClient,
		Recorder: events.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return provider, nil
		},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: agent.Namespace, Name: agent.Name}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if result.RequeueAfter != 30*time.Second {
		t.Fatalf("requeueAfter = %s, want 30s", result.RequeueAfter)
	}
	var updated agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(agent), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.VM.Name != "" {
		t.Fatalf("VM status = %#v, want missing managed VM cleared so it can be recreated", updated.Status.VM)
	}
	ready := meta.FindStatusCondition(updated.Status.Conditions, conditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "VMNotFound" {
		t.Fatalf("Ready condition = %#v, want VMNotFound False", ready)
	}
	provider.vmStatusErr = nil
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(agent)}); err != nil {
		t.Fatalf("recreate reconcile returned error: %v", err)
	}
	if len(provider.createRequests) != 1 {
		t.Fatalf("CreateVM requests = %#v, want one recreation", provider.createRequests)
	}
	wantRequest := VMCreateRequest{Name: agent.Name, ISOPath: provider.isoPath, OwnerUID: string(agent.UID)}
	if got := provider.createRequests[0]; got != wantRequest {
		t.Fatalf("recreation request = %#v, want %#v", got, wantRequest)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(agent), &updated); err != nil {
		t.Fatal(err)
	}
	ready = meta.FindStatusCondition(updated.Status.Conditions, conditionReady)
	if updated.Status.VM.Name != agent.Name || ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("recreated status = %#v, want VM restored and Ready", updated.Status)
	}

}

func TestVsphereAgentReconcileRejectsVMOwnedByAnotherAgent(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "vsphere-credentials"}}
	vm := newOwnedVMStatus("owned-worker")
	vm.OwnerUID = "owned-worker-uid"
	agent := testVsphereAgentForVM(pool, vm)
	agent.UID = types.UID(vm.OwnerUID)
	agent.Finalizers = []string{vsphereAgentFinalizerName}
	agent.Labels[vsphereAgentCreatedForLabel] = vsphereAgentCreatedForDemand

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, secret, agent).
		WithStatusSubresource(agent).
		Build()
	provider := &fakeVMProvider{vmStatusOwnerUID: "another-agent-uid"}
	reconciler := &VsphereAgentReconciler{
		Client:   k8sClient,
		Recorder: events.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return provider, nil
		},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: agent.Namespace, Name: agent.Name}})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if result.RequeueAfter != 30*time.Second {
		t.Fatalf("requeueAfter = %s, want 30s", result.RequeueAfter)
	}
	var updated agentforgev1alpha1.VsphereAgent
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(agent), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.VM.OwnerUID != vm.OwnerUID {
		t.Fatalf("VM owner UID = %q, want original %q", updated.Status.VM.OwnerUID, vm.OwnerUID)
	}
	ready := meta.FindStatusCondition(updated.Status.Conditions, conditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "VMOwnershipMismatch" {
		t.Fatalf("Ready condition = %#v, want VMOwnershipMismatch False", ready)
	}
}

func TestVsphereAgentDeleteCleansUpVMWhenStatusWasNeverPersisted(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	pool := reconcileTestPool()
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "vsphere-credentials"}}
	agent := &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         testNamespace,
			Name:              "status-write-lost",
			UID:               types.UID("status-write-lost-uid"),
			Finalizers:        []string{vsphereAgentFinalizerName},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
			Labels:            map[string]string{vsphereAgentPoolNameLabel: pool.Name},
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{PoolRef: agentforgev1alpha1.LocalObjectReference{Name: pool.Name}},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, secret, agent).
		WithStatusSubresource(agent).
		Build()
	provider := &fakeVMProvider{}
	reconciler := &VsphereAgentReconciler{
		Client:   k8sClient,
		Recorder: events.NewFakeRecorder(10),
		ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
			return provider, nil
		},
	}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: agent.Namespace, Name: agent.Name}}); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if len(provider.deletedVMNames) != 1 || provider.deletedVMNames[0] != agent.Name {
		t.Fatalf("deleted VMs = %#v, want fallback cleanup for %q", provider.deletedVMNames, agent.Name)
	}
	if provider.deletedVMs[0].OwnerUID != string(agent.UID) {
		t.Fatalf("deleted VM owner UID = %q, want %q", provider.deletedVMs[0].OwnerUID, agent.UID)
	}
}

type fakeVMProvider struct {
	ensureISOCalls   int
	createVMCalls    int
	deleteVMCalls    int
	createRequests   []VMCreateRequest
	createVMNames    []string
	deletedVMNames   []string
	deletedVMs       []agentforgev1alpha1.OwnedVMStatus
	deletedISOPaths  []string
	isoPath          string
	vmStatusErr      error
	vmStatusOwnerUID string
	deleteISOErr     error
	deleteVMErr      error
}

func (p *fakeVMProvider) EnsureISO(context.Context, *agentforgev1alpha1.VsphereAgentPool, string) (ISOEnsureResult, error) {
	p.ensureISOCalls++
	if p.isoPath == "" {
		p.isoPath = "agent-forge/demo/demo-worker/abc.iso"
	}
	return ISOEnsureResult{Path: p.isoPath, SHA256: "abc", SizeBytes: 3, Uploaded: true}, nil
}

func (p *fakeVMProvider) CreateVM(_ context.Context, _ *agentforgev1alpha1.VsphereAgentPool, req VMCreateRequest) (agentforgev1alpha1.OwnedVMStatus, error) {
	p.createVMCalls++
	p.createRequests = append(p.createRequests, req)
	p.createVMNames = append(p.createVMNames, req.Name)
	if req.Name == "" {
		return agentforgev1alpha1.OwnedVMStatus{}, fmt.Errorf("VM name is required")
	}
	return newOwnedVMStatus(req.Name), nil
}

func (p *fakeVMProvider) VMStatus(_ context.Context, _ *agentforgev1alpha1.VsphereAgentPool, name string) (agentforgev1alpha1.OwnedVMStatus, error) {
	if p.vmStatusErr != nil {
		return agentforgev1alpha1.OwnedVMStatus{}, p.vmStatusErr
	}
	vm := newOwnedVMStatus(name)
	vm.BIOSUUID = "423297c6-d72e-28bb-b279-1209c29ab72b"
	vm.MACAddress = "00-50-56-aa-bb-cc"
	vm.OwnerUID = p.vmStatusOwnerUID
	return vm, nil
}

func (p *fakeVMProvider) DeleteVM(_ context.Context, _ *agentforgev1alpha1.VsphereAgentPool, vm agentforgev1alpha1.OwnedVMStatus) error {
	p.deleteVMCalls++
	p.deletedVMNames = append(p.deletedVMNames, vm.Name)
	p.deletedVMs = append(p.deletedVMs, vm)
	return p.deleteVMErr
}

func (p *fakeVMProvider) DeleteISO(_ context.Context, _ *agentforgev1alpha1.VsphereAgentPool, path string) error {
	p.deletedISOPaths = append(p.deletedISOPaths, path)
	return p.deleteISOErr
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
				NumCPUs:   4,
				MemoryMiB: 16384,
				DiskGiB:   100,
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

func setAgentPrimaryMAC(t *testing.T, agent *unstructured.Unstructured, mac string) {
	t.Helper()
	if err := unstructured.SetNestedSlice(agent.Object, []any{map[string]any{"macAddress": mac}}, "status", "inventory", "interfaces"); err != nil {
		t.Fatal(err)
	}
}

func testVsphereAgentForVM(pool *agentforgev1alpha1.VsphereAgentPool, vm agentforgev1alpha1.OwnedVMStatus) *agentforgev1alpha1.VsphereAgent {
	return &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pool.Namespace,
			Name:      vm.Name,
			Labels: map[string]string{
				vsphereAgentPoolNameLabel: pool.Name,
			},
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: pool.Name},
		},
		Status: agentforgev1alpha1.VsphereAgentStatus{
			VM: vm,
		},
	}
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

func infraEnvNameIndexFunc(o client.Object) []string {
	pool := o.(*agentforgev1alpha1.VsphereAgentPool)
	if pool.Spec.InfraEnvRef.Name == "" {
		return nil
	}
	return []string{pool.Spec.InfraEnvRef.Name}
}

func TestRegressionLabelDriftMustNotDeleteLiveWorker(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pool := reconcileTestPool()
	vm := newOwnedVMStatus("live-worker")
	vm.Phase = phaseBound
	vm.AgentRef = agentObjectReference(pool, "live-worker")
	vm.MachineRef = machineObjectReference(pool, "live-worker-machine")
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{vm}
	pool.Spec.Agent.Labels[testCustomerKey] = "new-customer"
	va := testVsphereAgentForVM(pool, vm)
	va.Finalizers = []string{vsphereAgentFinalizerName}
	agent := testAgent(pool.Namespace, "live-worker", true, true)
	machine := testMachine(testControlPlaneNamespace, "live-worker-machine", "demo/demo-worker", false)
	am := testReadyAgentMachine(testControlPlaneNamespace, "live-worker-machine", "demo/demo-worker")
	infra := testInfraEnv(pool.Namespace, testInfraEnvName, "https://example.invalid/iso")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, va, agent, machine, am, infra).WithStatusSubresource(pool, va).Build()
	r := &VsphereAgentPoolReconciler{Client: c}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	var updated agentforgev1alpha1.VsphereAgentPool
	if err := c.Get(ctx, req.NamespacedName, &updated); err != nil {
		t.Fatal(err)
	}
	t.Logf("live worker after label change: %#v", updated.Status.OwnedVMs)
	if len(updated.Status.OwnedVMs) != 1 {
		t.Fatal("missing inventory")
	}
	updated.Status.OwnedVMs[0].LastTransitionTime = metav1.NewTime(time.Now().Add(-time.Hour))
	if err := c.Status().Update(ctx, &updated); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	var remaining agentforgev1alpha1.VsphereAgent
	if err := c.Get(ctx, client.ObjectKeyFromObject(va), &remaining); err != nil {
		t.Fatal(err)
	}
	if !remaining.DeletionTimestamp.IsZero() {
		t.Fatal("live worker selected for VM deletion solely because its Agent no longer matches pool labels")
	}
}

func TestRegressionExpiredDiscoveryNeedsRecovery(t *testing.T) {
	pool := reconcileTestPool()
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{{Name: "stuck-worker", Source: vmSourceVsphereAgent, Phase: phaseProvisioning, Reason: reasonAgentNotDiscovered, LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour))}}
	vms := refreshOwnedVMStatuses(pool, nil, nil)
	plan := buildPlan(pool, PoolSnapshot{AgentMachines: 1, WaitingAgentMachines: 1, AgentMachinesWithoutAgent: 1, OwnedVMs: vms})
	t.Logf("phase=%s reason=%s deficit=%d deletions=%d", vms[0].Phase, vms[0].Reason, plan.DemandDeficit, len(plan.VMsToDelete))
	if len(plan.VMsToDelete) == 0 {
		t.Fatal("expired discovery cannot be replaced while the requesting AgentMachine is still waiting")
	}
}

func TestRegressionWaitForVMFinalizerBeforeDeletingAgent(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pool := reconcileTestPool()
	vm := newOwnedVMStatus("worker")
	vm.Phase = phaseAvailable
	vm.AgentRef = agentObjectReference(pool, "worker")
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{vm}
	va := testVsphereAgentForVM(pool, vm)
	va.Finalizers = []string{vsphereAgentFinalizerName}
	agent := testAgent(pool.Namespace, "worker", false, true)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, va, agent).Build()
	r := &VsphereAgentPoolReconciler{Client: c}
	if err := r.applyPlan(ctx, pool, PoolPlan{VMsToDelete: []agentforgev1alpha1.OwnedVMStatus{vm}, AgentsToDelete: []AgentInfo{{Name: "worker"}}}); err != nil {
		t.Fatal(err)
	}
	var deleting agentforgev1alpha1.VsphereAgent
	if err := c.Get(ctx, client.ObjectKeyFromObject(va), &deleting); err != nil {
		t.Fatal(err)
	}
	if deleting.DeletionTimestamp.IsZero() {
		t.Fatal("expected VM finalizer to be pending")
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(agent), agent); err != nil {
		t.Fatalf("Agent removed while VM finalizer is still pending: %v", err)
	}
}

func TestRegressionPoolMustPreserveNewerISOCondition(t *testing.T) {
	current := agentforgev1alpha1.VsphereAgentPoolStatus{Conditions: []metav1.Condition{{Type: conditionISOReady, Status: metav1.ConditionTrue, Reason: "FreshUpload", LastTransitionTime: metav1.Now()}}}
	desired := agentforgev1alpha1.VsphereAgentPoolStatus{Conditions: []metav1.Condition{{Type: conditionISOReady, Status: metav1.ConditionFalse, Reason: "OldFailure", LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour))}}}
	merged := mergePoolReconcileStatus(current, desired)
	if got := meta.FindStatusCondition(merged.Conditions, conditionISOReady); got.Status != metav1.ConditionTrue {
		t.Fatalf("new ISO condition overwritten by stale pool snapshot: %#v", got)
	}
}

func TestRegressionISOQueryVersionMustInvalidateCache(t *testing.T) {
	pool := reconcileTestPool()
	applySpecDefaults(pool)
	pool.Status.ISO = agentforgev1alpha1.ISOCacheStatus{URL: redactedDownloadURL("https://example.invalid/image?version=4.21"), Path: isoPathPrefix(pool) + "/old.iso", SHA256: "old", CheckedAt: metav1.Now()}
	if !isoCacheDue(pool, "https://example.invalid/image?version=4.22", "", time.Now()) {
		t.Fatal("image version changed but old ISO remains eligible for new VMs")
	}
}

func TestRegressionForceISORefreshWithExistingVM(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pool := reconcileTestPool()
	pool.Annotations = map[string]string{forceISORefreshAnnotation: "refresh-now"}
	va := testVsphereAgentForVM(pool, newOwnedVMStatus("worker"))
	va.Finalizers = []string{vsphereAgentFinalizerName}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: pool.Namespace, Name: "vsphere-credentials"}}
	infra := testInfraEnv(pool.Namespace, testInfraEnvName, "https://example.invalid/iso")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, va, secret, infra).WithStatusSubresource(pool, va).Build()
	provider := &fakeVMProvider{}
	r := &VsphereAgentPoolReconciler{Client: c, ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
		return provider, nil
	}}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)}); err != nil {
		t.Fatal(err)
	}
	if provider.ensureISOCalls == 0 {
		t.Fatal("force refresh annotation is ignored when the pool only has existing VMs")
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pool), pool); err != nil {
		t.Fatal(err)
	}
	if pool.Status.ISO.ForceRefreshToken != "refresh-now" {
		t.Fatal("force refresh was not recorded")
	}
	pool.Status.ISO.CheckedAt = metav1.NewTime(time.Now().Add(-time.Hour))
	if err := c.Status().Update(ctx, pool); err != nil {
		t.Fatal(err)
	}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if provider.ensureISOCalls != 2 {
		t.Fatalf("expired cache was not refreshed: calls=%d", provider.ensureISOCalls)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if provider.ensureISOCalls != 2 {
		t.Fatal("fresh cache was downloaded again before its interval")
	}

}

func TestRegressionAgentMachineNameIsNotMachineName(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pool := reconcileTestPool()
	vm := newOwnedVMStatus("worker")
	vm.Phase = phaseBound
	vm.AgentRef = agentObjectReference(pool, "worker")
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{vm}
	agent := testAgent(pool.Namespace, "worker", true, true)
	labels := agent.GetLabels()
	labels[agentMachineRefKey] = "worker-infra"
	agent.SetLabels(labels)
	am := testReadyAgentMachine(testControlPlaneNamespace, "worker-infra", "demo/demo-worker")
	am.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "cluster.x-k8s.io/v1beta1", Kind: "Machine", Name: "worker-machine"}})
	machine := testMachine(testControlPlaneNamespace, "worker-machine", "demo/demo-worker", true)
	if err := unstructured.SetNestedField(machine.Object, "worker-infra", "spec", "infrastructureRef", "name"); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, agent, am, machine).Build()
	r := &VsphereAgentPoolReconciler{Client: c}
	agents, err := r.listMatchingAgents(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	machines, err := r.listNodePoolMachines(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	vms := refreshOwnedVMStatuses(pool, agents, machines)
	if len(vms) != 1 || vms[0].Reason != reasonMachineDeleting {
		t.Fatalf("Machine is deleting but VM remains bound to the AgentMachine name: %#v", vms)
	}
}

func TestRegressionDeletionGuardMustUseFreshAgent(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pool := reconcileTestPool()
	vm := newOwnedVMStatus("worker")
	vm.Phase = phaseAvailable
	vm.AgentRef = agentObjectReference(pool, "worker")
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{vm}
	va := testVsphereAgentForVM(pool, vm)
	va.Finalizers = []string{vsphereAgentFinalizerName}
	cachedAgent := testAgent(pool.Namespace, "worker", false, true)
	freshAgent := testAgent(pool.Namespace, "worker", true, true)
	cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, va, cachedAgent).Build()
	live := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, va.DeepCopy(), freshAgent).Build()
	r := &VsphereAgentPoolReconciler{Client: cached, APIReader: live}
	if err := r.applyPlan(ctx, pool, PoolPlan{VMsToDelete: []agentforgev1alpha1.OwnedVMStatus{vm}, AgentsToDelete: []AgentInfo{{Name: "worker"}}}); err != nil {
		t.Fatal(err)
	}
	var remaining agentforgev1alpha1.VsphereAgent
	if err := cached.Get(ctx, client.ObjectKeyFromObject(va), &remaining); err != nil {
		t.Fatal(err)
	}
	if !remaining.DeletionTimestamp.IsZero() {
		t.Fatal("VM deletion started although uncached Agent is bound to an AgentMachine")
	}
}

func TestVMFinalizerRetainsAgentUntilDeleteSucceeds(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pool := reconcileTestPool()
	vm := newOwnedVMStatus("worker")
	vm.Phase = phaseAvailable
	vm.AgentRef = agentObjectReference(pool, "worker")
	pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{vm}
	va := testVsphereAgentForVM(pool, vm)
	va.Finalizers = []string{vsphereAgentFinalizerName}
	agent := testAgent(pool.Namespace, "worker", false, true)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: pool.Namespace, Name: "vsphere-credentials"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, va, agent, secret).WithStatusSubresource(pool, va).Build()
	poolController := &VsphereAgentPoolReconciler{Client: c}
	if err := poolController.applyPlan(ctx, pool, PoolPlan{VMsToDelete: []agentforgev1alpha1.OwnedVMStatus{vm}}); err != nil {
		t.Fatal(err)
	}
	provider := &fakeVMProvider{deleteVMErr: fmt.Errorf("vCenter unavailable")}
	controller := &VsphereAgentReconciler{Client: c, ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
		return provider, nil
	}}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(va)}
	if err := c.Get(ctx, client.ObjectKeyFromObject(agent), agent); err != nil {
		t.Fatal(err)
	}
	labels := agent.GetLabels()
	labels[agentMachineRefKey] = "late-reservation"
	agent.SetLabels(labels)
	if err := c.Update(ctx, agent); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Reconcile(ctx, req); err == nil {
		t.Fatal("finalizer ignored a newly bound Agent")
	}
	if provider.deleteVMCalls != 0 {
		t.Fatal("finalizer deleted a newly bound VM")
	}
	delete(labels, agentMachineRefKey)
	agent.SetLabels(labels)
	if err := c.Update(ctx, agent); err != nil {
		t.Fatal(err)
	}

	if _, err := controller.Reconcile(ctx, req); err == nil {
		t.Fatal("expected vCenter deletion failure")
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(agent), agent); err != nil {
		t.Fatal("Agent removed before VM deletion:", err)
	}
	approved, _, _ := unstructured.NestedBool(agent.Object, "spec", "approved")
	if approved || agent.GetAnnotations()[agentCleanupAnnotation] != vm.Name {
		t.Fatal("Agent remains eligible for CAPI reservation during deletion")
	}
	if err := poolController.patchAgent(ctx, pool, agent.GetName(), vm.Name); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(agent), agent); err != nil {
		t.Fatal(err)
	}
	approved, _, _ = unstructured.NestedBool(agent.Object, "spec", "approved")
	if approved {
		t.Fatal("a stale preparation plan reapproved an Agent being deleted")
	}
	provider.deleteVMErr = nil
	if _, err := controller.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(agent), agent); !apierrors.IsNotFound(err) {
		t.Fatalf("Agent after VM deletion: %v", err)
	}
	if err := c.Get(ctx, req.NamespacedName, va); !apierrors.IsNotFound(err) {
		t.Fatalf("VsphereAgent finalizer did not finish: %v", err)
	}
}

func TestDeletionConflictsWithConcurrentCAPIReservation(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pool := reconcileTestPool()
	vm := newOwnedVMStatus("worker")
	vm.AgentRef = agentObjectReference(pool, "worker")
	agent := testAgent(pool.Namespace, "worker", false, true)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(agent).WithInterceptorFuncs(interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			reserved := agentWatchObject()
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), reserved); err != nil {
				return err
			}
			labels := reserved.GetLabels()
			labels[agentMachineRefKey] = "reserved-machine"
			reserved.SetLabels(labels)
			if err := c.Update(ctx, reserved); err != nil {
				return err
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}).Build()
	if err := prepareVMDeletion(ctx, c, c, pool, vm); !apierrors.IsConflict(err) {
		t.Fatalf("deletion must lose to concurrent reservation, got %v", err)
	}
}

func TestAgentMachineRecreatesDeletedVsphereAgent(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := agentforgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pool := reconcileTestPool()
	am := testAgentMachine(testControlPlaneNamespace, "worker", "demo/demo-worker")
	va := newVsphereAgentForAgentMachine(am.GetName(), pool, am)
	va.Finalizers = []string{vsphereAgentFinalizerName}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, am, va).
		WithIndex(&agentforgev1alpha1.VsphereAgentPool{}, vsphereAgentPoolControlPlaneNamespaceIndex, controlPlaneNamespaceIndexFunc).Build()
	controller := &AgentMachineReconciler{Client: c, Scheme: scheme}
	if err := c.Delete(ctx, va); err != nil {
		t.Fatal(err)
	}
	requests := controller.requestsForVsphereAgent(ctx, va)
	if len(requests) != 1 || requests[0].NamespacedName != client.ObjectKeyFromObject(am) {
		t.Fatalf("mapped requests: %v", requests)
	}
	result, err := controller.Reconcile(ctx, requests[0])
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("no retry scheduled while old VsphereAgent is finalizing")
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(va), va); err != nil {
		t.Fatal(err)
	}
	va.Finalizers = nil
	if err := c.Update(ctx, va); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Reconcile(ctx, requests[0]); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(va), va); err != nil {
		t.Fatal("no replacement created:", err)
	}
	if !va.DeletionTimestamp.IsZero() || va.Status.VM.Name != "" {
		t.Fatalf("replacement was not reset: %#v", va)
	}
}

func TestReplacementVMDoesNotInheritExpiredDiscovery(t *testing.T) {
	old := newOwnedVMStatus("worker")
	old.OwnerUID = "old-uid"
	old.Phase = phaseOrphaned
	old.Reason = "AgentDiscoveryExpired"
	replacement := newOwnedVMStatus("worker")
	replacement.OwnerUID = "new-uid"
	merged := mergeOwnedVMStatuses([]agentforgev1alpha1.OwnedVMStatus{old}, []agentforgev1alpha1.OwnedVMStatus{replacement})
	if len(merged) != 1 || merged[0].Phase == phaseOrphaned || merged[0].Reason == old.Reason {
		t.Fatalf("replacement inherited expired state: %#v", merged)
	}
}
