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
	ms := testMachineSet(testControlPlaneNamespace, testNodePool, 5, "demo/demo-worker")
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	agent1 := testAgent(testNamespace, "agent-1", true, true)
	agent2 := testAgent(testNamespace, "agent-2", true, true)
	agent3 := testAgent(testNamespace, "agent-3", true, true)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, ms, infraEnv, agent1, agent2, agent3).
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
	if updated.Status.DesiredReplicas != 5 {
		t.Fatalf("desired replicas = %d, want 5", updated.Status.DesiredReplicas)
	}
	if updated.Status.MatchingAgents != 3 {
		t.Fatalf("matching agents = %d, want 3", updated.Status.MatchingAgents)
	}
	if len(updated.Status.PlannedActions) != 2 {
		t.Fatalf("planned actions = %d, want 2", len(updated.Status.PlannedActions))
	}
	if updated.Status.PlannedActions[0].Type != actionCreateVM {
		t.Fatalf("first action = %s, want %s", updated.Status.PlannedActions[0].Type, actionCreateVM)
	}
}

func TestReconcileReportsMissingMachineSetCondition(t *testing.T) {
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
	condition := findCondition(updated.Status.Conditions, conditionMachineSetFound)
	if condition == nil {
		t.Fatal("MachineSetFound condition was not set")
	}
	if condition.Status != metav1.ConditionFalse {
		t.Fatalf("MachineSetFound status = %s, want False", condition.Status)
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
	ms := testMachineSet(testControlPlaneNamespace, testNodePool, 2, "demo/demo-worker")
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
		WithObjects(pool, ms, infraEnv, secret).
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
	ms := testMachineSet(testControlPlaneNamespace, testNodePool, 5, "demo/demo-worker")
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
		WithObjects(pool, ms, infraEnv, agent1, agent2, agent3, secret).
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
	ms := testMachineSet(testControlPlaneNamespace, testNodePool, 1, "demo/demo-worker")
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	agent := testCandidateAgent(testNamespace, "abcdef12-3456-7890-abcd-ef1234567890")

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, ms, infraEnv, agent).
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
	if !agentHostnamePattern.MatchString(hostname) {
		t.Fatalf("spec.hostname = %q, want demo-worker plus 4 random lowercase alphanumeric characters", hostname)
	}
	labels := updated.GetLabels()
	if labels[roleLabelKey] != testWorkerRole {
		t.Fatalf("role label = %q, want %q", labels[roleLabelKey], testWorkerRole)
	}
	if labels[testCustomerKey] != testCustomer {
		t.Fatalf("customer label = %q, want %q", labels[testCustomerKey], testCustomer)
	}
}

func TestReconcileDeletesExcessUnboundAgents(t *testing.T) {
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
	ms := testMachineSet(testControlPlaneNamespace, testNodePool, 1, "demo/demo-worker")
	infraEnv := testInfraEnv(testNamespace, testInfraEnvName, "https://example.invalid/discovery.iso")
	boundAgent := testAgent(testNamespace, "bound-agent", true, true)
	excessAgent1 := testAgent(testNamespace, "excess-agent-1", false, true)
	excessAgent2 := testAgent(testNamespace, "excess-agent-2", false, true)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, ms, infraEnv, boundAgent, excessAgent1, excessAgent2).
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

	for _, name := range []string{excessAgent1.GetName(), excessAgent2.GetName()} {
		var deleted unstructured.Unstructured
		deleted.SetGroupVersionKind(agentGVK)
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: name}, &deleted)
		if err == nil {
			t.Fatalf("excess Agent %s still exists", name)
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
	createISOPaths []string
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
	return newOwnedVMStatus(fmt.Sprintf("%s-%s-%c", pool.Spec.Template.NamePrefix, time.Now().UTC().Format("150405"), 'a'+req.Ordinal)), nil
}

func (*fakeVMProvider) DeleteVM(context.Context, *agentforgev1alpha1.VsphereAgentPool, agentforgev1alpha1.OwnedVMStatus) error {
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

func testMachineSet(namespace, name string, replicas int64, nodePool string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		testAPIVersionKey: "cluster.x-k8s.io/v1beta1",
		testKindKey:       "MachineSet",
		"spec": map[string]any{
			"replicas": replicas,
		},
	}}
	obj.SetGroupVersionKind(machineSetGVK)
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

func findCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}
