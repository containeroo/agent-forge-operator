//nolint:goconst // Fixtures keep API values visible in assertions.
package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	api "github.com/containeroo/agent-forge-operator/api/v1alpha1"
	osapi "github.com/containeroo/agent-forge-operator/internal/openstack"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type fakeOpenStack struct {
	resources       map[osapi.Kind]map[string]*osapi.Resource
	creates         map[osapi.Kind]int
	deleted         []osapi.Kind
	scope           string
	failCreate      osapi.Kind
	holdAttachments bool
	failDelete      osapi.Kind
	uploads         int
}

func newFakeOpenStack() *fakeOpenStack {
	p := &fakeOpenStack{resources: map[osapi.Kind]map[string]*osapi.Resource{}, creates: map[osapi.Kind]int{}, scope: "example-scope"}
	for _, kind := range []osapi.Kind{osapi.Port, osapi.OSVolume, osapi.DiscoveryVolume, osapi.Server, osapi.Image} {
		p.resources[kind] = map[string]*osapi.Resource{}
	}
	return p
}
func (p *fakeOpenStack) Scope() string { return p.scope }
func (p *fakeOpenStack) Find(_ context.Context, k osapi.Kind, name, owner string) (*osapi.Resource, error) {
	for _, resource := range p.resources[k] {
		if resource.Name == name {
			if resource.Owner != owner {
				return nil, osapi.ErrOwnership
			}
			copy := *resource
			return &copy, nil
		}
	}
	return nil, osapi.ErrNotFound
}
func (p *fakeOpenStack) Get(_ context.Context, k osapi.Kind, id, owner string) (*osapi.Resource, error) {
	r, ok := p.resources[k][id]
	if !ok {
		return nil, osapi.ErrNotFound
	}
	if r.Owner != owner {
		return nil, osapi.ErrOwnership
	}
	copy := *r
	return &copy, nil
}
func (p *fakeOpenStack) Create(_ context.Context, k osapi.Kind, name, owner string, a *api.OpenStackAgent) (*osapi.Resource, error) {
	p.creates[k]++
	resource := &osapi.Resource{ID: fmt.Sprintf("example-%s-%d", k, p.creates[k]), Name: name, Owner: owner, State: "available"}
	switch k {
	case osapi.Port:
		resource.MAC = "02:00:00:00:00:01"
	case osapi.Server:
		resource.State = "ACTIVE"
		p.resources[osapi.OSVolume][a.Status.OSVolumeID].Attached = true
		p.resources[osapi.DiscoveryVolume][a.Status.DiscoveryVolumeID].Attached = true
	case osapi.Image:
		resource.ID = a.Status.ImageID
		resource.State = "queued"
	}
	p.resources[k][resource.ID] = resource
	if p.failCreate == k {
		return nil, &osapi.Error{Reason: "OpenStackRequestFailed", Retryable: true}
	}
	copy := *resource
	return &copy, nil
}
func (p *fakeOpenStack) SetBootable(_ context.Context, id, owner string) error {
	v := p.resources[osapi.OSVolume][id]
	if v.Owner != owner {
		return osapi.ErrOwnership
	}
	v.Bootable = true
	return nil
}
func (p *fakeOpenStack) UploadImage(_ context.Context, id, owner string, data io.Reader) error {
	image := p.resources[osapi.Image][id]
	if image.Owner != owner {
		return osapi.ErrOwnership
	}
	size, err := io.Copy(io.Discard, data)
	if err != nil {
		return err
	}
	image.State = "active"
	image.SizeBytes = size
	p.uploads++
	return nil
}
func (p *fakeOpenStack) Delete(ctx context.Context, k osapi.Kind, id, owner string) error {
	resource, err := p.Get(ctx, k, id, owner)
	if errors.Is(err, osapi.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if resource.Attached || p.failDelete == k {
		return &osapi.Error{Reason: "ResourceStillAttachedOrBusy", Retryable: true}
	}
	p.deleted = append(p.deleted, k)
	delete(p.resources[k], id)
	if k == osapi.Server && !p.holdAttachments {
		for _, kind := range []osapi.Kind{osapi.OSVolume, osapi.DiscoveryVolume} {
			for _, v := range p.resources[kind] {
				v.Attached = false
			}
		}
	}
	return nil
}

type openStackFixture struct {
	c         client.Client
	p         *fakeOpenStack
	a         *api.OpenStackAgent
	am, infra *unstructured.Unstructured
	t         *testing.T
}

func newOpenStackFixture(t *testing.T) *openStackFixture {
	t.Helper()
	iso := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "synthetic discovery ISO") }))
	t.Cleanup(iso.Close)
	spec := api.OpenStackAgentPoolSpec{HostedClusterRef: api.LocalObjectReference{Name: "example-cluster"}, NodePoolRef: api.LocalObjectReference{Name: "example-workers"},
		InfraEnvRef: api.LocalObjectReference{Name: "example-infraenv"}, ControlPlaneNamespace: "example-control-plane",
		OpenStack: api.OpenStackPlacementSpec{CredentialsSecretRef: api.LocalObjectReference{Name: "example-credentials"}, Region: "example-region", NetworkName: "worker-network", FlavorName: "worker-flavor"},
		Agent:     api.AgentBindingSpec{Labels: map[string]string{"example-pool": "workers"}}}
	openStackDefaults(&spec)
	am := testAgentMachine(spec.ControlPlaneNamespace, "example-machine", "example-cluster/example-workers")
	am.SetUID("example-demand-uid")
	infra := testInfraEnv("example-cluster", spec.InfraEnvRef.Name, iso.URL)
	infra.SetUID("example-infraenv-uid")
	a := &api.OpenStackAgent{ObjectMeta: metav1.ObjectMeta{Name: "example-worker", Namespace: "example-cluster", UID: "example-worker-uid", Finalizers: []string{openStackFinalizer}},
		Spec: api.OpenStackAgentSpec{PoolRef: api.LocalObjectReference{Name: "example-pool"}, PoolUID: "example-pool-uid", Configuration: spec,
			AgentMachineRef: corev1.ObjectReference{Namespace: am.GetNamespace(), Name: am.GetName(), UID: am.GetUID()}}}
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	var sequence int
	var mu sync.Mutex
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.OpenStackAgent{}, &api.OpenStackAgentPool{}).
		WithObjects(a, am, infra, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: a.Namespace, Name: "example-credentials"}}).
		WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			mu.Lock()
			sequence++
			if obj.GetUID() == "" {
				obj.SetUID(types.UID(fmt.Sprintf("synthetic-uid-%d", sequence)))
			}
			mu.Unlock()
			return c.Create(ctx, obj, opts...)
		}}).Build()
	return &openStackFixture{c: c, p: newFakeOpenStack(), a: a, am: am, infra: infra, t: t}
}
func (f *openStackFixture) reconciler() *OpenStackAgentReconciler {
	return &OpenStackAgentReconciler{Client: f.c, APIReader: f.c, ProviderFactory: func(context.Context, api.OpenStackPlacementSpec, *corev1.Secret) (osapi.Client, error) {
		return f.p, nil
	}}
}
func (f *openStackFixture) current() *api.OpenStackAgent {
	f.t.Helper()
	a := &api.OpenStackAgent{}
	if err := f.c.Get(context.Background(), client.ObjectKeyFromObject(f.a), a); err != nil {
		f.t.Fatal(err)
	}
	return a
}
func (f *openStackFixture) step() error {
	_, err := f.reconciler().Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.a)})
	return err
}
func (f *openStackFixture) provision() *api.OpenStackAgent {
	f.t.Helper()
	for i := 0; i < 25; i++ {
		if err := f.step(); err != nil {
			f.t.Fatal(err)
		}
		if a := f.current(); a.Status.ServerID != "" {
			return a
		}
	}
	f.t.Fatalf("provisioning stalled: %#v", f.current().Status)
	return nil
}
func (f *openStackFixture) host(a *api.OpenStackAgent) *unstructured.Unstructured {
	h := agentWatchObject()
	h.SetNamespace(a.Namespace)
	h.SetName("example-discovered-agent")
	h.SetUID("example-host-uid")
	h.SetLabels(map[string]string{"infraenvs.agent-install.openshift.io": a.Spec.Configuration.InfraEnvRef.Name})
	h.Object["spec"] = map[string]any{"approved": false}
	h.Object["status"] = map[string]any{"inventory": map[string]any{
		"interfaces": []any{map[string]any{"macAddress": "02:00:00:00:00:01"}},
		"disks": []any{map[string]any{"id": "example-disk-id", "path": "/dev/vda", "serial": a.Status.OSVolumeID, "driveType": "HDD", "installationEligibility": map[string]any{"eligible": true}},
			map[string]any{"id": "example-iso-disk", "path": "/dev/sr0", "serial": a.Status.DiscoveryVolumeID, "driveType": "CDROM"}},
	}}
	return h
}

func TestOpenStackLifecycleKeepsISOAttachedAcrossInstallAndRestart(t *testing.T) {
	f := newOpenStackFixture(t)
	a := f.provision()
	ctx := context.Background()
	h := f.host(a)
	if err := f.c.Create(ctx, h); err != nil {
		t.Fatal(err)
	}
	if err := f.step(); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(h), h); err != nil {
		t.Fatal(err)
	}
	approved, _, _ := unstructured.NestedBool(h.Object, "spec", "approved")
	disk, _, _ := unstructured.NestedString(h.Object, "spec", "installation_disk_id")
	if !approved || disk != "example-disk-id" || h.GetLabels()[agentMachineSelectionLabel] != f.am.GetName() {
		t.Fatalf("Agent not safely prepared: %#v", h.Object)
	}
	labels := h.GetLabels()
	labels[agentMachineRefKey] = f.am.GetName()
	h.SetLabels(labels)
	if err := unstructured.SetNestedSlice(h.Object, []any{map[string]any{"type": "Installed", "status": "True"}}, "status", "conditions"); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Update(ctx, h); err != nil {
		t.Fatal(err)
	}
	before := a.Status
	if err := unstructured.SetNestedField(f.infra.Object, "https://changed.example.invalid/discovery.iso?synthetic-token=not-logged", "status", "isoDownloadURL"); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Update(ctx, f.infra); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := f.step(); err != nil {
			t.Fatal(err)
		}
	} // new reconciler each time
	a = f.current()
	if a.Status.Phase != "Ready" || !meta.IsStatusConditionTrue(a.Status.Conditions, "Ready") {
		t.Fatalf("not ready: %#v", a.Status)
	}
	if a.Status.DiscoveryVolumeID != before.DiscoveryVolumeID || a.Status.OSVolumeID != before.OSVolumeID || a.Status.ImageID != before.ImageID || a.Status.ServerID != before.ServerID || a.Status.PortID != before.PortID {
		t.Fatal("installed worker infrastructure was changed")
	}
	if !f.p.resources[osapi.DiscoveryVolume][a.Status.DiscoveryVolumeID].Attached || len(f.p.deleted) != 0 || f.p.uploads != 1 {
		t.Fatal("discovery media was modified after installation")
	}
	for kind, count := range f.p.creates {
		if count != 1 {
			t.Fatalf("duplicate %s: %d", kind, count)
		}
	}
}

func TestOpenStackCleanupWaitsForCAPIAndVolumeRelease(t *testing.T) {
	f := newOpenStackFixture(t)
	a := f.provision()
	ctx := context.Background()
	if err := f.c.Delete(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := f.step(); err == nil {
		t.Fatal("live AgentMachine must block cleanup")
	}
	if len(f.p.deleted) != 0 {
		t.Fatal("deleted live worker")
	}
	if err := f.c.Delete(ctx, f.am); err != nil {
		t.Fatal(err)
	}
	f.p.holdAttachments = true
	if err := f.step(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(f.p.deleted, []osapi.Kind{osapi.Server}) {
		t.Fatalf("server must be first: %v", f.p.deleted)
	}
	if err := f.step(); err == nil {
		t.Fatal("attached volume cleanup must wait")
	}
	for _, kind := range []osapi.Kind{osapi.OSVolume, osapi.DiscoveryVolume} {
		for _, v := range f.p.resources[kind] {
			v.Attached = false
		}
	}
	f.p.failDelete = osapi.DiscoveryVolume
	if err := f.step(); err == nil {
		t.Fatal("cleanup failure must be retried")
	}
	f.p.failDelete = ""
	for i := 0; i < 12; i++ {
		if err := f.step(); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(a), &api.OpenStackAgent{}); !apierrors.IsNotFound(err) {
		t.Fatalf("finalizer not released: %v", err)
	}
	want := []osapi.Kind{osapi.Server, osapi.DiscoveryVolume, osapi.OSVolume, osapi.Port, osapi.Image}
	if !reflect.DeepEqual(f.p.deleted, want) {
		t.Fatalf("deletion order: %v", f.p.deleted)
	}
}

func TestOpenStackUnknownCreateOutcomeRecoversWithoutDuplicate(t *testing.T) {
	for _, kind := range []osapi.Kind{osapi.Port, osapi.OSVolume, osapi.DiscoveryVolume, osapi.Server} {
		t.Run(string(kind), func(t *testing.T) {
			f := newOpenStackFixture(t)
			f.p.failCreate = kind
			for i := 0; i < 25; i++ {
				if err := f.step(); err != nil {
					break
				}
			}
			a := f.current()
			if a.Status.PendingCreate != string(kind) {
				t.Fatalf("intent not persisted: %#v", a.Status)
			}
			f.p.failCreate = ""
			f.provision()
			if f.p.creates[kind] != 1 || f.current().Status.PendingCreate != "" {
				t.Fatal("create replayed after ambiguous response")
			}
		})
	}
}

func TestOpenStackAmbiguousAbsentResourceIsNotRecreated(t *testing.T) {
	f := newOpenStackFixture(t)
	a := f.current()
	a.Status.ScopeHash = f.p.Scope()
	a.Status.PendingCreate = string(osapi.Server)
	if err := f.c.Status().Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := f.step(); err == nil {
			t.Fatal("ambiguous create should require recovery")
		}
	}
	if len(f.p.creates) != 0 {
		t.Fatal("replayed an ambiguous create")
	}
}

func TestOpenStackMissingInstalledServerIsNeverRebuilt(t *testing.T) {
	f := newOpenStackFixture(t)
	a := f.provision()
	delete(f.p.resources[osapi.Server], a.Status.ServerID)
	for i := 0; i < 3; i++ {
		if err := f.step(); err != nil {
			t.Fatal(err)
		}
	}
	if f.p.creates[osapi.Server] != 1 || f.current().Status.ServerID != a.Status.ServerID {
		t.Fatal("lost worker was rebuilt")
	}
}

func TestOpenStackAgentMatchingAndDiskSafety(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*unstructured.Unstructured)
	}{
		{"wrong disk serial", func(h *unstructured.Unstructured) {
			disks, _, _ := unstructured.NestedSlice(h.Object, "status", "inventory", "disks")
			disks[0].(map[string]any)["serial"] = "unrelated-volume"
			_ = unstructured.SetNestedSlice(h.Object, disks, "status", "inventory", "disks")
		}},
		{"CDROM target", func(h *unstructured.Unstructured) {
			disks, _, _ := unstructured.NestedSlice(h.Object, "status", "inventory", "disks")
			disks[0].(map[string]any)["driveType"] = "CDROM"
			_ = unstructured.SetNestedSlice(h.Object, disks, "status", "inventory", "disks")
		}},
		{"wrong InfraEnv", func(h *unstructured.Unstructured) {
			h.SetLabels(map[string]string{"infraenvs.agent-install.openshift.io": "other-infraenv"})
		}},
		{"foreign binding", func(h *unstructured.Unstructured) {
			labels := h.GetLabels()
			labels[agentMachineRefKey] = "other-demand"
			h.SetLabels(labels)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpenStackFixture(t)
			a := f.provision()
			h := f.host(a)
			tc.mutate(h)
			if err := f.c.Create(context.Background(), h); err != nil {
				t.Fatal(err)
			}
			if err := f.step(); err != nil {
				t.Fatal(err)
			}
			if f.current().Status.Phase != "Failed" {
				t.Fatal("unsafe Agent accepted")
			}
			if err := f.c.Get(context.Background(), client.ObjectKeyFromObject(h), h); err != nil {
				t.Fatal(err)
			}
			approved, _, _ := unstructured.NestedBool(h.Object, "spec", "approved")
			if approved {
				t.Fatal("unsafe Agent approved")
			}
		})
	}
	f := newOpenStackFixture(t)
	a := f.provision()
	h := f.host(a)
	h.Object["status"].(map[string]any)["inventory"].(map[string]any)["interfaces"] = []any{}
	_ = unstructured.SetNestedField(h.Object, a.Status.ServerID, "status", "inventory", "systemVendor", "serialNumber")
	if !agentHardwareMatches(a, h) {
		t.Fatal("BIOS UUID fallback did not match")
	}
}

func TestOpenStackDuplicateAgentsFailClosed(t *testing.T) {
	f := newOpenStackFixture(t)
	a := f.provision()
	for i := 0; i < 2; i++ {
		h := f.host(a)
		h.SetName(fmt.Sprintf("example-host-%d", i))
		h.SetUID(types.UID(h.GetName()))
		if err := f.c.Create(context.Background(), h); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.step(); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(f.current().Status.Conditions, "Ready"); c == nil || c.Reason != "AmbiguousAgentIdentity" {
		t.Fatalf("duplicate identity not rejected: %#v", c)
	}
}

func TestOpenStackRetentionAndScopeProtection(t *testing.T) {
	for _, retainAll := range []bool{false, true} {
		t.Run(fmt.Sprint(retainAll), func(t *testing.T) {
			f := newOpenStackFixture(t)
			a := f.current()
			a.Spec.Configuration.Template.OSVolumePolicy = api.CleanupPolicyRetain
			if retainAll {
				a.Spec.Configuration.CleanupPolicy = api.CleanupPolicyRetain
			}
			if err := f.c.Update(context.Background(), a); err != nil {
				t.Fatal(err)
			}
			a = f.provision()
			if err := f.c.Delete(context.Background(), f.am); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 15; i++ {
				if err := f.step(); err != nil {
					t.Fatal(err)
				}
			}
			if f.p.resources[osapi.OSVolume][a.Status.OSVolumeID] == nil {
				t.Fatal("retained OS volume deleted")
			}
			if retainAll && len(f.p.deleted) != 0 {
				t.Fatal("Retain deleted infrastructure")
			}
			if !retainAll && f.p.resources[osapi.DiscoveryVolume][a.Status.DiscoveryVolumeID] != nil {
				t.Fatal("ISO leaked under OS-only retention")
			}
		})
	}
	f := newOpenStackFixture(t)
	f.provision()
	f.p.scope = "other-project-scope"
	if err := f.step(); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(f.current().Status.Conditions, "Ready"); c.Reason != "CloudScopeChanged" {
		t.Fatal("changed cloud was accepted")
	}
}

func TestOpenStackDiscoveryTimeoutDoesNotDeleteWorker(t *testing.T) {
	f := newOpenStackFixture(t)
	a := f.provision()
	old := metav1.NewTime(time.Now().Add(-time.Hour))
	a.Status.InstanceCreatedAt = &old
	if err := f.c.Status().Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if err := f.step(); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(f.current().Status.Conditions, "Ready"); c.Reason != "AgentDiscoveryTimeout" {
		t.Fatal("discovery timeout not visible")
	}
	if len(f.p.deleted) != 0 {
		t.Fatal("discovery timeout deleted worker")
	}
}

func TestOpenStackSharedCacheReferenceAndGarbageCollection(t *testing.T) {
	f := newOpenStackFixture(t)
	a := f.provision()
	ctx := context.Background()
	r := f.reconciler()
	b := a.DeepCopy()
	b.UID = "example-second-worker-uid"
	b.Name = "example-second-worker"
	if _, err := r.acquireCache(ctx, b); err != nil {
		t.Fatal(err)
	}
	ready, err := r.prepareDiscoveryImage(ctx, f.p, b)
	if err != nil || !ready || f.p.uploads != 1 {
		t.Fatalf("cache not reused: ready=%v err=%v uploads=%d", ready, err, f.p.uploads)
	}
	done, err := r.releaseDiscoveryImage(ctx, f.p, a)
	if err != nil || !done {
		t.Fatalf("release failed: %v", err)
	}
	if len(f.p.resources[osapi.Image]) != 1 {
		t.Fatal("referenced image deleted")
	}
	done, err = r.releaseDiscoveryImage(ctx, f.p, b)
	if err != nil || done {
		t.Fatal("last release must first persist tombstone")
	}
	if _, err := r.acquireCache(ctx, a); err == nil {
		t.Fatal("acquired cache during GC")
	}
	for i := 0; i < 3; i++ {
		if _, err := r.releaseDiscoveryImage(ctx, f.p, b); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.p.resources[osapi.Image]) != 0 {
		t.Fatal("unreferenced image not collected")
	}
	if imageCacheKey("cloud-a", "infraenv", "sha") == imageCacheKey("cloud-b", "infraenv", "sha") || imageCacheKey("cloud-a", "infraenv", "sha") == imageCacheKey("cloud-a", "other-infraenv", "sha") {
		t.Fatal("cache not scoped")
	}
}

func TestOpenStackDemandDispatchAndProviderAmbiguity(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		t.Run(fmt.Sprint(ambiguous), func(t *testing.T) {
			f := newOpenStackFixture(t)
			pool := &api.OpenStackAgentPool{ObjectMeta: metav1.ObjectMeta{Name: "example-pool", Namespace: f.a.Namespace, UID: "example-pool-uid"}, Spec: f.a.Spec.Configuration}
			scheme := f.c.Scheme()
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, f.am).
				WithIndex(&api.VsphereAgentPool{}, vsphereAgentPoolControlPlaneNamespaceIndex, controlPlaneNamespaceIndexFunc)
			if ambiguous {
				builder = builder.WithObjects(&api.VsphereAgentPool{ObjectMeta: metav1.ObjectMeta{Name: "example-vsphere-pool", Namespace: f.a.Namespace}, Spec: api.VsphereAgentPoolSpec{ControlPlaneNamespace: pool.Spec.ControlPlaneNamespace, NodePoolRef: pool.Spec.NodePoolRef}})
			}
			c := builder.Build()
			r := &AgentMachineReconciler{Client: c, Scheme: scheme}
			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.am)})
			if (err != nil) != ambiguous {
				t.Fatalf("unexpected dispatch result: %v", err)
			}
			var workers api.OpenStackAgentList
			if err := c.List(context.Background(), &workers); err != nil {
				t.Fatal(err)
			}
			if ambiguous {
				if len(workers.Items) != 0 {
					t.Fatal("ambiguous pools provisioned")
				}
				return
			}
			if len(workers.Items) != 1 || workers.Items[0].Spec.AgentMachineRef.UID != f.am.GetUID() {
				t.Fatal("demand not pinned")
			}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.am)}); err != nil {
				t.Fatal(err)
			}
			if err := c.List(context.Background(), &workers); err != nil || len(workers.Items) != 1 {
				t.Fatal("duplicate demand provisioned")
			}
		})
	}
}

func TestOpenStackConcurrentCacheAcquisition(t *testing.T) {
	f := newOpenStackFixture(t)
	a := f.provision()
	r := f.reconciler()
	var wg sync.WaitGroup
	failures := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			worker := a.DeepCopy()
			worker.UID = types.UID(fmt.Sprintf("example-concurrent-worker-%d", i))
			for attempt := 0; attempt < 20; attempt++ {
				_, err := r.acquireCache(context.Background(), worker)
				if err == nil {
					return
				}
				if !apierrors.IsConflict(err) && !apierrors.IsAlreadyExists(err) {
					failures <- err
					return
				}
			}
			failures <- errors.New("cache acquisition retry budget exhausted")
		}(i)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	cache := &corev1.ConfigMap{}
	if err := f.c.Get(context.Background(), client.ObjectKey{Namespace: a.Namespace, Name: a.Status.ImageCacheName}, cache); err != nil {
		t.Fatal(err)
	}
	if len(cache.Data) != 9 || f.p.uploads != 1 {
		t.Fatalf("cache lost references: %d", len(cache.Data))
	}
}

func TestOpenStackPoolDeletionWaitsForChildFinalizers(t *testing.T) {
	f := newOpenStackFixture(t)
	ctx := context.Background()
	pool := &api.OpenStackAgentPool{ObjectMeta: metav1.ObjectMeta{Namespace: f.a.Namespace, Name: f.a.Spec.PoolRef.Name, UID: types.UID(f.a.Spec.PoolUID), Finalizers: []string{openStackFinalizer}}, Spec: f.a.Spec.Configuration}
	if err := f.c.Create(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Delete(ctx, pool); err != nil {
		t.Fatal(err)
	}
	r := &OpenStackAgentPoolReconciler{Client: f.c, APIReader: f.c, Scheme: f.c.Scheme()}
	result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)})
	if err != nil || result.RequeueAfter == 0 {
		t.Fatalf("pool did not wait: %v", err)
	}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(pool), pool); err != nil {
		t.Fatal("pool disappeared before child cleanup")
	}
	if f.current().DeletionTimestamp.IsZero() {
		t.Fatal("pool deletion did not request child cleanup")
	}
}

func TestVsphereExcludesOpenStackSecondaryNICIdentity(t *testing.T) {
	f := newOpenStackFixture(t)
	a := f.provision()
	host := f.host(a)
	interfaces, _, _ := unstructured.NestedSlice(host.Object, "status", "inventory", "interfaces")
	interfaces = append([]any{map[string]any{"macAddress": "02:00:00:00:00:02"}}, interfaces...)
	if err := unstructured.SetNestedSlice(host.Object, interfaces, "status", "inventory", "interfaces"); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Create(context.Background(), host); err != nil {
		t.Fatal(err)
	}
	pool := &api.VsphereAgentPool{ObjectMeta: metav1.ObjectMeta{Namespace: a.Namespace}, Spec: api.VsphereAgentPoolSpec{InfraEnvRef: a.Spec.Configuration.InfraEnvRef}}
	agents, err := (&VsphereAgentPoolReconciler{Client: f.c, APIReader: f.c}).listMatchingAgents(context.Background(), pool)
	if err != nil || len(agents) != 0 {
		t.Fatalf("vSphere adopted an OpenStack NIC: %v %v", agents, err)
	}
}

func TestOpenStackStaleAgentUIDAndFailedInstall(t *testing.T) {
	f := newOpenStackFixture(t)
	a := f.provision()
	host := f.host(a)
	ctx := context.Background()
	if err := f.c.Create(ctx, host); err != nil {
		t.Fatal(err)
	}
	if err := f.step(); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(host), host); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedSlice(host.Object, []any{map[string]any{"type": "Installed", "status": "False", "reason": "InstallationFailed"}}, "status", "conditions"); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Update(ctx, host); err != nil {
		t.Fatal(err)
	}
	if err := f.step(); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(f.current().Status.Conditions, "Ready"); c.Reason != "AgentInstallationFailed" {
		t.Fatal("installation failure not reported")
	}
	if err := f.c.Delete(ctx, host); err != nil {
		t.Fatal(err)
	}
	replacement := f.host(a)
	replacement.SetUID("example-replacement-agent-uid")
	if err := f.c.Create(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if err := f.step(); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(f.current().Status.Conditions, "Ready"); c.Reason != "AgentIdentityChanged" {
		t.Fatal("stale Agent reference silently rebound")
	}
}

func TestOpenStackApprovalRejectsInventoryChange(t *testing.T) {
	f := newOpenStackFixture(t)
	a := f.provision()
	host := f.host(a)
	ctx := context.Background()
	if err := f.c.Create(ctx, host); err != nil {
		t.Fatal(err)
	}
	check := agentPatchSafety{UID: host.GetUID(), ResourceVersion: host.GetResourceVersion(), InstallationDiskID: "example-disk-id"}
	host.SetAnnotations(map[string]string{"example-change": "changed-inventory"})
	if err := f.c.Update(ctx, host); err != nil {
		t.Fatal(err)
	}
	if err := patchDiscoveredAgent(ctx, f.c, f.c, a.Namespace, a.Spec.Configuration.Agent, host.GetName(), a.Spec.AgentMachineRef.Name, check); err == nil {
		t.Fatal("approved an Agent changed since disk verification")
	}
}
