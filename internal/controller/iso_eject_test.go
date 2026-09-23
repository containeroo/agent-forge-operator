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

//nolint:goconst // Keep lifecycle and provider fixtures readable.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
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

func TestEjectInstalledAgentISO(t *testing.T) {
	for _, tc := range []struct {
		name, status, state       string
		mismatch, missing, fail   bool
		released, deletingMachine bool
		disabled                  bool
		want                      int
	}{
		{name: "opt-in omitted", status: "True", disabled: true},
		{name: "opt-in false", status: "True", disabled: true},
		{name: "installed", status: "True", state: "installed", want: 1},
		{name: "added worker", status: "True", state: "added-to-existing-cluster", want: 1},
		{name: "bound but installing", status: "False", state: "installing"},
		{name: "unknown", status: "Unknown"},
		{name: "no condition", state: "installed"},
		{name: "missing agent", missing: true},
		{name: "released VM with stale Installed", status: "True", released: true},
		{name: "deleting Machine with stale Installed", status: "True", deletingMachine: true},
		{name: "wrong hardware", status: "True", mismatch: true},
		{name: "retry failed eject", status: "True", fail: true, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := reconcileTestPool()
			pool.Spec.ISO.EjectAfterInstall = !tc.disabled
			host := testAgent(testNamespace, "host", true, true)
			if tc.status != "" {
				_ = unstructured.SetNestedSlice(host.Object, []any{map[string]any{"type": "Installed", "status": tc.status}}, "status", "conditions")
			}
			_ = unstructured.SetNestedField(host.Object, tc.state, "status", "debugInfo", "state")
			mac := "00:50:56:aa:bb:cc"
			if tc.mismatch {
				mac = "00:50:56:00:00:00"
			}
			setAgentPrimaryMAC(t, host, mac)
			scheme := runtime.NewScheme()
			_ = agentforgev1alpha1.AddToScheme(scheme)
			_ = corev1.AddToScheme(scheme)
			va := &agentforgev1alpha1.VsphereAgent{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: testNamespace, UID: types.UID("owner"), Finalizers: []string{vsphereAgentFinalizerName}}, Spec: agentforgev1alpha1.VsphereAgentSpec{PoolRef: agentforgev1alpha1.LocalObjectReference{Name: pool.Name}}, Status: agentforgev1alpha1.VsphereAgentStatus{VM: agentforgev1alpha1.OwnedVMStatus{Name: "worker", AgentRef: agentObjectReference(pool, "host")}}}
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: pool.Spec.VSphere.CredentialsSecretRef.Name, Namespace: pool.Namespace}}
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, va, secret).WithStatusSubresource(va, pool)
			if !tc.missing {
				builder = builder.WithObjects(host)
			}
			if tc.released {
				va.Status.VM.Phase = phaseReleased
			}
			if tc.deletingMachine {
				machine := machineWatchObject()
				machine.SetNamespace(pool.Spec.ControlPlaneNamespace)
				machine.SetName("worker-machine")
				now := metav1.Now()
				machine.SetDeletionTimestamp(&now)
				machine.SetFinalizers([]string{"test/draining"})
				va.Status.VM.MachineRef = machineObjectReference(pool, machine.GetName())
				builder = builder.WithObjects(machine)
			}
			c := builder.Build()
			provider := &fakeVMProvider{}
			if tc.fail {
				provider.ejectISOErr = errors.New("locked")
			}
			r := &VsphereAgentReconciler{Client: c, ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
				return provider, nil
			}}
			req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: va.Namespace, Name: va.Name}}
			_, err := r.Reconcile(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if provider.ejectISOCalls != 0 {
				t.Fatal("ejected before operation was persisted")
			}
			if tc.want > 0 {
				_, err = r.Reconcile(context.Background(), req)
			}

			if (err != nil) != tc.fail {
				t.Fatalf("error=%v", err)
			}
			if tc.disabled && provider.prepareISOCalls != 0 {
				t.Fatal("disabled pool prepared an ejection")
			}
			if provider.ejectISOCalls != tc.want {
				t.Fatalf("ejections=%d want %d", provider.ejectISOCalls, tc.want)
			}
			if tc.fail {
				provider.ejectISOErr = nil
				if _, err = r.Reconcile(context.Background(), req); err != nil {
					t.Fatal(err)
				}
				if provider.ejectISOCalls != 2 {
					t.Fatal("failed eject was not retried")
				}
			}
			var updated agentforgev1alpha1.VsphereAgent
			if err = c.Get(context.Background(), req.NamespacedName, &updated); err != nil {
				t.Fatal(err)
			}
			if tc.want > 0 && !meta.IsStatusConditionTrue(updated.Status.Conditions, "ISOEjected") {
				t.Fatal("missing successful ejection condition")
			}
		})
	}
}

func TestGovcEjectISO(t *testing.T) {
	const uuid = "423297c6-d72e-28bb-b279-1209c29ab72b"
	digest := strings.Repeat("a", 64)
	for _, tc := range []struct {
		name, path, owner         string
		locked, unrelatedQuestion bool
		wantCalls                 bool
		wantErr                   bool
	}{
		{name: "cached ISO", path: "[iso-datastore] agent-forge/demo/demo-worker/" + digest + ".iso", owner: "owner", wantCalls: true},
		{name: "locked ISO", path: "[iso-datastore] agent-forge/demo/demo-worker/" + digest + ".iso", owner: "owner", locked: true, wantCalls: true},
		{name: "other media", path: "[iso-datastore] tools.iso", owner: "owner"},
		{name: "other pool", path: "[iso-datastore] agent-forge/other/" + digest + ".iso", owner: "owner"},
		{name: "empty drive", owner: "owner"},
		{name: "replaced VM", owner: "other", wantErr: true},
		{name: "unrelated question", path: "[iso-datastore] agent-forge/demo/demo-worker/" + digest + ".iso", owner: "owner", unrelatedQuestion: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("EJECT_TEST_DIR", dir)
			info := map[string]any{"virtualMachines": []any{map[string]any{"config": map[string]any{"uuid": uuid, "annotation": vmOwnerAnnotationPrefix + tc.owner, "hardware": map[string]any{"device": []any{map[string]any{"key": 3000, "backing": map[string]any{"fileName": tc.path}}}}}}}}
			data, _ := json.Marshal(info)
			_ = os.WriteFile(filepath.Join(dir, "info.json"), data, 0600)
			question := `{"virtualMachines":[{"runtime":{"question":{"id":"question-1","message":[{"id":"msg.cdromdisconnect.locked"}],"choice":{"choiceInfo":[{"key":"0","label":"No"},{"key":"1","label":"Yes"}]}}}}]}`
			if tc.unrelatedQuestion {
				question = strings.ReplaceAll(question, "msg.cdromdisconnect.locked", "msg.unrelated")
			}
			_ = os.WriteFile(filepath.Join(dir, "question.json"), []byte(question), 0600)
			if tc.locked || tc.unrelatedQuestion {
				_ = os.WriteFile(filepath.Join(dir, "lock"), nil, 0600)
			}
			script := `#!/bin/sh
printf '%s\n' "$*" >> "$EJECT_TEST_DIR/calls"
case "$1" in
vm.info)
 if test -f "$EJECT_TEST_DIR/ejected"; then echo '{"virtualMachines":[{}]}'; elif test -f "$EJECT_TEST_DIR/pending"; then cat "$EJECT_TEST_DIR/question.json"; else cat "$EJECT_TEST_DIR/info.json"; fi;;
device.disconnect)
 if test -f "$EJECT_TEST_DIR/lock"; then
  touch "$EJECT_TEST_DIR/pending"
  while test ! -f "$EJECT_TEST_DIR/answered"; do sleep 0.1; done
 fi;;
device.cdrom.eject) touch "$EJECT_TEST_DIR/ejected";;
vm.question) touch "$EJECT_TEST_DIR/answered"; rm "$EJECT_TEST_DIR/pending";;
esac
`
			cmd := filepath.Join(dir, "govc")
			_ = os.WriteFile(cmd, []byte(script), 0700)
			p := &govcVMProvider{command: cmd}
			op, err := p.PrepareISOEjection(context.Background(), providerTestPool(), agentforgev1alpha1.OwnedVMStatus{Name: "worker", BIOSUUID: uuid, OwnerUID: "owner"})
			if err == nil && op != nil {
				err = p.EjectISO(context.Background(), providerTestPool(), op, func() error { return nil })
			}

			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v", err)
			}
			calls, _ := os.ReadFile(filepath.Join(dir, "calls"))
			out := string(calls)
			if strings.Contains(out, "device.cdrom.eject") != tc.wantCalls {
				t.Fatalf("unexpected calls:\n%s", out)
			}
			if tc.locked && !strings.Contains(out, "vm.question -dc dc1 -vm.uuid "+uuid+" -answer 1") {
				t.Fatalf("lock not answered: %s", out)
			}
			if tc.unrelatedQuestion && strings.Contains(out, "vm.question") {
				t.Fatal("answered unrelated question")
			}
		})
	}
}

// The operator's deletion guards depend on Machine existence and Agent bindings,
// not on a CD attachment or a return to a particular discovery state.
func TestDeleteAfterISOEjection(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = agentforgev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	pool := reconcileTestPool()
	vm := newOwnedVMStatus("worker")
	vm.Phase = phaseReleased
	vm.Reason = reasonMachineDeleted
	vm.AgentRef = agentObjectReference(pool, "worker")
	vm.MachineRef = machineObjectReference(pool, "worker-machine")
	va := testVsphereAgentForVM(pool, vm)
	va.Finalizers = []string{vsphereAgentFinalizerName}
	va.Status.Conditions = []metav1.Condition{{Type: "ISOEjected", Status: metav1.ConditionTrue, Reason: "InstallationCompleted", LastTransitionTime: metav1.Now()}}
	host := testAgent(pool.Namespace, "worker", false, true)
	_ = unstructured.SetNestedField(host.Object, "unbinding-pending-user-action", "status", "debugInfo", "state")
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: pool.Namespace, Name: pool.Spec.VSphere.CredentialsSecretRef.Name}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, va, host, secret).WithStatusSubresource(va, pool).Build()
	if err := c.Delete(ctx, va); err != nil {
		t.Fatal(err)
	}
	provider := &fakeVMProvider{}
	r := &VsphereAgentReconciler{Client: c, ProviderFactory: func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
		return provider, nil
	}}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: va.Namespace, Name: va.Name}}); err != nil {
		t.Fatal(err)
	}
	if provider.deleteVMCalls != 1 || provider.ejectISOCalls != 0 {
		t.Fatalf("delete=%d eject=%d", provider.deleteVMCalls, provider.ejectISOCalls)
	}
}

func TestISOEjectionResumesAfterRestartAndOptOut(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = agentforgev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	pool := reconcileTestPool()
	pool.Spec.ISO.EjectAfterInstall = true
	host := testAgent(pool.Namespace, "worker", true, true)
	setAgentPrimaryMAC(t, host, "00:50:56:aa:bb:cc")
	_ = unstructured.SetNestedSlice(host.Object, []any{map[string]any{"type": "Installed", "status": "True"}}, "status", "conditions")
	vm := newOwnedVMStatus("worker")
	vm.AgentRef = agentObjectReference(pool, "worker")
	va := testVsphereAgentForVM(pool, vm)
	va.Finalizers = []string{vsphereAgentFinalizerName}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: pool.Namespace, Name: pool.Spec.VSphere.CredentialsSecretRef.Name}}
	rejectStatus := true
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, va, host, secret).WithStatusSubresource(va, pool).WithInterceptorFuncs(interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if rejectStatus {
				return errors.New("status write failed")
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	}).Build()
	provider := &fakeVMProvider{}
	factory := func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error) {
		return provider, nil
	}
	r := &VsphereAgentReconciler{Client: c, ProviderFactory: factory}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: va.Namespace, Name: va.Name}}
	if _, err := r.Reconcile(ctx, req); err == nil {
		t.Fatal("expected failed status checkpoint")
	}
	if err := c.Get(ctx, req.NamespacedName, va); err != nil {
		t.Fatal(err)
	}
	if va.Status.ISOEjection != nil || provider.ejectISOCalls != 0 {
		t.Fatal("unpersisted operation was executed")
	}
	rejectStatus = false
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, req.NamespacedName, va); err != nil {
		t.Fatal(err)
	}
	if va.Status.ISOEjection == nil || provider.ejectISOCalls != 0 {
		t.Fatal("operation must be persisted before mutation")
	}
	pool.Spec.ISO.EjectAfterInstall = false
	if err := c.Update(ctx, pool); err != nil {
		t.Fatal(err)
	}
	// The installation Agent can disappear during scale-down; recovery must not
	// depend on re-entering the installation gate or bypass deletion safeguards.
	if err := c.Delete(ctx, host); err != nil {
		t.Fatal(err)
	}
	r = &VsphereAgentReconciler{Client: c, ProviderFactory: factory}
	provider.ejectISOErr = errors.New("transient vCenter failure")
	if _, err := r.Reconcile(ctx, req); err == nil {
		t.Fatal("expected retryable failure")
	}
	if err := c.Get(ctx, req.NamespacedName, va); err != nil {
		t.Fatal(err)
	}
	if va.Status.ISOEjection == nil {
		t.Fatal("lost recovery record on failure")
	}
	provider.ejectISOErr = nil
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, req.NamespacedName, va); err != nil {
		t.Fatal(err)
	}
	if va.Status.ISOEjection != nil || !meta.IsStatusConditionTrue(va.Status.Conditions, "ISOEjected") {
		t.Fatal("recovery did not complete")
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if provider.prepareISOCalls != 2 || provider.ejectISOCalls != 2 {
		t.Fatalf("prepare=%d eject=%d", provider.prepareISOCalls, provider.ejectISOCalls)
	}
}

func TestGovcISOEjectionRecovery(t *testing.T) {
	const uuid = "423297c6-d72e-28bb-b279-1209c29ab72b"
	for _, tc := range []struct {
		name                                                                 string
		started, changedMedia, unrelated, changedQuestion, checkpointFailure bool
		noQuestion                                                           bool
		wantErr                                                              bool
	}{
		{name: "restart before observing lock", started: true},
		{name: "restart after persisting question", started: true},
		{name: "question before start", wantErr: true},
		{name: "different media", started: true, changedMedia: true, wantErr: true},
		{name: "unrelated question", started: true, unrelated: true, wantErr: true},
		{name: "different question ID", started: true, changedQuestion: true, wantErr: true},
		{name: "disconnect checkpoint failure", noQuestion: true, checkpointFailure: true, wantErr: true},
		{name: "question checkpoint failure", started: true, checkpointFailure: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("RECOVERY_TEST_DIR", dir)
			op := &agentforgev1alpha1.ISOEjectionStatus{VMName: "worker", BIOSUUID: uuid, OwnerUID: "owner", Datacenter: "original-dc", DeviceKey: 3000, ISOPath: "[datastore] original.iso", DisconnectStarted: tc.started}
			if tc.changedQuestion {
				op.QuestionID = "older-question"
			}
			if tc.name == "restart after persisting question" {
				op.QuestionID = "question-1"
			}
			filename := op.ISOPath
			if tc.changedMedia {
				filename = "[datastore] other.iso"
			}
			messageID := "msg.cdromdisconnect.locked"
			if tc.unrelated {
				messageID = "msg.other"
			}
			question := map[string]any{"id": "question-1", "message": []any{map[string]any{"id": messageID}}, "choice": map[string]any{"choiceInfo": []any{map[string]any{"key": "1", "label": "Yes"}}}}
			vm := map[string]any{"config": map[string]any{"uuid": uuid, "annotation": vmOwnerAnnotationPrefix + "owner", "hardware": map[string]any{"device": []any{map[string]any{"key": 3000, "backing": map[string]any{"fileName": filename}}}}}, "runtime": map[string]any{"question": question}}
			if tc.noQuestion {
				delete(vm, "runtime")
			}
			info := map[string]any{"virtualMachines": []any{vm}}
			data, _ := json.Marshal(info)
			_ = os.WriteFile(filepath.Join(dir, "pending.json"), data, 0600)
			delete(vm, "runtime")
			data, _ = json.Marshal(info)
			_ = os.WriteFile(filepath.Join(dir, "info.json"), data, 0600)
			vm["config"].(map[string]any)["hardware"] = map[string]any{"device": []any{}}
			data, _ = json.Marshal(info)
			_ = os.WriteFile(filepath.Join(dir, "done.json"), data, 0600)
			script := `#!/bin/sh
printf '%s\n' "$*" >> "$RECOVERY_TEST_DIR/calls"
case "$1" in
vm.info)
 if test -f "$RECOVERY_TEST_DIR/ejected"; then cat "$RECOVERY_TEST_DIR/done.json";
 elif test -f "$RECOVERY_TEST_DIR/answered"; then cat "$RECOVERY_TEST_DIR/info.json";
 else cat "$RECOVERY_TEST_DIR/pending.json"; fi;;
vm.question) touch "$RECOVERY_TEST_DIR/answered";;
device.cdrom.eject) touch "$RECOVERY_TEST_DIR/ejected";;
esac
`
			cmd := filepath.Join(dir, "govc")
			_ = os.WriteFile(cmd, []byte(script), 0700)
			p := &govcVMProvider{command: cmd}
			checkpoints := 0
			err := p.EjectISO(context.Background(), providerTestPool(), op, func() error {
				checkpoints++
				if !tc.noQuestion && op.QuestionID != "question-1" {
					t.Fatal("question identity not checkpointed")
				}
				if tc.checkpointFailure {
					return errors.New("API unavailable")
				}
				return nil
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v", err)
			}
			calls, _ := os.ReadFile(filepath.Join(dir, "calls"))
			out := string(calls)
			if tc.wantErr && (strings.Contains(out, "vm.question") || strings.Contains(out, "device.disconnect")) {
				t.Fatalf("unsafe recovery mutation:\n%s", out)
			}
			if !tc.wantErr {
				if !strings.Contains(out, "vm.question -dc original-dc -vm.uuid "+uuid+" -answer 1") {
					t.Fatalf("did not recover recorded target: %s", out)
				}
				if tc.name == "restart before observing lock" && checkpoints != 1 {
					t.Fatal("question not durably checkpointed")
				}
				if err := p.EjectISO(context.Background(), providerTestPool(), op, func() error { return nil }); err != nil {
					t.Fatalf("idempotent recovery: %v", err)
				}
			}
		})
	}
}
