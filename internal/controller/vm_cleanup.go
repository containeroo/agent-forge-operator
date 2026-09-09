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
	"context"
	"errors"
	"fmt"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var errVMDeletionBlocked = errors.New("VM deletion blocked")

const agentCleanupAnnotation = "agent-forge.containeroo.ch/cleanup-vm"

// Read without label filters: changing pool selectors must never hide a bound worker.
func agentsForVM(ctx context.Context, reader client.Reader, pool *agentforgev1alpha1.VsphereAgentPool, vm agentforgev1alpha1.OwnedVMStatus) ([]unstructured.Unstructured, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(agentGVK)
	if err := reader.List(ctx, list, client.InNamespace(pool.Namespace)); err != nil {
		return nil, err
	}
	var agents []unstructured.Unstructured
	for _, obj := range list.Items {
		serial, _, _ := unstructured.NestedString(obj.Object, "status", "inventory", "systemVendor", "serialNumber")
		hostname, _, _ := unstructured.NestedString(obj.Object, "spec", "hostname")
		inventoryHostname, _, _ := unstructured.NestedString(obj.Object, "status", "inventory", "hostname")
		info := AgentInfo{Name: obj.GetName(), BIOSUUID: normalizeVMwareSerialUUID(serial), MAC: normalizeMAC(agentPrimaryMAC(&obj)), Hostname: hostname, InventoryHostname: inventoryHostname}
		if vmMatchesAgentIdentity(vm, info) || vmMatchesAgentRef(vm, info) || (vm.Name == agentObservedHostname(info) && !vmIdentityConflictsAgent(vm, info)) {
			agents = append(agents, obj)
		}
	}
	return agents, nil
}

// Unapproval plus an optimistic lock excludes new CAPI reservations. An already
// in-flight reservation either wins first (and blocks us) or gets a conflict.
func prepareVMDeletion(ctx context.Context, c client.Client, reader client.Reader, pool *agentforgev1alpha1.VsphereAgentPool, vm agentforgev1alpha1.OwnedVMStatus) error {
	if vm.MachineRef != nil && vm.MachineRef.Name != "" {
		machine := machineWatchObject()
		key := client.ObjectKey{Namespace: vm.MachineRef.Namespace, Name: vm.MachineRef.Name}
		if key.Namespace == "" {
			key.Namespace = pool.Spec.ControlPlaneNamespace
		}
		if err := reader.Get(ctx, key, machine); err == nil {
			return fmt.Errorf("%w: VM %s still has Machine %s", errVMDeletionBlocked, vm.Name, key)
		} else if !apierrors.IsNotFound(err) {
			return err
		}
	}
	agents, err := agentsForVM(ctx, reader, pool, vm)
	if err != nil {
		return err
	}
	for i := range agents {
		agent := &agents[i]
		if blocker := agentDeletionBlocker(agent); blocker != "" {
			return fmt.Errorf("%w: VM %s: Agent %s %s", errVMDeletionBlocked, vm.Name, agent.GetName(), blocker)
		}
		before := agent.DeepCopy()
		if err := unstructured.SetNestedField(agent.Object, false, "spec", "approved"); err != nil {
			return err
		}
		annotations := agent.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[agentCleanupAnnotation] = vm.Name
		agent.SetAnnotations(annotations)
		if err := c.Patch(ctx, agent, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
	}
	return nil
}

// Called only after the provider confirms VM deletion. Keep the Agent until
// then so failed finalization remains observable and cannot create fresh demand.
func deleteVMAgents(ctx context.Context, c client.Client, reader client.Reader, pool *agentforgev1alpha1.VsphereAgentPool, vm agentforgev1alpha1.OwnedVMStatus) error {
	agents, err := agentsForVM(ctx, reader, pool, vm)
	if err != nil {
		return err
	}
	for i := range agents {
		agent := &agents[i]
		if blocker := agentDeletionBlocker(agent); blocker != "" {
			return fmt.Errorf("cannot delete Agent %s: %s", agent.GetName(), blocker)
		}
		uid, version := agent.GetUID(), agent.GetResourceVersion()
		if err := c.Delete(ctx, agent, client.Preconditions{UID: &uid, ResourceVersion: &version}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
