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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
)

const (
	vsphereAgentPoolNameLabel       = "agent-forge.containeroo.ch/vsphere-agent-pool"
	vsphereAgentMachineNameLabel    = "agent-forge.containeroo.ch/agent-machine"
	vsphereAgentMachineUIDLabel     = "agent-forge.containeroo.ch/agent-machine-uid"
	vsphereAgentCreatedForLabel     = "agent-forge.containeroo.ch/created-for"
	vsphereAgentCreatedForAdopted   = "adopted"
	vsphereAgentCreatedForDemand    = "agent-machine"
	vsphereAgentFinalizerName       = "agent-forge.containeroo.ch/vsphere-agent"
	vsphereAgentPoolOwnerFieldIndex = ".spec.poolRef.name"
)

// AgentMachineReconciler creates VsphereAgent resources when CAPI reports that
// an AgentMachine cannot find a suitable Assisted Installer Agent.
type AgentMachineReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=capi-provider.agent-install.openshift.io,resources=agentmachines,verbs=get;list;watch
// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=vsphereagentpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=vsphereagents,verbs=get;list;watch;create

func (r *AgentMachineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var agentMachine unstructured.Unstructured
	agentMachine.SetGroupVersionKind(agentMachineGVK)
	if err := r.Get(ctx, req.NamespacedName, &agentMachine); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if agentMachine.GetDeletionTimestamp() != nil || !agentMachineWaitingForAgent(&agentMachine) {
		return ctrl.Result{}, nil
	}

	var pools agentforgev1alpha1.VsphereAgentPoolList
	if err := r.List(ctx, &pools, client.MatchingFields{vsphereAgentPoolControlPlaneNamespaceIndex: req.Namespace}); err != nil {
		return ctrl.Result{}, err
	}
	for i := range pools.Items {
		pool := &pools.Items[i]
		applySpecDefaults(pool)
		if !controlPlaneObjectMatchesPool(&agentMachine, pool) {
			continue
		}
		if err := r.ensureVsphereAgentForAgentMachine(ctx, pool, &agentMachine); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *AgentMachineReconciler) ensureVsphereAgentForAgentMachine(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, agentMachine *unstructured.Unstructured) error {
	exists, err := r.vsphereAgentExistsForAgentMachine(ctx, pool, agentMachine)
	if err != nil || exists {
		return err
	}

	for i := 0; i < 5; i++ {
		name := vsphereAgentNameForAgentMachine(pool, agentMachine)
		agent := &agentforgev1alpha1.VsphereAgent{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: pool.Namespace,
				Name:      name,
				Labels: map[string]string{
					vsphereAgentPoolNameLabel:    pool.Name,
					vsphereAgentMachineNameLabel: agentMachine.GetName(),
					vsphereAgentMachineUIDLabel:  string(agentMachine.GetUID()),
					vsphereAgentCreatedForLabel:  vsphereAgentCreatedForDemand,
				},
			},
			Spec: agentforgev1alpha1.VsphereAgentSpec{
				PoolRef: agentforgev1alpha1.LocalObjectReference{Name: pool.Name},
			},
		}
		if err := controllerutil.SetControllerReference(pool, agent, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, agent); err != nil {
			if apierrors.IsAlreadyExists(err) {
				continue
			}
			return err
		}
		return nil
	}
	return apierrors.NewAlreadyExists(agentforgev1alpha1.GroupVersion.WithResource("vsphereagents").GroupResource(), vsphereAgentNameForAgentMachine(pool, agentMachine))
}

func (r *AgentMachineReconciler) vsphereAgentExistsForAgentMachine(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, agentMachine *unstructured.Unstructured) (bool, error) {
	labels := map[string]string{
		vsphereAgentPoolNameLabel:    pool.Name,
		vsphereAgentMachineNameLabel: agentMachine.GetName(),
	}
	if agentMachine.GetUID() != "" {
		labels[vsphereAgentMachineUIDLabel] = string(agentMachine.GetUID())
	}
	var list agentforgev1alpha1.VsphereAgentList
	if err := r.List(ctx, &list, client.InNamespace(pool.Namespace), client.MatchingLabels(labels)); err != nil {
		return false, err
	}
	return len(list.Items) > 0, nil
}

func vsphereAgentNameForAgentMachine(pool *agentforgev1alpha1.VsphereAgentPool, agentMachine *unstructured.Unstructured) string {
	return desiredAgentHostname(pool)
}

func (r *AgentMachineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(agentMachineWatchObject()).
		Named("agentmachine").
		Complete(r)
}
