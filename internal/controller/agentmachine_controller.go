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
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
	name := vsphereAgentNameForAgentMachine(pool, agentMachine)
	var existing agentforgev1alpha1.VsphereAgent
	err := r.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: name}, &existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

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
	return r.Create(ctx, agent)
}

func vsphereAgentNameForAgentMachine(pool *agentforgev1alpha1.VsphereAgentPool, agentMachine *unstructured.Unstructured) string {
	uid := string(agentMachine.GetUID())
	if uid == "" {
		uid = fmt.Sprintf("%s/%s", agentMachine.GetNamespace(), agentMachine.GetName())
	}
	sum := sha256.Sum256([]byte(uid))
	suffix := hex.EncodeToString(sum[:])[:10]
	prefix := pool.Name
	maxPrefixLen := 63 - len(suffix) - 1
	if len(prefix) > maxPrefixLen {
		prefix = prefix[:maxPrefixLen]
	}
	return fmt.Sprintf("%s-%s", prefix, suffix)
}

func (r *AgentMachineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(agentMachineWatchObject()).
		Named("agentmachine").
		Complete(r)
}
