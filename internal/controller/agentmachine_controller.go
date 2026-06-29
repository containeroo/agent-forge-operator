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
	"strings"

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
	vsphereAgentCreatedForLabel     = "agent-forge.containeroo.ch/created-for"
	vsphereAgentCreatedForAdopted   = "adopted"
	vsphereAgentCreatedForDemand    = "agent-machine"
	vsphereAgentFinalizerName       = "agent-forge.containeroo.ch/vsphere-agent"
	vsphereAgentPoolOwnerFieldIndex = ".spec.poolRef.name"
	agentMachineSelectionLabel      = "agent-forge.containeroo.ch/agent-machine"
	vsphereAgentVMNameAnnotation    = "agent-forge.containeroo.ch/vm-name"
)

// AgentMachineReconciler creates VsphereAgent resources when CAPI reports that
// an AgentMachine cannot find a suitable Assisted Installer Agent.
type AgentMachineReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=capi-provider.agent-install.openshift.io,resources=agentmachines,verbs=get;list;watch;patch
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
	var matchingPools []*agentforgev1alpha1.VsphereAgentPool
	for i := range pools.Items {
		pool := &pools.Items[i]
		if !pool.DeletionTimestamp.IsZero() {
			continue
		}
		applySpecDefaults(pool)
		if !controlPlaneObjectMatchesPool(&agentMachine, pool) {
			continue
		}
		matchingPools = append(matchingPools, pool)
	}
	if len(matchingPools) == 0 {
		return ctrl.Result{}, nil
	}
	if len(matchingPools) > 1 {
		return ctrl.Result{}, fmt.Errorf("AgentMachine %s/%s matches multiple VsphereAgentPools: %s", agentMachine.GetNamespace(), agentMachine.GetName(), matchingPoolNames(matchingPools))
	}
	pool := matchingPools[0]
	if err := ensureAgentMachineSelectorPinned(ctx, r.Client, &agentMachine); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureVsphereAgentForAgentMachine(ctx, pool, &agentMachine); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func ensureAgentMachineSelectorPinned(ctx context.Context, c client.Client, agentMachine *unstructured.Unstructured) error {
	matchLabels, _, err := unstructured.NestedStringMap(agentMachine.Object, "spec", "agentLabelSelector", "matchLabels")
	if err != nil {
		return err
	}
	if matchLabels == nil {
		matchLabels = map[string]string{}
	}
	if matchLabels[agentMachineSelectionLabel] == agentMachine.GetName() {
		return nil
	}

	before := agentMachine.DeepCopy()
	matchLabels[agentMachineSelectionLabel] = agentMachine.GetName()
	if err := unstructured.SetNestedStringMap(agentMachine.Object, matchLabels, "spec", "agentLabelSelector", "matchLabels"); err != nil {
		return err
	}
	return c.Patch(ctx, agentMachine, client.MergeFrom(before))
}

func (r *AgentMachineReconciler) ensureVsphereAgentForAgentMachine(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, agentMachine *unstructured.Unstructured) error {
	if existing, err := r.findVsphereAgentForAgentMachine(ctx, pool, agentMachine); err != nil {
		return err
	} else if existing != nil {
		return nil
	}

	for _, name := range []string{agentMachine.GetName(), alternateVsphereAgentName(pool, agentMachine)} {
		agent := newVsphereAgentForAgentMachine(name, pool, agentMachine)
		if err := controllerutil.SetControllerReference(pool, agent, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, agent); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return err
			}
			var existing agentforgev1alpha1.VsphereAgent
			if getErr := r.Get(ctx, client.ObjectKey{Namespace: agent.Namespace, Name: agent.Name}, &existing); getErr != nil {
				return getErr
			}
			if vsphereAgentCreatedForAgentMachine(&existing, pool, agentMachine) {
				return nil
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("could not create VsphereAgent for AgentMachine %s/%s because deterministic names are already used by other demand", agentMachine.GetNamespace(), agentMachine.GetName())
}

func (r *AgentMachineReconciler) findVsphereAgentForAgentMachine(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, agentMachine *unstructured.Unstructured) (*agentforgev1alpha1.VsphereAgent, error) {
	var list agentforgev1alpha1.VsphereAgentList
	if err := r.List(ctx, &list, client.InNamespace(pool.Namespace)); err != nil {
		return nil, err
	}
	for i := range list.Items {
		agent := &list.Items[i]
		if agent.GetDeletionTimestamp() != nil {
			continue
		}
		if vsphereAgentCreatedForAgentMachine(agent, pool, agentMachine) {
			return agent, nil
		}
	}
	return nil, nil
}

func newVsphereAgentForAgentMachine(name string, pool *agentforgev1alpha1.VsphereAgentPool, agentMachine *unstructured.Unstructured) *agentforgev1alpha1.VsphereAgent {
	return &agentforgev1alpha1.VsphereAgent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pool.Namespace,
			Name:      name,
			Labels: map[string]string{
				vsphereAgentPoolNameLabel:   pool.Name,
				vsphereAgentCreatedForLabel: vsphereAgentCreatedForDemand,
				agentMachineSelectionLabel:  agentMachine.GetName(),
			},
			Annotations: map[string]string{
				vsphereAgentVMNameAnnotation: agentMachine.GetName(),
			},
		},
		Spec: agentforgev1alpha1.VsphereAgentSpec{
			PoolRef: agentforgev1alpha1.LocalObjectReference{Name: pool.Name},
		},
	}
}

func vsphereAgentCreatedForAgentMachine(agent *agentforgev1alpha1.VsphereAgent, pool *agentforgev1alpha1.VsphereAgentPool, agentMachine *unstructured.Unstructured) bool {
	if !vsphereAgentBelongsToPool(agent, pool.Name) {
		return false
	}
	if agent.Labels[agentMachineSelectionLabel] == agentMachine.GetName() {
		return true
	}
	if agent.Annotations[vsphereAgentVMNameAnnotation] == agentMachine.GetName() {
		return true
	}
	return agent.Labels[vsphereAgentCreatedForLabel] == vsphereAgentCreatedForDemand && agent.Name == agentMachine.GetName()
}

func alternateVsphereAgentName(pool *agentforgev1alpha1.VsphereAgentPool, agentMachine *unstructured.Unstructured) string {
	sum := sha256.Sum256([]byte(pool.Namespace + "/" + pool.Name + "/" + agentMachine.GetNamespace() + "/" + agentMachine.GetName()))
	suffix := hex.EncodeToString(sum[:])[:10]
	base := strings.Trim(agentMachine.GetName()+"-"+pool.Name, "-")
	maxBaseLength := 253 - len(suffix) - 1
	if len(base) > maxBaseLength {
		base = strings.Trim(base[:maxBaseLength], "-")
	}
	if base == "" {
		return suffix
	}
	return base + "-" + suffix
}

func matchingPoolNames(pools []*agentforgev1alpha1.VsphereAgentPool) string {
	names := make([]string, 0, len(pools))
	for _, pool := range pools {
		names = append(names, pool.Namespace+"/"+pool.Name)
	}
	return strings.Join(names, ", ")
}

func vsphereAgentVMName(agent *agentforgev1alpha1.VsphereAgent) string {
	if agent.Annotations != nil && agent.Annotations[vsphereAgentVMNameAnnotation] != "" {
		return agent.Annotations[vsphereAgentVMNameAnnotation]
	}
	return agent.Name
}

func (r *AgentMachineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(agentMachineWatchObject()).
		Named("agentmachine").
		Complete(r)
}
