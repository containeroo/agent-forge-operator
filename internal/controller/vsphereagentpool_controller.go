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
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
)

const (
	finalizerName = "agent-forge.containeroo.ch/vsphere-agent-pool"

	forceISORefreshAnnotation  = "agent-forge.containeroo.ch/force-iso-refresh"
	orphanedOwnedVMGracePeriod = 30 * time.Minute

	nodePoolAnnotation = "hypershift.openshift.io/nodePool"
	roleLabelKey       = "hypershift.openshift.io/nodepool-role"
	agentMachineRefKey = "agentMachineRef"
	apiVersionV1Beta1  = "v1beta1"

	vsphereAgentPoolControlPlaneNamespaceIndex = ".spec.controlPlaneNamespace"
)

var (
	machineGVK      = schema.GroupVersionKind{Group: "cluster.x-k8s.io", Version: apiVersionV1Beta1, Kind: "Machine"}
	agentMachineGVK = schema.GroupVersionKind{Group: "capi-provider.agent-install.openshift.io", Version: apiVersionV1Beta1, Kind: "AgentMachine"}
	infraEnvGVK     = schema.GroupVersionKind{Group: "agent-install.openshift.io", Version: apiVersionV1Beta1, Kind: k8sKindInfraEnv}
	agentGVK        = schema.GroupVersionKind{Group: "agent-install.openshift.io", Version: apiVersionV1Beta1, Kind: k8sKindAgent}
)

// VsphereAgentPoolReconciler reconciles a VsphereAgentPool object.
type VsphereAgentPoolReconciler struct {
	client.Client
	APIReader       client.Reader
	Scheme          *runtime.Scheme
	Recorder        events.EventRecorder
	ProviderFactory VMProviderFactory
}

type MachineInfo struct {
	Name     string
	Deleting bool
}

type AgentMachineDemand struct {
	Total        int32
	Waiting      int32
	Unready      int32
	WithoutAgent int32
}

// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=vsphereagentpools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=vsphereagentpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=vsphereagentpools/finalizers,verbs=update
// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=vsphereagents,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines,verbs=get;list;watch
// +kubebuilder:rbac:groups=capi-provider.agent-install.openshift.io,resources=agentmachines,verbs=get;list;watch
// +kubebuilder:rbac:groups=agent-install.openshift.io,resources=infraenvs;agents,verbs=get;list;watch;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch;update

// Reconcile plans and applies vSphere-backed Agent inventory for one hosted
// cluster NodePool. Hypershift and CAPI remain the source of truth: this
// controller creates inventory only when AgentMachines report NoSuitableAgents.
func (r *VsphereAgentPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var pool agentforgev1alpha1.VsphereAgentPool
	if err := r.apiReader().Get(ctx, req.NamespacedName, &pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	applySpecDefaults(&pool)

	if pool.DeletionTimestamp.IsZero() {
		if controllerutil.AddFinalizer(&pool, finalizerName) {
			if err := r.patchFinalizer(ctx, &pool); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	} else {
		return r.reconcileDelete(ctx, &pool)
	}

	infraEnvAvailable, _, infraEnvMessage := r.infraEnvAvailable(ctx, &pool)
	if !infraEnvAvailable {
		r.setStatusError(&pool, reasonInfraEnvUnavailable, infraEnvMessage)
		meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
			Type:               conditionInfraEnvAvailable,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: pool.Generation,
			Reason:             reasonInfraEnvUnavailable,
			Message:            infraEnvMessage,
		})
		if err := r.updateStatus(ctx, &pool, PoolPlan{}); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	agents, err := r.listMatchingAgents(ctx, &pool)
	if err != nil {
		r.setStatusError(&pool, "AgentListFailed", err.Error())
		if statusErr := r.updateStatus(ctx, &pool, PoolPlan{}); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{}, err
	}
	machines, err := r.listNodePoolMachines(ctx, &pool)
	if err != nil {
		r.setStatusError(&pool, "MachineListFailed", err.Error())
		if statusErr := r.updateStatus(ctx, &pool, PoolPlan{}); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{}, err
	}
	agentMachineDemand, err := r.countAgentMachineDemand(ctx, &pool)
	if err != nil {
		r.setStatusError(&pool, "AgentMachineListFailed", err.Error())
		if statusErr := r.updateStatus(ctx, &pool, PoolPlan{}); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{}, err
	}
	ownedVMs, err := r.listVsphereAgentVMs(ctx, &pool)
	if err != nil {
		r.setStatusError(&pool, "VsphereAgentListFailed", err.Error())
		if statusErr := r.updateStatus(ctx, &pool, PoolPlan{}); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{}, err
	}
	if len(ownedVMs) == 0 && len(pool.Status.OwnedVMs) > 0 {
		ownedVMs = pool.Status.OwnedVMs
	}
	pool.Status.OwnedVMs = ownedVMs
	pool.Status.OwnedVMs = refreshOwnedVMStatuses(&pool, agents, machines)
	if err := r.adoptMatchingAgents(ctx, &pool, agents); err != nil {
		r.setStatusError(&pool, "VsphereAgentAdoptionFailed", err.Error())
		if statusErr := r.updateStatus(ctx, &pool, PoolPlan{}); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{}, err
	}

	plan := buildPlan(&pool, PoolSnapshot{
		AgentMachines:             agentMachineDemand.Total,
		WaitingAgentMachines:      agentMachineDemand.Waiting,
		UnreadyAgentMachines:      agentMachineDemand.Unready,
		AgentMachinesWithoutAgent: agentMachineDemand.WithoutAgent,
		MatchingAgents:            agents,
		OwnedVMs:                  pool.Status.OwnedVMs,
	})

	if err := r.applyPlan(ctx, &pool, plan); err != nil {
		errMessage := stableErrorMessage(err)
		r.recordWarning(&pool, "ApplyPlanFailed", errMessage)
		setPlanConditions(&pool, plan, errMessage)
		if statusErr := r.updateStatus(ctx, &pool, plan); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		log.Error(err, "apply plan failed", "retryAfter", 30*time.Second)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	refreshPlanOwnedVMCounts(&plan, &pool)

	r.recordPlan(&pool, plan, "PlanApplied")
	setPlanConditions(&pool, plan, "")
	if err := r.updateStatus(ctx, &pool, plan); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: time.Minute}, nil
}

func refreshPlanOwnedVMCounts(plan *PoolPlan, pool *agentforgev1alpha1.VsphereAgentPool) {
	plan.PendingOwnedVMs = countPendingOwnedVMs(pool.Status.OwnedVMs)
}

func (r *VsphereAgentPoolReconciler) reconcileDelete(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool) (ctrl.Result, error) {
	agents, err := r.listVsphereAgentsForPool(ctx, pool)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(agents.Items) > 0 {
		for i := range agents.Items {
			agent := &agents.Items[i]
			if agent.GetDeletionTimestamp() != nil {
				continue
			}
			if err := r.Delete(ctx, agent); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if cleanupEnabled(pool) && len(pool.Status.OwnedVMs) > 0 {
		provider, err := r.provider(ctx, pool)
		if err != nil {
			return ctrl.Result{}, err
		}
		for _, vm := range pool.Status.OwnedVMs {
			if vm.Name == "" {
				continue
			}
			if err := provider.DeleteVM(ctx, pool, vm); err != nil {
				recordVMOperation("delete", err)
				return ctrl.Result{}, err
			}
			recordVMOperation("delete", nil)
		}
	}
	controllerutil.RemoveFinalizer(pool, finalizerName)
	return ctrl.Result{}, r.patchFinalizer(ctx, pool)
}

func (r *VsphereAgentPoolReconciler) patchFinalizer(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool) error {
	current := &agentforgev1alpha1.VsphereAgentPool{}
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, current); err != nil {
		return err
	}
	before := current.DeepCopy()
	if controllerutil.ContainsFinalizer(pool, finalizerName) {
		controllerutil.AddFinalizer(current, finalizerName)
	} else {
		controllerutil.RemoveFinalizer(current, finalizerName)
	}
	return r.Patch(ctx, current, client.MergeFrom(before))
}

func (r *VsphereAgentPoolReconciler) listVsphereAgentsForPool(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool) (agentforgev1alpha1.VsphereAgentList, error) {
	var list agentforgev1alpha1.VsphereAgentList
	if err := r.List(ctx, &list, client.InNamespace(pool.Namespace)); err != nil {
		return list, err
	}
	filtered := agentforgev1alpha1.VsphereAgentList{ListMeta: list.ListMeta}
	for i := range list.Items {
		if vsphereAgentBelongsToPool(&list.Items[i], pool.Name) {
			filtered.Items = append(filtered.Items, list.Items[i])
		}
	}
	return filtered, nil
}

func vsphereAgentBelongsToPool(agent *agentforgev1alpha1.VsphereAgent, poolName string) bool {
	if agent.Spec.PoolRef.Name == poolName {
		return true
	}
	return agent.Labels[vsphereAgentPoolNameLabel] == poolName
}

func (r *VsphereAgentPoolReconciler) listVsphereAgentVMs(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool) ([]agentforgev1alpha1.OwnedVMStatus, error) {
	list, err := r.listVsphereAgentsForPool(ctx, pool)
	if err != nil {
		return nil, err
	}
	vms := make([]agentforgev1alpha1.OwnedVMStatus, 0, len(list.Items))
	for i := range list.Items {
		agent := &list.Items[i]
		if agent.GetDeletionTimestamp() != nil {
			continue
		}
		vm := agent.Status.VM
		if vm.Name == "" {
			vm = newOwnedVMStatus(agent.Name)
			vm.Reason = "VMCreatePending"
		}
		vms = append(vms, vm)
	}
	return vms, nil
}

func (r *VsphereAgentPoolReconciler) adoptMatchingAgents(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, agents []AgentInfo) error {
	for _, agent := range agents {
		hostname := agentObservedHostname(agent)
		if hostname == "" {
			continue
		}
		vm := ownedVMForAgent(pool, agent, hostname)
		if err := r.ensureVsphereAgentForAdoptedVM(ctx, pool, agent, vm); err != nil {
			return err
		}
	}
	return nil
}

func ownedVMForAgent(pool *agentforgev1alpha1.VsphereAgentPool, agent AgentInfo, hostname string) agentforgev1alpha1.OwnedVMStatus {
	for _, vm := range pool.Status.OwnedVMs {
		if vmMatchesAgentIdentity(vm, agent) || vmMatchesAgentRef(vm, agent) {
			return vm
		}
	}
	for _, vm := range pool.Status.OwnedVMs {
		if vm.Name == hostname && !vmIdentityConflictsAgent(vm, agent) {
			return vm
		}
	}
	return agentOwnedVMStatus(pool, agent, hostname)
}

func (r *VsphereAgentPoolReconciler) ensureVsphereAgentForAdoptedVM(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, agent AgentInfo, vm agentforgev1alpha1.OwnedVMStatus) error {
	name := adoptedVsphereAgentName(pool, agent, vm)
	current := &agentforgev1alpha1.VsphereAgent{}
	err := r.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: name}, current)
	if apierrors.IsNotFound(err) {
		existing, findErr := r.findVsphereAgentForAdoptedVM(ctx, pool, agent, vm)
		if findErr != nil {
			return findErr
		}
		if existing != nil {
			current = existing
		} else {
			current = &agentforgev1alpha1.VsphereAgent{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:  pool.Namespace,
					Name:       name,
					Finalizers: []string{vsphereAgentFinalizerName},
					Labels: map[string]string{
						vsphereAgentPoolNameLabel:   pool.Name,
						vsphereAgentCreatedForLabel: vsphereAgentCreatedForAdopted,
					},
				},
				Spec: agentforgev1alpha1.VsphereAgentSpec{
					PoolRef: agentforgev1alpha1.LocalObjectReference{Name: pool.Name},
				},
			}
			if err := controllerutil.SetControllerReference(pool, current, r.Scheme); err != nil {
				return err
			}
			if err := r.Create(ctx, current); err != nil {
				return err
			}
		}
	}
	if err == nil && !vsphereAgentBelongsToPool(current, pool.Name) {
		existing, findErr := r.findVsphereAgentForAdoptedVM(ctx, pool, agent, vm)
		if findErr != nil {
			return findErr
		}
		if existing == nil {
			return nil
		}
		current = existing
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	desiredVM := vm
	if desiredVM.Reason == "" || desiredVM.Phase == "" {
		applyAgentToOwnedVMStatus(pool, &desiredVM, agent)
	}
	if desiredVM.Reason == "" {
		desiredVM.Reason = reasonVMAdopted
	}
	if current.Status.VM.Name != "" &&
		vmIdentityConflictsAgent(current.Status.VM, agent) &&
		current.Labels[vsphereAgentCreatedForLabel] != vsphereAgentCreatedForAdopted {
		existing, findErr := r.findVsphereAgentForAdoptedVM(ctx, pool, agent, desiredVM)
		if findErr != nil {
			return findErr
		}
		if existing == nil || existing.Name == current.Name {
			return nil
		}
		current = existing
	}
	if current.Status.VM.Name != "" && adoptedVMStatusMatches(current.Status.VM, desiredVM) {
		return r.deleteDuplicateVsphereAgentsForVM(ctx, pool, current, desiredVM)
	}
	current.Status.VM = desiredVM
	if err := r.updateVsphereAgentStatus(ctx, current); err != nil {
		return err
	}
	return r.deleteDuplicateVsphereAgentsForVM(ctx, pool, current, desiredVM)
}

func (r *VsphereAgentPoolReconciler) findVsphereAgentForAdoptedVM(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, agent AgentInfo, vm agentforgev1alpha1.OwnedVMStatus) (*agentforgev1alpha1.VsphereAgent, error) {
	list, err := r.listVsphereAgentsForPool(ctx, pool)
	if err != nil {
		return nil, err
	}
	for i := range list.Items {
		current := &list.Items[i]
		if current.GetDeletionTimestamp() != nil {
			continue
		}
		if current.Status.VM.Name != "" && current.Status.VM.Name == vm.Name && !vmIdentityConflictsAgent(current.Status.VM, agent) {
			return current, nil
		}
		if vmMatchesAgentIdentity(current.Status.VM, agent) {
			return current, nil
		}
		if vmMatchesAgentRef(current.Status.VM, agent) {
			return current, nil
		}
	}
	return nil, nil
}

func (r *VsphereAgentPoolReconciler) deleteDuplicateVsphereAgentsForVM(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, current *agentforgev1alpha1.VsphereAgent, vm agentforgev1alpha1.OwnedVMStatus) error {
	if vm.Name == "" || current.Name != vm.Name {
		return nil
	}
	list, err := r.listVsphereAgentsForPool(ctx, pool)
	if err != nil {
		return err
	}
	for i := range list.Items {
		duplicate := &list.Items[i]
		if duplicate.Name == current.Name || duplicate.GetDeletionTimestamp() != nil || duplicate.Status.VM.Name != vm.Name {
			continue
		}
		if err := r.Delete(ctx, duplicate); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func adoptedVMStatusMatches(current, desired agentforgev1alpha1.OwnedVMStatus) bool {
	if current.Name != desired.Name ||
		current.BIOSUUID != desired.BIOSUUID ||
		current.MACAddress != desired.MACAddress ||
		current.Phase != desired.Phase ||
		current.Reason != desired.Reason {
		return false
	}
	return objectReferenceName(current.AgentRef) == objectReferenceName(desired.AgentRef) &&
		objectReferenceName(current.MachineRef) == objectReferenceName(desired.MachineRef)
}

func objectReferenceName(ref *corev1.ObjectReference) string {
	if ref == nil {
		return ""
	}
	return ref.Name
}

func (r *VsphereAgentPoolReconciler) updateVsphereAgentStatus(ctx context.Context, agent *agentforgev1alpha1.VsphereAgent) error {
	if err := r.Status().Update(ctx, agent); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	return r.Update(ctx, agent)
}

func adoptedVsphereAgentName(pool *agentforgev1alpha1.VsphereAgentPool, agent AgentInfo, vm agentforgev1alpha1.OwnedVMStatus) string {
	if vm.Name != "" {
		return vm.Name
	}
	if hostname := agentObservedHostname(agent); hostname != "" {
		return hostname
	}
	return fmt.Sprintf("%s-%s", pool.Name, agent.Name)
}

func (r *VsphereAgentPoolReconciler) applyPlan(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, plan PoolPlan) error {
	agentHostnames := assignedAgentHostnames(pool, plan.AgentsToPatch)
	for _, agent := range plan.AgentsToPatch {
		hostname := agentHostnames[agent.Name]
		if err := r.patchAgent(ctx, pool, agent.Name, hostname); err != nil {
			return err
		}
		if hostname != "" {
			pool.Status.OwnedVMs = markOwnedVMAvailable(pool, pool.Status.OwnedVMs, hostname, agent.Name)
		}
	}

	if len(plan.VMsToDelete) > 0 {
		agentsToDelete := agentDeleteSet(plan.AgentsToDelete)
		deletedAgents := map[string]struct{}{}
		for _, vm := range plan.VMsToDelete {
			agentName := ownedVMAgentName(vm)
			if _, shouldDeleteAgent := agentsToDelete[agentName]; shouldDeleteAgent {
				safe, blocker, err := r.agentCanBeDeleted(ctx, pool, agentName)
				if err != nil {
					return err
				}
				if !safe {
					r.recordNormal(pool, "AgentDeleteDeferred", fmt.Sprintf("waiting to delete Agent %s/%s: %s", pool.Namespace, agentName, blocker))
					continue
				}
			}
			if err := r.deleteVsphereAgentForVM(ctx, pool, vm); err != nil {
				return err
			}
			pool.Status.OwnedVMs = removeOwnedVM(pool.Status.OwnedVMs, vm.Name)
			if _, shouldDeleteAgent := agentsToDelete[agentName]; shouldDeleteAgent {
				if err := r.deleteAgent(ctx, pool, agentName); err != nil {
					return err
				}
				deletedAgents[agentName] = struct{}{}
			}
		}
		for _, agent := range plan.AgentsToDelete {
			if _, deleted := deletedAgents[agent.Name]; deleted {
				continue
			}
			r.recordNormal(pool, "AgentDeleteDeferred", fmt.Sprintf("waiting to delete Agent %s/%s until its VM is selected and deleted", pool.Namespace, agent.Name))
		}
	}

	return nil
}

func (r *VsphereAgentPoolReconciler) deleteVsphereAgentForVM(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, vm agentforgev1alpha1.OwnedVMStatus) error {
	list, err := r.listVsphereAgentsForPool(ctx, pool)
	if err != nil {
		return err
	}
	for i := range list.Items {
		agent := &list.Items[i]
		if agent.Status.VM.Name != vm.Name && agent.Name != vm.Name {
			continue
		}
		if err := r.Delete(ctx, agent); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *VsphereAgentPoolReconciler) ensureISOCache(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, provider VMProvider, isoDownloadURL string) (string, error) {
	now := metav1.Now()
	token := pool.GetAnnotations()[forceISORefreshAnnotation]
	if !isoCacheDue(pool, isoDownloadURL, token, now.Time) {
		return pool.Status.ISO.Path, nil
	}

	result, err := provider.EnsureISO(ctx, pool, ISOEnsureRequest{
		DownloadURL:   isoDownloadURL,
		CurrentSHA256: pool.Status.ISO.SHA256,
		CurrentPath:   pool.Status.ISO.Path,
	})
	if err != nil {
		recordISOOperation("ensure", err)
		return "", err
	}
	recordISOOperation("ensure", nil)

	previousPath := pool.Status.ISO.Path
	previousHistory := append([]agentforgev1alpha1.ISOCacheHistoryEntry(nil), pool.Status.ISO.History...)
	uploadedAt := pool.Status.ISO.UploadedAt
	if result.Uploaded || uploadedAt.IsZero() || result.Path != previousPath {
		uploadedAt = now
	}

	pool.Status.ISO.URL = isoDownloadURL
	pool.Status.ISO.Path = result.Path
	pool.Status.ISO.SHA256 = result.SHA256
	pool.Status.ISO.SizeBytes = result.SizeBytes
	pool.Status.ISO.CheckedAt = now
	pool.Status.ISO.UploadedAt = uploadedAt
	pool.Status.ISO.ForceRefreshToken = token
	retainVersions := isoRetainVersions(pool)
	pool.Status.ISO.History = updatedISOHistory(previousHistory, agentforgev1alpha1.ISOCacheHistoryEntry{
		Path:       result.Path,
		SHA256:     result.SHA256,
		SizeBytes:  result.SizeBytes,
		UploadedAt: uploadedAt,
	}, retainVersions)

	for _, stalePath := range staleISOPaths(previousHistory, previousPath, result.Path, retainVersions) {
		if err := provider.DeleteISO(ctx, pool, stalePath); err != nil {
			recordISOOperation("delete", err)
			r.recordWarning(pool, "ISOPruneFailed", stableErrorMessage(err))
		} else {
			recordISOOperation("delete", nil)
		}
	}

	reason := "Reused"
	message := fmt.Sprintf("Reused cached ISO %s", result.Path)
	if result.Uploaded {
		reason = "Uploaded"
		message = fmt.Sprintf("Uploaded cached ISO %s", result.Path)
	}
	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               conditionISOReady,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: pool.Generation,
		Reason:             reason,
		Message:            message,
	})
	if r.Recorder != nil {
		recordEvent(r.Recorder, pool, corev1.EventTypeNormal, "ISO"+reason, message)
	}

	return result.Path, nil
}

func (r *VsphereAgentPoolReconciler) provider(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool) (VMProvider, error) {
	factory := r.ProviderFactory
	if factory == nil {
		factory = NewGovcVMProvider
	}
	secretNamespace := pool.Spec.VSphere.CredentialsSecretRef.Namespace
	if secretNamespace == "" {
		secretNamespace = pool.Namespace
	}
	var secret corev1.Secret
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: secretNamespace, Name: pool.Spec.VSphere.CredentialsSecretRef.Name}, &secret); err != nil {
		return nil, err
	}
	return factory(ctx, pool, &secret)
}

func (r *VsphereAgentPoolReconciler) apiReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *VsphereAgentPoolReconciler) countAgentMachineDemand(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool) (AgentMachineDemand, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(agentMachineGVK)
	if err := r.List(ctx, list, client.InNamespace(pool.Spec.ControlPlaneNamespace)); err != nil {
		return AgentMachineDemand{}, err
	}
	var demand AgentMachineDemand
	for i := range list.Items {
		agentMachine := &list.Items[i]
		if agentMachine.GetDeletionTimestamp() != nil {
			continue
		}
		if !controlPlaneObjectMatchesPool(agentMachine, pool) {
			continue
		}
		demand.Total++
		ready := agentMachineReady(agentMachine)
		if !ready {
			demand.Unready++
		}
		if !ready && !agentMachineHasAssignedAgent(agentMachine) {
			demand.WithoutAgent++
		}
		if agentMachineWaitingForAgent(agentMachine) {
			demand.Waiting++
		}
	}
	return demand, nil
}

func (r *VsphereAgentPoolReconciler) listNodePoolMachines(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool) ([]MachineInfo, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(machineGVK)
	if err := r.List(ctx, list, client.InNamespace(pool.Spec.ControlPlaneNamespace)); err != nil {
		return nil, err
	}
	machines := make([]MachineInfo, 0, len(list.Items))
	for i := range list.Items {
		machine := &list.Items[i]
		if !controlPlaneObjectMatchesPool(machine, pool) {
			continue
		}
		machines = append(machines, MachineInfo{
			Name:     machine.GetName(),
			Deleting: machine.GetDeletionTimestamp() != nil || machinePhase(machine) == "Deleting",
		})
	}
	return machines, nil
}

func (r *VsphereAgentPoolReconciler) infraEnvAvailable(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool) (bool, string, string) {
	infraEnv := &unstructured.Unstructured{}
	infraEnv.SetGroupVersionKind(infraEnvGVK)
	if err := r.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Spec.InfraEnvRef.Name}, infraEnv); err != nil {
		if apierrors.IsNotFound(err) {
			return false, "", err.Error()
		}
		return false, "", fmt.Sprintf("failed to read InfraEnv: %v", err)
	}
	isoURL, _, _ := unstructured.NestedString(infraEnv.Object, "status", "isoDownloadURL")
	if isoURL == "" {
		return false, "", "InfraEnv status.isoDownloadURL is empty"
	}
	return true, isoURL, "InfraEnv exposes discovery ISO"
}

func (r *VsphereAgentPoolReconciler) listMatchingAgents(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool) ([]AgentInfo, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(agentGVK)
	listOpts := []client.ListOption{client.InNamespace(pool.Namespace)}
	if labels := agentCandidateLabels(pool); len(labels) > 0 {
		listOpts = append(listOpts, client.MatchingLabels(labels))
	}
	if err := r.List(ctx, list, listOpts...); err != nil {
		return nil, err
	}

	agents := make([]AgentInfo, 0, len(list.Items))
	for i := range list.Items {
		obj := &list.Items[i]
		if !agentBelongsToInfraEnv(obj, pool.Spec.InfraEnvRef.Name) {
			continue
		}
		labels := obj.GetLabels()
		approved, _, _ := unstructured.NestedBool(obj.Object, "spec", "approved")
		specRole, _, _ := unstructured.NestedString(obj.Object, "spec", "role")
		specHostname, _, _ := unstructured.NestedString(obj.Object, "spec", "hostname")
		inventoryHostname, _, _ := unstructured.NestedString(obj.Object, "status", "inventory", "hostname")
		serialNumber, _, _ := unstructured.NestedString(obj.Object, "status", "inventory", "systemVendor", "serialNumber")
		clusterName, _, _ := unstructured.NestedString(obj.Object, "spec", "clusterDeploymentName", "name")
		if clusterName != "" && clusterName != pool.Spec.HostedClusterRef.Name {
			continue
		}
		machineName := labels[agentMachineRefKey]
		agent := AgentInfo{
			Name:              obj.GetName(),
			Bound:             machineName != "" || clusterName != "",
			MachineName:       machineName,
			Approved:          approved,
			SpecRole:          specRole,
			RoleLabel:         labels[roleLabelKey],
			PoolLabel:         labels[poolLabelKey],
			Hostname:          specHostname,
			InventoryHostname: inventoryHostname,
			MAC:               normalizeMAC(agentPrimaryMAC(obj)),
			BIOSUUID:          normalizeVMwareSerialUUID(serialNumber),
		}
		if !agentMatchesPoolDiscriminator(pool, agent) {
			continue
		}
		agents = append(agents, agent)
	}
	return r.filterAgentsClaimedByOtherPools(ctx, pool, agents)
}

func agentMatchesPoolDiscriminator(pool *agentforgev1alpha1.VsphereAgentPool, agent AgentInfo) bool {
	desiredPoolLabel, hasPoolLabel := pool.Spec.Agent.Labels[poolLabelKey]
	if !hasPoolLabel {
		return true
	}
	if agent.PoolLabel == desiredPoolLabel {
		return true
	}
	if agent.PoolLabel != "" || agent.Bound {
		return false
	}
	return agentAssociatedWithOwnedVM(pool.Status.OwnedVMs, agent)
}

func (r *VsphereAgentPoolReconciler) filterAgentsClaimedByOtherPools(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, agents []AgentInfo) ([]AgentInfo, error) {
	if len(agents) == 0 {
		return agents, nil
	}

	var vsphereAgents agentforgev1alpha1.VsphereAgentList
	if err := r.List(ctx, &vsphereAgents, client.InNamespace(pool.Namespace)); err != nil {
		return nil, err
	}
	if len(vsphereAgents.Items) == 0 {
		return agents, nil
	}

	filtered := agents[:0]
	for _, agent := range agents {
		if agentClaimedByOtherPool(agent, pool, vsphereAgents.Items) {
			continue
		}
		filtered = append(filtered, agent)
	}
	return filtered, nil
}

func agentClaimedByOtherPool(agent AgentInfo, pool *agentforgev1alpha1.VsphereAgentPool, vsphereAgents []agentforgev1alpha1.VsphereAgent) bool {
	for i := range vsphereAgents {
		vsphereAgent := &vsphereAgents[i]
		if vsphereAgent.GetDeletionTimestamp() != nil ||
			!vsphereAgentHasPoolRef(vsphereAgent) ||
			vsphereAgentBelongsToPool(vsphereAgent, pool.Name) {
			continue
		}
		if vsphereAgentClaimsAgent(vsphereAgent, agent) {
			return true
		}
	}
	return false
}

func vsphereAgentHasPoolRef(agent *agentforgev1alpha1.VsphereAgent) bool {
	return agent.Spec.PoolRef.Name != "" || agent.Labels[vsphereAgentPoolNameLabel] != ""
}

func vsphereAgentClaimsAgent(vsphereAgent *agentforgev1alpha1.VsphereAgent, agent AgentInfo) bool {
	vm := vsphereAgent.Status.VM
	if objectReferenceName(vm.AgentRef) == agent.Name {
		return true
	}
	if vmMatchesAgentIdentity(vm, agent) {
		return true
	}

	hostname := agentObservedHostname(agent)
	if hostname == "" {
		return false
	}
	if vm.Name == hostname && !vmIdentityConflictsAgent(vm, agent) {
		return true
	}
	return vsphereAgent.Name == hostname && !vmIdentityConflictsAgent(vm, agent)
}

func assignedAgentHostnames(pool *agentforgev1alpha1.VsphereAgentPool, agents []AgentInfo) map[string]string {
	assigned := map[string]string{}
	reserved := map[string]struct{}{}
	for _, agent := range agents {
		for _, vm := range pool.Status.OwnedVMs {
			if vm.Name == "" || vm.Phase == phaseBound {
				continue
			}
			if !vmMatchesAgentIdentity(vm, agent) {
				continue
			}
			if _, exists := reserved[vm.Name]; exists {
				continue
			}
			assigned[agent.Name] = vm.Name
			reserved[vm.Name] = struct{}{}
			break
		}
	}

	for _, agent := range agents {
		if assigned[agent.Name] != "" || agent.Hostname == "" {
			continue
		}
		assigned[agent.Name] = agent.Hostname
		reserved[agent.Hostname] = struct{}{}
	}

	for _, agent := range agents {
		if assigned[agent.Name] != "" {
			continue
		}
		for _, vm := range pool.Status.OwnedVMs {
			if vm.Name == "" || vm.Phase == phaseBound {
				continue
			}
			if vm.AgentRef != nil && vm.AgentRef.Name != "" && vm.AgentRef.Name != agent.Name {
				continue
			}
			if _, exists := reserved[vm.Name]; exists {
				continue
			}
			assigned[agent.Name] = vm.Name
			reserved[vm.Name] = struct{}{}
			break
		}
		if assigned[agent.Name] == "" {
			hostname := desiredAgentHostname(pool)
			assigned[agent.Name] = hostname
			reserved[hostname] = struct{}{}
		}
	}
	return assigned
}

func refreshOwnedVMStatuses(pool *agentforgev1alpha1.VsphereAgentPool, agents []AgentInfo, machines []MachineInfo) []agentforgev1alpha1.OwnedVMStatus {
	agentLookup := newAgentIdentityLookup(agents)
	machineStates := map[string]MachineInfo{}
	for _, machine := range machines {
		machineStates[machine.Name] = machine
	}

	now := time.Now()
	vms := make([]agentforgev1alpha1.OwnedVMStatus, 0, len(pool.Status.OwnedVMs))
	matchedAgents := map[string]struct{}{}
	knownVMNames := map[string]struct{}{}
	for _, vm := range pool.Status.OwnedVMs {
		if vm.Name != "" {
			knownVMNames[vm.Name] = struct{}{}
		}
		agent, matched := agentLookup.match(vm)
		if !matched {
			markOwnedVMWithoutMatchingAgent(&vm, now)
			vms = append(vms, vm)
			continue
		}
		matchedAgents[agent.Name] = struct{}{}

		wasMachineDeleting := vm.Phase == phaseReleased && vm.Reason == reasonMachineDeleting
		previousMachineRef := vm.MachineRef
		applyAgentToOwnedVMStatus(pool, &vm, agent)
		if wasMachineDeleting {
			if (vm.MachineRef == nil || vm.MachineRef.Name == "") && previousMachineRef != nil && previousMachineRef.Name != "" {
				vm.MachineRef = previousMachineRef
			}
			setOwnedVMPhase(&vm, phaseReleased, reasonMachineDeleting)
		}
		applyMachineStateToOwnedVMStatus(&vm, machineStates)
		vms = append(vms, vm)
	}

	for _, agent := range agents {
		if _, exists := matchedAgents[agent.Name]; exists {
			continue
		}
		hostname := agentObservedHostname(agent)
		if hostname == "" {
			continue
		}
		if _, exists := knownVMNames[hostname]; exists {
			continue
		}
		vm := agentOwnedVMStatus(pool, agent, hostname)
		applyMachineStateToOwnedVMStatus(&vm, machineStates)
		vms = append(vms, vm)
		knownVMNames[hostname] = struct{}{}
	}
	return vms
}

type agentIdentityLookup struct {
	byHostname map[string]AgentInfo
	byName     map[string]AgentInfo
	byBIOSUUID map[string]AgentInfo
	byMAC      map[string]AgentInfo
}

func newAgentIdentityLookup(agents []AgentInfo) agentIdentityLookup {
	lookup := agentIdentityLookup{
		byHostname: map[string]AgentInfo{},
		byName:     map[string]AgentInfo{},
		byBIOSUUID: map[string]AgentInfo{},
		byMAC:      map[string]AgentInfo{},
	}
	for _, agent := range agents {
		lookup.byName[agent.Name] = agent
		if hostname := agentObservedHostname(agent); hostname != "" {
			lookup.byHostname[hostname] = agent
		}
		if agent.BIOSUUID != "" {
			lookup.byBIOSUUID[agent.BIOSUUID] = agent
		}
		if agent.MAC != "" {
			lookup.byMAC[agent.MAC] = agent
		}
	}
	return lookup
}

func (l agentIdentityLookup) match(vm agentforgev1alpha1.OwnedVMStatus) (AgentInfo, bool) {
	if vm.BIOSUUID != "" {
		if agent, matched := l.byBIOSUUID[vm.BIOSUUID]; matched {
			return agent, true
		}
	}
	if vm.MACAddress != "" {
		if agent, matched := l.byMAC[vm.MACAddress]; matched {
			return agent, true
		}
	}
	if agent, matched := l.matchHostname(vm); matched {
		return agent, true
	}
	return l.matchAgentRef(vm)
}

func (l agentIdentityLookup) matchHostname(vm agentforgev1alpha1.OwnedVMStatus) (AgentInfo, bool) {
	candidate, exists := l.byHostname[vm.Name]
	if exists && !vmIdentityConflictsAgent(vm, candidate) {
		return candidate, true
	}
	return AgentInfo{}, false
}

func (l agentIdentityLookup) matchAgentRef(vm agentforgev1alpha1.OwnedVMStatus) (AgentInfo, bool) {
	if vm.AgentRef == nil || vm.AgentRef.Name == "" {
		return AgentInfo{}, false
	}
	candidate, exists := l.byName[vm.AgentRef.Name]
	if exists && !vmIdentityConflictsAgent(vm, candidate) {
		return candidate, true
	}
	return AgentInfo{}, false
}

func markOwnedVMWithoutMatchingAgent(vm *agentforgev1alpha1.OwnedVMStatus, now time.Time) {
	hadDiscoveredAgent := vmHadDiscoveredAgent(*vm)
	vm.AgentRef = nil
	vm.MachineRef = nil
	if hadDiscoveredAgent {
		setOwnedVMPhase(vm, phaseOrphaned, "AgentMissing")
	} else if ownedVMDiscoveryExpired(*vm, now) {
		setOwnedVMPhase(vm, phaseOrphaned, "AgentDiscoveryExpired")
	} else {
		setOwnedVMPhase(vm, phaseProvisioning, reasonAgentNotDiscovered)
	}
}

func vmMatchesAgentIdentity(vm agentforgev1alpha1.OwnedVMStatus, agent AgentInfo) bool {
	if vm.BIOSUUID != "" && agent.BIOSUUID != "" && vm.BIOSUUID == agent.BIOSUUID {
		return true
	}
	return vm.MACAddress != "" && agent.MAC != "" && vm.MACAddress == agent.MAC
}

func vmMatchesAgentRef(vm agentforgev1alpha1.OwnedVMStatus, agent AgentInfo) bool {
	return vm.AgentRef != nil && vm.AgentRef.Name == agent.Name && !vmIdentityConflictsAgent(vm, agent)
}

func vmIdentityConflictsAgent(vm agentforgev1alpha1.OwnedVMStatus, agent AgentInfo) bool {
	checked := false
	if vm.BIOSUUID != "" && agent.BIOSUUID != "" {
		checked = true
		if vm.BIOSUUID == agent.BIOSUUID {
			return false
		}
	}
	if vm.MACAddress != "" && agent.MAC != "" {
		checked = true
		if vm.MACAddress == agent.MAC {
			return false
		}
	}
	return checked
}

func applyMachineStateToOwnedVMStatus(vm *agentforgev1alpha1.OwnedVMStatus, machines map[string]MachineInfo) {
	if vm.MachineRef == nil || vm.MachineRef.Name == "" {
		if vm.Phase == phaseReleased && vm.Reason == reasonMachineDeleting {
			setOwnedVMPhase(vm, phaseReleased, reasonMachineDeleted)
		}
		return
	}
	machine, exists := machines[vm.MachineRef.Name]
	if !exists {
		if vm.Phase == phaseReleased && vm.Reason == reasonMachineDeleting {
			setOwnedVMPhase(vm, phaseReleased, reasonMachineDeleted)
		}
		return
	}
	if machine.Deleting {
		setOwnedVMPhase(vm, phaseReleased, reasonMachineDeleting)
	}
}

func vmHadDiscoveredAgent(vm agentforgev1alpha1.OwnedVMStatus) bool {
	if vm.AgentRef != nil && vm.AgentRef.Name != "" {
		return true
	}
	return vm.Phase == phaseAvailable || vm.Phase == phaseBound || vm.Phase == phaseReleased
}

func ownedVMDiscoveryExpired(vm agentforgev1alpha1.OwnedVMStatus, now time.Time) bool {
	if vm.Phase == phaseOrphaned {
		return true
	}
	if vm.Phase != phaseProvisioning || vm.Reason != reasonAgentNotDiscovered || vm.LastTransitionTime.IsZero() {
		return false
	}
	return !now.Before(vm.LastTransitionTime.Add(orphanedOwnedVMGracePeriod))
}

func markOwnedVMAvailable(pool *agentforgev1alpha1.VsphereAgentPool, vms []agentforgev1alpha1.OwnedVMStatus, hostname, agentName string) []agentforgev1alpha1.OwnedVMStatus {
	for i := range vms {
		if vms[i].Name != hostname {
			continue
		}
		vms[i].AgentRef = agentObjectReference(pool, agentName)
		vms[i].MachineRef = nil
		setOwnedVMPhase(&vms[i], phaseAvailable, "AgentPrepared")
		return vms
	}
	return vms
}

func ownedVMAgentName(vm agentforgev1alpha1.OwnedVMStatus) string {
	if vm.AgentRef == nil {
		return ""
	}
	return vm.AgentRef.Name
}

func agentDeleteSet(agents []AgentInfo) map[string]struct{} {
	result := map[string]struct{}{}
	for _, agent := range agents {
		if agent.Name != "" {
			result[agent.Name] = struct{}{}
		}
	}
	return result
}

func agentObjectReference(pool *agentforgev1alpha1.VsphereAgentPool, name string) *corev1.ObjectReference {
	return &corev1.ObjectReference{
		APIVersion: agentGVK.GroupVersion().String(),
		Kind:       agentGVK.Kind,
		Namespace:  pool.Namespace,
		Name:       name,
	}
}

func machineObjectReference(pool *agentforgev1alpha1.VsphereAgentPool, name string) *corev1.ObjectReference {
	return &corev1.ObjectReference{
		APIVersion: machineGVK.GroupVersion().String(),
		Kind:       machineGVK.Kind,
		Namespace:  pool.Spec.ControlPlaneNamespace,
		Name:       name,
	}
}

func setOwnedVMPhase(vm *agentforgev1alpha1.OwnedVMStatus, phase, reason string) {
	if vm.Phase == phase && vm.Reason == reason {
		return
	}
	vm.Phase = phase
	vm.Reason = reason
	vm.LastTransitionTime = metav1.Now()
}

func agentObservedHostname(agent AgentInfo) string {
	if agent.Hostname != "" {
		return agent.Hostname
	}
	return agent.InventoryHostname
}

func agentOwnedVMStatus(pool *agentforgev1alpha1.VsphereAgentPool, agent AgentInfo, hostname string) agentforgev1alpha1.OwnedVMStatus {
	vm := agentforgev1alpha1.OwnedVMStatus{
		Name:               hostname,
		MACAddress:         agent.MAC,
		LastTransitionTime: metav1.Now(),
	}
	applyAgentToOwnedVMStatus(pool, &vm, agent)
	return vm
}

func applyAgentToOwnedVMStatus(pool *agentforgev1alpha1.VsphereAgentPool, vm *agentforgev1alpha1.OwnedVMStatus, agent AgentInfo) {
	vm.AgentRef = agentObjectReference(pool, agent.Name)
	if agent.MAC != "" {
		vm.MACAddress = agent.MAC
	}
	if agent.BIOSUUID != "" {
		vm.BIOSUUID = agent.BIOSUUID
	}
	if agent.MachineName != "" {
		vm.MachineRef = machineObjectReference(pool, agent.MachineName)
	} else {
		vm.MachineRef = nil
	}
	if agent.Bound {
		setOwnedVMPhase(vm, phaseBound, "AgentBound")
	} else if vm.Phase == phaseBound || vm.Phase == phaseReleased {
		setOwnedVMPhase(vm, phaseReleased, "AgentReleased")
	} else {
		setOwnedVMPhase(vm, phaseAvailable, "AgentAvailable")
	}
}

func (r *VsphereAgentPoolReconciler) patchAgent(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, name, hostname string) error {
	agent := &unstructured.Unstructured{}
	agent.SetGroupVersionKind(agentGVK)
	if err := r.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: name}, agent); err != nil {
		return err
	}
	before := agent.DeepCopy()

	labels := agent.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	for key, value := range pool.Spec.Agent.Labels {
		labels[key] = value
	}
	labels[roleLabelKey] = pool.Spec.Agent.Role
	agent.SetLabels(labels)
	if err := unstructured.SetNestedField(agent.Object, pool.Spec.Agent.Role, "spec", "role"); err != nil {
		return err
	}
	if hostname == "" {
		hostname = desiredAgentHostname(pool)
	}
	if currentHostname, _, _ := unstructured.NestedString(agent.Object, "spec", "hostname"); currentHostname != hostname {
		if err := unstructured.SetNestedField(agent.Object, hostname, "spec", "hostname"); err != nil {
			return err
		}
	}
	if approveAgents(pool) {
		if err := unstructured.SetNestedField(agent.Object, true, "spec", "approved"); err != nil {
			return err
		}
	}

	return r.Patch(ctx, agent, client.MergeFrom(before))
}

func (r *VsphereAgentPoolReconciler) deleteAgent(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, name string) error {
	agent := &unstructured.Unstructured{}
	agent.SetGroupVersionKind(agentGVK)
	if err := r.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: name}, agent); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if blocker := agentDeletionBlocker(agent); blocker != "" {
		return fmt.Errorf("refusing to delete Agent %s/%s: %s", pool.Namespace, name, blocker)
	}
	if err := r.Delete(ctx, agent); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if r.Recorder != nil {
		recordEventf(r.Recorder, pool, corev1.EventTypeNormal, "AgentDeleted", "deleted stale unbound Agent %s", name)
	}
	return nil
}

func (r *VsphereAgentPoolReconciler) agentCanBeDeleted(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, name string) (bool, string, error) {
	if name == "" {
		return true, "", nil
	}
	agent := &unstructured.Unstructured{}
	agent.SetGroupVersionKind(agentGVK)
	if err := r.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: name}, agent); err != nil {
		if apierrors.IsNotFound(err) {
			return true, "", nil
		}
		return false, "", err
	}
	if blocker := agentDeletionBlocker(agent); blocker != "" {
		return false, blocker, nil
	}
	return true, "", nil
}

func agentDeletionBlocker(agent *unstructured.Unstructured) string {
	if labels := agent.GetLabels(); labels[agentMachineRefKey] != "" {
		return fmt.Sprintf("still bound to AgentMachine %s", labels[agentMachineRefKey])
	}
	clusterName, _, _ := unstructured.NestedString(agent.Object, "spec", "clusterDeploymentName", "name")
	if clusterName != "" {
		return fmt.Sprintf("still bound to ClusterDeployment %s", clusterName)
	}
	return ""
}

func (r *VsphereAgentPoolReconciler) updateStatus(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, plan PoolPlan) error {
	desired := *pool.Status.DeepCopy()
	desired.ObservedGeneration = pool.Generation
	desired.DesiredReplicas = plan.DesiredReplicas
	desired.AgentMachines = plan.AgentMachines
	desired.WaitingAgentMachines = plan.WaitingAgentMachines
	desired.UnreadyAgentMachines = plan.UnreadyAgentMachines
	desired.AgentMachinesWithoutAgent = plan.AgentMachinesWithoutAgent
	desired.MatchingAgents = plan.MatchingAgents
	desired.BoundAgents = plan.BoundAgents
	desired.AvailableAgents = plan.AvailableAgents
	desired.PlannedActions = plan.Actions

	var updated agentforgev1alpha1.VsphereAgentPoolStatus
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var current agentforgev1alpha1.VsphereAgentPool
		if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, &current); err != nil {
			return err
		}
		next := mergePoolReconcileStatus(current.Status, desired)
		if reflect.DeepEqual(current.Status, next) {
			updated = current.Status
			return nil
		}
		current.Status = next
		if err := r.Status().Update(ctx, &current); err != nil {
			return err
		}
		updated = next
		return nil
	}); err != nil {
		return err
	}
	pool.Status = updated
	recordPoolCapacityMetrics(pool, plan)
	return nil
}

func mergePoolReconcileStatus(current, desired agentforgev1alpha1.VsphereAgentPoolStatus) agentforgev1alpha1.VsphereAgentPoolStatus {
	next := current
	next.ObservedGeneration = desired.ObservedGeneration
	next.DesiredReplicas = desired.DesiredReplicas
	next.AgentMachines = desired.AgentMachines
	next.WaitingAgentMachines = desired.WaitingAgentMachines
	next.UnreadyAgentMachines = desired.UnreadyAgentMachines
	next.AgentMachinesWithoutAgent = desired.AgentMachinesWithoutAgent
	next.MatchingAgents = desired.MatchingAgents
	next.BoundAgents = desired.BoundAgents
	next.AvailableAgents = desired.AvailableAgents
	next.OwnedVMs = desired.OwnedVMs
	next.PlannedActions = desired.PlannedActions
	next.Conditions = mergePoolReconcileConditions(current.Conditions, desired.Conditions)
	return next
}

func mergePoolReconcileConditions(current, desired []metav1.Condition) []metav1.Condition {
	result := append([]metav1.Condition(nil), current...)
	for _, condition := range desired {
		if !isPoolReconcileCondition(condition.Type) {
			continue
		}
		meta.SetStatusCondition(&result, condition)
	}
	return result
}

func isPoolReconcileCondition(conditionType string) bool {
	switch conditionType {
	case conditionReady,
		conditionAgentMachineDemand,
		conditionInfraEnvAvailable,
		conditionCapacitySatisfied,
		conditionVsphereReady,
		conditionISOReady:
		return true
	default:
		return false
	}
}

func (r *VsphereAgentPoolReconciler) setStatusError(pool *agentforgev1alpha1.VsphereAgentPool, reason, message string) {
	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: pool.Generation,
		Reason:             reason,
		Message:            message,
	})
	r.recordWarning(pool, reason, message)
}

func (r *VsphereAgentPoolReconciler) recordPlan(pool *agentforgev1alpha1.VsphereAgentPool, plan PoolPlan, reason string) {
	if r.Recorder == nil {
		return
	}
	if len(plan.Actions) == 1 && plan.Actions[0].Type == actionNoop {
		return
	}
	recordEventf(r.Recorder, pool, corev1.EventTypeNormal, reason, "planned %d action(s): %s", len(plan.Actions), summarizeActions(plan.Actions))
}

func (r *VsphereAgentPoolReconciler) recordWarning(pool *agentforgev1alpha1.VsphereAgentPool, reason, message string) {
	if r.Recorder != nil {
		recordEvent(r.Recorder, pool, corev1.EventTypeWarning, reason, message)
	}
}

func (r *VsphereAgentPoolReconciler) recordNormal(pool *agentforgev1alpha1.VsphereAgentPool, reason, message string) {
	if r.Recorder != nil {
		recordEvent(r.Recorder, pool, corev1.EventTypeNormal, reason, message)
	}
}

func setPlanConditions(pool *agentforgev1alpha1.VsphereAgentPool, plan PoolPlan, errMessage string) {
	nowStatus := metav1.ConditionTrue
	reason := "Reconciled"
	message := "Agent capacity bridge reconciled successfully"
	if errMessage != "" {
		nowStatus = metav1.ConditionFalse
		reason = "ApplyFailed"
		message = errMessage
	}

	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             nowStatus,
		ObservedGeneration: pool.Generation,
		Reason:             reason,
		Message:            message,
	})
	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               conditionAgentMachineDemand,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: pool.Generation,
		Reason:             "Observed",
		Message:            fmt.Sprintf("agentMachines=%d waitingForAgents=%d unready=%d withoutAgent=%d", plan.AgentMachines, plan.WaitingAgentMachines, plan.UnreadyAgentMachines, plan.AgentMachinesWithoutAgent),
	})
	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               conditionInfraEnvAvailable,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: pool.Generation,
		Reason:             phaseAvailable,
		Message:            "InfraEnv has a discovery ISO URL",
	})
	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               conditionCapacitySatisfied,
		Status:             conditionStatus(plan.VMsToCreate == 0),
		ObservedGeneration: pool.Generation,
		Reason:             boolReason(plan.VMsToCreate == 0, "Satisfied", "Deficit"),
		Message:            fmt.Sprintf("agentMachines=%d waitingAgentMachines=%d unreadyAgentMachines=%d agentMachinesWithoutAgent=%d matchingAgents=%d pendingOwnedVMs=%d boundAgents=%d availableAgents=%d", plan.AgentMachines, plan.WaitingAgentMachines, plan.UnreadyAgentMachines, plan.AgentMachinesWithoutAgent, plan.MatchingAgents, plan.PendingOwnedVMs, plan.BoundAgents, plan.AvailableAgents),
	})
	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               conditionVsphereReady,
		Status:             conditionStatus(errMessage == ""),
		ObservedGeneration: pool.Generation,
		Reason:             boolReason(errMessage == "", "Ready", "Error"),
		Message:            boolMessage(errMessage == "", "vSphere bridge did not report an error", errMessage),
	})
}

func applySpecDefaults(pool *agentforgev1alpha1.VsphereAgentPool) {
	if pool.Spec.VSphere.Datacenter == "" {
		pool.Spec.VSphere.Datacenter = defaultDatacenter
	}
	if pool.Spec.VSphere.GuestID == "" {
		pool.Spec.VSphere.GuestID = "rhel9_64Guest"
	}
	if pool.Spec.VSphere.SCSIType == "" {
		pool.Spec.VSphere.SCSIType = "pvscsi"
	}
	if pool.Spec.VSphere.Firmware == "" {
		pool.Spec.VSphere.Firmware = "efi"
	}
	if pool.Spec.VSphere.NetworkAdapterType == "" {
		pool.Spec.VSphere.NetworkAdapterType = "vmxnet3"
	}
	if pool.Spec.Template.NumCPUs == 0 {
		pool.Spec.Template.NumCPUs = 4
	}
	if pool.Spec.Template.MemoryMiB == 0 {
		pool.Spec.Template.MemoryMiB = 16384
	}
	if pool.Spec.Template.DiskGiB == 0 {
		pool.Spec.Template.DiskGiB = 100
	}
	if pool.Spec.Agent.Role == "" {
		pool.Spec.Agent.Role = defaultAgentRole
	}
	if pool.Spec.ISO.CheckInterval.Duration == 0 {
		pool.Spec.ISO.CheckInterval.Duration = 10 * time.Minute
	}
	if pool.Spec.ISO.RetainVersions == 0 {
		pool.Spec.ISO.RetainVersions = 2
	}
	if pool.Spec.CleanupPolicy == "" {
		pool.Spec.CleanupPolicy = agentforgev1alpha1.CleanupPolicyDelete
	}
}

func approveAgents(pool *agentforgev1alpha1.VsphereAgentPool) bool {
	return pool.Spec.Agent.Approve == nil || *pool.Spec.Agent.Approve
}

func cleanupEnabled(pool *agentforgev1alpha1.VsphereAgentPool) bool {
	return pool.Spec.CleanupPolicy != agentforgev1alpha1.CleanupPolicyRetain
}

func agentMachineWaitingForAgent(agentMachine *unstructured.Unstructured) bool {
	return objectConditionReason(agentMachine, conditionReady) == "NoSuitableAgents"
}

func agentMachineReady(agentMachine *unstructured.Unstructured) bool {
	return objectConditionStatus(agentMachine, conditionReady) == metav1.ConditionTrue
}

func agentMachineHasAssignedAgent(agentMachine *unstructured.Unstructured) bool {
	name, _, _ := unstructured.NestedString(agentMachine.Object, "status", "agentRef", "name")
	if name != "" {
		return true
	}
	providerID, _, _ := unstructured.NestedString(agentMachine.Object, "spec", "providerID")
	return strings.HasPrefix(providerID, "agent://")
}

func machinePhase(machine *unstructured.Unstructured) string {
	phase, _, _ := unstructured.NestedString(machine.Object, "status", "phase")
	return phase
}

func objectConditionReason(obj *unstructured.Unstructured, conditionType string) string {
	status, reason := objectConditionStatusReason(obj, conditionType)
	if status != metav1.ConditionFalse {
		return ""
	}
	return reason
}

func objectConditionStatus(obj *unstructured.Unstructured, conditionType string) metav1.ConditionStatus {
	status, _ := objectConditionStatusReason(obj, conditionType)
	return status
}

func objectConditionStatusReason(obj *unstructured.Unstructured, conditionType string) (metav1.ConditionStatus, string) {
	conditions, found, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if !found {
		return metav1.ConditionUnknown, ""
	}
	for _, item := range conditions {
		condition, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if condition["type"] != conditionType {
			continue
		}
		status, _ := condition["status"].(string)
		reason, _ := condition["reason"].(string)
		return metav1.ConditionStatus(status), reason
	}
	return metav1.ConditionUnknown, ""
}

func conditionStatus(value bool) metav1.ConditionStatus {
	if value {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

func boolReason(value bool, trueReason, falseReason string) string {
	if value {
		return trueReason
	}
	return falseReason
}

func boolMessage(value bool, trueMessage, falseMessage string) string {
	if value {
		return trueMessage
	}
	return falseMessage
}

var tempISOPathPattern = regexp.MustCompile(`/tmp/agent-forge-iso-[^[:space:]]+`)

func stableErrorMessage(err error) string {
	return tempISOPathPattern.ReplaceAllString(err.Error(), "<temp-iso>")
}

func isoCacheDue(pool *agentforgev1alpha1.VsphereAgentPool, isoDownloadURL, forceToken string, now time.Time) bool {
	if pool.Status.ISO.Path == "" || pool.Status.ISO.SHA256 == "" {
		return true
	}
	if pool.Status.ISO.URL != "" && pool.Status.ISO.URL != isoDownloadURL {
		return true
	}
	if !strings.HasPrefix(pool.Status.ISO.Path, isoPathPrefix(pool)+"/") {
		return true
	}
	if forceToken != "" && forceToken != pool.Status.ISO.ForceRefreshToken {
		return true
	}
	checkedAt := pool.Status.ISO.CheckedAt.Time
	if checkedAt.IsZero() {
		return true
	}
	return !now.Before(checkedAt.Add(isoCheckInterval(pool)))
}

func isoCheckInterval(pool *agentforgev1alpha1.VsphereAgentPool) time.Duration {
	if pool.Spec.ISO.CheckInterval.Duration > 0 {
		return pool.Spec.ISO.CheckInterval.Duration
	}
	return 10 * time.Minute
}

func isoRetainVersions(pool *agentforgev1alpha1.VsphereAgentPool) int {
	if pool.Spec.ISO.RetainVersions > 0 {
		return int(pool.Spec.ISO.RetainVersions)
	}
	return 2
}

func updatedISOHistory(history []agentforgev1alpha1.ISOCacheHistoryEntry, current agentforgev1alpha1.ISOCacheHistoryEntry, retain int) []agentforgev1alpha1.ISOCacheHistoryEntry {
	if retain < 1 {
		retain = 1
	}
	result := []agentforgev1alpha1.ISOCacheHistoryEntry{current}
	for _, entry := range history {
		if entry.Path == "" || entry.Path == current.Path || entry.SHA256 == current.SHA256 {
			continue
		}
		result = append(result, entry)
		if len(result) >= retain {
			break
		}
	}
	return result
}

func staleISOPaths(history []agentforgev1alpha1.ISOCacheHistoryEntry, previousPath, currentPath string, retain int) []string {
	if retain < 1 {
		retain = 1
	}
	kept := map[string]struct{}{currentPath: {}}
	keptCount := 1
	for _, entry := range history {
		if keptCount >= retain {
			break
		}
		if entry.Path == "" || entry.Path == currentPath {
			continue
		}
		kept[entry.Path] = struct{}{}
		keptCount++
	}
	var stale []string
	if previousPath != "" && previousPath != currentPath {
		if _, ok := kept[previousPath]; !ok {
			stale = append(stale, previousPath)
		}
	}
	for _, entry := range history {
		if entry.Path == "" || entry.Path == currentPath {
			continue
		}
		if _, ok := kept[entry.Path]; ok {
			continue
		}
		kept[entry.Path] = struct{}{}
		stale = append(stale, entry.Path)
	}
	return stale
}

func summarizeActions(actions []agentforgev1alpha1.PlannedActionStatus) string {
	parts := make([]string, 0, len(actions))
	for _, action := range actions {
		if action.Name == "" {
			parts = append(parts, action.Type)
			continue
		}
		parts = append(parts, fmt.Sprintf("%s/%s", action.Type, action.Name))
	}
	return strings.Join(parts, ", ")
}

func normalizeMAC(value string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(value)), ":", "-")
}

func normalizeVMwareSerialUUID(value string) string {
	value = strings.TrimSpace(strings.TrimPrefix(value, "VMware-"))
	value = strings.NewReplacer(" ", "", "-", "").Replace(value)
	if len(value) != 32 {
		return ""
	}
	return strings.ToLower(fmt.Sprintf("%s-%s-%s-%s-%s", value[0:8], value[8:12], value[12:16], value[16:20], value[20:32]))
}

func agentPrimaryMAC(agent *unstructured.Unstructured) string {
	interfaces, found, _ := unstructured.NestedSlice(agent.Object, "status", "inventory", "interfaces")
	if !found {
		return ""
	}
	for _, item := range interfaces {
		iface, ok := item.(map[string]any)
		if !ok {
			continue
		}
		for _, key := range []string{"macAddress", "mac"} {
			value, ok := iface[key].(string)
			if ok && strings.TrimSpace(value) != "" {
				return value
			}
		}
	}
	return ""
}

func agentCandidateLabels(pool *agentforgev1alpha1.VsphereAgentPool) map[string]string {
	labels := map[string]string{}
	for key, value := range pool.Spec.Agent.Labels {
		if key == roleLabelKey || key == poolLabelKey {
			continue
		}
		labels[key] = value
	}
	return labels
}

func agentBelongsToInfraEnv(agent *unstructured.Unstructured, infraEnvName string) bool {
	if agent.GetLabels()["infraenvs.agent-install.openshift.io"] == infraEnvName {
		return true
	}
	for _, ref := range agent.GetOwnerReferences() {
		if ref.Kind == k8sKindInfraEnv && ref.Name == infraEnvName {
			return true
		}
	}
	return false
}

func desiredAgentHostname(pool *agentforgev1alpha1.VsphereAgentPool) string {
	return agentHostnameWithSuffix(vmNamePrefix(pool), randomAlphaNumeric(4))
}

func agentHostnameWithSuffix(prefix, suffix string) string {
	maxPrefixLen := 63 - len(suffix) - 1
	if len(prefix) > maxPrefixLen {
		prefix = strings.TrimRight(prefix[:maxPrefixLen], "-")
	}
	if prefix == "" {
		return suffix
	}
	return fmt.Sprintf("%s-%s", prefix, suffix)
}

func removeOwnedVM(vms []agentforgev1alpha1.OwnedVMStatus, name string) []agentforgev1alpha1.OwnedVMStatus {
	result := vms[:0]
	for _, vm := range vms {
		if vm.Name != name {
			result = append(result, vm)
		}
	}
	return result
}

func agentMachineWatchObject() *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(agentMachineGVK)
	return obj
}

func machineWatchObject() *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(machineGVK)
	return obj
}

func agentWatchObject() *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(agentGVK)
	return obj
}

func agentChangePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool {
			return true
		},
		DeleteFunc: func(event.DeleteEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}
			if deletionTimestampChanged(e.ObjectOld, e.ObjectNew) {
				return true
			}
			if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
				return true
			}
			if !reflect.DeepEqual(e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels()) {
				return true
			}
			if !reflect.DeepEqual(e.ObjectOld.GetOwnerReferences(), e.ObjectNew.GetOwnerReferences()) {
				return true
			}
			oldObj, oldOK := e.ObjectOld.(*unstructured.Unstructured)
			newObj, newOK := e.ObjectNew.(*unstructured.Unstructured)
			if !oldOK || !newOK {
				return false
			}
			return agentInventoryIdentityChanged(oldObj, newObj)
		},
	}
}

func agentInventoryIdentityChanged(oldObj, newObj *unstructured.Unstructured) bool {
	for _, path := range [][]string{
		{unstructuredFieldStatus, "inventory", "hostname"},
		{unstructuredFieldStatus, "inventory", "systemVendor", "serialNumber"},
	} {
		oldValue, _, _ := unstructured.NestedString(oldObj.Object, path...)
		newValue, _, _ := unstructured.NestedString(newObj.Object, path...)
		if oldValue != newValue {
			return true
		}
	}
	return normalizeMAC(agentPrimaryMAC(oldObj)) != normalizeMAC(agentPrimaryMAC(newObj))
}

func agentMachineChangePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool {
			return true
		},
		DeleteFunc: func(event.DeleteEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldObj, oldOK := e.ObjectOld.(*unstructured.Unstructured)
			newObj, newOK := e.ObjectNew.(*unstructured.Unstructured)
			if !oldOK || !newOK {
				return false
			}
			if deletionTimestampChanged(oldObj, newObj) {
				return true
			}
			if !reflect.DeepEqual(oldObj.GetAnnotations(), newObj.GetAnnotations()) {
				return true
			}
			if objectConditionStatusReasonChanged(oldObj, newObj, conditionReady) {
				return true
			}
			return agentMachineAssignmentChanged(oldObj, newObj)
		},
	}
}

func agentMachineAssignmentChanged(oldObj, newObj *unstructured.Unstructured) bool {
	for _, path := range [][]string{
		{unstructuredFieldStatus, "agentRef", "name"},
		{unstructuredFieldSpec, "providerID"},
	} {
		oldValue, _, _ := unstructured.NestedString(oldObj.Object, path...)
		newValue, _, _ := unstructured.NestedString(newObj.Object, path...)
		if oldValue != newValue {
			return true
		}
	}
	return false
}

func objectConditionStatusReasonChanged(oldObj, newObj *unstructured.Unstructured, conditionType string) bool {
	oldStatus, oldReason := objectConditionStatusReason(oldObj, conditionType)
	newStatus, newReason := objectConditionStatusReason(newObj, conditionType)
	return oldStatus != newStatus || oldReason != newReason
}

func machineChangePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool {
			return true
		},
		DeleteFunc: func(event.DeleteEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldObj, oldOK := e.ObjectOld.(*unstructured.Unstructured)
			newObj, newOK := e.ObjectNew.(*unstructured.Unstructured)
			if !oldOK || !newOK {
				return false
			}
			if deletionTimestampChanged(oldObj, newObj) {
				return true
			}
			if !reflect.DeepEqual(oldObj.GetAnnotations(), newObj.GetAnnotations()) {
				return true
			}
			return machinePhase(oldObj) != machinePhase(newObj)
		},
	}
}

func vsphereAgentPoolChangePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool {
			return true
		},
		DeleteFunc: func(event.DeleteEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}
			if deletionTimestampChanged(e.ObjectOld, e.ObjectNew) {
				return true
			}
			if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
				return true
			}
			if !reflect.DeepEqual(e.ObjectOld.GetAnnotations(), e.ObjectNew.GetAnnotations()) {
				return true
			}
			return !reflect.DeepEqual(e.ObjectOld.GetFinalizers(), e.ObjectNew.GetFinalizers())
		},
	}
}

func deletionTimestampChanged(oldObj, newObj client.Object) bool {
	oldDeleting := oldObj.GetDeletionTimestamp() != nil
	newDeleting := newObj.GetDeletionTimestamp() != nil
	return oldDeleting != newDeleting
}

func (r *VsphereAgentPoolReconciler) requestsForControlPlaneObjectChange(ctx context.Context, o client.Object) []reconcile.Request {
	obj, ok := o.(*unstructured.Unstructured)
	if !ok {
		err := fmt.Errorf("expected an unstructured control plane object, got %T", o)
		ctrl.LoggerFrom(ctx).Error(err, "failed to get requests for control plane object change")
		return nil
	}

	var pools agentforgev1alpha1.VsphereAgentPoolList
	if err := r.List(ctx, &pools, client.MatchingFields{
		vsphereAgentPoolControlPlaneNamespaceIndex: obj.GetNamespace(),
	}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "failed to list VsphereAgentPools for control plane object change")
		return nil
	}

	reqs := make([]reconcile.Request, 0, len(pools.Items))
	for i := range pools.Items {
		pool := &pools.Items[i]
		if controlPlaneObjectMatchesPool(obj, pool) {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pool)})
		}
	}
	return reqs
}

func (r *VsphereAgentPoolReconciler) requestsForAgentChange(ctx context.Context, o client.Object) []reconcile.Request {
	agent, ok := o.(*unstructured.Unstructured)
	if !ok {
		err := fmt.Errorf("expected an unstructured Agent, got %T", o)
		ctrl.LoggerFrom(ctx).Error(err, "failed to get requests for Agent change")
		return nil
	}

	var pools agentforgev1alpha1.VsphereAgentPoolList
	if err := r.List(ctx, &pools, client.InNamespace(agent.GetNamespace())); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "failed to list VsphereAgentPools for Agent change")
		return nil
	}

	reqs := make([]reconcile.Request, 0, len(pools.Items))
	for i := range pools.Items {
		pool := &pools.Items[i]
		reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pool)})
	}
	return reqs
}

func controlPlaneObjectMatchesPool(obj *unstructured.Unstructured, pool *agentforgev1alpha1.VsphereAgentPool) bool {
	if pool.Spec.ControlPlaneNamespace != obj.GetNamespace() {
		return false
	}
	expectedNodePool := fmt.Sprintf("%s/%s", pool.Namespace, pool.Spec.NodePoolRef.Name)
	return obj.GetAnnotations()[nodePoolAnnotation] == expectedNodePool
}

// SetupWithManager sets up the controller with the Manager.
func (r *VsphereAgentPoolReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("vsphereagentpool-controller")
	}
	if err := mgr.GetFieldIndexer().IndexField(ctx, &agentforgev1alpha1.VsphereAgentPool{}, vsphereAgentPoolControlPlaneNamespaceIndex,
		func(o client.Object) []string {
			pool := o.(*agentforgev1alpha1.VsphereAgentPool)
			if pool.Spec.ControlPlaneNamespace == "" {
				return nil
			}
			return []string{pool.Spec.ControlPlaneNamespace}
		}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&agentforgev1alpha1.VsphereAgentPool{}, builder.WithPredicates(vsphereAgentPoolChangePredicate())).
		Owns(&agentforgev1alpha1.VsphereAgent{}).
		Watches(agentMachineWatchObject(), handler.EnqueueRequestsFromMapFunc(r.requestsForControlPlaneObjectChange), builder.WithPredicates(agentMachineChangePredicate())).
		Watches(machineWatchObject(), handler.EnqueueRequestsFromMapFunc(r.requestsForControlPlaneObjectChange), builder.WithPredicates(machineChangePredicate())).
		Watches(agentWatchObject(), handler.EnqueueRequestsFromMapFunc(r.requestsForAgentChange), builder.WithPredicates(agentChangePredicate())).
		Named("vsphereagentpool").
		Complete(r)
}
