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
	"k8s.io/client-go/tools/record"
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
	finalizerName = "agentforge.containeroo.ch/vsphere-agent-pool"

	forceISORefreshAnnotation  = "agentforge.containeroo.ch/force-iso-refresh"
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
	infraEnvGVK     = schema.GroupVersionKind{Group: "agent-install.openshift.io", Version: apiVersionV1Beta1, Kind: "InfraEnv"}
	agentGVK        = schema.GroupVersionKind{Group: "agent-install.openshift.io", Version: apiVersionV1Beta1, Kind: "Agent"}
)

// VsphereAgentPoolReconciler reconciles a VsphereAgentPool object.
type VsphereAgentPoolReconciler struct {
	client.Client
	APIReader       client.Reader
	Scheme          *runtime.Scheme
	Recorder        record.EventRecorder
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

// +kubebuilder:rbac:groups=agentforge.containeroo.ch,resources=vsphereagentpools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=agentforge.containeroo.ch,resources=vsphereagentpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agentforge.containeroo.ch,resources=vsphereagentpools/finalizers,verbs=update
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines,verbs=get;list;watch
// +kubebuilder:rbac:groups=capi-provider.agent-install.openshift.io,resources=agentmachines,verbs=get;list;watch
// +kubebuilder:rbac:groups=agent-install.openshift.io,resources=infraenvs;agents,verbs=get;list;watch;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

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

	infraEnvAvailable, infraEnvISOURL, infraEnvMessage := r.infraEnvAvailable(ctx, &pool)
	if !infraEnvAvailable {
		meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
			Type:               conditionInfraEnvAvailable,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: pool.Generation,
			Reason:             "InfraEnvUnavailable",
			Message:            infraEnvMessage,
		})
		_ = r.updateStatus(ctx, &pool, PoolPlan{})
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	agents, err := r.listMatchingAgents(ctx, &pool)
	if err != nil {
		r.setStatusError(&pool, conditionReady, "AgentListFailed", err.Error())
		_ = r.updateStatus(ctx, &pool, PoolPlan{})
		return ctrl.Result{}, err
	}
	machines, err := r.listNodePoolMachines(ctx, &pool)
	if err != nil {
		r.setStatusError(&pool, conditionReady, "MachineListFailed", err.Error())
		_ = r.updateStatus(ctx, &pool, PoolPlan{})
		return ctrl.Result{}, err
	}
	agentMachineDemand, err := r.countAgentMachineDemand(ctx, &pool)
	if err != nil {
		r.setStatusError(&pool, conditionReady, "AgentMachineListFailed", err.Error())
		_ = r.updateStatus(ctx, &pool, PoolPlan{})
		return ctrl.Result{}, err
	}
	pool.Status.OwnedVMs = refreshOwnedVMStatuses(&pool, agents, machines)

	plan := buildPlan(&pool, PoolSnapshot{
		AgentMachines:             agentMachineDemand.Total,
		WaitingAgentMachines:      agentMachineDemand.Waiting,
		UnreadyAgentMachines:      agentMachineDemand.Unready,
		AgentMachinesWithoutAgent: agentMachineDemand.WithoutAgent,
		MatchingAgents:            agents,
		OwnedVMs:                  pool.Status.OwnedVMs,
	})

	if pool.Spec.DryRun {
		r.recordPlan(&pool, plan, "DryRunPlan")
		setPlanConditions(&pool, plan, true, "")
		if err := r.updateStatus(ctx, &pool, plan); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("dry-run plan computed", "actions", plan.Actions, "desiredReplicas", plan.DesiredReplicas, "matchingAgents", plan.MatchingAgents)
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	if err := r.applyPlan(ctx, &pool, plan, infraEnvISOURL); err != nil {
		errMessage := stableErrorMessage(err)
		r.recordWarning(&pool, "ApplyPlanFailed", errMessage)
		setPlanConditions(&pool, plan, false, errMessage)
		if statusErr := r.updateStatus(ctx, &pool, plan); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		log.Error(err, "apply plan failed", "retryAfter", 30*time.Second)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	refreshPlanOwnedVMCounts(&plan, &pool)

	r.recordPlan(&pool, plan, "PlanApplied")
	setPlanConditions(&pool, plan, false, "")
	if err := r.updateStatus(ctx, &pool, plan); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: time.Minute}, nil
}

func refreshPlanOwnedVMCounts(plan *PoolPlan, pool *agentforgev1alpha1.VsphereAgentPool) {
	plan.PendingOwnedVMs = countPendingOwnedVMs(pool.Status.OwnedVMs)
}

func (r *VsphereAgentPoolReconciler) reconcileDelete(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool) (ctrl.Result, error) {
	if pool.Spec.DryRun || pool.Spec.Scaling.DeletePolicy == deletePolicyRetain || len(pool.Status.OwnedVMs) == 0 {
		controllerutil.RemoveFinalizer(pool, finalizerName)
		return ctrl.Result{}, r.patchFinalizer(ctx, pool)
	}

	provider, err := r.provider(ctx, pool)
	if err != nil {
		return ctrl.Result{}, err
	}
	for _, vm := range pool.Status.OwnedVMs {
		if err := provider.DeleteVM(ctx, pool, vm); err != nil {
			return ctrl.Result{}, err
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
	current.SetFinalizers(pool.GetFinalizers())
	return r.Patch(ctx, current, client.MergeFrom(before))
}

func (r *VsphereAgentPoolReconciler) applyPlan(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, plan PoolPlan, isoDownloadURL string) error {
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

	if plan.VMsToCreate > 0 || len(plan.VMsToDelete) > 0 {
		provider, err := r.provider(ctx, pool)
		if err != nil {
			return err
		}
		isoPath := pool.Status.ISO.Path
		if plan.VMsToCreate > 0 {
			isoPath, err = r.ensureISOCache(ctx, pool, provider, isoDownloadURL)
			if err != nil {
				r.setStatusError(pool, conditionISOReady, "ISORefreshFailed", err.Error())
				return err
			}
		}
		for i := int32(0); i < plan.VMsToCreate; i++ {
			vm, err := provider.CreateVM(ctx, pool, VMCreateRequest{Ordinal: i, ISOPath: isoPath})
			if err != nil {
				return err
			}
			pool.Status.OwnedVMs = upsertOwnedVM(pool.Status.OwnedVMs, vm)
		}
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
			if err := provider.DeleteVM(ctx, pool, vm); err != nil {
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
		return "", err
	}

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
			r.recordWarning(pool, "ISOPruneFailed", stableErrorMessage(err))
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
		r.Recorder.Event(pool, corev1.EventTypeNormal, "ISO"+reason, message)
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
		machineName := labels[agentMachineRefKey]
		agents = append(agents, AgentInfo{
			Name:              obj.GetName(),
			Bound:             machineName != "" || clusterName == pool.Spec.HostedClusterRef.Name,
			MachineName:       machineName,
			Approved:          approved,
			SpecRole:          specRole,
			RoleLabel:         labels[roleLabelKey],
			Hostname:          specHostname,
			InventoryHostname: inventoryHostname,
			MAC:               normalizeMAC(agentPrimaryMAC(obj)),
			BIOSUUID:          normalizeVMwareSerialUUID(serialNumber),
		})
	}
	return agents, nil
}

func assignedAgentHostnames(pool *agentforgev1alpha1.VsphereAgentPool, agents []AgentInfo) map[string]string {
	assigned := map[string]string{}
	reserved := map[string]struct{}{}
	for _, agent := range agents {
		if agent.Hostname == "" {
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
	byHostname := map[string]AgentInfo{}
	byName := map[string]AgentInfo{}
	byBIOSUUID := map[string]AgentInfo{}
	byMAC := map[string]AgentInfo{}
	for _, agent := range agents {
		byName[agent.Name] = agent
		if hostname := agentObservedHostname(agent); hostname != "" {
			byHostname[hostname] = agent
		}
		if agent.BIOSUUID != "" {
			byBIOSUUID[agent.BIOSUUID] = agent
		}
		if agent.MAC != "" {
			byMAC[agent.MAC] = agent
		}
	}
	machineStates := map[string]MachineInfo{}
	for _, machine := range machines {
		machineStates[machine.Name] = machine
	}

	vms := make([]agentforgev1alpha1.OwnedVMStatus, 0, len(pool.Status.OwnedVMs))
	matchedAgents := map[string]struct{}{}
	knownVMNames := map[string]struct{}{}
	for _, vm := range pool.Status.OwnedVMs {
		if vm.Name != "" {
			knownVMNames[vm.Name] = struct{}{}
		}
		agent, matched := byBIOSUUID[vm.BIOSUUID]
		if !matched && vm.MACAddress != "" {
			agent, matched = byMAC[vm.MACAddress]
		}
		if !matched {
			agent, matched = byHostname[vm.Name]
		}
		if !matched && vm.AgentRef != nil && vm.AgentRef.Name != "" {
			agent, matched = byName[vm.AgentRef.Name]
		}
		if !matched {
			hadDiscoveredAgent := vmHadDiscoveredAgent(vm)
			vm.AgentRef = nil
			vm.MachineRef = nil
			if hadDiscoveredAgent {
				setOwnedVMPhase(&vm, phaseOrphaned, "AgentMissing")
			} else if ownedVMDiscoveryExpired(vm, time.Now()) {
				setOwnedVMPhase(&vm, phaseOrphaned, "AgentDiscoveryExpired")
			} else {
				setOwnedVMPhase(&vm, phaseProvisioning, "AgentNotDiscovered")
			}
			vms = append(vms, vm)
			continue
		}
		matchedAgents[agent.Name] = struct{}{}

		wasMachineDeleting := vm.Phase == phaseReleased && vm.Reason == "MachineDeleting"
		previousMachineRef := vm.MachineRef
		applyAgentToOwnedVMStatus(pool, &vm, agent)
		if wasMachineDeleting {
			if (vm.MachineRef == nil || vm.MachineRef.Name == "") && previousMachineRef != nil && previousMachineRef.Name != "" {
				vm.MachineRef = previousMachineRef
			}
			setOwnedVMPhase(&vm, phaseReleased, "MachineDeleting")
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

func vmMatchesAgentIdentity(vm agentforgev1alpha1.OwnedVMStatus, agent AgentInfo) bool {
	if vm.BIOSUUID != "" && agent.BIOSUUID != "" && vm.BIOSUUID == agent.BIOSUUID {
		return true
	}
	return vm.MACAddress != "" && agent.MAC != "" && vm.MACAddress == agent.MAC
}

func applyMachineStateToOwnedVMStatus(vm *agentforgev1alpha1.OwnedVMStatus, machines map[string]MachineInfo) {
	if vm.MachineRef == nil || vm.MachineRef.Name == "" {
		if vm.Phase == phaseReleased && vm.Reason == "MachineDeleting" {
			setOwnedVMPhase(vm, phaseReleased, "MachineDeleted")
		}
		return
	}
	machine, exists := machines[vm.MachineRef.Name]
	if !exists {
		if vm.Phase == phaseReleased && vm.Reason == "MachineDeleting" {
			setOwnedVMPhase(vm, phaseReleased, "MachineDeleted")
		}
		return
	}
	if machine.Deleting {
		setOwnedVMPhase(vm, phaseReleased, "MachineDeleting")
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
	if vm.Phase != phaseProvisioning || vm.Reason != "AgentNotDiscovered" || vm.LastTransitionTime.IsZero() {
		return false
	}
	return !now.Before(vm.LastTransitionTime.Time.Add(orphanedOwnedVMGracePeriod))
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
		r.Recorder.Eventf(pool, corev1.EventTypeNormal, "AgentDeleted", "deleted stale unbound Agent %s", name)
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

	var current agentforgev1alpha1.VsphereAgentPool
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, &current); err != nil {
		return err
	}
	if reflect.DeepEqual(current.Status, desired) {
		pool.Status = desired
		return nil
	}
	current.Status = desired
	if err := r.Status().Update(ctx, &current); err != nil {
		return err
	}
	pool.Status = desired
	return nil
}

func (r *VsphereAgentPoolReconciler) setStatusError(pool *agentforgev1alpha1.VsphereAgentPool, conditionType, reason, message string) {
	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               conditionType,
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
		r.Recorder.Event(pool, corev1.EventTypeNormal, reason, plan.Actions[0].Reason)
		return
	}
	r.Recorder.Eventf(pool, corev1.EventTypeNormal, reason, "planned %d action(s): %s", len(plan.Actions), summarizeActions(plan.Actions))
}

func (r *VsphereAgentPoolReconciler) recordWarning(pool *agentforgev1alpha1.VsphereAgentPool, reason, message string) {
	if r.Recorder != nil {
		r.Recorder.Event(pool, corev1.EventTypeWarning, reason, message)
	}
}

func (r *VsphereAgentPoolReconciler) recordNormal(pool *agentforgev1alpha1.VsphereAgentPool, reason, message string) {
	if r.Recorder != nil {
		r.Recorder.Event(pool, corev1.EventTypeNormal, reason, message)
	}
}

func setPlanConditions(pool *agentforgev1alpha1.VsphereAgentPool, plan PoolPlan, dryRun bool, errMessage string) {
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
		Type:               conditionDryRun,
		Status:             conditionStatus(dryRun),
		ObservedGeneration: pool.Generation,
		Reason:             boolReason(dryRun, "Enabled", "Disabled"),
		Message:            boolMessage(dryRun, "Dry-run is enabled; no vSphere or Agent mutations are applied", "Dry-run is disabled; planned mutations may be applied"),
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
		pool.Spec.VSphere.Datacenter = "dc1"
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
	if pool.Spec.Scaling.MaxProvisioning == 0 {
		pool.Spec.Scaling.MaxProvisioning = 3
	}
	if pool.Spec.Scaling.DeletePolicy == "" {
		pool.Spec.Scaling.DeletePolicy = deletePolicyOwnedOnly
	}
	if pool.Spec.ISO.CheckInterval.Duration == 0 {
		pool.Spec.ISO.CheckInterval.Duration = 10 * time.Minute
	}
	if pool.Spec.ISO.RetainVersions == 0 {
		pool.Spec.ISO.RetainVersions = 2
	}
}

func approveAgents(pool *agentforgev1alpha1.VsphereAgentPool) bool {
	return pool.Spec.Agent.Approve == nil || *pool.Spec.Agent.Approve
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
		if key == roleLabelKey {
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
		if ref.Kind == "InfraEnv" && ref.Name == infraEnvName {
			return true
		}
	}
	return false
}

func desiredAgentHostname(pool *agentforgev1alpha1.VsphereAgentPool) string {
	prefix := vmNamePrefix(pool)
	suffix := randomAlphaNumeric(4)
	maxPrefixLen := 63 - len(suffix) - 1
	if len(prefix) > maxPrefixLen {
		prefix = strings.TrimRight(prefix[:maxPrefixLen], "-")
	}
	return fmt.Sprintf("%s-%s", prefix, suffix)
}

func upsertOwnedVM(vms []agentforgev1alpha1.OwnedVMStatus, vm agentforgev1alpha1.OwnedVMStatus) []agentforgev1alpha1.OwnedVMStatus {
	for i := range vms {
		if vms[i].Name == vm.Name {
			vms[i] = vm
			return vms
		}
	}
	return append(vms, vm)
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
			if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
				return true
			}
			if !reflect.DeepEqual(e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels()) {
				return true
			}
			return !reflect.DeepEqual(e.ObjectOld.GetOwnerReferences(), e.ObjectNew.GetOwnerReferences())
		},
	}
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
			if !reflect.DeepEqual(oldObj.GetAnnotations(), newObj.GetAnnotations()) {
				return true
			}
			return objectConditionStatusReasonChanged(oldObj, newObj, conditionReady)
		},
	}
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
			if !reflect.DeepEqual(oldObj.GetAnnotations(), newObj.GetAnnotations()) {
				return true
			}
			if oldObj.GetDeletionTimestamp() == nil && newObj.GetDeletionTimestamp() != nil {
				return true
			}
			if oldObj.GetDeletionTimestamp() != nil && newObj.GetDeletionTimestamp() == nil {
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
		if agentMatchesPool(agent, pool) {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pool)})
		}
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

func agentMatchesPool(agent *unstructured.Unstructured, pool *agentforgev1alpha1.VsphereAgentPool) bool {
	if agent.GetNamespace() != pool.Namespace {
		return false
	}
	if !agentBelongsToInfraEnv(agent, pool.Spec.InfraEnvRef.Name) {
		return false
	}
	labels := agent.GetLabels()
	for key, value := range agentCandidateLabels(pool) {
		if labels[key] != value {
			return false
		}
	}
	return true
}

// SetupWithManager sets up the controller with the Manager.
func (r *VsphereAgentPoolReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("vsphereagentpool-controller")
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
		Watches(agentMachineWatchObject(), handler.EnqueueRequestsFromMapFunc(r.requestsForControlPlaneObjectChange), builder.WithPredicates(agentMachineChangePredicate())).
		Watches(machineWatchObject(), handler.EnqueueRequestsFromMapFunc(r.requestsForControlPlaneObjectChange), builder.WithPredicates(machineChangePredicate())).
		Watches(agentWatchObject(), handler.EnqueueRequestsFromMapFunc(r.requestsForAgentChange), builder.WithPredicates(agentChangePredicate())).
		Named("vsphereagentpool").
		Complete(r)
}
