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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
)

const (
	finalizerName = "agentforge.containeroo.ch/vsphere-agent-pool"

	nodePoolAnnotation = "hypershift.openshift.io/nodePool"
	roleLabelKey       = "hypershift.openshift.io/nodepool-role"
	agentMachineRefKey = "agentMachineRef"
	apiVersionV1Beta1  = "v1beta1"
)

var (
	machineSetGVK = schema.GroupVersionKind{Group: "cluster.x-k8s.io", Version: apiVersionV1Beta1, Kind: "MachineSet"}
	infraEnvGVK   = schema.GroupVersionKind{Group: "agent-install.openshift.io", Version: apiVersionV1Beta1, Kind: "InfraEnv"}
	agentGVK      = schema.GroupVersionKind{Group: "agent-install.openshift.io", Version: apiVersionV1Beta1, Kind: "Agent"}
)

// VsphereAgentPoolReconciler reconciles a VsphereAgentPool object.
type VsphereAgentPoolReconciler struct {
	client.Client
	APIReader       client.Reader
	Scheme          *runtime.Scheme
	Recorder        record.EventRecorder
	ProviderFactory VMProviderFactory
}

// +kubebuilder:rbac:groups=agentforge.containeroo.ch,resources=vsphereagentpools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=agentforge.containeroo.ch,resources=vsphereagentpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agentforge.containeroo.ch,resources=vsphereagentpools/finalizers,verbs=update
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machinesets,verbs=get;list;watch
// +kubebuilder:rbac:groups=agent-install.openshift.io,resources=infraenvs;agents,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile plans and applies vSphere-backed Agent inventory for one hosted
// cluster NodePool. The hosted cluster autoscaler remains the source of truth:
// this controller only reacts to the rendered CAPI MachineSet replica count.
func (r *VsphereAgentPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var pool agentforgev1alpha1.VsphereAgentPool
	if err := r.Get(ctx, req.NamespacedName, &pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	applySpecDefaults(&pool)

	if pool.DeletionTimestamp.IsZero() {
		if controllerutil.AddFinalizer(&pool, finalizerName) {
			if err := r.Update(ctx, &pool); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	} else {
		return r.reconcileDelete(ctx, &pool)
	}

	machineSet, err := r.findMachineSet(ctx, &pool)
	if err != nil {
		r.setStatusError(&pool, conditionMachineSetFound, "MachineSetNotFound", err.Error())
		_ = r.updateStatus(ctx, &pool, PoolPlan{})
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
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

	replicas := machineSetReplicas(machineSet)
	plan := buildPlan(&pool, PoolSnapshot{
		MachineSetReplicas: replicas,
		MatchingAgents:     agents,
		OwnedVMs:           pool.Status.OwnedVMs,
	})

	pool.Status.ObservedMachineSet = machineSet.GetName()
	pool.Status.MachineSetReplicas = replicas

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
		r.recordWarning(&pool, "ApplyPlanFailed", err.Error())
		setPlanConditions(&pool, plan, false, err.Error())
		if statusErr := r.updateStatus(ctx, &pool, plan); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	r.recordPlan(&pool, plan, "PlanApplied")
	setPlanConditions(&pool, plan, false, "")
	if err := r.updateStatus(ctx, &pool, plan); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: time.Minute}, nil
}

func (r *VsphereAgentPoolReconciler) reconcileDelete(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool) (ctrl.Result, error) {
	if pool.Spec.DryRun || pool.Spec.Scaling.DeletePolicy == deletePolicyRetain || len(pool.Status.OwnedVMs) == 0 {
		controllerutil.RemoveFinalizer(pool, finalizerName)
		return ctrl.Result{}, r.Update(ctx, pool)
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
	return ctrl.Result{}, r.Update(ctx, pool)
}

func (r *VsphereAgentPoolReconciler) applyPlan(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, plan PoolPlan, isoDownloadURL string) error {
	for _, action := range plan.Actions {
		if action.Type == actionPatchAgent {
			if err := r.patchAgent(ctx, pool, action.Name); err != nil {
				return err
			}
		}
	}

	if plan.VMsToCreate > 0 || len(plan.VMsToDelete) > 0 {
		provider, err := r.provider(ctx, pool)
		if err != nil {
			return err
		}
		for i := int32(0); i < plan.VMsToCreate; i++ {
			vm, err := provider.CreateVM(ctx, pool, VMCreateRequest{Ordinal: i, ISODownloadURL: isoDownloadURL})
			if err != nil {
				return err
			}
			pool.Status.OwnedVMs = upsertOwnedVM(pool.Status.OwnedVMs, vm)
		}
		for _, vm := range plan.VMsToDelete {
			if err := provider.DeleteVM(ctx, pool, vm); err != nil {
				return err
			}
			pool.Status.OwnedVMs = removeOwnedVM(pool.Status.OwnedVMs, vm.Name)
		}
	}

	return nil
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

func (r *VsphereAgentPoolReconciler) findMachineSet(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool) (*unstructured.Unstructured, error) {
	if pool.Spec.MachineSetName != "" {
		ms := &unstructured.Unstructured{}
		ms.SetGroupVersionKind(machineSetGVK)
		err := r.Get(ctx, types.NamespacedName{Namespace: pool.Spec.ControlPlaneNamespace, Name: pool.Spec.MachineSetName}, ms)
		return ms, err
	}

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(machineSetGVK)
	if err := r.List(ctx, list, client.InNamespace(pool.Spec.ControlPlaneNamespace)); err != nil {
		return nil, err
	}
	expectedNodePool := fmt.Sprintf("%s/%s", pool.Namespace, pool.Spec.NodePoolRef.Name)
	for i := range list.Items {
		if list.Items[i].GetAnnotations()[nodePoolAnnotation] == expectedNodePool {
			return &list.Items[i], nil
		}
	}
	return nil, fmt.Errorf("no MachineSet in namespace %q has annotation %s=%q", pool.Spec.ControlPlaneNamespace, nodePoolAnnotation, expectedNodePool)
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
	if err := r.List(ctx, list, client.InNamespace(pool.Namespace), client.MatchingLabels(pool.Spec.Agent.Labels)); err != nil {
		return nil, err
	}

	agents := make([]AgentInfo, 0, len(list.Items))
	for i := range list.Items {
		obj := &list.Items[i]
		labels := obj.GetLabels()
		approved, _, _ := unstructured.NestedBool(obj.Object, "spec", "approved")
		hostname, _, _ := unstructured.NestedString(obj.Object, "spec", "hostname")
		if hostname == "" {
			hostname, _, _ = unstructured.NestedString(obj.Object, "status", "inventory", "hostname")
		}
		clusterName, _, _ := unstructured.NestedString(obj.Object, "spec", "clusterDeploymentName", "name")
		agents = append(agents, AgentInfo{
			Name:      obj.GetName(),
			Bound:     labels[agentMachineRefKey] != "" || clusterName == pool.Spec.HostedClusterRef.Name,
			Approved:  approved,
			RoleLabel: labels[roleLabelKey],
			Hostname:  hostname,
			MAC:       normalizeMAC(hostname),
		})
	}
	return agents, nil
}

func (r *VsphereAgentPoolReconciler) patchAgent(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, name string) error {
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
	if approveAgents(pool) {
		if err := unstructured.SetNestedField(agent.Object, true, "spec", "approved"); err != nil {
			return err
		}
	}

	return r.Patch(ctx, agent, client.MergeFrom(before))
}

func (r *VsphereAgentPoolReconciler) updateStatus(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, plan PoolPlan) error {
	pool.Status.ObservedGeneration = pool.Generation
	pool.Status.DesiredReplicas = plan.DesiredReplicas
	pool.Status.MatchingAgents = plan.MatchingAgents
	pool.Status.BoundAgents = plan.BoundAgents
	pool.Status.AvailableAgents = plan.AvailableAgents
	pool.Status.PlannedActions = plan.Actions
	return r.Status().Update(ctx, pool)
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
		Type:               conditionMachineSetFound,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: pool.Generation,
		Reason:             "Found",
		Message:            fmt.Sprintf("Following MachineSet %s", pool.Status.ObservedMachineSet),
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
		Message:            fmt.Sprintf("desired=%d matchingAgents=%d boundAgents=%d availableAgents=%d", plan.DesiredReplicas, plan.MatchingAgents, plan.BoundAgents, plan.AvailableAgents),
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
}

func machineSetReplicas(machineSet *unstructured.Unstructured) int32 {
	replicas, found, _ := unstructured.NestedInt64(machineSet.Object, "spec", "replicas")
	if !found {
		return 0
	}
	return int32(replicas) //nolint:gosec // Kubernetes replica counts fit in int32.
}

func approveAgents(pool *agentforgev1alpha1.VsphereAgentPool) bool {
	return pool.Spec.Agent.Approve == nil || *pool.Spec.Agent.Approve
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

// SetupWithManager sets up the controller with the Manager.
func (r *VsphereAgentPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("vsphereagentpool-controller")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&agentforgev1alpha1.VsphereAgentPool{}).
		Named("vsphereagentpool").
		Complete(r)
}
