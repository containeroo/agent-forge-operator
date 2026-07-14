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
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
)

// VsphereAgentReconciler reconciles one VsphereAgent into one vSphere VM.
type VsphereAgentReconciler struct {
	client.Client
	APIReader       client.Reader
	Scheme          *runtime.Scheme
	Recorder        events.EventRecorder
	ProviderFactory VMProviderFactory
}

// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=vsphereagents,verbs=get;list;watch;patch;delete
// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=vsphereagents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=vsphereagents/finalizers,verbs=update
// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=vsphereagentpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=vsphereagentpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agent-install.openshift.io,resources=infraenvs,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch;update

func (r *VsphereAgentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var agent agentforgev1alpha1.VsphereAgent
	if err := r.apiReader().Get(ctx, req.NamespacedName, &agent); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	var pool agentforgev1alpha1.VsphereAgentPool
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: agent.Namespace, Name: agent.Spec.PoolRef.Name}, &pool); err != nil {
		if apierrors.IsNotFound(err) {
			if !agent.DeletionTimestamp.IsZero() {
				if agent.Status.VM.Name != "" && r.Recorder != nil {
					recordEvent(r.Recorder, &agent, corev1.EventTypeWarning, "PoolNotFound", "referenced VsphereAgentPool is gone; retaining VM and removing finalizer")
				}
				controllerutil.RemoveFinalizer(&agent, vsphereAgentFinalizerName)
				return ctrl.Result{}, r.patchFinalizer(ctx, &agent)
			}
			meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
				Type:               conditionReady,
				Status:             metav1.ConditionFalse,
				ObservedGeneration: agent.Generation,
				Reason:             "PoolNotFound",
				Message:            "referenced VsphereAgentPool does not exist",
			})
			if statusErr := r.updateStatus(ctx, &agent); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}
		return ctrl.Result{}, err
	}
	applySpecDefaults(&pool)

	if agent.DeletionTimestamp.IsZero() {
		if controllerutil.AddFinalizer(&agent, vsphereAgentFinalizerName) {
			return ctrl.Result{}, r.patchFinalizer(ctx, &agent)
		}
	} else {
		return r.reconcileDelete(ctx, &agent, &pool)
	}

	vmName := vsphereAgentVMName(&agent)
	if agent.Status.VM.Name != "" {
		return r.reconcileExistingVM(ctx, &agent, &pool)
	}

	if agent.Labels[vsphereAgentCreatedForLabel] == vsphereAgentCreatedForAdopted {
		agent.Status.VM = newOwnedVMStatus(vmName)
		agent.Status.VM.Reason = reasonVMAdopted
		vm, err := r.refreshVMIdentity(ctx, &pool, agent.Status.VM, "")
		if err != nil {
			meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
				Type:               conditionReady,
				Status:             metav1.ConditionFalse,
				ObservedGeneration: agent.Generation,
				Reason:             "AdoptedVMUnavailable",
				Message:            stableErrorMessage(err),
			})
			if statusErr := r.updateStatus(ctx, &agent); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		agent.Status.VM.BIOSUUID = vm.BIOSUUID
		agent.Status.VM.MACAddress = vm.MACAddress
		agent.Status.VM.OwnerUID = vm.OwnerUID
		meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
			Type:               conditionReady,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: agent.Generation,
			Reason:             reasonVMAdopted,
			Message:            "vSphere VM has been adopted",
		})
		if statusErr := r.updateStatus(ctx, &agent); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	poolOps := r.poolReconciler()
	infraEnvAvailable, infraEnvISOURL, infraEnvMessage := poolOps.infraEnvAvailable(ctx, &pool)
	if !infraEnvAvailable {
		meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
			Type:               conditionReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: agent.Generation,
			Reason:             reasonInfraEnvUnavailable,
			Message:            infraEnvMessage,
		})
		if statusErr := r.updateStatus(ctx, &agent); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	provider, err := poolOps.provider(ctx, &pool)
	if err != nil {
		meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
			Type:               conditionReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: agent.Generation,
			Reason:             "ProviderUnavailable",
			Message:            stableErrorMessage(err),
		})
		if statusErr := r.updateStatus(ctx, &agent); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	isoPath, err := poolOps.ensureISOCache(ctx, &pool, provider, infraEnvISOURL)
	if err != nil {
		meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
			Type:               conditionReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: agent.Generation,
			Reason:             "ISORefreshFailed",
			Message:            stableErrorMessage(err),
		})
		if statusErr := r.updateStatus(ctx, &agent); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if err := r.patchPoolISOStatus(ctx, &pool); err != nil {
		return ctrl.Result{}, err
	}

	vm, err := provider.CreateVM(ctx, &pool, VMCreateRequest{Name: vmName, ISOPath: isoPath, OwnerUID: string(agent.UID)})
	if err != nil {
		recordVMOperation("create", err)
		meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
			Type:               conditionReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: agent.Generation,
			Reason:             "VMCreateFailed",
			Message:            stableErrorMessage(err),
		})
		if statusErr := r.updateStatus(ctx, &agent); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	recordVMOperation("create", nil)
	agent.Status.VM = vm
	meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: agent.Generation,
		Reason:             "VMCreated",
		Message:            "vSphere VM has been created",
	})
	return ctrl.Result{RequeueAfter: time.Minute}, r.updateStatus(ctx, &agent)
}

func (r *VsphereAgentReconciler) reconcileExistingVM(ctx context.Context, agent *agentforgev1alpha1.VsphereAgent, pool *agentforgev1alpha1.VsphereAgentPool) (ctrl.Result, error) {
	if vm, found := ownedVMStatusForVsphereAgent(pool, agent); found {
		agent.Status.VM = vm
	}
	expectedOwnerUID := ""
	if agent.Labels[vsphereAgentCreatedForLabel] != vsphereAgentCreatedForAdopted {
		expectedOwnerUID = string(agent.UID)
	}
	vm, err := r.refreshVMIdentity(ctx, pool, agent.Status.VM, expectedOwnerUID)
	if err != nil {
		logf.FromContext(ctx).Error(err, "failed to refresh VM status", "vm", agent.Status.VM.Name)
		reason := "VMStatusFailed"
		message := stableErrorMessage(err)
		if errors.Is(err, errVMNotFound) {
			reason = "VMNotFound"
			message = "vSphere VM no longer exists"
			if agent.Labels[vsphereAgentCreatedForLabel] != vsphereAgentCreatedForAdopted {
				agent.Status.VM = agentforgev1alpha1.OwnedVMStatus{}
			}
		} else if errors.Is(err, errVMOwnershipMismatch) {
			reason = "VMOwnershipMismatch"
			message = "vSphere VM ownership does not match this VsphereAgent"
		}
		meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
			Type:               conditionReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: agent.Generation,
			Reason:             reason,
			Message:            message,
		})
		if statusErr := r.updateStatus(ctx, agent); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	agent.Status.VM.BIOSUUID = vm.BIOSUUID
	agent.Status.VM.MACAddress = vm.MACAddress
	if vm.OwnerUID != "" {
		agent.Status.VM.OwnerUID = vm.OwnerUID
	}
	readyReason := "VMCreated"
	readyMessage := "vSphere VM has been created"
	if agent.Labels[vsphereAgentCreatedForLabel] == vsphereAgentCreatedForAdopted {
		readyReason = reasonVMAdopted
		readyMessage = "vSphere VM has been adopted"
	}
	meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: agent.Generation,
		Reason:             readyReason,
		Message:            readyMessage,
	})
	return ctrl.Result{RequeueAfter: time.Minute}, r.updateStatus(ctx, agent)
}

func (r *VsphereAgentReconciler) refreshVMIdentity(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, vm agentforgev1alpha1.OwnedVMStatus, expectedOwnerUID string) (agentforgev1alpha1.OwnedVMStatus, error) {
	if vm.Name == "" {
		return vm, nil
	}
	provider, err := r.poolReconciler().provider(ctx, pool)
	if err != nil {
		return vm, err
	}
	discovered, err := provider.VMStatus(ctx, pool, vm.Name)
	if err != nil {
		return vm, err
	}
	if expectedOwnerUID != "" {
		if discovered.OwnerUID != "" && discovered.OwnerUID != expectedOwnerUID {
			return vm, fmt.Errorf("%w: VM %q belongs to another VsphereAgent", errVMOwnershipMismatch, vm.Name)
		}
		if vm.OwnerUID != "" && discovered.OwnerUID == "" {
			return vm, fmt.Errorf("%w: VM %q ownership annotation is missing", errVMOwnershipMismatch, vm.Name)
		}
	}
	if discovered.BIOSUUID != "" {
		vm.BIOSUUID = discovered.BIOSUUID
	}
	if discovered.MACAddress != "" {
		vm.MACAddress = discovered.MACAddress
	}
	if discovered.OwnerUID != "" {
		vm.OwnerUID = discovered.OwnerUID
	}
	return vm, nil
}

func ownedVMStatusForVsphereAgent(pool *agentforgev1alpha1.VsphereAgentPool, agent *agentforgev1alpha1.VsphereAgent) (agentforgev1alpha1.OwnedVMStatus, bool) {
	if agent.Status.VM.Name == "" {
		return agentforgev1alpha1.OwnedVMStatus{}, false
	}
	for _, vm := range pool.Status.OwnedVMs {
		if vm.Name == agent.Status.VM.Name {
			return vm, true
		}
	}
	return agentforgev1alpha1.OwnedVMStatus{}, false
}

func (r *VsphereAgentReconciler) reconcileDelete(ctx context.Context, agent *agentforgev1alpha1.VsphereAgent, pool *agentforgev1alpha1.VsphereAgentPool) (ctrl.Result, error) {
	vm := agent.Status.VM
	if vm.Name == "" {
		vm = newOwnedVMStatus(vsphereAgentVMName(agent))
		vm.OwnerUID = string(agent.UID)
	}
	if cleanupEnabled(pool) && vm.Name != "" {
		managedByAnotherAgent, err := r.vmManagedByAnotherVsphereAgent(ctx, agent, vm)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !managedByAnotherAgent {
			provider, err := r.poolReconciler().provider(ctx, pool)
			if err != nil {
				return ctrl.Result{}, err
			}
			if err := provider.DeleteVM(ctx, pool, vm); err != nil {
				recordVMOperation("delete", err)
				return ctrl.Result{}, err
			}
			recordVMOperation("delete", nil)
		}
	}
	controllerutil.RemoveFinalizer(agent, vsphereAgentFinalizerName)
	return ctrl.Result{}, r.patchFinalizer(ctx, agent)
}

func (r *VsphereAgentReconciler) vmManagedByAnotherVsphereAgent(ctx context.Context, agent *agentforgev1alpha1.VsphereAgent, vm agentforgev1alpha1.OwnedVMStatus) (bool, error) {
	if vm.Name == "" || agent.Spec.PoolRef.Name == "" {
		return false, nil
	}
	var list agentforgev1alpha1.VsphereAgentList
	if err := r.List(ctx, &list, client.InNamespace(agent.Namespace)); err != nil {
		return false, err
	}
	for i := range list.Items {
		other := &list.Items[i]
		if other.Name == agent.Name || other.GetDeletionTimestamp() != nil {
			continue
		}
		if !vsphereAgentBelongsToPool(other, agent.Spec.PoolRef.Name) {
			continue
		}
		if other.Status.VM.Name == vm.Name || vsphereAgentVMName(other) == vm.Name {
			return true, nil
		}
		if sameVMIdentity(other.Status.VM, vm) && duplicateCanBeDeletedWithoutVMDelete(agent, other) {
			return true, nil
		}
	}
	return false, nil
}

func sameVMIdentity(a, b agentforgev1alpha1.OwnedVMStatus) bool {
	if a.BIOSUUID != "" && b.BIOSUUID != "" && a.BIOSUUID == b.BIOSUUID {
		return true
	}
	return a.MACAddress != "" && b.MACAddress != "" && a.MACAddress == b.MACAddress
}

func duplicateCanBeDeletedWithoutVMDelete(current, other *agentforgev1alpha1.VsphereAgent) bool {
	currentCreatedFor := current.Labels[vsphereAgentCreatedForLabel]
	otherCreatedFor := other.Labels[vsphereAgentCreatedForLabel]
	if currentCreatedFor == vsphereAgentCreatedForAdopted && otherCreatedFor != vsphereAgentCreatedForAdopted {
		return true
	}
	return current.Name != current.Status.VM.Name
}

func (r *VsphereAgentReconciler) patchFinalizer(ctx context.Context, agent *agentforgev1alpha1.VsphereAgent) error {
	current := &agentforgev1alpha1.VsphereAgent{}
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: agent.Namespace, Name: agent.Name}, current); err != nil {
		return err
	}
	before := current.DeepCopy()
	if controllerutil.ContainsFinalizer(agent, vsphereAgentFinalizerName) {
		controllerutil.AddFinalizer(current, vsphereAgentFinalizerName)
	} else {
		controllerutil.RemoveFinalizer(current, vsphereAgentFinalizerName)
	}
	return r.Patch(ctx, current, client.MergeFrom(before))
}

func (r *VsphereAgentReconciler) updateStatus(ctx context.Context, agent *agentforgev1alpha1.VsphereAgent) error {
	desired := *agent.Status.DeepCopy()
	desired.ObservedGeneration = agent.Generation
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var current agentforgev1alpha1.VsphereAgent
		if err := r.Get(ctx, types.NamespacedName{Namespace: agent.Namespace, Name: agent.Name}, &current); err != nil {
			return err
		}
		if reflect.DeepEqual(current.Status, desired) {
			return nil
		}
		current.Status = desired
		return r.Status().Update(ctx, &current)
	}); err != nil {
		return err
	}
	agent.Status = desired
	return nil
}

func (r *VsphereAgentReconciler) patchPoolISOStatus(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var current agentforgev1alpha1.VsphereAgentPool
		if err := r.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, &current); err != nil {
			return err
		}
		before := *current.Status.DeepCopy()
		current.Status.ISO = pool.Status.ISO
		meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
			Type:               conditionISOReady,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: current.Generation,
			Reason:             conditionISOReady,
			Message:            "InfraEnv discovery ISO is cached for new vSphere VMs",
		})
		if reflect.DeepEqual(before, current.Status) {
			return nil
		}
		return r.Status().Update(ctx, &current)
	})
}

func (r *VsphereAgentReconciler) apiReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *VsphereAgentReconciler) poolReconciler() *VsphereAgentPoolReconciler {
	return &VsphereAgentPoolReconciler{
		Client:          r.Client,
		APIReader:       r.APIReader,
		Scheme:          r.Scheme,
		Recorder:        r.Recorder,
		ProviderFactory: r.ProviderFactory,
	}
}

func (r *VsphereAgentReconciler) requestsForPool(ctx context.Context, o client.Object) []reconcile.Request {
	var agents agentforgev1alpha1.VsphereAgentList
	if err := r.List(ctx, &agents, client.InNamespace(o.GetNamespace()), client.MatchingFields{vsphereAgentPoolOwnerFieldIndex: o.GetName()}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "failed to list VsphereAgents for pool change")
		return nil
	}
	requests := make([]reconcile.Request, 0, len(agents.Items))
	for i := range agents.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: agents.Items[i].Namespace,
			Name:      agents.Items[i].Name,
		}})
	}
	return requests
}

func (r *VsphereAgentReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("vsphereagent-controller")
	}
	if err := mgr.GetFieldIndexer().IndexField(ctx, &agentforgev1alpha1.VsphereAgent{}, vsphereAgentPoolOwnerFieldIndex,
		func(o client.Object) []string {
			agent := o.(*agentforgev1alpha1.VsphereAgent)
			if agent.Spec.PoolRef.Name == "" {
				return nil
			}
			return []string{agent.Spec.PoolRef.Name}
		}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&agentforgev1alpha1.VsphereAgent{}).
		Watches(&agentforgev1alpha1.VsphereAgentPool{}, handler.EnqueueRequestsFromMapFunc(r.requestsForPool)).
		Named("vsphereagent").
		Complete(r)
}
