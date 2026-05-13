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
	"reflect"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
)

// VsphereAgentReconciler reconciles one VsphereAgent into one vSphere VM.
type VsphereAgentReconciler struct {
	client.Client
	APIReader       client.Reader
	Scheme          *runtime.Scheme
	Recorder        record.EventRecorder
	ProviderFactory VMProviderFactory
}

// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=vsphereagents,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=vsphereagents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=vsphereagents/finalizers,verbs=update
// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=vsphereagentpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=vsphereagentpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agent-install.openshift.io,resources=infraenvs,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *VsphereAgentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var agent agentforgev1alpha1.VsphereAgent
	if err := r.apiReader().Get(ctx, req.NamespacedName, &agent); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	var pool agentforgev1alpha1.VsphereAgentPool
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: agent.Namespace, Name: agent.Spec.PoolRef.Name}, &pool); err != nil {
		if apierrors.IsNotFound(err) {
			meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
				Type:               conditionReady,
				Status:             metav1.ConditionFalse,
				ObservedGeneration: agent.Generation,
				Reason:             "PoolNotFound",
				Message:            "referenced VsphereAgentPool does not exist",
			})
			_ = r.updateStatus(ctx, &agent)
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

	if agent.Status.VM.Name != "" {
		meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
			Type:               conditionReady,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: agent.Generation,
			Reason:             "VMCreated",
			Message:            "vSphere VM has been created",
		})
		return ctrl.Result{RequeueAfter: time.Minute}, r.updateStatus(ctx, &agent)
	}

	if agent.Labels[vsphereAgentCreatedForLabel] == vsphereAgentCreatedForAdopted {
		meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
			Type:               conditionReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: agent.Generation,
			Reason:             "AdoptionPending",
			Message:            "waiting for VsphereAgentPool to initialize adopted VM status",
		})
		_ = r.updateStatus(ctx, &agent)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
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
		_ = r.updateStatus(ctx, &agent)
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
		_ = r.updateStatus(ctx, &agent)
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
		_ = r.updateStatus(ctx, &agent)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if err := r.patchPoolISOStatus(ctx, &pool); err != nil {
		return ctrl.Result{}, err
	}

	vm, err := provider.CreateVM(ctx, &pool, VMCreateRequest{ISOPath: isoPath})
	if err != nil {
		meta.SetStatusCondition(&agent.Status.Conditions, metav1.Condition{
			Type:               conditionReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: agent.Generation,
			Reason:             "VMCreateFailed",
			Message:            stableErrorMessage(err),
		})
		_ = r.updateStatus(ctx, &agent)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
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

func (r *VsphereAgentReconciler) reconcileDelete(ctx context.Context, agent *agentforgev1alpha1.VsphereAgent, pool *agentforgev1alpha1.VsphereAgentPool) (ctrl.Result, error) {
	if agent.Status.VM.Name != "" {
		provider, err := r.poolReconciler().provider(ctx, pool)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := provider.DeleteVM(ctx, pool, agent.Status.VM); err != nil {
			return ctrl.Result{}, err
		}
	}
	controllerutil.RemoveFinalizer(agent, vsphereAgentFinalizerName)
	return ctrl.Result{}, r.patchFinalizer(ctx, agent)
}

func (r *VsphereAgentReconciler) patchFinalizer(ctx context.Context, agent *agentforgev1alpha1.VsphereAgent) error {
	current := &agentforgev1alpha1.VsphereAgent{}
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: agent.Namespace, Name: agent.Name}, current); err != nil {
		return err
	}
	before := current.DeepCopy()
	current.SetFinalizers(agent.GetFinalizers())
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
		current.Status.ISO = pool.Status.ISO
		meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
			Type:               conditionISOReady,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: current.Generation,
			Reason:             "ISOReady",
			Message:            "InfraEnv discovery ISO is cached for new vSphere VMs",
		})
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
		r.Recorder = mgr.GetEventRecorderFor("vsphereagent-controller")
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
