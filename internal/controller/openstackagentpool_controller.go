package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	api "github.com/containeroo/agent-forge-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const openStackPhaseDeleting = "Deleting"
const reasonCreateOutcomeUnknown = "CreateOutcomeUnknown"

const openStackFinalizer = "agent-forge.containeroo.ch/openstack"
const openStackPoolLabel = "agent-forge.containeroo.ch/openstack-agent-pool"
const demandClaimAnnotation = "agent-forge.containeroo.ch/demand-owner"

func openStackDefaults(s *api.OpenStackAgentPoolSpec) {
	if s.OpenStack.APITimeoutSeconds == 0 {
		s.OpenStack.APITimeoutSeconds = 60
	}
	if s.Template.DiskGiB == 0 {
		s.Template.DiskGiB = 120
	}
	if s.Template.DiscoveryVolumeMinGiB == 0 {
		s.Template.DiscoveryVolumeMinGiB = 2
	}
	if s.Template.OSDiskBus == "" {
		s.Template.OSDiskBus = "virtio"
	}
	if s.Template.DiscoveryDiskBus == "" {
		s.Template.DiscoveryDiskBus = "ide"
	}
	if s.Template.InstallationDisk == "" {
		s.Template.InstallationDisk = "/dev/vda"
	}
	if s.Template.OSVolumePolicy == "" {
		s.Template.OSVolumePolicy = api.CleanupPolicyDelete
	}
	if s.CleanupPolicy == "" {
		s.CleanupPolicy = api.CleanupPolicyDelete
	}
	if s.Agent.Role == "" {
		s.Agent.Role = "worker"
	}
	if s.DiscoveryTimeoutSeconds == 0 {
		s.DiscoveryTimeoutSeconds = 1800
	}
}

func openStackDemandMatches(obj *unstructured.Unstructured, p *api.OpenStackAgentPool) bool {
	return obj.GetNamespace() == p.Spec.ControlPlaneNamespace && obj.GetAnnotations()[nodePoolAnnotation] == p.Namespace+"/"+p.Spec.NodePoolRef.Name
}

// claimDemand uses an API-server optimistic lock to exclude concurrent providers.
// Claims persist for the AgentMachine lifetime and are never stolen by a new pool.
func claimDemand(ctx context.Context, c client.Client, am *unstructured.Unstructured, kind string, uid types.UID) error {
	claim := kind + "/" + string(uid)
	annotations := am.GetAnnotations()
	if existing := annotations[demandClaimAnnotation]; existing != "" {
		if existing != claim {
			return errors.New("AgentMachine is claimed by another infrastructure pool")
		}
		return nil
	}
	before := am.DeepCopy()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[demandClaimAnnotation] = claim
	am.SetAnnotations(annotations)
	return c.Patch(ctx, am, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func (r *AgentMachineReconciler) matchingOpenStackPools(ctx context.Context, am *unstructured.Unstructured) ([]*api.OpenStackAgentPool, error) {
	var pools api.OpenStackAgentPoolList
	if err := r.List(ctx, &pools); err != nil {
		return nil, err
	}
	var matches []*api.OpenStackAgentPool
	for i := range pools.Items {
		p := &pools.Items[i]
		if p.DeletionTimestamp.IsZero() && openStackDemandMatches(am, p) {
			matches = append(matches, p)
		}
	}
	return matches, nil
}

func (r *AgentMachineReconciler) ensureOpenStackAgent(ctx context.Context, p *api.OpenStackAgentPool, am *unstructured.Unstructured) error {
	if agentMachineHasAssignedAgent(am) {
		return nil
	}
	if err := claimDemand(ctx, r.Client, am, "OpenStackAgentPool", p.UID); err != nil {
		return err
	}
	if err := ensureAgentMachineSelectorPinned(ctx, r.Client, am); err != nil {
		return err
	}
	// Honor an explicitly created worker (for example, with a fixed IP) for the
	// same demand. Never create a second candidate while one is deleting.
	var existing api.OpenStackAgentList
	if err := r.List(ctx, &existing, client.InNamespace(p.Namespace)); err != nil {
		return err
	}
	matches := 0
	for i := range existing.Items {
		worker := &existing.Items[i]
		if worker.Spec.AgentMachineRef.UID == am.GetUID() {
			if worker.Spec.PoolUID != string(p.UID) {
				return errors.New("AgentMachine already has another pool's worker")
			}
			matches++
		}
	}
	if matches > 1 {
		return errors.New("AgentMachine has multiple OpenStack workers")
	}
	if matches == 1 {
		return nil
	}
	spec := *p.Spec.DeepCopy()
	openStackDefaults(&spec)
	// AgentMachine UID makes replacement demand distinct even when a name is reused.
	name := "osa-" + string(am.GetUID())
	a := &api.OpenStackAgent{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: p.Namespace,
		Labels:     map[string]string{openStackPoolLabel: p.Name, agentMachineSelectionLabel: am.GetName()},
		Finalizers: []string{openStackFinalizer}}, Spec: api.OpenStackAgentSpec{
		PoolRef: api.LocalObjectReference{Name: p.Name}, PoolUID: string(p.UID), Configuration: spec,
		AgentMachineRef: corev1.ObjectReference{APIVersion: am.GetAPIVersion(), Kind: am.GetKind(), Namespace: am.GetNamespace(), Name: am.GetName(), UID: am.GetUID()},
	}}
	for _, owner := range am.GetOwnerReferences() {
		if owner.Kind == machineGVK.Kind {
			a.Spec.MachineRef = &corev1.ObjectReference{APIVersion: owner.APIVersion, Kind: owner.Kind, Namespace: am.GetNamespace(), Name: owner.Name, UID: owner.UID}
		}
	}
	if err := controllerutil.SetControllerReference(p, a, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, a); !apierrors.IsAlreadyExists(err) {
		return err
	}
	current := &api.OpenStackAgent{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(a), current); err != nil {
		return err
	}
	if current.Spec.PoolUID != string(p.UID) || current.Spec.AgentMachineRef.UID != am.GetUID() {
		return errors.New("OpenStackAgent demand ownership mismatch")
	}
	return nil
}

// OpenStackAgentPoolReconciler reports capacity and retains its configuration until
// every child has finished external cleanup. Counts never trigger VM deletion.
type OpenStackAgentPoolReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
}

// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=openstackagentpools;openstackagents,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=openstackagentpools/status;openstackagents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=agent-forge.containeroo.ch,resources=openstackagentpools/finalizers;openstackagents/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;create;update;delete

func (r *OpenStackAgentPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	p := &api.OpenStackAgentPool{}
	if err := reader.Get(ctx, req.NamespacedName, p); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if p.DeletionTimestamp.IsZero() && controllerutil.AddFinalizer(p, openStackFinalizer) {
		return ctrl.Result{}, r.Update(ctx, p)
	}
	var children api.OpenStackAgentList
	if err := reader.List(ctx, &children, client.InNamespace(p.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	var count, ready int32
	for i := range children.Items {
		a := &children.Items[i]
		if a.Spec.PoolUID != string(p.UID) {
			continue
		}
		count++
		if a.Status.Phase == conditionReady {
			ready++
		}
		if !p.DeletionTimestamp.IsZero() && a.DeletionTimestamp.IsZero() {
			if err := r.Delete(ctx, a); err != nil {
				return ctrl.Result{}, client.IgnoreNotFound(err)
			}
		}
	}
	if !p.DeletionTimestamp.IsZero() {
		if count > 0 {
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}
		controllerutil.RemoveFinalizer(p, openStackFinalizer)
		return ctrl.Result{}, r.Update(ctx, p)
	}
	demand := &unstructured.UnstructuredList{}
	demand.SetGroupVersionKind(agentMachineGVK)
	if err := reader.List(ctx, demand, client.InNamespace(p.Spec.ControlPlaneNamespace)); err != nil {
		return ctrl.Result{}, err
	}
	var desired int32
	for i := range demand.Items {
		if demand.Items[i].GetDeletionTimestamp() == nil && openStackDemandMatches(&demand.Items[i], p) {
			desired++
		}
	}
	previousStatus := p.Status.DeepCopy()
	p.Status.ObservedGeneration = p.Generation
	p.Status.DesiredReplicas = desired
	p.Status.ReadyReplicas = ready
	meta.SetStatusCondition(&p.Status.Conditions, metav1.Condition{Type: conditionReady, Status: conditionStatus(ready == desired), ObservedGeneration: p.Generation,
		Reason: "CapacityObserved", Message: fmt.Sprintf("%d installed workers for %d AgentMachines", ready, desired)})
	if reflect.DeepEqual(*previousStatus, p.Status) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, r.Status().Update(ctx, p)
}
func (r *OpenStackAgentPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&api.OpenStackAgentPool{}).Owns(&api.OpenStackAgent{}).Named("openstackagentpool").Complete(r)
}
