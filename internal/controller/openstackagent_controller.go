package controller

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"time"

	api "github.com/containeroo/agent-forge-operator/api/v1alpha1"
	osapi "github.com/containeroo/agent-forge-operator/internal/openstack"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimecontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// OpenStackAgentReconciler advances one durable infrastructure step per reconcile.
// No phase ever detaches media or changes the power state of an existing server.
type OpenStackAgentReconciler struct {
	client.Client
	APIReader       client.Reader
	ProviderFactory osapi.Factory
}

func (r *OpenStackAgentReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}
func (r *OpenStackAgentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ProviderFactory == nil {
		r.ProviderFactory = osapi.NewCachedFactory()
	}
	return ctrl.NewControllerManagedBy(mgr).For(&api.OpenStackAgent{}).WithOptions(runtimecontroller.Options{MaxConcurrentReconciles: 4}).Named("openstackagent").Complete(r)
}
func (r *OpenStackAgentReconciler) provider(ctx context.Context, a *api.OpenStackAgent) (osapi.Client, error) {
	secret := &corev1.Secret{}
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: a.Spec.Configuration.OpenStack.CredentialsSecretRef.Name}, secret); err != nil {
		return nil, &osapi.Error{Reason: "CredentialsUnavailable", Rejected: true}
	}
	factory := r.ProviderFactory
	if factory == nil {
		factory = osapi.New
	}
	return factory(ctx, a.Spec.Configuration.OpenStack, secret)
}
func (r *OpenStackAgentReconciler) status(ctx context.Context, a *api.OpenStackAgent) error {
	a.Status.ObservedGeneration = a.Generation
	current := &api.OpenStackAgent{}
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(a), current); err != nil {
		return err
	}
	if reflect.DeepEqual(current.Status, a.Status) {
		return nil
	}
	return r.Status().Update(ctx, a)
}
func (r *OpenStackAgentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	a := &api.OpenStackAgent{}
	if err := r.reader().Get(ctx, req.NamespacedName, a); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	openStackDefaults(&a.Spec.Configuration)
	if a.DeletionTimestamp.IsZero() && controllerutil.AddFinalizer(a, openStackFinalizer) {
		return ctrl.Result{}, r.Update(ctx, a)
	}
	if !controllerutil.ContainsFinalizer(a, openStackFinalizer) {
		return ctrl.Result{}, nil
	}
	// CAPI disappearance, checked through the uncached reader, authorizes scale-down.
	present, deleting, err := r.demandPresent(ctx, a)
	if err != nil {
		return ctrl.Result{}, err
	}
	if a.DeletionTimestamp.IsZero() && !present {
		return ctrl.Result{}, r.Delete(ctx, a)
	}
	if a.DeletionTimestamp.IsZero() && deleting {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if !a.DeletionTimestamp.IsZero() && a.Spec.Configuration.CleanupPolicy == api.CleanupPolicyRetain {
		controllerutil.RemoveFinalizer(a, openStackFinalizer)
		return ctrl.Result{}, r.Update(ctx, a)
	}
	p, err := r.provider(ctx, a)
	if err == nil {
		if a.Status.ScopeHash == "" {
			a.Status.ScopeHash = p.Scope()
			return ctrl.Result{RequeueAfter: time.Second}, r.status(ctx, a)
		}
		if a.Status.ScopeHash != p.Scope() {
			err = &osapi.Error{Reason: "CloudScopeChanged", Rejected: true}
		}
	}
	if err == nil {
		if !a.DeletionTimestamp.IsZero() {
			err = r.cleanup(ctx, p, a, present)
		} else {
			err = r.provision(ctx, p, a)
		}
	}
	if err != nil {
		// Only sanitized diagnostics leave the provider boundary. Kubernetes and
		// ISO errors are also replaced rather than exposing arbitrary response text.
		safe := &osapi.Error{Reason: "ReconcileFailed", Retryable: true}
		_ = errors.As(err, &safe)
		reason := safe.Reason
		previous := meta.FindStatusCondition(a.Status.Conditions, conditionReady)
		changed := previous == nil || previous.Reason != reason || previous.Message != safe.Error()
		meta.SetStatusCondition(&a.Status.Conditions, metav1.Condition{Type: conditionReady, Status: metav1.ConditionFalse,
			ObservedGeneration: a.Generation, Reason: reason, Message: safe.Error()})
		if !safe.Retryable && a.Status.Phase != openStackPhaseRefreshingImage {
			a.Status.Phase = "Failed"
		}
		if statusErr := r.status(ctx, a); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		if safe.Retryable {
			return ctrl.Result{}, safe
		} // controller-runtime exponential backoff
		if changed {
			ctrl.LoggerFrom(ctx).Error(safe, "OpenStack reconciliation failed")
		}
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}
	if !controllerutil.ContainsFinalizer(a, openStackFinalizer) {
		return ctrl.Result{}, nil
	}
	meta.SetStatusCondition(&a.Status.Conditions, metav1.Condition{Type: conditionReady, Status: conditionStatus(a.Status.Phase == conditionReady),
		ObservedGeneration: a.Generation, Reason: a.Status.Phase, Message: a.Status.Phase})
	return ctrl.Result{RequeueAfter: 15 * time.Second}, r.status(ctx, a)
}

func (r *OpenStackAgentReconciler) demandPresent(ctx context.Context, a *api.OpenStackAgent) (bool, bool, error) {
	ref := a.Spec.AgentMachineRef
	if ref.UID == "" || ref.Name == "" || ref.Namespace == "" {
		return true, false, errors.New("missing AgentMachine identity")
	}
	am := agentMachineWatchObject()
	err := r.reader().Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, am)
	if apierrors.IsNotFound(err) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if am.GetUID() != ref.UID {
		return false, false, nil
	}
	return true, !am.GetDeletionTimestamp().IsZero(), nil
}

func resourceName(a *api.OpenStackAgent, k osapi.Kind) string {
	return "agent-forge-" + string(a.UID) + "-" + string(k)
}

// ensureResource checkpoints intent BEFORE POST. A crash or transport failure is
// recovered by exact ownership lookup. Absence after an ambiguous POST is not
// enough evidence to create again (OpenStack listings can be eventually consistent).
func (r *OpenStackAgentReconciler) ensureResource(ctx context.Context, p osapi.Client, a *api.OpenStackAgent, k osapi.Kind, id *string) (*osapi.Resource, error) {
	owner := string(a.UID)
	if *id != "" {
		return p.Get(ctx, k, *id, owner)
	}
	found, err := p.Find(ctx, k, resourceName(a, k), owner)
	if err == nil {
		*id = found.ID
		a.Status.PendingCreate = ""
		return found, nil
	}
	if !errors.Is(err, osapi.ErrNotFound) {
		return nil, err
	}
	if a.Status.PendingCreate != "" {
		return nil, &osapi.Error{Reason: reasonCreateOutcomeUnknown, Retryable: true}
	}
	a.Status.PendingCreate = string(k)
	if err := r.status(ctx, a); err != nil {
		return nil, err
	}
	found, err = p.Create(ctx, k, resourceName(a, k), owner, a)
	if err != nil {
		var safe *osapi.Error
		if errors.As(err, &safe) && safe.Rejected {
			a.Status.PendingCreate = ""
		}
		return nil, err
	}
	if found == nil || found.ID == "" {
		return nil, &osapi.Error{Reason: reasonCreateOutcomeUnknown, Retryable: true}
	}
	*id = found.ID
	a.Status.PendingCreate = ""
	return found, nil
}
func resourceFailed(resource *osapi.Resource) error {
	if strings.Contains(strings.ToLower(resource.State), "error") {
		return &osapi.Error{Reason: "InfrastructureResourceFailed", Rejected: true}
	}
	return nil
}

func (r *OpenStackAgentReconciler) provision(ctx context.Context, p osapi.Client, a *api.OpenStackAgent) error {
	// Preserve and complete the cache-release journal before other phase updates.
	if a.Status.Phase == openStackPhaseRefreshingImage {
		return r.refreshDiscoveryImage(ctx, p, a)
	}
	if a.Status.PendingCreate != "" {
		kind := osapi.Kind(a.Status.PendingCreate)
		found, err := p.Find(ctx, kind, resourceName(a, kind), string(a.UID))
		if errors.Is(err, osapi.ErrNotFound) {
			return &osapi.Error{Reason: reasonCreateOutcomeUnknown, Retryable: true}
		}
		if err != nil {
			return err
		}
		switch kind {
		case osapi.Port:
			a.Status.PortID = found.ID
			a.Status.MACAddress = normalizeMAC(found.MAC)
		case osapi.OSVolume:
			a.Status.OSVolumeID = found.ID
		case osapi.DiscoveryVolume:
			a.Status.DiscoveryVolumeID = found.ID
		case osapi.Server:
			a.Status.ServerID = found.ID
			now := metav1.Now()
			a.Status.InstanceCreatedAt = &now
		}
		a.Status.PendingCreate = ""
		a.Status.Phase = "RecoveringResource"
		return nil
	}
	// Never rebuild, reconfigure, or refresh discovery media on an existing worker.
	if a.Status.ServerID != "" {
		server, err := p.Get(ctx, osapi.Server, a.Status.ServerID, string(a.UID))
		if err != nil {
			return err
		}
		if err := resourceFailed(server); err != nil {
			return err
		}
		if server.State != "ACTIVE" {
			a.Status.Phase = "CreatingInstance"
			return nil
		}
		return r.reconcileDiscoveredAgent(ctx, a)
	}
	if a.Status.PortID == "" || a.Status.MACAddress == "" {
		a.Status.Phase = "CreatingNetwork"
		port, err := r.ensureResource(ctx, p, a, osapi.Port, &a.Status.PortID)
		if err != nil {
			return err
		}
		a.Status.MACAddress = normalizeMAC(port.MAC)
		return nil
	}
	a.Status.Phase = "CreatingOSVolume"
	volume, err := r.ensureResource(ctx, p, a, osapi.OSVolume, &a.Status.OSVolumeID)
	if err != nil {
		return err
	}
	if err := resourceFailed(volume); err != nil {
		return err
	}
	if volume.State != "available" {
		return nil
	}
	if !volume.Bootable {
		return p.SetBootable(ctx, volume.ID, string(a.UID))
	}
	a.Status.Phase = "PreparingDiscoveryImage"
	ready, err := r.prepareDiscoveryImage(ctx, p, a)
	if err != nil || !ready {
		return err
	}
	a.Status.Phase = "CreatingDiscoveryVolume"
	iso, err := r.ensureResource(ctx, p, a, osapi.DiscoveryVolume, &a.Status.DiscoveryVolumeID)
	if err != nil {
		return err
	}
	if err := resourceFailed(iso); err != nil {
		return err
	}
	if iso.State != "available" {
		return nil
	}
	// Persist both volume identities before creating the instance.
	if err := r.status(ctx, a); err != nil {
		return err
	}
	a.Status.Phase = "CreatingInstance"
	server, err := r.ensureResource(ctx, p, a, osapi.Server, &a.Status.ServerID)
	if err != nil {
		return err
	}
	if a.Status.InstanceCreatedAt == nil {
		now := metav1.Now()
		a.Status.InstanceCreatedAt = &now
	}
	return resourceFailed(server)
}

func agentHardwareMatches(a *api.OpenStackAgent, host *unstructured.Unstructured) bool {
	interfaces, _, _ := unstructured.NestedSlice(host.Object, "status", "inventory", "interfaces")
	for _, raw := range interfaces {
		iface, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		for _, key := range []string{"macAddress", "mac"} {
			mac, _ := iface[key].(string)
			if a.Status.MACAddress != "" && normalizeMAC(mac) == normalizeMAC(a.Status.MACAddress) {
				return true
			}
		}
	}
	// Nova's server UUID is commonly exposed as the guest DMI product UUID.
	uuid, _, _ := unstructured.NestedString(host.Object, "status", "inventory", "systemVendor", "productUUID")
	if uuid == "" {
		uuid, _, _ = unstructured.NestedString(host.Object, "status", "inventory", "systemVendor", "serialNumber")
	}
	return uuid != "" && a.Status.ServerID != "" && strings.EqualFold(uuid, a.Status.ServerID)
}
func (r *OpenStackAgentReconciler) matchingHosts(ctx context.Context, a *api.OpenStackAgent) ([]unstructured.Unstructured, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(agentGVK)
	if err := r.reader().List(ctx, list, client.InNamespace(a.Namespace)); err != nil {
		return nil, err
	}
	var matches []unstructured.Unstructured
	for _, host := range list.Items {
		if agentHardwareMatches(a, &host) {
			matches = append(matches, host)
		}
	}
	return matches, nil
}

func installationDisk(a *api.OpenStackAgent, host *unstructured.Unstructured) (string, error) {
	disks, _, _ := unstructured.NestedSlice(host.Object, "status", "inventory", "disks")
	var ids []string
	for _, raw := range disks {
		disk, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		path, _ := disk["path"].(string)
		serial, _ := disk["serial"].(string)
		id, _ := disk["id"].(string)
		drive, _ := disk["driveType"].(string)
		eligible, _, _ := unstructured.NestedBool(disk, "installationEligibility", "eligible")
		serialMatches := serial == a.Status.OSVolumeID || (len(serial) >= 20 && strings.HasPrefix(a.Status.OSVolumeID, serial))
		if id != "" && path == a.Spec.Configuration.Template.InstallationDisk && eligible && !strings.EqualFold(drive, "CDROM") && serialMatches {
			ids = append(ids, id)
		}
	}
	if len(ids) != 1 {
		return "", &osapi.Error{Reason: "InstallationDiskNotVerified", Rejected: true}
	}
	return ids[0], nil
}

func (r *OpenStackAgentReconciler) reconcileDiscoveredAgent(ctx context.Context, a *api.OpenStackAgent) error {
	a.Status.Phase = "WaitingForDiscovery"
	hosts, err := r.matchingHosts(ctx, a)
	if err != nil {
		return err
	}
	if len(hosts) == 0 {
		if a.Status.InstanceCreatedAt != nil && time.Since(a.Status.InstanceCreatedAt.Time) > time.Duration(a.Spec.Configuration.DiscoveryTimeoutSeconds)*time.Second {
			return &osapi.Error{Reason: "AgentDiscoveryTimeout", Rejected: true}
		}
		return nil
	}
	if len(hosts) != 1 {
		return &osapi.Error{Reason: "AmbiguousAgentIdentity", Rejected: true}
	}
	host := &hosts[0]
	if !host.GetDeletionTimestamp().IsZero() || !agentBelongsToInfraEnv(host, a.Spec.Configuration.InfraEnvRef.Name) {
		return &osapi.Error{Reason: "AgentIdentityConflict", Rejected: true}
	}
	if ref := a.Status.AgentRef; ref != nil && ref.UID != host.GetUID() {
		return &osapi.Error{Reason: "AgentIdentityChanged", Rejected: true}
	}
	if claim := host.GetLabels()[agentMachineSelectionLabel]; claim != "" && claim != a.Spec.AgentMachineRef.Name {
		return &osapi.Error{Reason: "AgentClaimConflict", Rejected: true}
	}
	bound := host.GetLabels()[agentMachineRefKey]
	if bound != "" && (bound != a.Spec.AgentMachineRef.Name || (host.GetAnnotations()["agentMachineRefNamespace"] != "" && host.GetAnnotations()["agentMachineRefNamespace"] != a.Spec.AgentMachineRef.Namespace)) {
		return &osapi.Error{Reason: "AgentBindingConflict", Rejected: true}
	}
	a.Status.AgentRef = &corev1.ObjectReference{APIVersion: host.GetAPIVersion(), Kind: host.GetKind(), Namespace: host.GetNamespace(), Name: host.GetName(), UID: host.GetUID()}
	if objectConditionStatus(host, "Installed") == metav1.ConditionTrue {
		if bound == a.Spec.AgentMachineRef.Name {
			a.Status.Phase = conditionReady
		} else {
			a.Status.Phase = "WaitingForAgent"
		}
		return nil
	}
	if objectConditionReason(host, "Installed") == "InstallationFailed" {
		return &osapi.Error{Reason: "AgentInstallationFailed", Rejected: true}
	}
	if bound != "" {
		a.Status.Phase = "WaitingForInstallation"
		return nil
	}
	disk, err := installationDisk(a, host)
	if err != nil {
		return err
	}
	a.Status.Phase = "WaitingForAgent"
	return patchDiscoveredAgent(ctx, r.Client, r.reader(), a.Namespace, a.Spec.Configuration.Agent, host.GetName(), a.Spec.AgentMachineRef.Name, agentPatchSafety{UID: host.GetUID(), ResourceVersion: host.GetResourceVersion(), InstallationDiskID: disk})
}

func (r *OpenStackAgentReconciler) authorizeCleanup(ctx context.Context, a *api.OpenStackAgent, present bool) error {
	if present {
		return &osapi.Error{Reason: "AgentMachineStillPresent", Retryable: true, Rejected: true}
	}
	if ref := a.Spec.MachineRef; ref != nil {
		machine := machineWatchObject()
		err := r.reader().Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, machine)
		if err == nil && (ref.UID == "" || ref.UID == machine.GetUID()) {
			return &osapi.Error{Reason: "MachineStillPresent", Retryable: true, Rejected: true}
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	hosts, err := r.matchingHosts(ctx, a)
	if err != nil {
		return err
	}
	for i := range hosts {
		host := &hosts[i]
		if agentDeletionBlocker(host) != "" {
			return &osapi.Error{Reason: "AgentStillBound", Retryable: true, Rejected: true}
		}
		before := host.DeepCopy()
		if err := unstructured.SetNestedField(host.Object, false, "spec", "approved"); err != nil {
			return err
		}
		annotations := host.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[agentCleanupAnnotation] = a.Name
		host.SetAnnotations(annotations)
		if err := r.Patch(ctx, host, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
	}
	return nil
}

// deleteResource waits for observed absence; volume attachment checks live in the
// SDK boundary too, so neither errors nor finalizer retries can force deletion.
func deleteResource(ctx context.Context, p osapi.Client, a *api.OpenStackAgent, k osapi.Kind, id string) (bool, error) {
	var resource *osapi.Resource
	var err error
	if id == "" {
		resource, err = p.Find(ctx, k, resourceName(a, k), string(a.UID))
	} else {
		resource, err = p.Get(ctx, k, id, string(a.UID))
	}
	if errors.Is(err, osapi.ErrNotFound) {
		if a.Status.PendingCreate == string(k) {
			return false, &osapi.Error{Reason: reasonCreateOutcomeUnknown, Retryable: true}
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if resource.Attached {
		return false, &osapi.Error{Reason: "ResourceStillAttachedOrBusy", Retryable: true}
	}
	if a.Status.PendingCreate == string(k) {
		a.Status.PendingCreate = ""
	}
	return false, p.Delete(ctx, k, resource.ID, string(a.UID))
}
func (r *OpenStackAgentReconciler) cleanup(ctx context.Context, p osapi.Client, a *api.OpenStackAgent, present bool) error {
	a.Status.Phase = openStackPhaseDeleting
	if a.Spec.Configuration.CleanupPolicy == api.CleanupPolicyRetain {
		// Keep the image cache reference as an intentional retention record.
		controllerutil.RemoveFinalizer(a, openStackFinalizer)
		return r.Update(ctx, a)
	}
	if err := r.authorizeCleanup(ctx, a, present); err != nil {
		return err
	}
	done, err := deleteResource(ctx, p, a, osapi.Server, a.Status.ServerID)
	if err != nil || !done {
		return err
	}
	done, err = deleteResource(ctx, p, a, osapi.DiscoveryVolume, a.Status.DiscoveryVolumeID)
	if err != nil || !done {
		return err
	}
	if a.Spec.Configuration.Template.OSVolumePolicy != api.CleanupPolicyRetain {
		done, err = deleteResource(ctx, p, a, osapi.OSVolume, a.Status.OSVolumeID)
		if err != nil || !done {
			return err
		}
	}
	done, err = deleteResource(ctx, p, a, osapi.Port, a.Status.PortID)
	if err != nil || !done {
		return err
	}
	done, err = r.releaseDiscoveryImage(ctx, p, a)
	if err != nil || !done {
		return err
	}
	hosts, err := r.matchingHosts(ctx, a)
	if err != nil {
		return err
	}
	for i := range hosts {
		host := &hosts[i]
		if agentDeletionBlocker(host) != "" {
			return &osapi.Error{Reason: "AgentStillBound", Retryable: true}
		}
		uid, version := host.GetUID(), host.GetResourceVersion()
		if err := r.Delete(ctx, host, client.Preconditions{UID: &uid, ResourceVersion: &version}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	controllerutil.RemoveFinalizer(a, openStackFinalizer)
	return r.Update(ctx, a)
}

func openStackOwnsHost(host *unstructured.Unstructured, workers []api.OpenStackAgent) bool {
	for i := range workers {
		worker := &workers[i]
		if agentHardwareMatches(worker, host) || (worker.Status.AgentRef != nil && worker.Status.AgentRef.UID == host.GetUID()) {
			return true
		}
	}
	return false
}
