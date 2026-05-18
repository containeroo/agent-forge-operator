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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LocalObjectReference identifies another object in the same namespace as the
// VsphereAgentPool.
type LocalObjectReference struct {
	// Name is the referenced object's metadata.name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Name string `json:"name"`
}

// SecretReference identifies a Secret. When namespace is empty, the
// VsphereAgentPool namespace is used.
type SecretReference struct {
	// Name is the Secret metadata.name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Name string `json:"name"`

	// Namespace is the Secret metadata.namespace. Leave empty to use the
	// VsphereAgentPool namespace.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^$|^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// VspherePlacementSpec describes where worker VMs are created in vSphere.
type VspherePlacementSpec struct {
	// CredentialsSecretRef references a Secret containing vSphere credentials.
	// The Secret must contain server, username, and password keys. It may also
	// contain an insecure key with "true" when the vCenter certificate should not
	// be verified.
	CredentialsSecretRef SecretReference `json:"credentialsSecretRef"`

	// Datacenter is the target vSphere datacenter name.
	// +kubebuilder:default=dc1
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Datacenter string `json:"datacenter,omitempty"`

	// DatastoreCluster is the datastore cluster used for VM disks. It maps to
	// the static module's vsphere_datastore_cluster input.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	DatastoreCluster string `json:"datastoreCluster"`

	// ISODatastore is the datastore that contains the uploaded discovery ISO.
	// It maps to the static module's vsphere_iso_datastore input.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	ISODatastore string `json:"isoDatastore"`

	// ResourcePool is the vSphere resource pool path, for example
	// cluster/Resources.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	ResourcePool string `json:"resourcePool"`

	// Folder is the VM folder path. When empty, the operator uses the hosted
	// cluster name.
	// +kubebuilder:validation:MaxLength=512
	// +optional
	Folder string `json:"folder,omitempty"`

	// Network is the vSphere network name attached to the VM NIC.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Network string `json:"network"`

	// VMTags contains optional vSphere tag IDs to attach to each VM.
	// +kubebuilder:validation:MaxItems=64
	// +optional
	VMTags []string `json:"vmTags,omitempty"`

	// GuestID is the vSphere guest OS identifier used for from-scratch VMs.
	// +kubebuilder:default=rhel9_64Guest
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.-]+$`
	// +optional
	GuestID string `json:"guestID,omitempty"`

	// SCSIType is the SCSI controller type used for from-scratch VMs.
	// +kubebuilder:default=pvscsi
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.-]+$`
	// +optional
	SCSIType string `json:"scsiType,omitempty"`

	// Firmware is the VM firmware type.
	// +kubebuilder:validation:Enum=efi;bios
	// +kubebuilder:default=efi
	// +optional
	Firmware string `json:"firmware,omitempty"`

	// NetworkAdapterType is the vSphere NIC adapter type.
	// +kubebuilder:default=vmxnet3
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.-]+$`
	// +optional
	NetworkAdapterType string `json:"networkAdapterType,omitempty"`

	// DiskEagerlyScrub controls eager scrubbing for the primary disk.
	// +optional
	DiskEagerlyScrub bool `json:"diskEagerlyScrub,omitempty"`
}

// VMTemplateSpec describes the VM hardware profile.
type VMTemplateSpec struct {
	// NamePrefix prefixes operator-created VM names. When empty, the operator
	// uses <hostedCluster>-<agent.role>.
	// +kubebuilder:validation:MaxLength=58
	// +kubebuilder:validation:Pattern=`^$|^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +optional
	NamePrefix string `json:"namePrefix,omitempty"`

	// NumCPUs is the VM vCPU count.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=128
	// +kubebuilder:default=4
	NumCPUs int32 `json:"numCPUs,omitempty"`

	// MemoryMiB is the VM memory size in MiB.
	// +kubebuilder:validation:Minimum=1024
	// +kubebuilder:validation:Maximum=1048576
	// +kubebuilder:default=16384
	MemoryMiB int32 `json:"memoryMiB,omitempty"`

	// DiskGiB is the primary disk size in GiB.
	// +kubebuilder:validation:Minimum=20
	// +kubebuilder:validation:Maximum=65536
	// +kubebuilder:default=100
	DiskGiB int32 `json:"diskGiB,omitempty"`
}

// AgentBindingSpec describes how a discovered Assisted Installer Agent should
// be made consumable by the Hypershift Agent NodePool.
type AgentBindingSpec struct {
	// Role is the Hypershift NodePool role label value to apply to discovered
	// Agents. For worker pools this is normally "worker".
	// +kubebuilder:default=worker
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$`
	Role string `json:"role,omitempty"`

	// Labels are required on a discovered Agent before the Agent CAPI provider
	// can bind it to an AgentMachine. These should match the NodePool
	// spec.platform.agent.agentLabelSelector labels.
	// +kubebuilder:validation:MinProperties=1
	// +kubebuilder:validation:MaxProperties=32
	Labels map[string]string `json:"labels"`

	// Approve controls whether matching discovered Agents are automatically
	// patched with spec.approved=true.
	// +kubebuilder:default=true
	// +optional
	Approve *bool `json:"approve,omitempty"`
}

// ISOCacheSpec controls how the InfraEnv discovery ISO is cached in vSphere.
type ISOCacheSpec struct {
	// CheckInterval controls how often the operator downloads and hashes the
	// InfraEnv ISO to detect content changes when the URL remains stable.
	// +optional
	CheckInterval metav1.Duration `json:"checkInterval,omitempty"`

	// RetainVersions controls how many content-addressed ISO objects are kept in
	// the datastore. The current ISO is always retained.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=20
	// +kubebuilder:default=2
	// +optional
	RetainVersions int32 `json:"retainVersions,omitempty"`

	// PathPrefix is the datastore directory used for content-addressed ISO
	// objects. When empty, the operator uses
	// agent-forge/<namespace>/<vsphereAgentPool>.
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:Pattern=`^$|^[^/\s][^\s]*$`
	// +optional
	PathPrefix string `json:"pathPrefix,omitempty"`
}

// CleanupPolicy controls whether the operator deletes external inventory when
// demand disappears or the VsphereAgentPool is deleted.
// +kubebuilder:validation:Enum=Delete;Retain
type CleanupPolicy string

const (
	// CleanupPolicyDelete preserves the historical behavior: stale
	// operator-created VMs and unbound Agents are deleted when no longer needed.
	CleanupPolicyDelete CleanupPolicy = "Delete"

	// CleanupPolicyRetain keeps external VMs and Agents in place. The operator
	// still removes its own Kubernetes bookkeeping objects during pool deletion.
	CleanupPolicyRetain CleanupPolicy = "Retain"
)

// VsphereAgentPoolSpec defines the desired state of VsphereAgentPool.
type VsphereAgentPoolSpec struct {
	// HostedClusterRef references the Hypershift HostedCluster this pool serves.
	HostedClusterRef LocalObjectReference `json:"hostedClusterRef"`

	// NodePoolRef references the Hypershift NodePool this bridge follows.
	NodePoolRef LocalObjectReference `json:"nodePoolRef"`

	// InfraEnvRef references the Assisted Installer InfraEnv that exposes the
	// discovery ISO and labels newly discovered Agents.
	InfraEnvRef LocalObjectReference `json:"infraEnvRef"`

	// ControlPlaneNamespace is the hosted control plane namespace that contains
	// the CAPI AgentMachine and Machine objects rendered by Hypershift, for
	// example demo-demo.
	// +kubebuilder:validation:MinLength=1
	ControlPlaneNamespace string `json:"controlPlaneNamespace"`

	// VSphere configures placement and VM platform settings.
	VSphere VspherePlacementSpec `json:"vsphere"`

	// Template configures the worker VM hardware profile.
	Template VMTemplateSpec `json:"template"`

	// Agent configures Agent labels, hostname assignment, and approval.
	Agent AgentBindingSpec `json:"agent"`

	// ISO configures content-addressed caching of the InfraEnv discovery ISO.
	// +optional
	ISO ISOCacheSpec `json:"iso,omitempty"`

	// CleanupPolicy controls whether stale vSphere VMs and unbound Assisted
	// Installer Agents are deleted by the operator. Use Retain for conservative
	// production rollouts where external inventory cleanup is handled manually.
	// +kubebuilder:default=Delete
	// +optional
	CleanupPolicy CleanupPolicy `json:"cleanupPolicy,omitempty"`
}

// OwnedVMStatus records a VM created or managed by this VsphereAgentPool.
type OwnedVMStatus struct {
	// Name is the vSphere VM name.
	Name string `json:"name"`

	// BIOSUUID is the VM BIOS UUID when known.
	// +optional
	BIOSUUID string `json:"biosUUID,omitempty"`

	// MACAddress is the primary NIC MAC address normalized with hyphens.
	// +optional
	MACAddress string `json:"macAddress,omitempty"`

	// AgentRef points to the discovered Assisted Installer Agent, when matched.
	// +optional
	AgentRef *corev1.ObjectReference `json:"agentRef,omitempty"`

	// MachineRef points to the CAPI Machine, when bound.
	// +optional
	MachineRef *corev1.ObjectReference `json:"machineRef,omitempty"`

	// Phase is the current bridge view of the VM lifecycle, such as
	// Provisioning, Available, Bound, Released, or Orphaned.
	// +optional
	Phase string `json:"phase,omitempty"`

	// Reason provides a short machine-readable explanation for Phase.
	// +optional
	Reason string `json:"reason,omitempty"`

	// LastTransitionTime is updated when Phase changes.
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
}

// PlannedActionStatus records the latest create/delete/patch actions the
// operator planned or executed.
type PlannedActionStatus struct {
	// Type is the action type, such as CreateVM, DeleteVM, DeleteAgent,
	// PatchAgent, or Noop.
	Type string `json:"type"`

	// Name is the target VM or Agent name when known.
	// +optional
	Name string `json:"name,omitempty"`

	// Reason explains why the action is needed.
	Reason string `json:"reason"`
}

// ISOCacheHistoryEntry records one uploaded content-addressed ISO object.
type ISOCacheHistoryEntry struct {
	// Path is the datastore path to the ISO object.
	Path string `json:"path"`

	// SHA256 is the ISO content digest.
	SHA256 string `json:"sha256"`

	// SizeBytes is the downloaded ISO size.
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty"`

	// UploadedAt is when this ISO object was uploaded.
	// +optional
	UploadedAt metav1.Time `json:"uploadedAt,omitempty"`
}

// ISOCacheStatus records the active cached InfraEnv discovery ISO.
type ISOCacheStatus struct {
	// URL is the InfraEnv status.isoDownloadURL used for the last check.
	// +optional
	URL string `json:"url,omitempty"`

	// Path is the datastore path inserted into newly created VMs.
	// +optional
	Path string `json:"path,omitempty"`

	// SHA256 is the content digest of the active ISO.
	// +optional
	SHA256 string `json:"sha256,omitempty"`

	// SizeBytes is the downloaded ISO size.
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty"`

	// CheckedAt is when the operator last downloaded and hashed the ISO.
	// +optional
	CheckedAt metav1.Time `json:"checkedAt,omitempty"`

	// UploadedAt is when the active ISO object was uploaded.
	// +optional
	UploadedAt metav1.Time `json:"uploadedAt,omitempty"`

	// ForceRefreshToken stores the last processed force refresh annotation
	// value.
	// +optional
	ForceRefreshToken string `json:"forceRefreshToken,omitempty"`

	// History records retained content-addressed ISO objects, newest first.
	// +optional
	History []ISOCacheHistoryEntry `json:"history,omitempty"`
}

// VsphereAgentPoolStatus defines the observed state of VsphereAgentPool.
type VsphereAgentPoolStatus struct {
	// ObservedGeneration is the most recent metadata.generation reconciled by
	// the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// DesiredReplicas is the observed AgentMachine count.
	// +optional
	DesiredReplicas int32 `json:"desiredReplicas,omitempty"`

	// AgentMachines is the number of non-deleting AgentMachines observed for
	// spec.nodePoolRef in spec.controlPlaneNamespace.
	// +optional
	AgentMachines int32 `json:"agentMachines,omitempty"`

	// WaitingAgentMachines is the number of AgentMachines reporting
	// Ready=False with reason NoSuitableAgents.
	// +optional
	WaitingAgentMachines int32 `json:"waitingAgentMachines,omitempty"`

	// UnreadyAgentMachines is the number of observed AgentMachines whose Ready
	// condition is not True. This includes AgentMachines waiting for suitable
	// Agents and AgentMachines still installing.
	// +optional
	UnreadyAgentMachines int32 `json:"unreadyAgentMachines,omitempty"`

	// AgentMachinesWithoutAgent is the number of unready AgentMachines that do
	// not yet have an assigned Agent. Surplus available Agents are retained
	// while this is non-zero.
	// +optional
	AgentMachinesWithoutAgent int32 `json:"agentMachinesWithoutAgent,omitempty"`

	// MatchingAgents is the number of Agents matching spec.agent.labels.
	// +optional
	MatchingAgents int32 `json:"matchingAgents,omitempty"`

	// BoundAgents is the number of matching Agents already bound to CAPI.
	// +optional
	BoundAgents int32 `json:"boundAgents,omitempty"`

	// AvailableAgents is the number of matching Agents not yet bound to CAPI.
	// +optional
	AvailableAgents int32 `json:"availableAgents,omitempty"`

	// OwnedVMs records VMs created or tracked by this bridge.
	// +optional
	OwnedVMs []OwnedVMStatus `json:"ownedVMs,omitempty"`

	// PlannedActions records the most recent actions planned or executed.
	// +optional
	PlannedActions []PlannedActionStatus `json:"plannedActions,omitempty"`

	// ISO records the active cached InfraEnv discovery ISO.
	// +optional
	ISO ISOCacheStatus `json:"iso,omitempty"`

	// Conditions summarizes readiness, discovery, and errors.
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=vap
// +kubebuilder:printcolumn:name="Waiting",type=integer,JSONPath=`.status.waitingAgentMachines`
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.status.desiredReplicas`
// +kubebuilder:printcolumn:name="Agents",type=integer,JSONPath=`.status.matchingAgents`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// VsphereAgentPool is a namespace-scoped bridge between a Hypershift Agent
// NodePool and vSphere VM inventory. It watches CAPI AgentMachine demand and
// ensures matching Assisted Installer Agents exist for the Agent CAPI provider
// to consume.
type VsphereAgentPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VsphereAgentPoolSpec   `json:"spec,omitempty"`
	Status VsphereAgentPoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// VsphereAgentPoolList contains a list of VsphereAgentPool.
type VsphereAgentPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VsphereAgentPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&VsphereAgentPool{}, &VsphereAgentPoolList{})
}
