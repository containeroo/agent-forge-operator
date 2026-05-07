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
	Name string `json:"name"`
}

// SecretReference identifies a Secret. When namespace is empty, the
// VsphereAgentPool namespace is used.
type SecretReference struct {
	// Name is the Secret metadata.name.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Namespace is the Secret metadata.namespace. Leave empty to use the
	// VsphereAgentPool namespace.
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
	Datacenter string `json:"datacenter,omitempty"`

	// DatastoreCluster is the datastore cluster used for VM disks. It maps to
	// the static module's vsphere_datastore_cluster input.
	// +kubebuilder:validation:MinLength=1
	DatastoreCluster string `json:"datastoreCluster"`

	// ISODatastore is the datastore that contains the uploaded discovery ISO.
	// It maps to the static module's vsphere_iso_datastore input.
	// +kubebuilder:validation:MinLength=1
	ISODatastore string `json:"isoDatastore"`

	// ResourcePool is the vSphere resource pool path, for example
	// cluster/Resources.
	// +kubebuilder:validation:MinLength=1
	ResourcePool string `json:"resourcePool"`

	// Folder is the VM folder path. When empty, the operator uses the hosted
	// cluster name.
	// +optional
	Folder string `json:"folder,omitempty"`

	// Network is the vSphere network name attached to the VM NIC.
	// +kubebuilder:validation:MinLength=1
	Network string `json:"network"`

	// VMTags contains optional vSphere tag IDs to attach to each VM.
	// +optional
	VMTags []string `json:"vmTags,omitempty"`

	// GuestID is the vSphere guest OS identifier used for from-scratch VMs.
	// +kubebuilder:default=rhel9_64Guest
	// +optional
	GuestID string `json:"guestID,omitempty"`

	// SCSIType is the SCSI controller type used for from-scratch VMs.
	// +kubebuilder:default=pvscsi
	// +optional
	SCSIType string `json:"scsiType,omitempty"`

	// Firmware is the VM firmware type.
	// +kubebuilder:validation:Enum=efi;bios
	// +kubebuilder:default=efi
	// +optional
	Firmware string `json:"firmware,omitempty"`

	// NetworkAdapterType is the vSphere NIC adapter type.
	// +kubebuilder:default=vmxnet3
	// +optional
	NetworkAdapterType string `json:"networkAdapterType,omitempty"`

	// DiskEagerlyScrub controls eager scrubbing for the primary disk.
	// +optional
	DiskEagerlyScrub bool `json:"diskEagerlyScrub,omitempty"`

	// ISOPath is the datastore path to the InfraEnv discovery ISO. When empty,
	// the operator derives iso/<infraEnvName>-discovery.iso.
	// +optional
	ISOPath string `json:"isoPath,omitempty"`
}

// VMTemplateSpec describes the VM hardware profile.
type VMTemplateSpec struct {
	// NamePrefix prefixes operator-created VM names. When empty, the operator
	// uses <hostedCluster>-<agent.role>.
	// +optional
	NamePrefix string `json:"namePrefix,omitempty"`

	// NumCPUs is the VM vCPU count.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=4
	NumCPUs int32 `json:"numCPUs,omitempty"`

	// MemoryMiB is the VM memory size in MiB.
	// +kubebuilder:validation:Minimum=1024
	// +kubebuilder:default=16384
	MemoryMiB int32 `json:"memoryMiB,omitempty"`

	// DiskGiB is the primary disk size in GiB.
	// +kubebuilder:validation:Minimum=20
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
	Role string `json:"role,omitempty"`

	// Labels are required on a discovered Agent before the Agent CAPI provider
	// can bind it to an AgentMachine. These should match the NodePool
	// spec.platform.agent.agentLabelSelector labels.
	// +kubebuilder:validation:MinProperties=1
	Labels map[string]string `json:"labels"`

	// Approve controls whether matching discovered Agents are automatically
	// patched with spec.approved=true.
	// +kubebuilder:default=true
	// +optional
	Approve *bool `json:"approve,omitempty"`
}

// ScalingPolicySpec contains bridge-side guardrails. It does not replace the
// hosted cluster autoscaler; MachineSet.spec.replicas remains the source of
// truth for desired node count.
type ScalingPolicySpec struct {
	// BufferAgents is the number of extra matching, unbound Agents to keep
	// available beyond MachineSet demand. Use 0 for strict cost control.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=0
	// +optional
	BufferAgents int32 `json:"bufferAgents,omitempty"`

	// MaxProvisioning limits how many VMs this operator creates in one
	// reconciliation. It throttles large autoscaler jumps to protect vSphere,
	// DHCP, storage, and Assisted Installer.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3
	// +optional
	MaxProvisioning int32 `json:"maxProvisioning,omitempty"`

	// DeletePolicy controls scale-down cleanup. OwnedOnly means only VMs
	// recorded in this object's status are destroyed. Retain never destroys VMs.
	// +kubebuilder:validation:Enum=OwnedOnly;Retain
	// +kubebuilder:default=OwnedOnly
	// +optional
	DeletePolicy string `json:"deletePolicy,omitempty"`
}

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
	// the CAPI MachineSet rendered by Hypershift, for example
	// demo-demo.
	// +kubebuilder:validation:MinLength=1
	ControlPlaneNamespace string `json:"controlPlaneNamespace"`

	// MachineSetName optionally pins the CAPI MachineSet name. When empty, the
	// operator discovers it by the hypershift.openshift.io/nodePool annotation.
	// +optional
	MachineSetName string `json:"machineSetName,omitempty"`

	// DryRun makes the operator compute and report actions without creating,
	// patching, or deleting VMs/Agents. This is the recommended first mode for a
	// new hosted cluster.
	// +kubebuilder:default=true
	// +optional
	DryRun bool `json:"dryRun,omitempty"`

	// VSphere configures placement and VM platform settings.
	VSphere VspherePlacementSpec `json:"vsphere"`

	// Template configures the worker VM hardware profile.
	Template VMTemplateSpec `json:"template"`

	// Agent configures Agent labels, hostname assignment, and approval.
	Agent AgentBindingSpec `json:"agent"`

	// Scaling configures bridge-side buffering, throttling, and deletion safety.
	// +optional
	Scaling ScalingPolicySpec `json:"scaling,omitempty"`
}

// OwnedVMStatus records a VM created or managed by this VsphereAgentPool.
type OwnedVMStatus struct {
	// Name is the vSphere VM name.
	Name string `json:"name"`

	// MoRef is the vSphere Managed Object Reference value when known.
	// +optional
	MoRef string `json:"moRef,omitempty"`

	// BIOSUUID is the VM BIOS UUID when known.
	// +optional
	BIOSUUID string `json:"biosUUID,omitempty"`

	// InstanceUUID is the VM instance UUID when known.
	// +optional
	InstanceUUID string `json:"instanceUUID,omitempty"`

	// MACAddress is the primary NIC MAC address normalized with hyphens.
	// +optional
	MACAddress string `json:"macAddress,omitempty"`

	// AgentRef points to the discovered Assisted Installer Agent, when matched.
	// +optional
	AgentRef *corev1.ObjectReference `json:"agentRef,omitempty"`

	// MachineRef points to the CAPI Machine, when bound.
	// +optional
	MachineRef *corev1.ObjectReference `json:"machineRef,omitempty"`

	// Phase is the current bridge view of the VM lifecycle.
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
	// Type is the action type, such as CreateVM, DeleteVM, PatchAgent, or Noop.
	Type string `json:"type"`

	// Name is the target VM or Agent name when known.
	// +optional
	Name string `json:"name,omitempty"`

	// Reason explains why the action is needed.
	Reason string `json:"reason"`

	// DryRun is true when the action was only reported.
	DryRun bool `json:"dryRun"`
}

// VsphereAgentPoolStatus defines the observed state of VsphereAgentPool.
type VsphereAgentPoolStatus struct {
	// ObservedGeneration is the most recent metadata.generation reconciled by
	// the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ObservedMachineSet is the CAPI MachineSet currently followed by the
	// bridge.
	// +optional
	ObservedMachineSet string `json:"observedMachineSet,omitempty"`

	// DesiredReplicas is MachineSet.spec.replicas plus any configured buffer.
	// +optional
	DesiredReplicas int32 `json:"desiredReplicas,omitempty"`

	// MachineSetReplicas is the raw MachineSet.spec.replicas value.
	// +optional
	MachineSetReplicas int32 `json:"machineSetReplicas,omitempty"`

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

	// Conditions summarizes readiness, dry-run state, discovery, and errors.
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
// +kubebuilder:printcolumn:name="MachineSet",type=string,JSONPath=`.status.observedMachineSet`
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.status.desiredReplicas`
// +kubebuilder:printcolumn:name="Agents",type=integer,JSONPath=`.status.matchingAgents`
// +kubebuilder:printcolumn:name="DryRun",type=boolean,JSONPath=`.spec.dryRun`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// VsphereAgentPool is a namespace-scoped bridge between a Hypershift Agent
// NodePool and vSphere VM inventory. It watches CAPI MachineSet demand created
// by the hosted cluster autoscaler and ensures matching Assisted Installer
// Agents exist for the Agent CAPI provider to consume.
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
