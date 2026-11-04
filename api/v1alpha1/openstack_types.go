package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// OpenStackPlacementSpec selects one project and region. Credentials are read only
// from the referenced Secret; no process environment or clouds.yaml is consumed.
// +kubebuilder:validation:XValidation:rule="!has(self.dhcpEnabled) || has(self.subnetID)",message="dhcpEnabled requires subnetID"
// +kubebuilder:validation:XValidation:rule="has(self.networkID) != has(self.networkName)",message="set exactly one of networkID or networkName"
// +kubebuilder:validation:XValidation:rule="has(self.flavorID) != has(self.flavorName)",message="set exactly one of flavorID or flavorName"
// +kubebuilder:validation:XValidation:rule="!has(self.portSecurityEnabled) || self.portSecurityEnabled || !has(self.securityGroupIDs) || size(self.securityGroupIDs) == 0",message="security groups require port security"
type OpenStackPlacementSpec struct {
	CredentialsSecretRef LocalObjectReference `json:"credentialsSecretRef"`
	// +kubebuilder:validation:MinLength=1
	Region string `json:"region"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	// +optional
	NetworkID string `json:"networkID,omitempty"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	// +optional
	NetworkName string `json:"networkName,omitempty"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	// +optional
	FlavorID string `json:"flavorID,omitempty"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	// +optional
	FlavorName string `json:"flavorName,omitempty"`
	// SubnetID limits Neutron address allocation. DHCP is controlled by that subnet;
	// the operator never changes a shared subnet's DHCP configuration.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	// +optional
	SubnetID string `json:"subnetID,omitempty"`
	// DHCPEnabled optionally requires the selected subnet to have this DHCP
	// setting. The shared subnet is validated, never mutated by the operator.
	// +optional
	DHCPEnabled *bool `json:"dhcpEnabled,omitempty"`
	// +kubebuilder:default=true
	// +optional
	PortSecurityEnabled *bool `json:"portSecurityEnabled,omitempty"`
	// +optional
	SecurityGroupIDs []string `json:"securityGroupIDs,omitempty"`
	// PortTags are applied in addition to ownership stored in the port description.
	// +kubebuilder:validation:MaxItems=40
	// +optional
	PortTags []string `json:"portTags,omitempty"`
	// +optional
	AvailabilityZone string `json:"availabilityZone,omitempty"`
	// +kubebuilder:default=60
	// +kubebuilder:validation:Minimum=5
	// +kubebuilder:validation:Maximum=600
	// +optional
	APITimeoutSeconds int32 `json:"apiTimeoutSeconds,omitempty"`
}

// OpenStackTemplateSpec describes Cinder storage and Nova block device buses.
type OpenStackTemplateSpec struct {
	// +kubebuilder:default=120
	// +kubebuilder:validation:Minimum=20
	// +kubebuilder:validation:Maximum=65536
	// +optional
	DiskGiB int32 `json:"diskGiB,omitempty"`
	// +optional
	VolumeType string `json:"volumeType,omitempty"`
	// +optional
	VolumeAvailabilityZone string `json:"volumeAvailabilityZone,omitempty"`
	// +optional
	VolumeMetadata map[string]string `json:"volumeMetadata,omitempty"`
	// DiscoveryVolumeMinGiB is a floor; actual ISO size is always rounded up.
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65536
	// +optional
	DiscoveryVolumeMinGiB int32 `json:"discoveryVolumeMinGiB,omitempty"`
	// +optional
	DiscoveryVolumeType string `json:"discoveryVolumeType,omitempty"`
	// +kubebuilder:default=virtio
	// +kubebuilder:validation:Enum=virtio;scsi;sata;ide
	// +optional
	OSDiskBus string `json:"osDiskBus,omitempty"`
	// +kubebuilder:default=ide
	// +kubebuilder:validation:Enum=ide;scsi;sata
	// +optional
	DiscoveryDiskBus string `json:"discoveryDiskBus,omitempty"`
	// InstallationDisk is an explicit Assisted Installer inventory disk path.
	// It must match the OS volume's serial and must never identify a CD-ROM.
	// +kubebuilder:default="/dev/vda"
	// +kubebuilder:validation:Pattern=`^/dev/[a-zA-Z0-9/_-]+$`
	// +optional
	InstallationDisk string `json:"installationDisk,omitempty"`
	// OSVolumePolicy retains only the OS volume when set to Retain. The instance,
	// discovery volume and port are still deleted under cleanupPolicy Delete.
	// +kubebuilder:default=Delete
	// +optional
	OSVolumePolicy CleanupPolicy `json:"osVolumePolicy,omitempty"`
}

// OpenStackAgentPoolSpec follows the same AgentMachine demand as vSphere pools.
type OpenStackAgentPoolSpec struct {
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="hostedClusterRef is immutable"
	HostedClusterRef LocalObjectReference `json:"hostedClusterRef"`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="nodePoolRef is immutable"
	NodePoolRef LocalObjectReference `json:"nodePoolRef"`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="infraEnvRef is immutable"
	InfraEnvRef LocalObjectReference `json:"infraEnvRef"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="controlPlaneNamespace is immutable"
	ControlPlaneNamespace string                 `json:"controlPlaneNamespace"`
	OpenStack             OpenStackPlacementSpec `json:"openstack"`
	Template              OpenStackTemplateSpec  `json:"template"`
	Agent                 AgentBindingSpec       `json:"agent"`
	// +kubebuilder:default=Delete
	// +optional
	CleanupPolicy CleanupPolicy `json:"cleanupPolicy,omitempty"`
	// +kubebuilder:default=1800
	// +kubebuilder:validation:Minimum=60
	// +optional
	DiscoveryTimeoutSeconds int32 `json:"discoveryTimeoutSeconds,omitempty"`
}

// OpenStackAgentPoolStatus reports demand without inferring destructive scale-down
// from replica counts. Only disappearance of the original CAPI demand authorizes it.
type OpenStackAgentPoolStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	DesiredReplicas int32 `json:"desiredReplicas,omitempty"`
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// OpenStackAgentSpec freezes placement at creation so pool edits cannot move or
// replace an installed worker. Pool edits affect subsequent workers only.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="worker configuration is immutable"
type OpenStackAgentSpec struct {
	PoolRef LocalObjectReference `json:"poolRef"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	PoolUID string `json:"poolUID"`
	// +kubebuilder:validation:XValidation:rule="has(self.name) && size(self.name) > 0 && has(self.namespace) && size(self.namespace) > 0 && has(self.uid) && size(self.uid) > 0",message="AgentMachine name, namespace, and UID are required"
	AgentMachineRef corev1.ObjectReference `json:"agentMachineRef"`
	// MachineRef preserves the CAPI owner identity for conservative cleanup.
	// +optional
	MachineRef    *corev1.ObjectReference `json:"machineRef,omitempty"`
	Configuration OpenStackAgentPoolSpec  `json:"configuration"`
	// FixedIPAddress optionally requests one address from the selected subnet.
	// Pool-created workers use Neutron allocation; never share one fixed IP pool-wide.
	// +optional
	FixedIPAddress string `json:"fixedIPAddress,omitempty"`
}

// OpenStackAgentStatus persists external identities before advancing provisioning.
type OpenStackAgentStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	Phase string `json:"phase,omitempty"`
	// ScopeHash detects credentials moving to another cloud/project/region.
	// +optional
	ScopeHash string `json:"scopeHash,omitempty"`
	// PendingCreate is an at-most-once create journal. An ambiguous result is
	// recovered by ownership lookup, never by blindly replaying POST.
	// +optional
	PendingCreate string `json:"pendingCreate,omitempty"`
	// +optional
	PortID string `json:"portID,omitempty"`
	// +optional
	MACAddress string `json:"macAddress,omitempty"`
	// +optional
	OSVolumeID string `json:"osVolumeID,omitempty"`
	// +optional
	DiscoveryVolumeID string `json:"discoveryVolumeID,omitempty"`
	// +optional
	ImageID string `json:"imageID,omitempty"`
	// +optional
	ImageCacheName string `json:"imageCacheName,omitempty"`
	// +optional
	ISOSHA256 string `json:"isoSHA256,omitempty"`
	// +optional
	ISOSizeBytes int64 `json:"isoSizeBytes,omitempty"`
	// +optional
	InfraEnvUID string `json:"infraEnvUID,omitempty"`
	// +optional
	ServerID string `json:"serverID,omitempty"`
	// +optional
	AgentRef *corev1.ObjectReference `json:"agentRef,omitempty"`
	// +optional
	InstanceCreatedAt *metav1.Time `json:"instanceCreatedAt,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=osap
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.status.desiredReplicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// OpenStackAgentPool provisions workers for one HyperShift Agent NodePool.
type OpenStackAgentPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              OpenStackAgentPoolSpec   `json:"spec,omitempty"`
	Status            OpenStackAgentPoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// OpenStackAgentPoolList contains OpenStackAgentPools.
type OpenStackAgentPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []OpenStackAgentPool `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=osa
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.poolRef.name`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// OpenStackAgent owns one persistent port, OS volume, discovery volume and server.
type OpenStackAgent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              OpenStackAgentSpec   `json:"spec,omitempty"`
	Status            OpenStackAgentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// OpenStackAgentList contains OpenStackAgents.
type OpenStackAgentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []OpenStackAgent `json:"items"`
}
