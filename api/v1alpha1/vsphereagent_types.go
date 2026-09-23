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

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// VsphereAgentSpec defines the desired state of a single vSphere-backed
// Assisted Installer Agent candidate.
type VsphereAgentSpec struct {
	// PoolRef references the VsphereAgentPool whose configuration is used to
	// create and manage this VM.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="poolRef is immutable"
	PoolRef LocalObjectReference `json:"poolRef"`
}

// VsphereAgentStatus defines the observed state of a VsphereAgent.
type VsphereAgentStatus struct {
	// ObservedGeneration is the most recent metadata.generation reconciled by
	// the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// VM records the vSphere VM created for this VsphereAgent.
	// +optional
	VM OwnedVMStatus `json:"vm,omitempty"`

	// ISOEjection records a single authorized media operation before vSphere is
	// changed. It survives restarts and is completed even if opt-in is disabled.
	// +optional
	ISOEjection *ISOEjectionStatus `json:"isoEjection,omitempty"`

	// Conditions summarizes readiness and provider errors.
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// ISOEjectionStatus identifies the exact VM and media authorized for ejection.
// These values are captured before the disconnect begins, so recovery cannot
// act on a replacement VM or newly attached media.
type ISOEjectionStatus struct {
	VMName   string `json:"vmName"`
	BIOSUUID string `json:"biosUUID"`
	// +optional
	OwnerUID   string      `json:"ownerUID,omitempty"`
	Datacenter string      `json:"datacenter"`
	DeviceKey  int         `json:"deviceKey"`
	ISOPath    string      `json:"isoPath"`
	StartedAt  metav1.Time `json:"startedAt"`
	// DisconnectStarted is persisted immediately before issuing the disconnect.
	// A pre-existing question is never answered for an operation not yet started.
	// +optional
	DisconnectStarted bool `json:"disconnectStarted,omitempty"`
	// QuestionID is persisted before answering a CD-lock question. A different
	// question on retry is never automatically answered.
	// +optional
	QuestionID string `json:"questionID,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=va
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.poolRef.name`
// +kubebuilder:printcolumn:name="VM",type=string,JSONPath=`.status.vm.name`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.vm.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// VsphereAgent is one vSphere VM requested to satisfy AgentMachine demand.
type VsphereAgent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VsphereAgentSpec   `json:"spec,omitempty"`
	Status VsphereAgentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// VsphereAgentList contains a list of VsphereAgent.
type VsphereAgentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VsphereAgent `json:"items"`
}
