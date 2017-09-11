package libcalicostub

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TODO: actually import libcalico
// * Need to get our changes merged in
// * Wait for libcalico to use recent enough version of client-go such that tools/leaderelection package is present
// * At that point, we should be able to import libcalico and share client-go vendor codebase

// Until then, we can copypasta what we need here
const (
	NodeBgpReflectorLabel   = "projectcalico.org/reflector"
	NodeBgpIpv4NetworkLabel = "projectcalico.org/ipv4-network"

	GlobalBGPConfigResourceName = "GlobalBGPConfig"
	GlobalBGPConfigCRDName      = "globalbgpconfigs"

	ReflectorsPerSubnetName       = "reflectorspersubnet"
	ReflectorsPerSubnetProperName = "ReflectorsPerSubnet"
)

var (
	GroupVersion = schema.GroupVersion{
		Group:   "crd.projectcalico.org",
		Version: "v1",
	}
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:deepcopy-gen=true
type GlobalBGPConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              GlobalBGPConfigSpec `json:"spec"`
}

type GlobalBGPConfigSpec struct {
	// The reason we have Name field in Spec is because k8s metadata
	// name field requires the string to be lowercase, so Name field
	// in Spec is to preserve the casing.
	Name  string `json:"name"`
	Value string `json:"value"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:deepcopy-gen=true
type GlobalBGPConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GlobalBGPConfig `json:"items"`
}
