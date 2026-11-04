//nolint:goconst // Fixtures keep API values explicit for review.
package v1alpha1

import (
	"context"
	"os"
	"testing"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	crdvalidation "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/yaml"
)

func loadOpenStackSchema(t *testing.T, name string) *apiextensions.JSONSchemaProps {
	t.Helper()
	data, err := os.ReadFile("../../config/crd/bases/agent-forge.containeroo.ch_" + name + ".yaml")
	if err != nil {
		t.Fatal(err)
	}
	external := &extv1.CustomResourceDefinition{}
	if err := yaml.Unmarshal(data, external); err != nil {
		t.Fatal(err)
	}
	extv1.SetDefaults_CustomResourceDefinition(external)
	internal := &apiextensions.CustomResourceDefinition{}
	if err := extv1.Convert_v1_CustomResourceDefinition_To_apiextensions_CustomResourceDefinition(external, internal, nil); err != nil {
		t.Fatal(err)
	}
	internal.Status.StoredVersions = []string{"v1alpha1"}
	if errs := crdvalidation.ValidateCustomResourceDefinition(context.Background(), internal); len(errs) > 0 {
		t.Fatalf("CRD is not installable: %v", errs)
	}
	if internal.Spec.Validation != nil {
		return internal.Spec.Validation.OpenAPIV3Schema
	}
	return internal.Spec.Versions[0].Schema.OpenAPIV3Schema
}

func TestOpenStackCRDsAreInstallable(t *testing.T) {
	for _, name := range []string{"openstackagentpools", "openstackagents"} {
		t.Run(name, func(t *testing.T) { loadOpenStackSchema(t, name) })
	}
}
func TestOpenStackPoolDefaultsAndValidation(t *testing.T) {
	props := loadOpenStackSchema(t, "openstackagentpools")
	structural, err := schema.NewStructural(props)
	if err != nil {
		t.Fatal(err)
	}
	validator, _, err := validation.NewSchemaValidator(props)
	if err != nil {
		t.Fatal(err)
	}
	celValidator := cel.NewValidator(structural, true, 10000000)
	base := map[string]any{"apiVersion": GroupVersion.String(), "kind": "OpenStackAgentPool", "metadata": map[string]any{"name": "example-workers"}, "spec": map[string]any{
		"hostedClusterRef": map[string]any{"name": "example-cluster"}, "nodePoolRef": map[string]any{"name": "example-workers"}, "infraEnvRef": map[string]any{"name": "example-infraenv"}, "controlPlaneNamespace": "example-control-plane",
		"openstack": map[string]any{"credentialsSecretRef": map[string]any{"name": "example-credentials"}, "region": "example-region", "networkName": "worker-network", "flavorName": "worker-flavor"},
		"template":  map[string]any{}, "agent": map[string]any{"labels": map[string]any{"example-pool": "workers"}},
	}}
	defaulting.Default(base, structural)
	spec := base["spec"].(map[string]any)
	if spec["openstack"].(map[string]any)["portSecurityEnabled"] != true || spec["template"].(map[string]any)["osDiskBus"] != "virtio" || spec["template"].(map[string]any)["discoveryDiskBus"] != "ide" {
		t.Fatal("unsafe boot/network defaults")
	}
	for _, tc := range []struct {
		name    string
		invalid bool
		mutate  func(map[string]any)
	}{
		{"valid", false, func(map[string]any) {}},
		{"other cloud", false, func(s map[string]any) {
			s["openstack"].(map[string]any)["credentialsSecretRef"] = map[string]any{"name": "other-cloud-credentials"}
		}},
		{"missing secret", true, func(s map[string]any) { delete(s["openstack"].(map[string]any), "credentialsSecretRef") }},
		{"both network selectors", true, func(s map[string]any) { s["openstack"].(map[string]any)["networkID"] = "example-network-id" }},
		{"no flavor", true, func(s map[string]any) { delete(s["openstack"].(map[string]any), "flavorName") }},
		{"security conflict", true, func(s map[string]any) {
			o := s["openstack"].(map[string]any)
			o["portSecurityEnabled"] = false
			o["securityGroupIDs"] = []any{"example-group"}
		}},
		{"small disk", true, func(s map[string]any) { s["template"].(map[string]any)["diskGiB"] = int64(1) }},
		{"invalid retention", true, func(s map[string]any) { s["cleanupPolicy"] = "DestroyEverything" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obj := runtime.DeepCopyJSON(base)
			tc.mutate(obj["spec"].(map[string]any))
			errs := validation.ValidateCustomResource(field.NewPath(""), obj, validator)
			celErrs, _ := celValidator.Validate(context.Background(), field.NewPath(""), structural, obj, nil, 10000000)
			errs = append(errs, celErrs...)
			if (len(errs) > 0) != tc.invalid {
				t.Fatalf("validation errors: %v", errs)
			}
		})
	}
	changed := runtime.DeepCopyJSON(base)
	changed["spec"].(map[string]any)["nodePoolRef"] = map[string]any{"name": "other-workers"}
	errs, _ := celValidator.Validate(context.Background(), field.NewPath(""), structural, changed, base, 10000000)
	if len(errs) == 0 {
		t.Fatal("immutable NodePool reference changed")
	}
}
