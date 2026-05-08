package v1alpha1

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestVsphereAgentPoolDryRunFalseSerializes(t *testing.T) {
	pool := &VsphereAgentPool{
		Spec: VsphereAgentPoolSpec{
			DryRun: false,
		},
	}

	payload, err := json.Marshal(pool)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), `"dryRun":false`) {
		t.Fatalf("serialized VsphereAgentPool = %s, want explicit dryRun=false", string(payload))
	}
}
