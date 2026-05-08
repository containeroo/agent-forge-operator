package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
)

func TestGovcCreateVMUsesDatastoreClusterPlacement(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	commandLog := filepath.Join(tmpDir, "govc-args.log")
	govcPath := filepath.Join(tmpDir, "govc")
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$GOVC_ARG_LOG"
exit 0
`
	if err := os.WriteFile(govcPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOVC_ARG_LOG", commandLog)

	isoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("iso"))
	}))
	defer isoServer.Close()

	provider := &govcVMProvider{
		command: govcPath,
		config: govcConfig{
			Server:   "vcenter.example.invalid",
			Username: "user",
			Password: "pass",
			Insecure: "true",
		},
	}

	pool := providerTestPool()
	if _, err := provider.CreateVM(ctx, pool, VMCreateRequest{ISODownloadURL: isoServer.URL}); err != nil {
		t.Fatalf("CreateVM returned error: %v", err)
	}

	logBytes, err := os.ReadFile(commandLog)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(logBytes)), "\n")
	var createArgs string
	for _, line := range lines {
		if strings.HasPrefix(line, "vm.create ") {
			createArgs = line
			break
		}
	}
	if createArgs == "" {
		t.Fatalf("vm.create was not called; calls:\n%s", string(logBytes))
	}
	if !strings.Contains(createArgs, "-datastore-cluster workload-datastore-cluster") {
		t.Fatalf("vm.create args = %q, want datastore cluster placement", createArgs)
	}
	if strings.Contains(createArgs, "-ds workload-datastore-cluster") {
		t.Fatalf("vm.create args = %q, must not pass datastore cluster through -ds", createArgs)
	}
	if !strings.Contains(string(logBytes), "device.cdrom.insert") || !strings.Contains(string(logBytes), "-ds iso-datastore") {
		t.Fatalf("cdrom insertion did not use iso datastore; calls:\n%s", string(logBytes))
	}
}

func providerTestPool() *agentforgev1alpha1.VsphereAgentPool {
	return &agentforgev1alpha1.VsphereAgentPool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "demo",
			Name:      "demo-worker",
		},
		Spec: agentforgev1alpha1.VsphereAgentPoolSpec{
			HostedClusterRef: agentforgev1alpha1.LocalObjectReference{Name: "demo"},
			VSphere: agentforgev1alpha1.VspherePlacementSpec{
				Datacenter:         "dc1",
				DatastoreCluster:   "workload-datastore-cluster",
				ISODatastore:       "iso-datastore",
				ResourcePool:       "cluster/Resources",
				Folder:             "demo",
				Network:            "VM Network",
				NetworkAdapterType: "vmxnet3",
				GuestID:            "rhcos_64Guest",
				Firmware:           "efi",
				SCSIType:           "pvscsi",
				ISOPath:            "agent-forge/demo.iso",
			},
			Template: agentforgev1alpha1.VMTemplateSpec{
				NamePrefix: "demo-worker",
				NumCPUs:    4,
				MemoryMiB:  16384,
				DiskGiB:    120,
			},
			Agent: agentforgev1alpha1.AgentBindingSpec{
				Role: "worker",
			},
		},
	}
}
