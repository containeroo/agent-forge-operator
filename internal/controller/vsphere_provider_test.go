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
	if _, err := provider.CreateVM(ctx, pool, VMCreateRequest{ISOPath: "agent-forge/demo/demo-worker/cached.iso"}); err != nil {
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
	createFields := strings.Fields(createArgs)
	vmName := createFields[len(createFields)-1]
	if !agentHostnamePattern.MatchString(vmName) {
		t.Fatalf("vm.create name = %q, want demo-worker plus 4 random lowercase alphanumeric characters", vmName)
	}
	if !strings.Contains(string(logBytes), "device.cdrom.insert") ||
		!strings.Contains(string(logBytes), "-ds iso-datastore") ||
		!strings.Contains(string(logBytes), "agent-forge/demo/demo-worker/cached.iso") {
		t.Fatalf("cdrom insertion did not use iso datastore; calls:\n%s", string(logBytes))
	}
}

func TestGovcEnsureISOUploadsContentAddressedPath(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	commandLog := filepath.Join(tmpDir, "govc-args.log")
	govcPath := filepath.Join(tmpDir, "govc")
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$GOVC_ARG_LOG"
if [ "$1" = "datastore.ls" ]; then
  exit 1
fi
exit 0
`
	if err := os.WriteFile(govcPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOVC_ARG_LOG", commandLog)

	isoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("iso-v1"))
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

	result, err := provider.EnsureISO(ctx, providerTestPool(), ISOEnsureRequest{DownloadURL: isoServer.URL})
	if err != nil {
		t.Fatalf("EnsureISO returned error: %v", err)
	}
	if !result.Uploaded {
		t.Fatal("EnsureISO did not report upload")
	}
	if result.SHA256 != "9d8a03fda862703f60c30a0c83fae3cff00beb7e3d718ff78e0a791e6fe71048" {
		t.Fatalf("sha = %s, want iso-v1 digest", result.SHA256)
	}
	if result.Path != "agent-forge/demo/demo-worker/"+result.SHA256+".iso" {
		t.Fatalf("path = %s, want content-addressed path", result.Path)
	}
	logBytes, err := os.ReadFile(commandLog)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(logBytes), "datastore.upload") || !strings.Contains(string(logBytes), result.Path) {
		t.Fatalf("upload was not called for content-addressed path; calls:\n%s", string(logBytes))
	}
}

func TestGovcEnsureISOReusesSameDigestWhenDatastoreObjectExists(t *testing.T) {
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
		_, _ = w.Write([]byte("iso-v1"))
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
	sha := "9d8a03fda862703f60c30a0c83fae3cff00beb7e3d718ff78e0a791e6fe71048"
	path := "agent-forge/demo/demo-worker/" + sha + ".iso"

	result, err := provider.EnsureISO(ctx, providerTestPool(), ISOEnsureRequest{
		DownloadURL:   isoServer.URL,
		CurrentSHA256: sha,
		CurrentPath:   path,
	})
	if err != nil {
		t.Fatalf("EnsureISO returned error: %v", err)
	}
	if result.Uploaded {
		t.Fatal("EnsureISO uploaded even though digest and datastore object matched")
	}
	logBytes, err := os.ReadFile(commandLog)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(logBytes), "datastore.upload") {
		t.Fatalf("unexpected upload for reusable ISO; calls:\n%s", string(logBytes))
	}
	if !strings.Contains(string(logBytes), "datastore.ls") {
		t.Fatalf("datastore object existence was not checked; calls:\n%s", string(logBytes))
	}
}

func TestGovcDeleteVMUsesInventoryPath(t *testing.T) {
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

	provider := &govcVMProvider{
		command: govcPath,
		config: govcConfig{
			Server:   "vcenter.example.invalid",
			Username: "user",
			Password: "pass",
			Insecure: "true",
		},
	}

	if err := provider.DeleteVM(ctx, providerTestPool(), newOwnedVMStatus("demo-worker-ab12")); err != nil {
		t.Fatalf("DeleteVM returned error: %v", err)
	}

	logBytes, err := os.ReadFile(commandLog)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.TrimSpace(string(logBytes))
	if args != "vm.destroy -dc dc1 -vm.ipath demo/demo-worker-ab12" {
		t.Fatalf("vm.destroy args = %q, want folder-qualified inventory path", args)
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
