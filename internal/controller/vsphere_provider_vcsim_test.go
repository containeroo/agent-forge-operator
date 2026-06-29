//go:build vcsim

package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
)

type vcsimTestEnv struct {
	govcPath string
	url      string
	logPath  string
}

func TestGovcProviderVcsimEnsureISOUploadsReusesAndDeletes(t *testing.T) {
	env := startVcsim(t)
	ctx := context.Background()
	content := []byte("vcsim-discovery-iso")
	isoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(content)
	}))
	defer isoServer.Close()

	provider := env.provider()
	pool := vcsimProviderTestPool()
	result, err := provider.EnsureISO(ctx, pool, ISOEnsureRequest{DownloadURL: isoServer.URL})
	if err != nil {
		t.Fatalf("EnsureISO returned error: %v", err)
	}
	wantSHA := sha256.Sum256(content)
	if result.SHA256 != hex.EncodeToString(wantSHA[:]) {
		t.Fatalf("sha = %s, want digest of simulator ISO content", result.SHA256)
	}
	if result.Path != "agent-forge/vcsim/demo-worker/"+result.SHA256+".iso" {
		t.Fatalf("path = %s, want content-addressed path under simulator prefix", result.Path)
	}
	if !result.Uploaded {
		t.Fatal("EnsureISO did not report an upload")
	}

	reused, err := provider.EnsureISO(ctx, pool, ISOEnsureRequest{DownloadURL: isoServer.URL})
	if err != nil {
		t.Fatalf("second EnsureISO returned error: %v", err)
	}
	if reused.Uploaded {
		t.Fatal("second EnsureISO uploaded even though the simulator datastore already had the object")
	}

	if err := provider.DeleteISO(ctx, pool, result.Path); err != nil {
		t.Fatalf("DeleteISO returned error: %v", err)
	}
	if _, err := provider.datastorePathExists(ctx, pool, result.Path); !isGovcDatastorePathNotFound(err) {
		t.Fatalf("datastorePathExists after DeleteISO error = %v, want datastore path not found", err)
	}
}

func TestGovcProviderVcsimVMStatusAndDelete(t *testing.T) {
	env := startVcsim(t)
	ctx := context.Background()
	pool := vcsimProviderTestPool()
	name := "demo-worker-vcsim"

	env.runGovc(t,
		"vm.create",
		"-dc", pool.Spec.VSphere.Datacenter,
		"-ds", pool.Spec.VSphere.ISODatastore,
		"-pool", "/DC0/host/DC0_C0/Resources",
		"-net", pool.Spec.VSphere.Network,
		"-on=false",
		name,
	)

	provider := env.provider()
	vm, err := provider.VMStatus(ctx, pool, name)
	if err != nil {
		t.Fatalf("VMStatus returned error: %v", err)
	}
	if vm.Name != name {
		t.Fatalf("VMStatus name = %s, want %s", vm.Name, name)
	}
	if vm.BIOSUUID == "" {
		t.Fatal("VMStatus did not discover a BIOS UUID from vcsim")
	}
	if vm.MACAddress == "" || strings.Contains(vm.MACAddress, ":") {
		t.Fatalf("VMStatus MACAddress = %q, want normalized hyphen-separated MAC", vm.MACAddress)
	}

	if err := provider.DeleteVM(ctx, pool, vm); err != nil {
		t.Fatalf("DeleteVM returned error: %v", err)
	}
	if _, err := provider.VMStatus(ctx, pool, name); !isGovcVMNotFound(err) {
		t.Fatalf("VMStatus after DeleteVM error = %v, want VM not found", err)
	}
	if err := provider.DeleteVM(ctx, pool, vm); err != nil {
		t.Fatalf("second DeleteVM returned error for already deleted VM: %v", err)
	}
}

func startVcsim(t *testing.T) *vcsimTestEnv {
	t.Helper()

	govcPath := os.Getenv("GOVC_PATH")
	if govcPath == "" {
		t.Skip("GOVC_PATH is required for vcsim integration tests")
	}
	vcsimPath := os.Getenv("VCSIM_PATH")
	if vcsimPath == "" {
		t.Skip("VCSIM_PATH is required for vcsim integration tests")
	}

	addr := freeLocalAddress(t)
	logFile, err := os.CreateTemp(t.TempDir(), "vcsim-*.log")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(vcsimPath,
		"-l", addr,
		"-dc", "1",
		"-cluster", "1",
		"-host", "1",
		"-pool", "1",
		"-ds", "1",
		"-folder", "0",
		"-vm", "0",
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("start vcsim: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
		_ = logFile.Close()
	})

	env := &vcsimTestEnv{
		govcPath: govcPath,
		url:      "https://" + addr + "/sdk",
		logPath:  logFile.Name(),
	}
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if _, err := env.runGovcOutput("about"); err == nil {
			return env
		} else {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	logBytes, _ := os.ReadFile(env.logPath)
	t.Fatalf("vcsim did not become ready: %v\n%s", lastErr, string(logBytes))
	return nil
}

func freeLocalAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = listener.Close()
	}()
	return listener.Addr().String()
}

func (e *vcsimTestEnv) provider() *govcVMProvider {
	return &govcVMProvider{
		command: e.govcPath,
		config: govcConfig{
			Server:   e.url,
			Username: "user",
			Password: "pass",
			Insecure: "true",
		},
	}
}

func (e *vcsimTestEnv) runGovc(t *testing.T, args ...string) []byte {
	t.Helper()

	output, err := e.runGovcOutput(args...)
	if err != nil {
		t.Fatalf("govc %s failed: %v\n%s", strings.Join(args, " "), err, string(output))
	}
	return output
}

func (e *vcsimTestEnv) runGovcOutput(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, e.govcPath, args...)
	cmd.Env = append(os.Environ(),
		"HOME=/tmp",
		"GOVC_PERSIST_SESSION=false",
		"GOVC_URL="+e.url,
		"GOVC_USERNAME=user",
		"GOVC_PASSWORD=pass",
		"GOVC_INSECURE=true",
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	err := cmd.Run()
	if ctx.Err() != nil {
		return output.Bytes(), fmt.Errorf("%w: %w", err, ctx.Err())
	}
	return output.Bytes(), err
}

func vcsimProviderTestPool() *agentforgev1alpha1.VsphereAgentPool {
	pool := providerTestPool()
	pool.Spec.HostedClusterRef.Name = ""
	pool.Spec.VSphere.Datacenter = "DC0"
	pool.Spec.VSphere.Folder = ""
	pool.Spec.VSphere.ISODatastore = "LocalDS_0"
	pool.Spec.VSphere.Network = "VM Network"
	pool.Spec.ISO.PathPrefix = "agent-forge/vcsim/demo-worker"
	return pool
}
