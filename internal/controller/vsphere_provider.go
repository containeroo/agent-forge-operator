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

package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
)

// VMCreateRequest carries per-reconcile VM creation details. Ordinal is only
// unique within one reconciliation; providers still create collision-safe names.
type VMCreateRequest struct {
	Ordinal        int32
	ISODownloadURL string
}

// VMProviderFactory builds a VMProvider from a pool and credentials Secret.
type VMProviderFactory func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error)

// VMProvider abstracts vSphere VM lifecycle operations so reconciliation logic
// stays testable.
type VMProvider interface {
	CreateVM(context.Context, *agentforgev1alpha1.VsphereAgentPool, VMCreateRequest) (agentforgev1alpha1.OwnedVMStatus, error)
	DeleteVM(context.Context, *agentforgev1alpha1.VsphereAgentPool, agentforgev1alpha1.OwnedVMStatus) error
}

// NewGovcVMProvider returns the default vSphere implementation. It shells out
// to govc, which is shipped in the manager image. The controller keeps all
// mutation calls behind this interface so dry-run planning and unit tests do
// not depend on vSphere.
func NewGovcVMProvider(_ context.Context, _ *agentforgev1alpha1.VsphereAgentPool, secret *corev1.Secret) (VMProvider, error) {
	cfg := govcConfig{
		Server:   string(secret.Data["server"]),
		Username: string(secret.Data["username"]),
		Password: string(secret.Data["password"]),
		Insecure: string(secret.Data["insecure"]),
	}
	for key, value := range map[string]string{
		"server":   cfg.Server,
		"username": cfg.Username,
		"password": cfg.Password,
	} {
		if value == "" {
			return nil, fmt.Errorf("vSphere credentials Secret %s/%s is missing key %q", secret.Namespace, secret.Name, key)
		}
	}
	if cfg.Insecure == "" {
		cfg.Insecure = "false"
	}
	command := os.Getenv("GOVC_PATH")
	if command == "" {
		command = "/usr/local/bin/govc"
	}
	return &govcVMProvider{config: cfg, command: command}, nil
}

type govcConfig struct {
	Server   string
	Username string
	Password string
	Insecure string
}

type govcVMProvider struct {
	config  govcConfig
	command string
}

func (p *govcVMProvider) CreateVM(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, req VMCreateRequest) (agentforgev1alpha1.OwnedVMStatus, error) {
	name := fmt.Sprintf("%s-%s", vmNamePrefix(pool), randomHex(3))
	isoPath := isoPath(pool)

	if err := p.ensureISO(ctx, pool, req.ISODownloadURL, isoPath); err != nil {
		return agentforgev1alpha1.OwnedVMStatus{}, err
	}

	args := []string{
		"vm.create",
		"-dc", pool.Spec.VSphere.Datacenter,
		"-ds", pool.Spec.VSphere.DatastoreCluster,
		"-pool", pool.Spec.VSphere.ResourcePool,
		"-folder", vmFolder(pool),
		"-net", pool.Spec.VSphere.Network,
		"-net.adapter", pool.Spec.VSphere.NetworkAdapterType,
		"-g", pool.Spec.VSphere.GuestID,
		"-firmware", pool.Spec.VSphere.Firmware,
		"-c", strconv.Itoa(int(pool.Spec.Template.NumCPUs)),
		"-m", strconv.Itoa(int(pool.Spec.Template.MemoryMiB)),
		"-disk", fmt.Sprintf("%dG", pool.Spec.Template.DiskGiB),
		"-disk.controller", pool.Spec.VSphere.SCSIType,
		"-on=false",
		name,
	}
	if err := p.run(ctx, args...); err != nil {
		return agentforgev1alpha1.OwnedVMStatus{}, err
	}

	_ = p.run(ctx, "vm.change", "-vm", name, "-e", "disk.enableUUID=TRUE")
	_ = p.run(ctx, "device.cdrom.add", "-vm", name)
	if err := p.run(ctx, "device.cdrom.insert", "-vm", name, "-ds", pool.Spec.VSphere.ISODatastore, isoPath); err != nil {
		return agentforgev1alpha1.OwnedVMStatus{}, err
	}
	for _, tag := range pool.Spec.VSphere.VMTags {
		_ = p.run(ctx, "tags.attach", tag, name)
	}
	if err := p.run(ctx, "vm.power", "-on", name); err != nil {
		return agentforgev1alpha1.OwnedVMStatus{}, err
	}

	return newOwnedVMStatus(name), nil
}

func (p *govcVMProvider) DeleteVM(ctx context.Context, _ *agentforgev1alpha1.VsphereAgentPool, vm agentforgev1alpha1.OwnedVMStatus) error {
	if strings.TrimSpace(vm.Name) == "" {
		return fmt.Errorf("cannot delete VM with empty name")
	}
	return p.run(ctx, "vm.destroy", vm.Name)
}

func (p *govcVMProvider) ensureISO(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, isoURL, isoPath string) error {
	if isoURL == "" {
		return fmt.Errorf("InfraEnv ISO download URL is empty")
	}

	tmpDir, err := os.MkdirTemp("", "agent-forge-iso-")
	if err != nil {
		return err
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	tmpFile := filepath.Join(tmpDir, filepath.Base(isoPath))
	if err := downloadFile(ctx, isoURL, tmpFile); err != nil {
		return err
	}
	if dir := filepath.Dir(isoPath); dir != "." && dir != "" {
		_ = p.run(ctx, "datastore.mkdir", "-dc", pool.Spec.VSphere.Datacenter, "-ds", pool.Spec.VSphere.ISODatastore, dir)
	}
	return p.run(ctx, "datastore.upload", "-f", "-dc", pool.Spec.VSphere.Datacenter, "-ds", pool.Spec.VSphere.ISODatastore, tmpFile, isoPath)
}

func (p *govcVMProvider) run(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, p.command, args...)
	cmd.Env = append(os.Environ(),
		"GOVC_URL="+p.config.Server,
		"GOVC_USERNAME="+p.config.Username,
		"GOVC_PASSWORD="+p.config.Password,
		"GOVC_INSECURE="+p.config.Insecure,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("govc %s failed: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

func downloadFile(ctx context.Context, url, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("download %s returned HTTP %d", url, resp.StatusCode)
	}
	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = out.Close()
	}()
	_, err = io.Copy(out, resp.Body)
	return err
}

func newOwnedVMStatus(name string) agentforgev1alpha1.OwnedVMStatus {
	return agentforgev1alpha1.OwnedVMStatus{
		Name:               name,
		Phase:              "Provisioning",
		Reason:             "CreateRequested",
		LastTransitionTime: metav1.Now(),
	}
}

func vmNamePrefix(pool *agentforgev1alpha1.VsphereAgentPool) string {
	if pool.Spec.Template.NamePrefix != "" {
		return pool.Spec.Template.NamePrefix
	}
	return fmt.Sprintf("%s-%s", pool.Spec.HostedClusterRef.Name, pool.Spec.Agent.Role)
}

func vmFolder(pool *agentforgev1alpha1.VsphereAgentPool) string {
	if pool.Spec.VSphere.Folder != "" {
		return pool.Spec.VSphere.Folder
	}
	return pool.Spec.HostedClusterRef.Name
}

func isoPath(pool *agentforgev1alpha1.VsphereAgentPool) string {
	if pool.Spec.VSphere.ISOPath != "" {
		return pool.Spec.VSphere.ISOPath
	}
	return fmt.Sprintf("iso/%s-discovery.iso", pool.Spec.InfraEnvRef.Name)
}

func randomHex(bytes int) string {
	value := make([]byte, bytes)
	if _, err := rand.Read(value); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(value)
}
