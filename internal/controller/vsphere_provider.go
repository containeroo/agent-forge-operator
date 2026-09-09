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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
)

const (
	govcCommandTimeout            = 30 * time.Minute
	partialVMCleanupTimeout       = 5 * time.Minute
	isoDownloadTimeout            = 30 * time.Minute
	maxISODownloadSize      int64 = 10 << 30
	vmOwnerAnnotationPrefix       = "agent-forge.containeroo.ch/vsphere-agent-uid="
	vmSourceVsphereAgent          = "VsphereAgent"
	vmSourceDiscoveredAgent       = "DiscoveredAgent"
)

var (
	errVMNotFound          = errors.New("vSphere VM not found")
	errVMOwnershipMismatch = errors.New("vSphere VM ownership mismatch")
)

// VMCreateRequest carries per-reconcile VM creation details.
type VMCreateRequest struct {
	Name     string
	ISOPath  string
	OwnerUID string
}

// ISOEnsureResult records the ISO object that should be inserted into new VMs.
type ISOEnsureResult struct {
	Path      string
	SHA256    string
	SizeBytes int64
	Uploaded  bool
}

// VMProviderFactory builds a VMProvider from a pool and credentials Secret.
type VMProviderFactory func(context.Context, *agentforgev1alpha1.VsphereAgentPool, *corev1.Secret) (VMProvider, error)

// VMProvider abstracts vSphere VM lifecycle operations so reconciliation logic
// stays testable.
type VMProvider interface {
	EnsureISO(context.Context, *agentforgev1alpha1.VsphereAgentPool, string) (ISOEnsureResult, error)
	CreateVM(context.Context, *agentforgev1alpha1.VsphereAgentPool, VMCreateRequest) (agentforgev1alpha1.OwnedVMStatus, error)
	VMStatus(context.Context, *agentforgev1alpha1.VsphereAgentPool, string) (agentforgev1alpha1.OwnedVMStatus, error)
	DeleteVM(context.Context, *agentforgev1alpha1.VsphereAgentPool, agentforgev1alpha1.OwnedVMStatus) error
	DeleteISO(context.Context, *agentforgev1alpha1.VsphereAgentPool, string) error
}

// NewGovcVMProvider returns the default vSphere implementation. It shells out
// to govc, which is shipped in the manager image. The controller keeps all
// mutation calls behind this interface so unit tests do not depend on vSphere.
func NewGovcVMProvider(_ context.Context, _ *agentforgev1alpha1.VsphereAgentPool, secret *corev1.Secret) (VMProvider, error) {
	cfg := govcConfig{
		Server:   string(secret.Data[secretKeyServer]),
		Username: string(secret.Data[secretKeyUsername]),
		Password: string(secret.Data[secretKeyPassword]),
		Insecure: string(secret.Data[secretKeyInsecure]),
	}
	for key, value := range map[string]string{
		secretKeyServer:   cfg.Server,
		secretKeyUsername: cfg.Username,
		secretKeyPassword: cfg.Password,
	} {
		if value == "" {
			return nil, fmt.Errorf("vSphere credentials Secret %s/%s is missing key %q", secret.Namespace, secret.Name, key)
		}
	}
	if cfg.Insecure == "" {
		cfg.Insecure = "false"
	}
	insecure, err := strconv.ParseBool(cfg.Insecure)
	if err != nil {
		return nil, fmt.Errorf("vSphere credentials Secret %s/%s has invalid %q value %q: %w", secret.Namespace, secret.Name, secretKeyInsecure, cfg.Insecure, err)
	}
	cfg.Insecure = strconv.FormatBool(insecure)
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
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return agentforgev1alpha1.OwnedVMStatus{}, fmt.Errorf("VM name is required")
	}
	if req.ISOPath == "" {
		return agentforgev1alpha1.OwnedVMStatus{}, fmt.Errorf("cached ISO path is empty")
	}
	ownerUID := strings.TrimSpace(req.OwnerUID)
	if ownerUID == "" {
		return agentforgev1alpha1.OwnedVMStatus{}, fmt.Errorf("VsphereAgent owner UID is required")
	}
	ownerAnnotation := vmOwnerAnnotationPrefix + ownerUID

	args := []string{
		"vm.create",
		"-annotation", ownerAnnotation,
		"-dc", pool.Spec.VSphere.Datacenter,
		"-datastore-cluster", pool.Spec.VSphere.DatastoreCluster,
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
	}
	if pool.Spec.VSphere.DiskEagerlyScrub {
		args = append(args, "-disk.eager")
	}
	args = append(args, "-on=false", name)
	created := true
	if err := p.run(ctx, args...); err != nil {
		if isGovcVMAlreadyExists(err) {
			created = false
			existing, statusErr := p.vmDetails(ctx, pool, name)
			if statusErr != nil {
				return agentforgev1alpha1.OwnedVMStatus{}, fmt.Errorf("verify existing VM %q ownership: %w", name, statusErr)
			}
			if existing.Config.Annotation != ownerAnnotation {
				return agentforgev1alpha1.OwnedVMStatus{}, fmt.Errorf("refusing to manage existing VM %q: ownership annotation does not match VsphereAgent UID", name)
			}
		} else {
			return agentforgev1alpha1.OwnedVMStatus{}, err
		}
	}

	cleanupPartialVM := func(cause error) (agentforgev1alpha1.OwnedVMStatus, error) {
		if !created {
			return agentforgev1alpha1.OwnedVMStatus{}, cause
		}
		vm := newOwnedVMStatus(name)
		vm.OwnerUID = ownerUID
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), partialVMCleanupTimeout)
		defer cancel()
		if err := p.DeleteVM(cleanupCtx, pool, vm); err != nil {
			return agentforgev1alpha1.OwnedVMStatus{}, fmt.Errorf("%w; failed to clean up partially created VM %q: %v", cause, name, err)
		}
		return agentforgev1alpha1.OwnedVMStatus{}, cause
	}
	vmPath := vmInventoryPath(pool, name)
	if err := p.run(ctx, "vm.change", "-dc", pool.Spec.VSphere.Datacenter, "-vm", vmPath, "-e", "disk.enableUUID=TRUE"); err != nil {
		return cleanupPartialVM(err)
	}
	if err := p.ensureCDROM(ctx, pool, name); err != nil {
		return cleanupPartialVM(err)
	}
	if err := p.run(ctx, "device.cdrom.insert", "-dc", pool.Spec.VSphere.Datacenter, "-vm", vmPath, "-ds", pool.Spec.VSphere.ISODatastore, req.ISOPath); err != nil {
		return cleanupPartialVM(err)
	}
	for _, tag := range pool.Spec.VSphere.VMTags {
		if err := p.run(ctx, "tags.attach", "-dc", pool.Spec.VSphere.Datacenter, tag, vmPath); err != nil {
			if !isGovcTagAlreadyAttached(err) {
				return cleanupPartialVM(err)
			}
		}
	}
	if err := p.run(ctx, "vm.power", "-on", "-dc", pool.Spec.VSphere.Datacenter, "-vm.ipath", vmPath); err != nil {
		if !isGovcVMAlreadyPoweredOn(err) {
			return cleanupPartialVM(err)
		}
	}

	vm := newOwnedVMStatus(name)
	vm.OwnerUID = ownerUID
	if discovered, err := p.VMStatus(ctx, pool, name); err == nil {
		if discovered.OwnerUID != "" {
			vm.OwnerUID = discovered.OwnerUID
		}
		vm.BIOSUUID = discovered.BIOSUUID
		vm.MACAddress = discovered.MACAddress
	} else {
		logf.FromContext(ctx).Error(err, "failed to discover VM identity after create", "vm", name)
	}
	return vm, nil
}

func (p *govcVMProvider) ensureCDROM(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, name string) error {
	vmPath := vmInventoryPath(pool, name)
	output, err := p.runOutput(ctx, "device.ls", "-dc", pool.Spec.VSphere.Datacenter, "-vm", vmPath)
	if err != nil {
		return err
	}
	if hasCDROMDevice(output) {
		return nil
	}
	if err := p.run(ctx, "device.cdrom.add", "-dc", pool.Spec.VSphere.Datacenter, "-vm", vmPath); err != nil && !isGovcDeviceAlreadyExists(err) {
		return err
	}
	return nil
}

func hasCDROMDevice(output []byte) bool {
	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "cdrom-") {
			return true
		}
	}
	return false
}

func (p *govcVMProvider) EnsureISO(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, downloadURL string) (ISOEnsureResult, error) {
	if downloadURL == "" {
		return ISOEnsureResult{}, fmt.Errorf("InfraEnv ISO download URL is empty")
	}

	tmpDir, err := os.MkdirTemp("", "agent-forge-iso-")
	if err != nil {
		return ISOEnsureResult{}, err
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	tmpFile := filepath.Join(tmpDir, "discovery.iso")
	sha, sizeBytes, err := downloadFileWithSHA256(ctx, downloadURL, tmpFile)
	if err != nil {
		return ISOEnsureResult{}, err
	}
	isoPath := isoContentPath(pool, sha)

	valid, err := p.verifyISO(ctx, pool, isoPath, filepath.Join(tmpDir, "cached.iso"), sha)
	if err != nil {
		return ISOEnsureResult{}, err
	}
	if valid {
		return ISOEnsureResult{Path: isoPath, SHA256: sha, SizeBytes: sizeBytes}, nil
	}
	if err := p.ensureDatastoreDirectory(ctx, pool, path.Dir(isoPath)); err != nil {
		return ISOEnsureResult{}, err
	}
	stagingPath := isoPath + "." + filepath.Base(tmpDir) + ".partial"
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), partialVMCleanupTimeout)
		defer cancel()
		_ = p.DeleteISO(cleanupCtx, pool, stagingPath)
	}()
	if err := p.run(ctx, "datastore.upload", "-dc", pool.Spec.VSphere.Datacenter, "-ds", pool.Spec.VSphere.ISODatastore, tmpFile, stagingPath); err != nil {
		return ISOEnsureResult{}, err
	}
	valid, err = p.verifyISO(ctx, pool, stagingPath, filepath.Join(tmpDir, "uploaded.iso"), sha)
	if err != nil {
		return ISOEnsureResult{}, err
	}
	if !valid {
		return ISOEnsureResult{}, fmt.Errorf("uploaded ISO failed content verification")
	}
	if err := p.run(ctx, "datastore.mv", "-f=true", "-dc", pool.Spec.VSphere.Datacenter, "-ds", pool.Spec.VSphere.ISODatastore, stagingPath, isoPath); err != nil {
		return ISOEnsureResult{}, err
	}

	return ISOEnsureResult{Path: isoPath, SHA256: sha, SizeBytes: sizeBytes, Uploaded: true}, nil
}

func (p *govcVMProvider) verifyISO(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, remote, local, digest string) (bool, error) {
	if err := p.run(ctx, "datastore.download", "-dc", pool.Spec.VSphere.Datacenter, "-ds", pool.Spec.VSphere.ISODatastore, remote, local); err != nil {
		if isGovcDatastorePathNotFound(err) {
			return false, nil
		}
		return false, err
	}
	file, err := os.Open(local)
	if err != nil {
		return false, err
	}
	defer func() { _ = file.Close() }()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return false, err
	}
	return hex.EncodeToString(hash.Sum(nil)) == digest, nil
}

func downloadURLHash(rawURL string) string {
	sum := sha256.Sum256([]byte(rawURL))
	return hex.EncodeToString(sum[:])
}

func (p *govcVMProvider) ensureDatastoreDirectory(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, dir string) error {
	dir = strings.Trim(path.Clean("/"+strings.TrimSpace(dir)), "/")
	if dir == "" || dir == "." {
		return nil
	}

	var current string
	for _, part := range strings.Split(dir, "/") {
		if part == "" {
			continue
		}
		current = path.Join(current, part)
		if err := p.run(ctx, "datastore.mkdir", "-dc", pool.Spec.VSphere.Datacenter, "-ds", pool.Spec.VSphere.ISODatastore, current); err != nil && !isGovcDatastorePathAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func (p *govcVMProvider) DeleteVM(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, vm agentforgev1alpha1.OwnedVMStatus) error {
	if strings.TrimSpace(vm.Name) == "" {
		return fmt.Errorf("cannot delete VM with empty name")
	}
	if vm.OwnerUID != "" {
		var discovered govcVirtualMachine
		var err error
		if vm.BIOSUUID != "" {
			discovered, err = p.vmDetailsByUUID(ctx, pool, vm.BIOSUUID)

		} else {
			discovered, err = p.vmDetails(ctx, pool, vm.Name)
		}
		if errors.Is(err, errVMNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("verify VM %q ownership before delete: %w", vm.Name, err)
		}
		if discovered.Config.Annotation != vmOwnerAnnotationPrefix+vm.OwnerUID {
			return fmt.Errorf("refusing to delete VM %q: ownership annotation does not match VsphereAgent UID", vm.Name)
		}
		if discoveredUUID := normalizeVMwareSerialUUID(discovered.Config.UUID); discoveredUUID != "" {
			vm.BIOSUUID = discoveredUUID
		}
	}
	if strings.TrimSpace(vm.BIOSUUID) != "" {
		err := p.run(ctx, "vm.destroy", "-dc", pool.Spec.VSphere.Datacenter, "-vm.uuid", vm.BIOSUUID)
		if err == nil {
			return nil
		}
		if isGovcVMNotFound(err) {
			return nil
		}
		return err
	}
	err := p.run(ctx, "vm.destroy", "-dc", pool.Spec.VSphere.Datacenter, "-vm.ipath", vmInventoryPath(pool, vm.Name))
	if isGovcVMNotFound(err) {
		return nil
	}
	return err
}

func (p *govcVMProvider) DeleteISO(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, isoPath string) error {
	if strings.TrimSpace(isoPath) == "" {
		return nil
	}
	err := p.run(ctx, "datastore.rm", "-f", "-dc", pool.Spec.VSphere.Datacenter, "-ds", pool.Spec.VSphere.ISODatastore, isoPath)
	if isGovcDatastorePathNotFound(err) {
		return nil
	}
	return err
}

func (p *govcVMProvider) datastorePathExists(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, isoPath string) (bool, error) {
	err := p.run(ctx, "datastore.ls", "-dc", pool.Spec.VSphere.Datacenter, "-ds", pool.Spec.VSphere.ISODatastore, isoPath)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (p *govcVMProvider) run(ctx context.Context, args ...string) error {
	_, err := p.runOutput(ctx, args...)
	return err
}

func (p *govcVMProvider) runOutput(ctx context.Context, args ...string) ([]byte, error) {
	commandCtx, cancel := contextWithDefaultTimeout(ctx, govcCommandTimeout)
	defer cancel()

	cmd := exec.CommandContext(commandCtx, p.command, args...)
	cmd.Env = append(os.Environ(),
		"HOME=/tmp",
		"GOVC_PERSIST_SESSION=false",
		"GOVC_URL="+p.config.Server,
		"GOVC_USERNAME="+p.config.Username,
		"GOVC_PASSWORD="+p.config.Password,
		"GOVC_INSECURE="+p.config.Insecure,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if commandCtx.Err() != nil {
			return nil, fmt.Errorf("govc %s failed: %w", strings.Join(args, " "), commandCtx.Err())
		}
		return nil, fmt.Errorf("govc %s failed: %w: %s", strings.Join(args, " "), err, sanitizeCommandOutput(string(output)))
	}
	return output, nil
}

func contextWithDefaultTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok || timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

var ansiEscapePattern = regexp.MustCompile(`\x1b\[[0-9;?]*[ -/]*[@-~]`)

func sanitizeCommandOutput(output string) string {
	cleaned := ansiEscapePattern.ReplaceAllString(output, "")
	cleaned = strings.ReplaceAll(cleaned, "\r", "\n")

	var lines []string
	for _, line := range strings.Split(cleaned, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.Contains(line, "Uploading...") {
			continue
		}
		lines = append(lines, line)
	}
	if len(lines) == 0 {
		return strings.TrimSpace(cleaned)
	}
	if len(lines) > 3 {
		lines = lines[len(lines)-3:]
	}
	return strings.Join(lines, "; ")
}

func isGovcVMNotFound(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "no such vm") || strings.Contains(message, "vm ") && strings.Contains(message, " not found")
}

func isGovcVMAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "already exists") ||
		strings.Contains(message, "duplicatename") ||
		strings.Contains(message, "duplicate name")
}

func isGovcDeviceAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "already exists") ||
		strings.Contains(message, "already present") ||
		strings.Contains(message, "device") && strings.Contains(message, "exists")
}

func isGovcTagAlreadyAttached(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "already attached") ||
		strings.Contains(message, "already exists")
}

func isGovcVMAlreadyPoweredOn(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "already powered on") ||
		strings.Contains(message, "current state") && strings.Contains(message, "poweredon")
}

func isGovcDatastorePathNotFound(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "no such file") ||
		strings.Contains(message, "filenotfound") ||
		strings.Contains(message, "not found") ||
		strings.Contains(message, "no such object")
}

func isGovcDatastorePathAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "already exists") ||
		strings.Contains(message, "file exists")
}

func downloadFileWithSHA256(ctx context.Context, rawURL, path string) (string, int64, error) {
	downloadCtx, cancel := contextWithDefaultTimeout(ctx, isoDownloadTimeout)
	defer cancel()
	safeURL := redactedDownloadURL(rawURL)

	req, err := http.NewRequestWithContext(downloadCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", 0, fmt.Errorf("invalid ISO download URL %q", safeURL)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if downloadCtx.Err() != nil {
			return "", 0, fmt.Errorf("download %s failed: %w", safeURL, downloadCtx.Err())
		}
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return "", 0, fmt.Errorf("download %s failed: %w", safeURL, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", 0, fmt.Errorf("download %s returned HTTP %d", safeURL, resp.StatusCode)
	}
	if resp.ContentLength > maxISODownloadSize {
		return "", 0, fmt.Errorf("download %s is %d bytes, exceeding the %d-byte limit", safeURL, resp.ContentLength, maxISODownloadSize)
	}
	out, err := os.Create(path)
	if err != nil {
		return "", 0, err
	}
	defer func() {
		_ = out.Close()
	}()
	hash := sha256.New()
	sizeBytes, err := io.Copy(io.MultiWriter(out, hash), io.LimitReader(resp.Body, maxISODownloadSize+1))
	if err != nil {
		if downloadCtx.Err() != nil {
			return "", 0, fmt.Errorf("download %s failed: %w", safeURL, downloadCtx.Err())
		}
		return "", 0, err
	}
	if sizeBytes > maxISODownloadSize {
		return "", 0, fmt.Errorf("download %s exceeds the %d-byte limit", safeURL, maxISODownloadSize)
	}
	return hex.EncodeToString(hash.Sum(nil)), sizeBytes, nil
}

func redactedDownloadURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "<invalid-url>"
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	return parsed.String()
}

func newOwnedVMStatus(name string) agentforgev1alpha1.OwnedVMStatus {
	return agentforgev1alpha1.OwnedVMStatus{
		Name:               name,
		Source:             vmSourceVsphereAgent,
		Phase:              phaseProvisioning,
		Reason:             reasonVMCreateRequested,
		LastTransitionTime: metav1.Now(),
	}
}

func (p *govcVMProvider) VMStatus(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, name string) (agentforgev1alpha1.OwnedVMStatus, error) {
	vm, err := p.vmDetails(ctx, pool, name)
	if err != nil {
		return agentforgev1alpha1.OwnedVMStatus{}, err
	}
	status := newOwnedVMStatus(name)
	if strings.HasPrefix(vm.Config.Annotation, vmOwnerAnnotationPrefix) {
		status.OwnerUID = strings.TrimPrefix(vm.Config.Annotation, vmOwnerAnnotationPrefix)
	}
	status.BIOSUUID = normalizeVMwareSerialUUID(vm.Config.UUID)
	for _, device := range vm.Config.Hardware.Device {
		if strings.TrimSpace(device.MACAddress) == "" {
			continue
		}
		status.MACAddress = normalizeMAC(device.MACAddress)
		break
	}
	return status, nil
}

func (p *govcVMProvider) vmDetails(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, name string) (govcVirtualMachine, error) {
	return p.vmDetailsWithSelector(ctx, pool, "-vm.ipath", vmInventoryPath(pool, name), name)
}

func (p *govcVMProvider) vmDetailsByUUID(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, uuid string) (govcVirtualMachine, error) {
	return p.vmDetailsWithSelector(ctx, pool, "-vm.uuid", uuid, uuid)
}

func (p *govcVMProvider) vmDetailsWithSelector(ctx context.Context, pool *agentforgev1alpha1.VsphereAgentPool, selector, value, identity string) (govcVirtualMachine, error) {
	output, err := p.runOutput(ctx, "vm.info", "-json", "-dc", pool.Spec.VSphere.Datacenter, selector, value)
	if err != nil {
		if isGovcVMNotFound(err) {
			return govcVirtualMachine{}, fmt.Errorf("%w: %v", errVMNotFound, err)
		}
		return govcVirtualMachine{}, err
	}
	var info govcVMInfo
	if err := json.Unmarshal(output, &info); err != nil {
		return govcVirtualMachine{}, err
	}
	if len(info.VirtualMachines) == 0 {
		return govcVirtualMachine{}, fmt.Errorf("%w: vm.info returned no VM for %s", errVMNotFound, identity)
	}
	return info.VirtualMachines[0], nil
}

type govcVMInfo struct {
	VirtualMachines []govcVirtualMachine `json:"virtualMachines"`
}

type govcVirtualMachine struct {
	Config govcVMConfig `json:"config"`
}

type govcVMConfig struct {
	UUID       string       `json:"uuid"`
	Annotation string       `json:"annotation"`
	Hardware   govcHardware `json:"hardware"`
}

type govcHardware struct {
	Device []govcDevice `json:"device"`
}

type govcDevice struct {
	MACAddress string `json:"macAddress"`
}

func vmFolder(pool *agentforgev1alpha1.VsphereAgentPool) string {
	if pool.Spec.VSphere.Folder != "" {
		return pool.Spec.VSphere.Folder
	}
	return pool.Spec.HostedClusterRef.Name
}

func vmInventoryPath(pool *agentforgev1alpha1.VsphereAgentPool, name string) string {
	folder := strings.Trim(vmFolder(pool), "/")
	if folder == "" {
		return fmt.Sprintf("/%s/vm/%s", pool.Spec.VSphere.Datacenter, name)
	}
	return fmt.Sprintf("/%s/vm/%s/%s", pool.Spec.VSphere.Datacenter, folder, name)
}

func isoContentPath(pool *agentforgev1alpha1.VsphereAgentPool, sha string) string {
	return fmt.Sprintf("%s/%s.iso", isoPathPrefix(pool), sha)
}

func isoPathPrefix(pool *agentforgev1alpha1.VsphereAgentPool) string {
	prefix := strings.Trim(strings.TrimSpace(pool.Spec.ISO.PathPrefix), "/")
	if prefix == "" {
		prefix = fmt.Sprintf("agent-forge/%s/%s", pool.Namespace, pool.Name)
	}
	prefix = strings.Trim(path.Clean("/"+prefix), "/")
	if prefix == "" {
		return fmt.Sprintf("agent-forge/%s/%s", pool.Namespace, pool.Name)
	}
	return prefix
}
