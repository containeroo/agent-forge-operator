//go:build openstackintegration

package openstack

import (
	"context"
	"flag"
	"strings"
	"testing"
	"time"

	api "github.com/containeroo/agent-forge-operator/api/v1alpha1"
	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/servers"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var liveEnabled = flag.Bool("openstack-live", false, "Explicitly authorize live infrastructure access")
var liveReboot = flag.Bool("allow-reboot", false, "Explicitly authorize reboot of the selected installed worker")
var managementConfig = flag.String("management-kubeconfig", "", "Explicit management kubeconfig path")
var guestConfig = flag.String("guest-kubeconfig", "", "Explicit HostedCluster kubeconfig path (read-only Node checks)")
var liveNamespace = flag.String("worker-namespace", "", "OpenStackAgent namespace")
var liveWorker = flag.String("worker-name", "", "OpenStackAgent name")
var liveNode = flag.String("node-name", "", "Installed HostedCluster Node name")

// TestLivePersistentISOReboot is deliberately excluded from normal CI. It never
// provisions resources or loads default kubeconfigs/environment credentials.
func TestLivePersistentISOReboot(t *testing.T) {
	if !*liveEnabled || !*liveReboot {
		t.Skip("requires explicit live access and reboot authorization")
	}
	for _, value := range []string{*managementConfig, *guestConfig, *liveNamespace, *liveWorker, *liveNode} {
		if value == "" {
			t.Fatal("all explicit kubeconfig and worker/node selectors are required")
		}
	}
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	management := liveKubernetesClient(t, *managementConfig, scheme)
	guest := liveKubernetesClient(t, *guestConfig, scheme)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	a := &api.OpenStackAgent{}
	if err := management.Get(ctx, client.ObjectKey{Namespace: *liveNamespace, Name: *liveWorker}, a); err != nil {
		t.Fatal("could not read selected OpenStackAgent")
	}
	if a.Status.Phase != "Ready" || a.Status.ServerID == "" || !a.DeletionTimestamp.IsZero() {
		t.Fatal("selected worker is not installed and ready")
	}
	node := &corev1.Node{}
	if err := guest.Get(ctx, client.ObjectKey{Name: *liveNode}, node); err != nil {
		t.Fatal("could not read selected Node")
	}
	if !strings.EqualFold(node.Status.NodeInfo.SystemUUID, a.Status.ServerID) {
		t.Fatal("Node system UUID does not match selected Nova server")
	}
	if !liveNodeReady(node) {
		t.Fatal("selected Node must be Ready and running RHCOS before reboot")
	}
	oldBootID, oldNodeUID := node.Status.NodeInfo.BootID, node.UID
	secret := &corev1.Secret{}
	if err := management.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: a.Spec.Configuration.OpenStack.CredentialsSecretRef.Name}, secret); err != nil {
		t.Fatal("could not read explicitly selected credentials")
	}
	provider, err := New(ctx, a.Spec.Configuration.OpenStack, secret)
	if err != nil {
		t.Fatal(err)
	}
	if provider.Scope() != a.Status.ScopeHash {
		t.Fatal("cloud scope changed")
	}
	c := provider.(*cloud)
	liveCheckAttachments(t, ctx, c, a)
	if err := servers.Reboot(ctx, c.compute, a.Status.ServerID, servers.RebootOpts{Type: servers.SoftReboot}).ExtractErr(); err != nil {
		t.Fatal(SafeError(err))
	}
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("worker did not return to RHCOS with a new boot ID within timeout")
		case <-ticker.C:
			if err := guest.Get(ctx, client.ObjectKey{Name: *liveNode}, node); err != nil {
				continue
			}
			if node.UID != oldNodeUID {
				t.Fatal("Node was replaced instead of rebooted")
			}
			if node.Status.NodeInfo.BootID == "" || node.Status.NodeInfo.BootID == oldBootID || !liveNodeReady(node) {
				continue
			}
			liveCheckAttachments(t, ctx, c, a)
			current := &api.OpenStackAgent{}
			if err := management.Get(ctx, client.ObjectKeyFromObject(a), current); err != nil {
				t.Fatal("worker disappeared")
			}
			if current.Status.ServerID != a.Status.ServerID || current.Status.OSVolumeID != a.Status.OSVolumeID || current.Status.DiscoveryVolumeID != a.Status.DiscoveryVolumeID || current.Status.PortID != a.Status.PortID {
				t.Fatal("worker identities changed across reboot")
			}
			return
		}
	}
}
func liveKubernetesClient(t *testing.T, path string, scheme *runtime.Scheme) client.Client {
	t.Helper()
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		t.Fatal("could not load explicitly supplied kubeconfig")
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal("could not create Kubernetes client")
	}
	return c
}
func liveNodeReady(n *corev1.Node) bool {
	if !strings.Contains(strings.ToLower(n.Status.NodeInfo.OSImage), "coreos") {
		return false
	}
	for _, condition := range n.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
func liveCheckAttachments(t *testing.T, ctx context.Context, c *cloud, a *api.OpenStackAgent) {
	t.Helper()
	if _, err := c.Get(ctx, Server, a.Status.ServerID, string(a.UID)); err != nil {
		t.Fatal(err)
	}
	server, err := servers.Get(ctx, c.compute, a.Status.ServerID).Extract()
	if err != nil {
		t.Fatal(SafeError(err))
	}
	attached := map[string]bool{}
	for _, volume := range server.AttachedVolumes {
		attached[volume.ID] = true
	}
	if !attached[a.Status.OSVolumeID] || !attached[a.Status.DiscoveryVolumeID] {
		t.Fatal("both persistent volumes must remain attached")
	}
	for kind, id := range map[Kind]string{OSVolume: a.Status.OSVolumeID, DiscoveryVolume: a.Status.DiscoveryVolumeID} {
		volume, err := c.Get(ctx, kind, id, string(a.UID))
		if err != nil {
			t.Fatal(err)
		}
		if !volume.Attached {
			t.Fatal("Cinder did not confirm attachment")
		}
	}
}
