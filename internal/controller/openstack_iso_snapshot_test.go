//nolint:goconst // Synthetic fixtures keep cache ownership and failure cases explicit.
package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	api "github.com/containeroo/agent-forge-operator/api/v1alpha1"
	osapi "github.com/containeroo/agent-forge-operator/internal/openstack"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type snapshotProvider struct {
	*fakeOpenStack
	fixture    *openStackFixture
	failUpload string
	paths      []string
}

func (p *snapshotProvider) Create(ctx context.Context, kind osapi.Kind, name, owner string, a *api.OpenStackAgent) (*osapi.Resource, error) {
	if kind == osapi.Image {
		current := p.fixture.current()
		if current.Status.ImageID != a.Status.ImageID || current.Status.ImageCacheName != a.Status.ImageCacheName || current.Status.ISOSHA256 != a.Status.ISOSHA256 {
			p.fixture.t.Fatal("image creation preceded durable identity")
		}
	}
	return p.fakeOpenStack.Create(ctx, kind, name, owner, a)
}

func (p *snapshotProvider) UploadImage(ctx context.Context, id, owner string, data io.Reader) error {
	p.fixture.t.Helper()
	body, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	current := p.fixture.current()
	sum := sha256.Sum256(body)
	if current.Status.ImageID != id || current.Status.ISOSHA256 != hex.EncodeToString(sum[:]) || current.Status.ISOSizeBytes != int64(len(body)) {
		p.fixture.t.Fatal("upload differs from the durable image identity")
	}
	if file, ok := data.(*os.File); ok {
		p.paths = append(p.paths, file.Name())
	}
	failure := p.failUpload
	p.failUpload = ""
	if failure == "queued" {
		return &osapi.Error{Reason: "OpenStackRequestFailed", Retryable: true}
	}
	if err := p.fakeOpenStack.UploadImage(ctx, id, owner, bytes.NewReader(body)); err != nil {
		return err
	}
	if failure == "saving" {
		p.resources[osapi.Image][id].State = "saving"
	}
	if failure != "" {
		return &osapi.Error{Reason: "OpenStackRequestFailed", Retryable: true}
	}
	return nil
}

func changingDiscoverySource(t *testing.T, f *openStackFixture) *atomic.Int64 {
	t.Helper()
	requests := &atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, "synthetic-discovery-snapshot-%d", requests.Add(1))
	}))
	t.Cleanup(server.Close)
	if err := unstructured.SetNestedField(f.infra.Object, server.URL, "status", "isoDownloadURL"); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Update(context.Background(), f.infra); err != nil {
		t.Fatal(err)
	}
	return requests
}

func snapshotStep(f *openStackFixture, p osapi.Client) error {
	// Construct a fresh reconciler on every step to exercise restart recovery.
	r := f.reconciler()
	r.ProviderFactory = func(context.Context, api.OpenStackPlacementSpec, *corev1.Secret) (osapi.Client, error) { return p, nil }
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.a)})
	return err
}

func finishSnapshotProvisioning(t *testing.T, f *openStackFixture, p osapi.Client) *api.OpenStackAgent {
	t.Helper()
	for i := 0; i < 30; i++ {
		if err := snapshotStep(f, p); err != nil {
			t.Fatal(err)
		}
		if a := f.current(); a.Status.ServerID != "" {
			return a
		}
	}
	t.Fatalf("worker stalled: %#v", f.current().Status)
	return nil
}

// Seed the exact pre-upload state left by v1.3.1, with a port and bootable OS
// volume already present and a pinned queued Glance image for older bytes.
func legacyQueuedDiscovery(t *testing.T, f *openStackFixture) *api.OpenStackAgent {
	t.Helper()
	for i := 0; i < 10; i++ {
		if err := f.step(); err != nil {
			t.Fatal(err)
		}
		a := f.current()
		if volume := f.p.resources[osapi.OSVolume][a.Status.OSVolumeID]; volume != nil && volume.Bootable {
			break
		}
	}
	a := f.current()
	if a.Status.OSVolumeID == "" || !f.p.resources[osapi.OSVolume][a.Status.OSVolumeID].Bootable {
		t.Fatal("OS volume not prepared")
	}
	old := []byte("synthetic-old-iso")
	sum := sha256.Sum256(old)
	a.Status.ISOSHA256 = hex.EncodeToString(sum[:])
	a.Status.ISOSizeBytes = int64(len(old))
	a.Status.InfraEnvUID = string(f.infra.GetUID())
	a.Status.ImageCacheName = imageCacheKey(f.p.Scope(), a.Status.InfraEnvUID, a.Status.ISOSHA256)
	cache, err := f.reconciler().acquireCache(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	a.Status.ImageID = imageIDForCache(cache)
	if _, err := f.p.Create(context.Background(), osapi.Image, a.Status.ImageCacheName, string(cache.UID), a); err != nil {
		t.Fatal(err)
	}
	a.Status.Phase = "Failed"
	meta.SetStatusCondition(&a.Status.Conditions, metav1.Condition{Type: conditionReady, Status: metav1.ConditionFalse, Reason: "DiscoveryImageChangedBeforeUpload", Message: "DiscoveryImageChangedBeforeUpload"})
	if err := f.c.Status().Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	return a.DeepCopy()
}

func TestOpenStackUploadsTheHashedSnapshotOnce(t *testing.T) {
	f := newOpenStackFixture(t)
	requests := changingDiscoverySource(t, f)
	p := &snapshotProvider{fakeOpenStack: f.p, fixture: f}
	a := finishSnapshotProvisioning(t, f, p)
	if requests.Load() != 1 || p.uploads != 1 {
		t.Fatalf("downloads=%d uploads=%d", requests.Load(), p.uploads)
	}
	if a.Status.DiscoveryVolumeID == "" || len(p.paths) != 1 {
		t.Fatal("discovery media not provisioned")
	}
	for _, path := range p.paths {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("temporary ISO retained after upload")
		}
	}
}

func TestOpenStackRecoversLegacyChangedImagePreservingDisks(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(fmt.Sprint(shared), func(t *testing.T) {
			f := newOpenStackFixture(t)
			old := legacyQueuedDiscovery(t, f)
			if shared {
				b := old.DeepCopy()
				b.UID = "example-other-worker"
				if _, err := f.reconciler().acquireCache(context.Background(), b); err != nil {
					t.Fatal(err)
				}
			}
			requests := changingDiscoverySource(t, f)
			p := &snapshotProvider{fakeOpenStack: f.p, fixture: f}
			a := finishSnapshotProvisioning(t, f, p)
			if a.Status.OSVolumeID != old.Status.OSVolumeID || a.Status.PortID != old.Status.PortID || p.creates[osapi.OSVolume] != 1 || p.creates[osapi.Port] != 1 {
				t.Fatal("recovery replaced existing infrastructure")
			}
			if a.Status.ImageID == old.Status.ImageID || a.Status.ISOSHA256 == old.Status.ISOSHA256 {
				t.Fatal("changed bytes reused old image identity")
			}
			if p.uploads != 1 || requests.Load() != 2 {
				t.Fatalf("recovery failed to use a single new snapshot: downloads=%d uploads=%d", requests.Load(), p.uploads)
			}
			for _, kind := range p.deleted {
				if kind != osapi.Image {
					t.Fatalf("recovery deleted %s", kind)
				}
			}
			_, exists := p.resources[osapi.Image][old.Status.ImageID]
			if exists != shared {
				t.Fatal("incorrect shared image collection")
			}
			cache := &corev1.ConfigMap{}
			err := f.c.Get(context.Background(), client.ObjectKey{Namespace: a.Namespace, Name: old.Status.ImageCacheName}, cache)
			if shared {
				if err != nil || len(cache.Data) != 1 || cache.Data["example-other-worker"] != "reference" {
					t.Fatal("other consumer's reference changed")
				}
			} else if !apierrors.IsNotFound(err) {
				t.Fatal("obsolete cache not collected")
			}
		})
	}
}

func TestOpenStackUploadFailureRecoveryObservesGlance(t *testing.T) {
	for _, state := range []string{"queued", "saving", "active"} {
		t.Run(state, func(t *testing.T) {
			f := newOpenStackFixture(t)
			requests := changingDiscoverySource(t, f)
			p := &snapshotProvider{fakeOpenStack: f.p, fixture: f, failUpload: state}
			failed := false
			for i := 0; i < 15; i++ {
				if err := snapshotStep(f, p); err != nil {
					failed = true
					break
				}
			}
			if !failed {
				t.Fatal("upload failure not reached")
			}
			before := f.current()
			if state == "saving" {
				for i := 0; i < 3; i++ {
					if err := snapshotStep(f, p); err != nil {
						t.Fatal(err)
					}
				}
				if requests.Load() != 1 || len(p.deleted) != 0 {
					t.Fatal("in-flight upload refreshed")
				}
				p.resources[osapi.Image][before.Status.ImageID].State = "active"
			}
			a := finishSnapshotProvisioning(t, f, p)
			if a.Status.OSVolumeID != before.Status.OSVolumeID {
				t.Fatal("OS disk replaced")
			}
			if state == "queued" {
				if a.Status.ImageID == before.Status.ImageID || requests.Load() != 3 {
					t.Fatal("failed queued upload not safely repinned")
				}
			} else if a.Status.ImageID != before.Status.ImageID || requests.Load() != 1 {
				t.Fatal("successful or in-flight upload was replaced")
			}
		})
	}
}

func TestOpenStackRefreshJournalSurvivesCleanupFailure(t *testing.T) {
	f := newOpenStackFixture(t)
	old := legacyQueuedDiscovery(t, f)
	changingDiscoverySource(t, f)
	if err := f.step(); err != nil {
		t.Fatal(err)
	} // checkpoint RefreshingDiscoveryImage
	if f.current().Status.Phase != openStackPhaseRefreshingImage {
		t.Fatal("refresh journal missing")
	}
	if err := f.step(); err != nil {
		t.Fatal(err)
	} // tombstone
	f.p.failDelete = osapi.Image
	if err := f.step(); err == nil {
		t.Fatal("cleanup failure hidden")
	}
	if a := f.current(); a.Status.Phase != openStackPhaseRefreshingImage || a.Status.ImageID != old.Status.ImageID {
		t.Fatal("cleanup failure lost recovery journal")
	}
	// A permanent ownership error must preserve the journal too.
	f.p.failDelete = ""
	image := f.p.resources[osapi.Image][old.Status.ImageID]
	owner := image.Owner
	image.Owner = "example-foreign-owner"
	if err := f.step(); err != nil {
		t.Fatal(err)
	}
	if f.current().Status.Phase != openStackPhaseRefreshingImage {
		t.Fatal("permanent error overwrote recovery journal")
	}
	image.Owner = owner
	finishSnapshotProvisioning(t, f, f.p)
}

func TestOpenStackRefreshNeverTouchesProvisionedOrAmbiguousMedia(t *testing.T) {
	for _, field := range []string{"server", "volume", "pending-volume", "pending-server", "agent", "created"} {
		t.Run(field, func(t *testing.T) {
			f := newOpenStackFixture(t)
			a := legacyQueuedDiscovery(t, f)
			a.Status.Phase = openStackPhaseRefreshingImage
			switch field {
			case "server":
				a.Status.ServerID = "example-server"
			case "volume":
				a.Status.DiscoveryVolumeID = "example-volume"
			case "pending-volume":
				a.Status.PendingCreate = string(osapi.DiscoveryVolume)
			case "pending-server":
				a.Status.PendingCreate = string(osapi.Server)
			case "agent":
				a.Status.AgentRef = &corev1.ObjectReference{Name: "example-agent"}
			case "created":
				now := metav1.Now()
				a.Status.InstanceCreatedAt = &now
			}
			if err := f.c.Status().Update(context.Background(), a); err != nil {
				t.Fatal(err)
			}
			if err := f.step(); err != nil {
				t.Fatal(err)
			}
			condition := meta.FindStatusCondition(f.current().Status.Conditions, conditionReady)
			if condition == nil || condition.Reason != "DiscoveryImageRefreshBlocked" || len(f.p.deleted) != 0 || f.p.uploads != 0 {
				t.Fatal("unsafe image refresh")
			}
		})
	}
}

func TestOpenStackRefreshAfterCacheRecreatedDoesNotDeleteNewCache(t *testing.T) {
	f := newOpenStackFixture(t)
	old := legacyQueuedDiscovery(t, f)
	r := f.reconciler()
	ctx := context.Background()
	// Emulate a crash after release/GC completed but before the worker's status
	// was cleared, followed by another worker acquiring the same content key.
	for i := 0; i < 4; i++ {
		if _, err := r.releaseDiscoveryImage(ctx, f.p, old); err != nil {
			t.Fatal(err)
		}
	}
	other := old.DeepCopy()
	other.UID = "example-new-consumer"
	other.Status.ImageID = ""
	cache, err := r.acquireCache(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	other.Status.ImageID = imageIDForCache(cache)
	if _, err := f.p.Create(ctx, osapi.Image, other.Status.ImageCacheName, string(cache.UID), other); err != nil {
		t.Fatal(err)
	}
	old.Status.Phase = openStackPhaseRefreshingImage
	if err := f.c.Status().Update(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := f.step(); err != nil {
		t.Fatal(err)
	}
	if f.current().Status.ImageCacheName != "" {
		t.Fatal("old journal not cleared")
	}
	current := &corev1.ConfigMap{}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(cache), current); err != nil {
		t.Fatal(err)
	}
	if current.UID != cache.UID || current.Data[string(other.UID)] != "reference" || f.p.resources[osapi.Image][other.Status.ImageID] == nil {
		t.Fatal("new cache was touched")
	}
}

func TestOpenStackRestartReusesUnchangedQueuedSnapshot(t *testing.T) {
	f := newOpenStackFixture(t)
	old := legacyQueuedDiscovery(t, f)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "synthetic-old-iso") }))
	defer server.Close()
	if err := unstructured.SetNestedField(f.infra.Object, server.URL, "status", "isoDownloadURL"); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Update(context.Background(), f.infra); err != nil {
		t.Fatal(err)
	}
	p := &snapshotProvider{fakeOpenStack: f.p, fixture: f}
	a := finishSnapshotProvisioning(t, f, p)
	if a.Status.ImageID != old.Status.ImageID || p.uploads != 1 || len(p.deleted) != 0 {
		t.Fatal("unchanged snapshot was replaced")
	}
}
