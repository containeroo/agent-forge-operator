//nolint:goconst // Keep HTTP and cache fixture values readable.
package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/log"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The source can generate different signatures in otherwise unchanged ISO
// responses. Its modification date identifies the source revision, while the
// stored SHA256 continues to identify the exact bytes in the datastore.
func TestISOConditionalRevalidation(t *testing.T) {
	const original = "ISO with original token signature"
	const regenerated = "ISO with freshly generated token signature"
	modified := time.Date(2026, 9, 23, 1, 24, 34, 0, time.UTC)
	for _, tc := range []struct {
		name                                                                                                                string
		corrupt, missing, denied, changed, changedURL, changedPath, force, noValidator, invalidValidator, noServerValidator bool
		mounted, inspectDenied, otherISO, wantDeferred                                                                      bool
		wantConditional, wantReuse, wantErr                                                                                 bool
		wantRequests                                                                                                        int
	}{
		{name: "mounted unchanged ISO defers verification", mounted: true, wantDeferred: true, wantConditional: true, wantReuse: true, wantRequests: 1},
		{name: "other ISO attached still verifies", mounted: true, otherISO: true, wantConditional: true, wantReuse: true, wantRequests: 1},
		{name: "inventory failure remains error", inspectDenied: true, wantConditional: true, wantErr: true, wantRequests: 1},
		{name: "changed source with old ISO mounted", mounted: true, changed: true, wantConditional: true, wantRequests: 1},
		{name: "force refresh bypasses mounted reuse", mounted: true, force: true, wantRequests: 1},
		{name: "signature churn reuses verified bytes", wantConditional: true, wantReuse: true, wantRequests: 1},
		{name: "source configuration changed", changed: true, wantConditional: true, wantRequests: 1},
		{name: "missing cached file repaired", missing: true, wantConditional: true, wantRequests: 2},
		{name: "corrupt cached file repaired", corrupt: true, wantConditional: true, wantRequests: 2},
		{name: "datastore permission failure", denied: true, wantConditional: true, wantErr: true, wantRequests: 1},
		{name: "URL changed", changedURL: true, wantRequests: 1},
		{name: "cache prefix changed", changedPath: true, wantRequests: 1},
		{name: "explicit refresh", force: true, wantRequests: 1},
		{name: "upgrade without validator", noValidator: true, wantRequests: 1},
		{name: "invalid persisted validator", invalidValidator: true, wantRequests: 1},
		{name: "source no longer provides validator", noServerValidator: true, wantConditional: true, wantRequests: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("REVALIDATION_DIR", dir)
			script := `#!/bin/sh
printf '%s\n' "$*" >> "$REVALIDATION_DIR/calls"
case "$1" in
 vm.info)
  if test -f "$REVALIDATION_DIR/inspect-denied"; then echo 'permission denied' >&2; exit 1; fi
  cat "$REVALIDATION_DIR/vm.json";;
 datastore.download)
  if test -f "$REVALIDATION_DIR/denied"; then echo 'permission denied' >&2; exit 1; fi
  case "$6" in
   *.partial) cp "$REVALIDATION_DIR/staging" "$7";;
   *) if test -f "$REVALIDATION_DIR/cache"; then cp "$REVALIDATION_DIR/cache" "$7"; else echo 'file not found' >&2; exit 1; fi;;
  esac;;
 datastore.upload) cp "$6" "$REVALIDATION_DIR/staging";;
 datastore.mv) cp "$REVALIDATION_DIR/staging" "$REVALIDATION_DIR/cache";;
esac
`
			command := filepath.Join(dir, "govc")
			if err := os.WriteFile(command, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			cached := original
			if tc.corrupt {
				cached = "truncated"
			}
			if !tc.missing {
				if err := os.WriteFile(filepath.Join(dir, "cache"), []byte(cached), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.denied {
				if err := os.WriteFile(filepath.Join(dir, "denied"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			requests := make(chan string, 3)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- r.Header.Get("If-Modified-Since")
				date := modified
				if tc.changed {
					date = date.Add(time.Hour)
				}
				if tc.noServerValidator {
					date = time.Time{}
				}
				http.ServeContent(w, r, "discovery.iso", date, bytes.NewReader([]byte(regenerated)))
			}))
			defer server.Close()
			pool := providerTestPool()
			digest := sha256.Sum256([]byte(original))
			sha := hex.EncodeToString(digest[:])
			pool.Status.ISO = agentforgev1alpha1.ISOCacheStatus{Path: isoContentPath(pool, sha), SHA256: sha, SizeBytes: int64(len(original)), URLHash: downloadURLHash(server.URL), LastModified: modified.Format(http.TimeFormat)}
			if tc.changedURL {
				pool.Status.ISO.URLHash = downloadURLHash(server.URL + "?old-token")
			}
			if tc.changedPath {
				pool.Spec.ISO.PathPrefix = "different/cache"
			}
			if tc.force {
				pool.Annotations = map[string]string{forceISORefreshAnnotation: "new-refresh"}
			}
			if tc.noValidator {
				pool.Status.ISO.LastModified = ""
			}
			if tc.invalidValidator {
				pool.Status.ISO.LastModified = "invalid date"
			}
			configureMountedISOFixture(t, pool, dir, tc.mounted, tc.inspectDenied, tc.otherISO)
			provider := &govcVMProvider{command: command}
			result, err := provider.EnsureISO(context.Background(), pool, server.URL)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v", err)
			}
			if len(requests) != tc.wantRequests {
				t.Fatalf("requests=%d want %d", len(requests), tc.wantRequests)
			}
			first := <-requests
			if (first != "") != tc.wantConditional {
				t.Fatalf("conditional header=%q", first)
			}
			if tc.wantRequests == 2 && <-requests != "" {
				t.Fatal("repair request was conditional")
			}
			if tc.wantErr {
				return
			}
			if result.Uploaded == tc.wantReuse {
				t.Fatalf("uploaded=%v", result.Uploaded)
			}
			if tc.wantReuse {
				if result.SHA256 != sha || result.Path != pool.Status.ISO.Path || result.SizeBytes != int64(len(original)) {
					t.Fatalf("cached identity changed: %+v", result)
				}
			} else {
				expected := sha256.Sum256([]byte(regenerated))
				if result.SHA256 != hex.EncodeToString(expected[:]) || result.SizeBytes != int64(len(regenerated)) {
					t.Fatalf("download identity=%+v", result)
				}
			}
			if result.VerificationDeferred != tc.wantDeferred {
				t.Fatalf("deferred=%v", result.VerificationDeferred)
			}
			if tc.wantDeferred {
				result = resumeDeferredISOFixture(t, dir, provider, pool, server.URL)
			}
			checkRevalidationMetadata(t, result, modified, tc.changed, tc.noServerValidator, tc.wantReuse, dir)
		})
	}
}

func TestDownloadISORejectsUnsolicitedNotModified(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotModified) }))
	defer server.Close()
	if _, err := downloadISO(context.Background(), server.URL, filepath.Join(t.TempDir(), "iso"), ""); err == nil {
		t.Fatal("accepted 304 without cache validator")
	}
}

func TestISOCachePersistsValidatorWithoutRotatingHistory(t *testing.T) {
	pool := reconcileTestPool()
	old := metav1.NewTime(time.Now().Add(-time.Hour))
	pool.Status.ISO = agentforgev1alpha1.ISOCacheStatus{Path: "cache/current.iso", SHA256: "digest", SizeBytes: 123, UploadedAt: old, History: []agentforgev1alpha1.ISOCacheHistoryEntry{{Path: "cache/current.iso", SHA256: "digest", UploadedAt: old}, {Path: "cache/previous.iso", SHA256: "previous", UploadedAt: old}}}
	provider := &fakeVMProvider{isoResult: &ISOEnsureResult{Path: "cache/current.iso", SHA256: "digest", SizeBytes: 123, LastModified: "Wed, 23 Sep 2026 01:24:34 GMT"}}
	r := &VsphereAgentReconciler{}
	if _, err := r.ensureISOCache(context.Background(), pool, provider, "https://example.invalid/iso"); err != nil {
		t.Fatal(err)
	}
	if pool.Status.ISO.LastModified != provider.isoResult.LastModified {
		t.Fatal("validator not persisted")
	}
	if !pool.Status.ISO.UploadedAt.Equal(&old) {
		t.Fatal("reuse reset upload time")
	}
	if len(pool.Status.ISO.History) != 2 || pool.Status.ISO.History[1].Path != "cache/previous.iso" {
		t.Fatal("reuse rotated history")
	}
}

func checkRevalidationMetadata(t *testing.T, result ISOEnsureResult, modified time.Time, changed, noServerValidator, wantReuse bool, dir string) {
	t.Helper()
	wantDate := modified
	if changed {
		wantDate = wantDate.Add(time.Hour)
	}
	wantValidator := wantDate.Format(http.TimeFormat)
	if noServerValidator {
		wantValidator = ""
	}
	if result.LastModified != wantValidator {
		t.Fatalf("validator=%q want %q", result.LastModified, wantValidator)
	}
	calls, err := os.ReadFile(filepath.Join(dir, "calls"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(calls), "datastore.download") {
		t.Fatal("cached bytes were not verified")
	}
	if wantReuse && strings.Contains(string(calls), "datastore.upload") {
		t.Fatal("unchanged source uploaded")
	}
}

func TestDeferredISOVerificationPreservesCacheStatus(t *testing.T) {
	pool := reconcileTestPool()
	old := metav1.NewTime(time.Now().Add(-time.Hour))
	pool.Status.ISO = agentforgev1alpha1.ISOCacheStatus{Path: "cache/current.iso", SHA256: "digest", CheckedAt: old, UploadedAt: old}
	before := pool.DeepCopy().Status.ISO
	provider := &fakeVMProvider{isoResult: &ISOEnsureResult{Path: pool.Status.ISO.Path, VerificationDeferred: true}}
	r := &VsphereAgentReconciler{}
	if _, err := r.ensureISOCache(context.Background(), pool, provider, "https://example.invalid/iso"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, pool.Status.ISO) {
		t.Fatal("deferral changed cache identity or verification timestamps")
	}
	condition := meta.FindStatusCondition(pool.Status.Conditions, conditionISOReady)
	if condition == nil || condition.Reason != "VerificationDeferred" || condition.Status != metav1.ConditionTrue {
		t.Fatalf("condition=%+v", condition)
	}
	if !isoCacheDue(pool, "https://example.invalid/iso", "", time.Now()) {
		t.Fatal("verification no longer due after deferral")
	}
}

type failingISOProvider struct{ fakeVMProvider }

func (*failingISOProvider) EnsureISO(context.Context, *agentforgev1alpha1.VsphereAgentPool, string) (ISOEnsureResult, error) {
	return ISOEnsureResult{}, errors.New("verify cached ISO: permission denied")
}

func TestISOFailureRemainsObservableAfterRecovery(t *testing.T) {
	pool := reconcileTestPool()
	recorder := events.NewFakeRecorder(10)
	var logs []string
	ctx := log.IntoContext(context.Background(), funcr.New(func(_, message string) { logs = append(logs, message) }, funcr.Options{}))
	r := &VsphereAgentReconciler{Recorder: recorder}
	if _, err := r.ensureISOCache(ctx, pool, &failingISOProvider{}, "https://example.invalid/iso"); err == nil {
		t.Fatal("failure suppressed")
	}
	if _, err := r.ensureISOCache(ctx, pool, &fakeVMProvider{}, "https://example.invalid/iso"); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "Warning ISORefreshFailed") || !strings.Contains(event, "permission denied") {
			t.Fatalf("event=%s", event)
		}
	default:
		t.Fatal("no failure event recorded")
	}
	if len(logs) != 1 || !strings.Contains(logs[0], "permission denied") || !strings.Contains(logs[0], pool.Name) {
		t.Fatalf("logs=%v", logs)
	}
}

func configureMountedISOFixture(t *testing.T, pool *agentforgev1alpha1.VsphereAgentPool, dir string, mounted, inspectDenied, otherISO bool) {
	t.Helper()
	if mounted || inspectDenied {
		pool.Status.OwnedVMs = []agentforgev1alpha1.OwnedVMStatus{{Name: "installer", BIOSUUID: "vm-uuid"}}
		backing := "[" + pool.Spec.VSphere.ISODatastore + "] " + pool.Status.ISO.Path
		if otherISO {
			backing += ".other"
		}
		vm := govcVirtualMachine{}
		device := govcDevice{}
		device.Backing.FileName = backing
		vm.Config.Hardware.Device = []govcDevice{device}
		data, _ := json.Marshal(govcVMInfo{VirtualMachines: []govcVirtualMachine{vm}})
		if err := os.WriteFile(filepath.Join(dir, "vm.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
		if inspectDenied {
			if err := os.WriteFile(filepath.Join(dir, "inspect-denied"), nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func resumeDeferredISOFixture(t *testing.T, dir string, provider *govcVMProvider, pool *agentforgev1alpha1.VsphereAgentPool, url string) ISOEnsureResult {
	t.Helper()
	calls, _ := os.ReadFile(filepath.Join(dir, "calls"))
	if strings.Contains(string(calls), "datastore.") {
		t.Fatalf("accessed mounted datastore file: %s", calls)
	}
	if err := os.WriteFile(filepath.Join(dir, "vm.json"), []byte(`{"virtualMachines":[{}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	result, err := provider.EnsureISO(context.Background(), pool, url)
	if err != nil || result.VerificationDeferred {
		t.Fatalf("verification did not resume: %+v %v", result, err)
	}
	return result
}
