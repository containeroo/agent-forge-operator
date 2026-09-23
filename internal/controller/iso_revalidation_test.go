//nolint:goconst // Keep HTTP and cache fixture values readable.
package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
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
		wantConditional, wantReuse, wantErr                                                                                 bool
		wantRequests                                                                                                        int
	}{
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
