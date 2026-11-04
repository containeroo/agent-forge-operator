//nolint:goconst // Explicit synthetic values make transport policy assertions clear.
package openstack

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	api "github.com/containeroo/agent-forge-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// All endpoints below are local mocks. The catalog deliberately uses HTTP even
// when Keystone uses TLS so the policy is exercised at both boundaries.
func mockHTTPCloud(t *testing.T, tlsAuth bool) (*corev1.Secret, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	authCalls, apiCalls := &atomic.Int32{}, &atomic.Int32{}
	var apiURL string
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/ports/") {
			version := "v2.0"
			if strings.Contains(r.URL.Path, "compute") {
				version = "v2.1"
			}
			if strings.Contains(r.URL.Path, "volumev3") {
				version = "v3.0"
			}
			respond(w, 200, fmt.Sprintf(`{"versions":[{"id":%q,"status":"CURRENT","links":[{"rel":"self","href":%q}]}]}`, version, apiURL+r.URL.Path))
			return
		}
		apiCalls.Add(1)
		if r.Header.Get("X-Auth-Token") != "synthetic-test-token" {
			t.Error("token not forwarded to mock API")
		}
		respond(w, 200, `{"port":{"id":"example-port","project_id":"example-project","description":"agent_forge_owner:example-owner"}}`)
	}))
	apiURL = apiServer.URL
	t.Cleanup(apiServer.Close)
	authServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authCalls.Add(1)
		if r.Method != "POST" || r.URL.Path != "/v3/auth/tokens" {
			t.Errorf("unexpected auth request: %s %s", r.Method, r.URL.Path)
		}
		catalog := make([]map[string]any, 0, 4)
		for _, service := range []string{"compute", "network", "volumev3", "image"} {
			catalog = append(catalog, map[string]any{"type": service, "endpoints": []map[string]any{{"interface": "public", "region": "example-region", "url": apiServer.URL + "/" + service + "/"}}})
		}
		w.Header().Set("X-Subject-Token", "synthetic-test-token")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		if err := json.NewEncoder(w).Encode(map[string]any{"token": map[string]any{"expires_at": "2099-01-01T00:00:00Z", "project": map[string]any{"id": "example-project"}, "catalog": catalog}}); err != nil {
			t.Error(err)
		}
	}))
	if tlsAuth {
		authServer.StartTLS()
	} else {
		authServer.Start()
	}
	t.Cleanup(authServer.Close)
	secret := &corev1.Secret{Data: map[string][]byte{"auth-url": []byte(authServer.URL + "/v3"), "project-id": []byte("example-project"), "application-credential-id": []byte("example-credential"), "application-credential-secret": []byte("synthetic-test-secret")}}
	if tlsAuth {
		secret.Data["ca.crt"] = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: authServer.Certificate().Raw})
	}
	return secret, authCalls, apiCalls
}

func TestHTTPAuthenticationRequiresExplicitOptIn(t *testing.T) {
	secret, authCalls, apiCalls := mockHTTPCloud(t, false)
	for _, policy := range []string{"absent", "false", "", "TRUE", "yes", " true ", "synthetic-sensitive-value", "true"} {
		t.Run(policy, func(t *testing.T) {
			authCalls.Store(0)
			apiCalls.Store(0)
			delete(secret.Data, "allow-insecure-http")
			if policy != "absent" {
				secret.Data["allow-insecure-http"] = []byte(policy)
			}
			c, err := New(context.Background(), api.OpenStackPlacementSpec{Region: "example-region"}, secret)
			if policy == "true" {
				if err != nil {
					t.Fatal(err)
				}
				if _, err := c.Get(context.Background(), Port, "example-port", "example-owner"); err != nil {
					t.Fatal(err)
				}
				if authCalls.Load() != 1 || apiCalls.Load() != 1 {
					t.Fatal("opt-in did not enable both authentication and service APIs")
				}
				return
			}
			want := "InvalidHTTPPolicy"
			if policy == "absent" || policy == "false" {
				want = "HTTPSRequired"
			}
			var safe *Error
			if !errors.As(err, &safe) || safe.Reason != want {
				t.Fatalf("got %v, want %s", err, want)
			}
			if authCalls.Load() != 0 || apiCalls.Load() != 0 {
				t.Fatal("request sent without valid explicit opt-in")
			}
			assertRedacted(t, err)
		})
	}
}

func TestHTTPCatalogPolicyAndCacheRotation(t *testing.T) {
	secret, authCalls, apiCalls := mockHTTPCloud(t, true)
	factory := NewCachedFactory()
	cfg := api.OpenStackPlacementSpec{Region: "example-region"}
	for i, policy := range []string{"false", "true", "false"} {
		secret.ResourceVersion = []string{"1", "2", "3"}[i]
		secret.Data["allow-insecure-http"] = []byte(policy)
		c, err := factory(context.Background(), cfg, secret)
		if policy == "false" {
			if err == nil || !strings.Contains(err.Error(), "HTTPSRequired") || !strings.Contains(err.Error(), "service=nova; operation=discover-endpoint") {
				t.Fatalf("HTTP catalog rejection missing or cached policy bypassed: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		cached, err := factory(context.Background(), cfg, secret)
		if err != nil || cached != c {
			t.Fatal("client not cached")
		}
		if _, err := c.Get(context.Background(), Port, "example-port", "example-owner"); err != nil {
			t.Fatal(err)
		}
	}
	if authCalls.Load() != 3 || apiCalls.Load() != 1 {
		t.Fatalf("requests auth=%d api=%d", authCalls.Load(), apiCalls.Load())
	}
}

func TestHTTPOptInNeverDisablesTLSVerification(t *testing.T) {
	secret, authCalls, _ := mockHTTPCloud(t, true)
	delete(secret.Data, "ca.crt")
	secret.Data["allow-insecure-http"] = []byte("true")
	_, err := New(context.Background(), api.OpenStackPlacementSpec{Region: "example-region"}, secret)
	if err == nil || !strings.Contains(err.Error(), "TLSUnknownAuthority") || authCalls.Load() != 0 {
		t.Fatalf("TLS verification bypassed: %v", err)
	}
}

func TestHTTPOptInNeverFollowsRedirects(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { targetCalls.Add(1); w.WriteHeader(500) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	secret := &corev1.Secret{Data: map[string][]byte{"auth-url": []byte(source.URL + "/v3"), "project-id": []byte("example-project"), "application-credential-id": []byte("example-credential"), "application-credential-secret": []byte("synthetic-test-secret"), "allow-insecure-http": []byte("true")}}
	_, err := New(context.Background(), api.OpenStackPlacementSpec{}, secret)
	if err == nil || !strings.Contains(err.Error(), "HTTP 307") || targetCalls.Load() != 0 {
		t.Fatalf("authenticated redirect followed: %v", err)
	}
}

func TestInvalidAuthenticationURLDoesNotEchoInput(t *testing.T) {
	for _, endpoint := range []string{"ftp://example.invalid", "https://example.invalid/#synthetic-sensitive-value", "https://example.invalid/?token=synthetic-sensitive-value", "https://synthetic-sensitive-value@example.invalid", "synthetic-sensitive-value", "%"} {
		secret := &corev1.Secret{Data: map[string][]byte{"auth-url": []byte(endpoint), "allow-insecure-http": []byte("true")}}
		_, err := New(context.Background(), api.OpenStackPlacementSpec{}, secret)
		if err == nil || !strings.HasPrefix(err.Error(), "InvalidAuthenticationURL;") {
			t.Fatalf("invalid URL accepted: %v", err)
		}
		assertRedacted(t, err)
	}
}

func TestHTTPSAgainstHTTPServerReportsSchemeMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadRequest) }))
	defer server.Close()
	secret := &corev1.Secret{Data: map[string][]byte{"auth-url": []byte(strings.Replace(server.URL, "http://", "https://", 1) + "/v3"), "project-id": []byte("example-project"), "application-credential-id": []byte("example-credential"), "application-credential-secret": []byte("synthetic-test-secret")}}
	_, err := New(context.Background(), api.OpenStackPlacementSpec{}, secret)
	if err == nil || !strings.Contains(err.Error(), "category=HTTPResponseToHTTPSClient") || !strings.Contains(err.Error(), "operation=authenticate") {
		t.Fatalf("scheme mismatch hidden: %v", err)
	}
}
