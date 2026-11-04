//nolint:goconst // Explicit synthetic values make redaction assertions reviewable.
package openstack

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"syscall"
	"testing"

	api "github.com/containeroo/agent-forge-operator/api/v1alpha1"
	"github.com/gophercloud/gophercloud/v2"
	corev1 "k8s.io/api/core/v1"
)

const syntheticRequestID = "req-00000000-0000-4000-8000-000000000001"
const sensitiveFixture = "synthetic-sensitive-value"

func TestSafeErrorHTTPDiagnostics(t *testing.T) {
	for _, code := range []int{301, 400, 401, 403, 404, 405, 409, 413, 422, 429, 500, 503} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			raw := gophercloud.ErrUnexpectedResponseCode{
				URL:    "https://example.invalid/" + sensitiveFixture + "?token=" + sensitiveFixture,
				Method: sensitiveFixture, Actual: code, Body: []byte(sensitiveFixture),
				ResponseHeader: http.Header{"X-Openstack-Request-Id": {syntheticRequestID}, "X-Auth-Token": {sensitiveFixture}, "Set-Cookie": {sensitiveFixture}},
			}
			for _, cause := range []error{raw, &raw} {
				err := safeErrorAt(fmt.Errorf("%s: %w", sensitiveFixture, cause), "nova", "create-server")
				safe := err.(*Error)
				for _, expected := range []string{"service=nova", "operation=create-server", "category=HTTPResponse", fmt.Sprintf("HTTP %d", code), "request_id=" + syntheticRequestID} {
					if !strings.Contains(err.Error(), expected) {
						t.Fatalf("missing %q in %v", expected, err)
					}
				}
				assertRedacted(t, err)
				rejected := code == 400 || code == 401 || code == 403 || code == 404 || code == 405 || code == 413 || code == 422 || code == 429
				if safe.Rejected != rejected || safe.Retryable != (!rejected || code == 429) {
					t.Fatalf("changed retry semantics: %#v", safe)
				}
				if (code == 404) != errors.Is(err, ErrNotFound) || (code == 429) != errors.Is(err, ErrThrottle) {
					t.Fatal("sentinel matching lost")
				}
			}
		})
	}
}

func assertRedacted(t *testing.T, err error) {
	t.Helper()
	for _, rendered := range []string{err.Error(), fmt.Sprintf("%+v", err), fmt.Sprintf("%#v", err)} {
		if strings.Contains(rendered, sensitiveFixture) || strings.Contains(rendered, "example.invalid") {
			t.Fatalf("unsafe error: %s", rendered)
		}
	}
	if errors.Unwrap(err) != nil {
		t.Fatal("raw cause can be recovered")
	}
}

func TestSafeErrorRequestIDValidation(t *testing.T) {
	for _, id := range []string{sensitiveFixture, "req-" + sensitiveFixture, syntheticRequestID + "\n" + sensitiveFixture, syntheticRequestID + " " + syntheticRequestID, strings.Repeat("a", 10000)} {
		err := SafeError(gophercloud.ErrUnexpectedResponseCode{Actual: 503, ResponseHeader: http.Header{"X-Openstack-Request-Id": {id}}})
		if strings.Contains(err.Error(), "request_id=") {
			t.Fatalf("unvalidated request ID: %v", err)
		}
		assertRedacted(t, err)
	}
	err := SafeError(gophercloud.ErrUnexpectedResponseCode{Actual: 503, ResponseHeader: http.Header{"X-Compute-Request-Id": {syntheticRequestID}}})
	if !strings.Contains(err.Error(), syntheticRequestID) {
		t.Fatal("legacy Nova request ID lost")
	}
}

func TestSafeErrorTransportAndSDKClassification(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cause    error
		category string
	}{
		{"scheme-mismatch", http.ErrSchemeMismatch, "HTTPResponseToHTTPSClient"},
		{"cancel", context.Canceled, "RequestCanceled"},
		{"deadline", context.DeadlineExceeded, "RequestTimeout"},
		{"unknown-ca", x509.UnknownAuthorityError{Cert: &x509.Certificate{}}, "TLSUnknownAuthority"},
		{"hostname", x509.HostnameError{Certificate: &x509.Certificate{}, Host: sensitiveFixture}, "TLSHostnameMismatch"},
		{"certificate", x509.CertificateInvalidError{Cert: &x509.Certificate{}, Detail: sensitiveFixture}, "TLSCertificateInvalid"},
		{"verification", &tls.CertificateVerificationError{Err: errors.New(sensitiveFixture)}, "TLSHandshakeFailed"},
		{"tls-record", tls.RecordHeaderError{Msg: sensitiveFixture}, "TLSHandshakeFailed"},
		{"tls-alert", tls.AlertError(40), "TLSHandshakeFailed"},
		{"dns", &net.DNSError{Err: sensitiveFixture, Name: sensitiveFixture, Server: sensitiveFixture}, "DNSResolutionFailed"},
		{"refused", &net.OpError{Op: sensitiveFixture, Err: syscall.ECONNREFUSED}, "ConnectionRefused"},
		{"reset", syscall.ECONNRESET, "ConnectionReset"},
		{"unreachable", syscall.ENETUNREACH, "NetworkUnreachable"},
		{"endpoint", gophercloud.ErrEndpointNotFound{BaseError: gophercloud.BaseError{Info: sensitiveFixture}}, "EndpointNotFound"},
		{"service", gophercloud.ErrServiceNotFound{}, "ServiceNotFound"},
		{"endpoint-pointer", &gophercloud.ErrEndpointNotFound{}, "EndpointNotFound"},
		{"service-pointer", &gophercloud.ErrServiceNotFound{}, "ServiceNotFound"},
		{"input-pointer", &gophercloud.ErrInvalidInput{Value: sensitiveFixture}, "InvalidSDKInput"},
		{"missing-pointer", &gophercloud.ErrMissingInput{Argument: sensitiveFixture}, "InvalidSDKInput"},
		{"input", gophercloud.ErrInvalidInput{Value: sensitiveFixture}, "InvalidSDKInput"},
		{"missing", gophercloud.ErrMissingInput{Argument: sensitiveFixture}, "InvalidSDKInput"},
		{"json", &json.UnmarshalTypeError{Value: sensitiveFixture, Field: sensitiveFixture}, "InvalidAPIResponse"},
		{"eof", io.ErrUnexpectedEOF, "UnexpectedEndOfResponse"},
		{"unknown", errors.New(sensitiveFixture), "UnclassifiedClientError"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := safeErrorAt(&url.Error{Op: sensitiveFixture, URL: "https://example.invalid/?token=" + sensitiveFixture, Err: tc.cause}, "keystone", "authenticate")
			if !strings.Contains(err.Error(), "category="+tc.category) {
				t.Fatalf("wrong classification: %v", err)
			}
			safe := err.(*Error)
			if !safe.Retryable || safe.Rejected {
				t.Fatal("diagnostics changed ambiguous request semantics")
			}
			assertRedacted(t, err)
		})
	}
	if SafeError(nil) != nil || safeErrorAt(nil, "nova", "get-server") != nil {
		t.Fatal("nil became error")
	}
}

func TestSafeErrorContextDoesNotMutateSentinels(t *testing.T) {
	for _, sentinel := range []*Error{ErrNotFound, ErrOwnership, ErrThrottle} {
		err := safeErrorAt(sentinel, "cinder", "get-os-volume")
		if !errors.Is(err, sentinel) || sentinel.Error() != sentinel.Reason {
			t.Fatal("sentinel mutated or identity lost")
		}
		outer := safeErrorAt(err, "nova", "create-server")
		if outer.Error() != err.Error() {
			t.Fatal("outer operation replaced actual failing operation")
		}
	}
}

func TestSDKOperationDiagnostics(t *testing.T) {
	for _, kind := range []Kind{Port, OSVolume, DiscoveryVolume, Server, Image} {
		for _, action := range []string{"get", "create", "find", "delete"} {
			if action == "find" && kind == Image {
				continue
			}
			t.Run(string(kind)+"/"+action, func(t *testing.T) {
				c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
					if action == "delete" && r.Method == "GET" {
						switch kind {
						case Port:
							respond(w, 200, `{"port":{"project_id":"example-project","description":"agent_forge_owner:example-owner"}}`)
						case OSVolume, DiscoveryVolume:
							respond(w, 200, `{"volume":{"status":"available","os-vol-tenant-attr:tenant_id":"example-project","metadata":{"agent_forge_owner":"example-owner"}}}`)
						case Server:
							respond(w, 200, `{"server":{"tenant_id":"example-project","metadata":{"agent_forge_owner":"example-owner"}}}`)
						case Image:
							respond(w, 200, `{"owner":"example-project","agent_forge_owner":"example-owner"}`)
						}
						return
					}
					w.Header().Set("X-Openstack-Request-Id", syntheticRequestID)
					respond(w, 503, sensitiveFixture)
				})
				var err error
				switch action {
				case "get":
					_, err = c.Get(context.Background(), kind, "example-resource", "example-owner")
				case "create":
					_, err = c.Create(context.Background(), kind, "example-resource", "example-owner", testWorker())
				case "find":
					_, err = c.Find(context.Background(), kind, "example-resource", "example-owner")
				case "delete":
					err = c.Delete(context.Background(), kind, "example-resource", "example-owner")
				}
				expected := safeResourceError(gophercloud.ErrUnexpectedResponseCode{Actual: 503, ResponseHeader: http.Header{"X-Openstack-Request-Id": {syntheticRequestID}}}, kind, action)
				if err == nil || err.Error() != expected.Error() {
					t.Fatalf("got %v; want %v", err, expected)
				}
				assertRedacted(t, err)
			})
		}
	}
}

func TestSDKLookupAndActionDiagnostics(t *testing.T) {
	for _, operation := range []string{"resolve-network", "resolve-flavor", "get-subnet", "set-bootable", "upload-image"} {
		t.Run(operation, func(t *testing.T) {
			c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" && operation == "set-bootable" {
					respond(w, 200, `{"volume":{"os-vol-tenant-attr:tenant_id":"example-project","metadata":{"agent_forge_owner":"example-owner"}}}`)
					return
				}
				if r.Method == "GET" && operation == "upload-image" {
					respond(w, 200, `{"owner":"example-project","agent_forge_owner":"example-owner"}`)
					return
				}
				respond(w, 503, sensitiveFixture)
			})
			a := testWorker()
			var err error
			switch operation {
			case "resolve-network":
				a.Spec.Configuration.OpenStack.NetworkID = ""
				_, err = c.Create(context.Background(), Port, "example-port", "example-owner", a)
			case "resolve-flavor":
				a.Spec.Configuration.OpenStack.FlavorID = ""
				_, err = c.Create(context.Background(), Server, "example-server", "example-owner", a)
			case "get-subnet":
				enabled := true
				a.Spec.Configuration.OpenStack.DHCPEnabled = &enabled
				a.Spec.Configuration.OpenStack.SubnetID = "example-subnet"
				_, err = c.Create(context.Background(), Port, "example-port", "example-owner", a)
			case "set-bootable":
				err = c.SetBootable(context.Background(), "example-volume", "example-owner")
			case "upload-image":
				err = c.UploadImage(context.Background(), "example-image", "example-owner", strings.NewReader(sensitiveFixture))
			}
			if err == nil || !strings.Contains(err.Error(), "operation="+operation) {
				t.Fatalf("wrong operation: %v", err)
			}
			assertRedacted(t, err)
		})
	}
}

func TestAuthenticationFailureDiagnostics(t *testing.T) {
	for _, scenario := range []string{"authentication", "untrusted-ca", "missing-service", "wrong-region"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if scenario == "authentication" {
					w.Header().Set("X-Openstack-Request-Id", syntheticRequestID)
					respond(w, 401, sensitiveFixture)
					return
				}
				w.Header().Set("X-Subject-Token", sensitiveFixture)
				catalog := `[]`
				if scenario == "wrong-region" {
					catalog = `[{"type":"compute","endpoints":[{"region":"another-region","interface":"public","url":"https://example.invalid/"}]}]`
				}
				respond(w, 201, `{"token":{"expires_at":"2099-01-01T00:00:00Z","project":{"id":"example-project"},"catalog":`+catalog+`}}`)
			}))
			defer server.Close()
			secret := &corev1.Secret{Data: map[string][]byte{
				"auth-url": []byte(server.URL + "/v3"), "project-id": []byte("example-project"),
				"application-credential-id": []byte("example-credential"), "application-credential-secret": []byte(sensitiveFixture),
			}}
			if scenario != "untrusted-ca" {
				secret.Data["ca.crt"] = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			}
			_, err := New(context.Background(), api.OpenStackPlacementSpec{Region: "example-region"}, secret)
			want := "service=keystone; operation=authenticate"
			switch scenario {
			case "authentication":
				want += "; category=HTTPResponse; HTTP 401 Unauthorized; request_id=" + syntheticRequestID
			case "untrusted-ca":
				want += "; category=TLSUnknownAuthority"
			case "missing-service":
				want = "service=nova; operation=discover-endpoint; category=EndpointNotFound"
			case "wrong-region":
				want = "service=nova; operation=discover-endpoint; category=EndpointNotFound"
			}
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("got %v; want %s", err, want)
			}
			assertRedacted(t, err)
		})
	}
}
