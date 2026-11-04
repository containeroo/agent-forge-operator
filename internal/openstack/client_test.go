//nolint:goconst // Fixtures keep API values explicit for review.
package openstack

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	api "github.com/containeroo/agent-forge-operator/api/v1alpha1"
	"github.com/gophercloud/gophercloud/v2"
	corev1 "k8s.io/api/core/v1"
)

func testCloud(t *testing.T, handler http.HandlerFunc) *cloud {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	provider := &gophercloud.ProviderClient{HTTPClient: *server.Client()}
	service := &gophercloud.ServiceClient{ProviderClient: provider, Endpoint: server.URL + "/"}
	return &cloud{compute: service, network: service, storage: service, image: service, project: "example-project", scope: "example-scope"}
}
func testWorker() *api.OpenStackAgent {
	security := true
	return &api.OpenStackAgent{Spec: api.OpenStackAgentSpec{Configuration: api.OpenStackAgentPoolSpec{
		OpenStack: api.OpenStackPlacementSpec{NetworkID: "worker-network", FlavorID: "worker-flavor", PortSecurityEnabled: &security},
		Template:  api.OpenStackTemplateSpec{DiskGiB: 120, DiscoveryVolumeMinGiB: 2, OSDiskBus: "virtio", DiscoveryDiskBus: "ide", VolumeMetadata: map[string]string{OwnerKey: "must-not-override-owner"}},
	}}, Status: api.OpenStackAgentStatus{PortID: "example-port", OSVolumeID: "example-os-volume", DiscoveryVolumeID: "example-iso-volume", ImageID: "example-image", ISOSizeBytes: 3*(1<<30) + 1}}
}
func decodeBody(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body
}
func respond(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, body)
}

func TestNovaPersistentBlockDevices(t *testing.T) {
	a := testWorker()
	c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/servers" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		server := decodeBody(t, r)["server"].(map[string]any)
		if ref := server["imageRef"]; ref != nil && ref != "" {
			t.Fatal("image-to-local boot mapping must not be sent")
		}
		devices := server["block_device_mapping_v2"].([]any)
		if len(devices) != 2 {
			t.Fatal("expected exactly two persistent devices")
		}
		for i, raw := range devices {
			device := raw.(map[string]any)
			if device["boot_index"] != float64(i) || device["source_type"] != "volume" || device["destination_type"] != "volume" || device["delete_on_termination"] != false {
				t.Fatalf("unsafe mapping: %#v", device)
			}
		}
		osDisk, iso := devices[0].(map[string]any), devices[1].(map[string]any)
		if osDisk["uuid"] != a.Status.OSVolumeID || osDisk["device_type"] != "disk" || osDisk["disk_bus"] != "virtio" {
			t.Fatalf("wrong OS mapping: %#v", osDisk)
		}
		if iso["uuid"] != a.Status.DiscoveryVolumeID || iso["device_type"] != "cdrom" || iso["disk_bus"] != "ide" {
			t.Fatalf("wrong ISO mapping: %#v", iso)
		}
		if server["networks"].([]any)[0].(map[string]any)["port"] != a.Status.PortID {
			t.Fatal("missing dedicated port")
		}
		respond(w, 202, `{"server":{"id":"example-server"}}`)
	})
	if _, err := c.Create(context.Background(), Server, "example-worker", "example-owner", a); err != nil {
		t.Fatal(err)
	}
}

func TestPortSecurityAndFixedIP(t *testing.T) {
	for _, security := range []bool{true, false} {
		t.Run(fmt.Sprint(security), func(t *testing.T) {
			a := testWorker()
			a.Spec.Configuration.OpenStack.PortSecurityEnabled = &security
			a.Spec.Configuration.OpenStack.SubnetID = "example-subnet"
			a.Spec.Configuration.OpenStack.PortTags = []string{"example-tag"}
			a.Spec.FixedIPAddress = "192.0.2.10" // RFC 5737 documentation address.
			c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
				p := decodeBody(t, r)["port"].(map[string]any)
				if p["port_security_enabled"] != security || p["description"] != OwnerKey+":example-owner" {
					t.Fatalf("port configuration: %#v", p)
				}
				fixed := p["fixed_ips"].([]any)[0].(map[string]any)
				if fixed["subnet_id"] != "example-subnet" || fixed["ip_address"] != "192.0.2.10" {
					t.Fatal("fixed address not propagated")
				}
				if len(p["security_groups"].([]any)) != 0 {
					t.Fatal("unexpected security groups")
				}
				respond(w, 201, `{"port":{"id":"example-port","mac_address":"02:00:00:00:00:01","description":"agent_forge_owner:example-owner"}}`)
			})
			port, err := c.Create(context.Background(), Port, "example-port", "example-owner", a)
			if err != nil || port.MAC != "02:00:00:00:00:01" {
				t.Fatalf("port = %#v, %v", port, err)
			}
		})
	}
}

func TestCinderCreateAndBootable(t *testing.T) {
	a := testWorker()
	for _, kind := range []Kind{OSVolume, DiscoveryVolume} {
		t.Run(string(kind), func(t *testing.T) {
			c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
				body := decodeBody(t, r)["volume"].(map[string]any)
				if body["metadata"].(map[string]any)[OwnerKey] != "example-owner" {
					t.Fatal("ownership metadata overridden")
				}
				if kind == OSVolume {
					if body["size"] != float64(120) || body["imageRef"] != nil {
						t.Fatalf("OS volume must be blank: %#v", body)
					}
				} else if body["size"] != float64(4) || body["imageRef"] != a.Status.ImageID {
					t.Fatalf("ISO size/image: %#v", body)
				}
				respond(w, 202, `{"volume":{"id":"example-volume","status":"creating"}}`)
			})
			if _, err := c.Create(context.Background(), kind, "example-volume", "example-owner", a); err != nil {
				t.Fatal(err)
			}
		})
	}
	called := false
	c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			respond(w, 200, `{"volume":{"id":"example-volume","status":"available","os-vol-tenant-attr:tenant_id":"example-project","metadata":{"agent_forge_owner":"example-owner"}}}`)
			return
		}
		body := decodeBody(t, r)
		if body["os-set_bootable"].(map[string]any)["bootable"] != true {
			t.Fatal("bootable flag missing")
		}
		called = true
		respond(w, 200, "")
	})
	if err := c.SetBootable(context.Background(), "example-volume", "example-owner"); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("bootable action not issued")
	}
}

func TestDeletionOwnershipAndAttachments(t *testing.T) {
	for _, tc := range []struct {
		name, owner, project, state, attachments string
		wantDelete                               bool
	}{
		{"attached", "example-owner", "example-project", "in-use", `[{"server_id":"example-server"}]`, false},
		{"foreign owner", "other-owner", "example-project", "available", `[]`, false},
		{"foreign project", "example-owner", "other-project", "available", `[]`, false},
		{"missing owner", "", "example-project", "available", `[]`, false},
		{"available", "example-owner", "example-project", "available", `[]`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deleted := false
			c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "DELETE" {
					deleted = true
					respond(w, 202, "")
					return
				}
				respond(w, 200, fmt.Sprintf(`{"volume":{"id":"example-volume","status":%q,"os-vol-tenant-attr:tenant_id":%q,"metadata":{"agent_forge_owner":%q},"attachments":%s}}`, tc.state, tc.project, tc.owner, tc.attachments))
			})
			err := c.Delete(context.Background(), DiscoveryVolume, "example-volume", "example-owner")
			if deleted != tc.wantDelete || (err == nil) != tc.wantDelete {
				t.Fatalf("deleted=%v err=%v", deleted, err)
			}
		})
	}
}

func TestGlancePrivateISOUpload(t *testing.T) {
	uploaded := false
	a := testWorker()
	c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			b := decodeBody(t, r)
			if b["disk_format"] != "iso" || b["container_format"] != "bare" || b["visibility"] != "private" || b[OwnerKey] != "example-cache" || b["id"] != a.Status.ImageID {
				t.Fatalf("unsafe image: %#v", b)
			}
			respond(w, 201, `{"id":"example-image","status":"queued"}`)
		case "GET":
			respond(w, 200, `{"id":"example-image","owner":"example-project","agent_forge_owner":"example-cache","status":"queued"}`)
		case "PUT":
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != "synthetic-iso" || r.URL.Path != "/images/example-image/file" {
				t.Fatal("incorrect upload")
			}
			uploaded = true
			respond(w, 204, "")
		}
	})
	if _, err := c.Create(context.Background(), Image, "example-image", "example-cache", a); err != nil {
		t.Fatal(err)
	}
	if err := c.UploadImage(context.Background(), a.Status.ImageID, "example-cache", strings.NewReader("synthetic-iso")); err != nil {
		t.Fatal(err)
	}
	if !uploaded {
		t.Fatal("image not uploaded")
	}
}

func TestFindNeverAdoptsAmbiguousOrForeignResources(t *testing.T) {
	for _, body := range []string{
		`{"servers":[{"id":"one","name":"example-worker","tenant_id":"example-project","metadata":{"agent_forge_owner":"other-owner"}}]}`,
		`{"servers":[{"id":"one","name":"example-worker","tenant_id":"example-project","metadata":{"agent_forge_owner":"example-owner"}},{"id":"two","name":"example-worker","tenant_id":"example-project","metadata":{"agent_forge_owner":"example-owner"}}]}`,
	} {
		c := testCloud(t, func(w http.ResponseWriter, _ *http.Request) { respond(w, 200, body) })
		if _, err := c.Find(context.Background(), Server, "example-worker", "example-owner"); err == nil {
			t.Fatal("unsafe adoption accepted")
		}
	}
}

func TestAuthenticationCustomCAAndProjectScope(t *testing.T) {
	var endpoint string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			version := "v2.0"
			if strings.Contains(r.URL.Path, "compute") {
				version = "v2.1"
			}
			if strings.Contains(r.URL.Path, "volumev3") {
				version = "v3.0"
			}
			respond(w, 200, fmt.Sprintf(`{"versions":[{"id":%q,"status":"CURRENT","links":[{"rel":"self","href":%q}]}]}`, version, endpoint+r.URL.Path))
			return
		}
		if r.Method != "POST" || r.URL.Path != "/v3/auth/tokens" {
			t.Fatalf("unexpected auth request %s %s", r.Method, r.URL.Path)
		}
		body := decodeBody(t, r)
		app := body["auth"].(map[string]any)["identity"].(map[string]any)["application_credential"].(map[string]any)
		if app["id"] != "example-credential" || app["secret"] != "synthetic-test-secret" {
			t.Fatal("application credentials not supplied")
		}
		w.Header().Set("X-Subject-Token", "synthetic-test-token")
		catalog := []map[string]any{}
		for _, service := range []string{"compute", "network", "volumev3", "image"} {
			catalog = append(catalog, map[string]any{"type": service, "name": service, "endpoints": []map[string]any{{"interface": "public", "region": "example-region", "url": endpoint + "/" + service + "/"}}})
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_ = json.NewEncoder(w).Encode(map[string]any{"token": map[string]any{"expires_at": "2099-01-01T00:00:00Z", "project": map[string]any{"id": "example-project"}, "catalog": catalog}})
	}))
	defer server.Close()
	endpoint = server.URL
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	secret := &corev1.Secret{Data: map[string][]byte{"auth-url": []byte(endpoint + "/v3"), "project-id": []byte("example-project"), "application-credential-id": []byte("example-credential"), "application-credential-secret": []byte("synthetic-test-secret"), "ca.crt": cert}}
	cfg := api.OpenStackPlacementSpec{Region: "example-region"}
	c, err := New(context.Background(), cfg, secret)
	if err != nil || c.Scope() == "" {
		t.Fatalf("authentication failed: %v", err)
	}
	secret.Data["project-id"] = []byte("other-project")
	if _, err := New(context.Background(), cfg, secret); err == nil || err.Error() != "ProjectScopeMismatch" {
		t.Fatalf("scope mismatch: %v", err)
	}
	secret.Data["auth-url"] = []byte("http://cloud.example.invalid/v3")
	if _, err := New(context.Background(), cfg, secret); err == nil {
		t.Fatal("plaintext authentication accepted")
	}
}

func TestErrorsAndCancellationAreSanitized(t *testing.T) {
	for _, code := range []int{400, 401, 403, 429, 503} {
		c := testCloud(t, func(w http.ResponseWriter, _ *http.Request) { respond(w, code, "synthetic-sensitive-response") })
		_, err := c.Get(context.Background(), Server, "example-server", "example-owner")
		var safe *Error
		if !errors.As(err, &safe) || strings.Contains(err.Error(), "sensitive") {
			t.Fatalf("unsafe error: %v", err)
		}
		if safe.Retryable != (code == 429 || code == 503) {
			t.Fatalf("wrong retry classification: %#v", safe)
		}
	}
	c := testCloud(t, func(_ http.ResponseWriter, _ *http.Request) { t.Error("cancelled request was sent") })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Get(ctx, Server, "example-server", "example-owner"); err == nil {
		t.Fatal("cancellation ignored")
	}
}

func TestNeutronSubnetDHCPValidation(t *testing.T) {
	for _, matches := range []bool{true, false} {
		t.Run(fmt.Sprint(matches), func(t *testing.T) {
			a := testWorker()
			enabled := true
			a.Spec.Configuration.OpenStack.DHCPEnabled = &enabled
			a.Spec.Configuration.OpenStack.SubnetID = "example-subnet"
			created := false
			c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					respond(w, 200, fmt.Sprintf(`{"subnet":{"id":"example-subnet","network_id":"worker-network","enable_dhcp":%v}}`, matches))
					return
				}
				created = true
				respond(w, 201, `{"port":{"id":"example-port"}}`)
			})
			_, err := c.Create(context.Background(), Port, "example-port", "example-owner", a)
			if created != matches || (err == nil) != matches {
				t.Fatalf("DHCP validation failed: created=%v err=%v", created, err)
			}
		})
	}
}

func TestOwnedResourceRecoveryAndExactNames(t *testing.T) {
	for _, tc := range []struct {
		kind     Kind
		response string
	}{
		{Port, `{"ports":[{"id":"example-port","name":"example-resource","project_id":"example-project","description":"agent_forge_owner:example-owner"}]}`},
		{OSVolume, `{"volumes":[{"id":"example-volume","name":"example-resource","os-vol-tenant-attr:tenant_id":"example-project","metadata":{"agent_forge_owner":"example-owner"}}]}`},
		{Server, `{"servers":[{"id":"example-server","name":"example-resource","tenant_id":"example-project","metadata":{"agent_forge_owner":"example-owner"}},{"id":"unrelated-server","name":"example-resource-other","tenant_id":"example-project"}]}`},
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			c := testCloud(t, func(w http.ResponseWriter, _ *http.Request) { respond(w, 200, tc.response) })
			resource, err := c.Find(context.Background(), tc.kind, "example-resource", "example-owner")
			if err != nil || resource == nil || resource.Owner != "example-owner" {
				t.Fatalf("recovery failed: %#v %v", resource, err)
			}
			if _, err := c.Find(context.Background(), tc.kind, "missing-resource", "example-owner"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("nonexact name adopted: %v", err)
			}
		})
	}
}

func TestPortAndImageOwnershipCannotBeInferred(t *testing.T) {
	for _, tc := range []struct {
		kind Kind
		body string
	}{
		{Port, `{"port":{"id":"example-port","project_id":"example-project","description":"example-owner"}}`},
		{Image, `{"id":"example-image","owner":"other-project","agent_forge_owner":"example-owner"}`},
		{Server, `{"server":{"id":"example-server","tenant_id":"other-project","metadata":{"agent_forge_owner":"example-owner"}}}`},
	} {
		c := testCloud(t, func(w http.ResponseWriter, _ *http.Request) { respond(w, 200, tc.body) })
		if _, err := c.Get(context.Background(), tc.kind, "example-resource", "example-owner"); !errors.Is(err, ErrOwnership) {
			t.Fatalf("foreign resource accepted: %v", err)
		}
	}
}

func TestNamedNetworkAndFlavorResolution(t *testing.T) {
	a := testWorker()
	a.Spec.Configuration.OpenStack.NetworkID = ""
	a.Spec.Configuration.OpenStack.NetworkName = "worker-network"
	a.Spec.Configuration.OpenStack.FlavorID = ""
	a.Spec.Configuration.OpenStack.FlavorName = "worker-flavor"
	c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/networks":
			respond(w, 200, `{"networks":[{"id":"example-network-id","name":"worker-network"}]}`)
		case "/flavors/detail":
			respond(w, 200, `{"flavors":[{"id":"example-flavor-id","name":"worker-flavor"}]}`)
		case "/ports":
			body := decodeBody(t, r)["port"].(map[string]any)
			if body["network_id"] != "example-network-id" {
				t.Fatal("network name unresolved")
			}
			respond(w, 201, `{"port":{"id":"example-port"}}`)
		case "/servers":
			body := decodeBody(t, r)["server"].(map[string]any)
			if body["flavorRef"] != "example-flavor-id" {
				t.Fatal("flavor name unresolved")
			}
			respond(w, 202, `{"server":{"id":"example-server"}}`)
		default:
			t.Fatalf("unexpected request: %s", r.URL.Path)
		}
	})
	for _, kind := range []Kind{Port, Server} {
		if _, err := c.Create(context.Background(), kind, "example-worker", "example-owner", a); err != nil {
			t.Fatal(err)
		}
	}
}
