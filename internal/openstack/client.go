// Package openstack implements project-scoped, context-aware infrastructure APIs.
package openstack

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	api "github.com/containeroo/agent-forge-operator/api/v1alpha1"
	"github.com/gophercloud/gophercloud/v2"
	sdk "github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/volumes"
	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/flavors"
	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/servers"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/tokens"
	"github.com/gophercloud/gophercloud/v2/openstack/image/v2/imagedata"
	"github.com/gophercloud/gophercloud/v2/openstack/image/v2/images"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/networks"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/ports"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/subnets"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
)

// Kind distinguishes resources with separate OpenStack APIs.
const reasonInvalidResourceKind = "InvalidResourceKind"

type Kind string

const (
	Port            Kind = "port"
	OSVolume        Kind = "os-volume"
	DiscoveryVolume Kind = "discovery-volume"
	Server          Kind = "server"
	Image           Kind = "image"
	OwnerKey             = "agent_forge_owner"
)

// Error deliberately excludes API response bodies, URLs, and request headers.
// Rejected means the request is known not to have created a resource.
type Error struct {
	Reason              string
	Retryable, Rejected bool
}

func (e *Error) Error() string { return e.Reason }

var (
	ErrNotFound  = &Error{Reason: "ResourceNotFound", Rejected: true}
	ErrOwnership = &Error{Reason: "ResourceOwnershipMismatch", Rejected: true}
	ErrThrottle  = &Error{Reason: "RateLimited", Retryable: true, Rejected: true}
)

// SafeError converts SDK errors at the boundary before controllers can log them.
func SafeError(err error) error {
	if err == nil {
		return nil
	}
	var safe *Error
	if errors.As(err, &safe) {
		return safe
	}
	if gophercloud.ResponseCodeIs(err, 404) {
		return ErrNotFound
	}
	for _, code := range []int{400, 401, 403, 405, 413, 422} {
		if gophercloud.ResponseCodeIs(err, code) {
			return &Error{Reason: "ConfigurationRejected", Rejected: true}
		}
	}
	if gophercloud.ResponseCodeIs(err, 429) {
		return ErrThrottle
	}
	return &Error{Reason: "OpenStackRequestFailed", Retryable: true}
}

// Resource is the minimum observation needed by the declarative reconciler.
type Resource struct {
	ID, Name, Owner, State, MAC string
	Attached, Bootable          bool
	SizeBytes                   int64
}

// Client deliberately has no stop, reboot, detach, rebuild, or volume-reset API.
type Client interface {
	Scope() string
	Find(context.Context, Kind, string, string) (*Resource, error)
	Get(context.Context, Kind, string, string) (*Resource, error)
	Create(context.Context, Kind, string, string, *api.OpenStackAgent) (*Resource, error)
	SetBootable(context.Context, string, string) error
	UploadImage(context.Context, string, string, io.Reader) error
	Delete(context.Context, Kind, string, string) error
}

// Factory creates clients from an explicit Kubernetes Secret.
type Factory func(context.Context, api.OpenStackPlacementSpec, *corev1.Secret) (Client, error)
type cloud struct {
	compute, network, storage, image *gophercloud.ServiceClient
	scope, project                   string
	expires                          time.Time
}

var apiLimiter = rate.NewLimiter(20, 40)

type secureTransport struct{ base http.RoundTripper }

func (t secureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != "https" {
		return nil, &Error{Reason: "HTTPSRequired", Rejected: true}
	}
	if !apiLimiter.Allow() {
		return nil, ErrThrottle
	}
	return t.base.RoundTrip(r)
}

// New authenticates with application credentials and verifies the resulting project.
// TLS verification is mandatory. ca.crt augments system trust for private CAs.
func New(ctx context.Context, cfg api.OpenStackPlacementSpec, secret *corev1.Secret) (Client, error) {
	authURL := string(secret.Data["auth-url"])
	parsed, err := url.Parse(authURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" {
		return nil, &Error{Reason: "InvalidAuthenticationURL", Rejected: true}
	}
	for _, key := range []string{"application-credential-id", "application-credential-secret", "project-id"} {
		if len(secret.Data[key]) == 0 {
			return nil, &Error{Reason: "MissingCredentialKey", Rejected: true}
		}
	}
	provider, err := sdk.NewClient(authURL)
	if err != nil {
		return nil, SafeError(err)
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		roots = x509.NewCertPool()
	}
	if cert := secret.Data["ca.crt"]; len(cert) > 0 && !roots.AppendCertsFromPEM(cert) {
		return nil, &Error{Reason: "InvalidCACertificate", Rejected: true}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	timeout := time.Duration(cfg.APITimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = time.Minute
	}
	provider.HTTPClient = http.Client{Transport: secureTransport{transport}, Timeout: timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	// No SDK retries: mutations with ambiguous responses must not be replayed.
	err = sdk.AuthenticateV3(ctx, provider, &gophercloud.AuthOptions{
		IdentityEndpoint: authURL, ApplicationCredentialID: string(secret.Data["application-credential-id"]),
		ApplicationCredentialSecret: string(secret.Data["application-credential-secret"]), AllowReauth: false,
	}, gophercloud.EndpointOpts{})
	if err != nil {
		return nil, SafeError(err)
	}
	result, ok := provider.GetAuthResult().(interface {
		ExtractProject() (*tokens.Project, error)
	})
	if !ok {
		return nil, &Error{Reason: "ProjectScopeUnavailable", Rejected: true}
	}
	project, err := result.ExtractProject()
	if err != nil || project == nil || project.ID != string(secret.Data["project-id"]) {
		return nil, &Error{Reason: "ProjectScopeMismatch", Rejected: true}
	}
	c := &cloud{project: project.ID, expires: time.Now().Add(5 * time.Minute)}
	if auth, ok := provider.GetAuthResult().(interface{ ExtractToken() (*tokens.Token, error) }); ok {
		token, err := auth.ExtractToken()
		if err != nil {
			return nil, SafeError(err)
		}
		if expiry := token.ExpiresAt.Add(-time.Minute); expiry.Before(c.expires) {
			c.expires = expiry
		}
	}
	eo := gophercloud.EndpointOpts{Region: cfg.Region, Availability: gophercloud.AvailabilityPublic}
	c.compute, err = sdk.NewComputeV2(provider, eo)
	if err != nil {
		return nil, SafeError(err)
	}
	c.network, err = sdk.NewNetworkV2(provider, eo)
	if err != nil {
		return nil, SafeError(err)
	}
	c.storage, err = sdk.NewBlockStorageV3(provider, eo)
	if err != nil {
		return nil, SafeError(err)
	}
	c.image, err = sdk.NewImageV2(provider, eo)
	if err != nil {
		return nil, SafeError(err)
	}
	sum := sha256.Sum256([]byte(strings.TrimRight(authURL, "/") + "/" + project.ID + "/" + cfg.Region))
	c.scope = hex.EncodeToString(sum[:])
	return c, nil
}
func (c *cloud) Scope() string { return c.scope }

func portResource(p *ports.Port) *Resource {
	owner := ""
	if strings.HasPrefix(p.Description, OwnerKey+":") {
		owner = strings.TrimPrefix(p.Description, OwnerKey+":")
	}
	return &Resource{ID: p.ID, Name: p.Name, Owner: owner, State: p.Status, MAC: p.MACAddress, Attached: p.DeviceID != ""}
}
func volumeResource(v *volumes.Volume) *Resource {
	return &Resource{ID: v.ID, Name: v.Name, Owner: v.Metadata[OwnerKey], State: v.Status, Attached: len(v.Attachments) > 0, Bootable: v.Bootable == "true"}
}
func serverResource(s *servers.Server) *Resource {
	return &Resource{ID: s.ID, Name: s.Name, Owner: s.Metadata[OwnerKey], State: s.Status}
}
func imageResource(i *images.Image) *Resource {
	owner, _ := i.Properties[OwnerKey].(string)
	return &Resource{ID: i.ID, Name: i.Name, Owner: owner, State: string(i.Status), SizeBytes: i.SizeBytes}
}
func owned(r *Resource, owner string) (*Resource, error) {
	if r == nil || owner == "" || r.Owner != owner {
		return nil, ErrOwnership
	}
	return r, nil
}

func (c *cloud) Get(ctx context.Context, kind Kind, id, owner string) (*Resource, error) {
	var r *Resource
	switch kind {
	case Port:
		p, err := ports.Get(ctx, c.network, id).Extract()
		if err != nil {
			return nil, SafeError(err)
		}
		r = portResource(p)
		if p.ProjectID != c.project {
			return nil, ErrOwnership
		}
	case OSVolume, DiscoveryVolume:
		v, err := volumes.Get(ctx, c.storage, id).Extract()
		if err != nil {
			return nil, SafeError(err)
		}
		r = volumeResource(v)
		if v.TenantID != c.project {
			return nil, ErrOwnership
		}
	case Server:
		s, err := servers.Get(ctx, c.compute, id).Extract()
		if err != nil {
			return nil, SafeError(err)
		}
		r = serverResource(s)
		if s.TenantID != c.project {
			return nil, ErrOwnership
		}
	case Image:
		i, err := images.Get(ctx, c.image, id).Extract()
		if err != nil {
			return nil, SafeError(err)
		}
		r = imageResource(i)
		if i.Owner != c.project {
			return nil, ErrOwnership
		}
	default:
		return nil, &Error{Reason: reasonInvalidResourceKind, Rejected: true}
	}
	return owned(r, owner)
}

// Find only adopts an exact, unique name AND owner match. Collisions fail closed.
func (c *cloud) Find(ctx context.Context, kind Kind, name, owner string) (*Resource, error) {
	var found []*Resource
	switch kind {
	case Port:
		pages, err := ports.List(c.network, ports.ListOpts{Name: name, ProjectID: c.project}).AllPages(ctx)
		if err != nil {
			return nil, SafeError(err)
		}
		list, err := ports.ExtractPorts(pages)
		if err != nil {
			return nil, SafeError(err)
		}
		for i := range list {
			if list[i].Name == name {
				found = append(found, portResource(&list[i]))
			}
		}
	case OSVolume, DiscoveryVolume:
		pages, err := volumes.List(c.storage, volumes.ListOpts{Name: name}).AllPages(ctx)
		if err != nil {
			return nil, SafeError(err)
		}
		list, err := volumes.ExtractVolumes(pages)
		if err != nil {
			return nil, SafeError(err)
		}
		for i := range list {
			if list[i].Name == name && list[i].TenantID == c.project {
				found = append(found, volumeResource(&list[i]))
			}
		}
	case Server:
		pages, err := servers.List(c.compute, servers.ListOpts{Name: name}).AllPages(ctx)
		if err != nil {
			return nil, SafeError(err)
		}
		list, err := servers.ExtractServers(pages)
		if err != nil {
			return nil, SafeError(err)
		}
		for i := range list {
			if list[i].Name == name && list[i].TenantID == c.project {
				found = append(found, serverResource(&list[i]))
			}
		}
	default:
		return nil, &Error{Reason: reasonInvalidResourceKind, Rejected: true}
	}
	if len(found) == 0 {
		return nil, ErrNotFound
	}
	if len(found) != 1 {
		return nil, &Error{Reason: "AmbiguousResourceIdentity", Rejected: true}
	}
	return owned(found[0], owner)
}

func (c *cloud) networkID(ctx context.Context, cfg api.OpenStackPlacementSpec) (string, error) {
	if cfg.NetworkID != "" {
		return cfg.NetworkID, nil
	}
	pages, err := networks.List(c.network, networks.ListOpts{Name: cfg.NetworkName}).AllPages(ctx)
	if err != nil {
		return "", SafeError(err)
	}
	list, err := networks.ExtractNetworks(pages)
	if err != nil {
		return "", SafeError(err)
	}
	if len(list) != 1 {
		return "", &Error{Reason: "NetworkNotUnique", Rejected: true}
	}
	return list[0].ID, nil
}
func (c *cloud) flavorID(ctx context.Context, cfg api.OpenStackPlacementSpec) (string, error) {
	if cfg.FlavorID != "" {
		return cfg.FlavorID, nil
	}
	pages, err := flavors.ListDetail(c.compute, flavors.ListOpts{}).AllPages(ctx)
	if err != nil {
		return "", SafeError(err)
	}
	list, err := flavors.ExtractFlavors(pages)
	if err != nil {
		return "", SafeError(err)
	}
	var ids []string
	for _, f := range list {
		if f.Name == cfg.FlavorName {
			ids = append(ids, f.ID)
		}
	}
	if len(ids) != 1 {
		return "", &Error{Reason: "FlavorNotUnique", Rejected: true}
	}
	return ids[0], nil
}

type portCreateOpts struct {
	ports.CreateOpts
	Security bool
	Tags     []string
}

func (o portCreateOpts) ToPortCreateMap() (map[string]any, error) {
	m, err := o.CreateOpts.ToPortCreateMap()
	if err != nil {
		return nil, err
	}
	p := m["port"].(map[string]any)
	p["port_security_enabled"] = o.Security
	if len(o.Tags) > 0 {
		p["tags"] = o.Tags
	}
	return m, nil
}

// BlockDevices always keeps persistent OS storage first and discovery media second.
func BlockDevices(a *api.OpenStackAgent) []servers.BlockDevice {
	t := a.Spec.Configuration.Template
	return []servers.BlockDevice{
		{UUID: a.Status.OSVolumeID, SourceType: servers.SourceVolume, DestinationType: servers.DestinationVolume, DeviceType: "disk", DiskBus: t.OSDiskBus, BootIndex: 0, DeleteOnTermination: false},
		{UUID: a.Status.DiscoveryVolumeID, SourceType: servers.SourceVolume, DestinationType: servers.DestinationVolume, DeviceType: "cdrom", DiskBus: t.DiscoveryDiskBus, BootIndex: 1, DeleteOnTermination: false},
	}
}

func (c *cloud) Create(ctx context.Context, kind Kind, name, owner string, a *api.OpenStackAgent) (*Resource, error) {
	cfg, t := a.Spec.Configuration.OpenStack, a.Spec.Configuration.Template
	switch kind {
	case Port:
		network, err := c.networkID(ctx, cfg)
		if err != nil {
			return nil, err
		}
		if cfg.DHCPEnabled != nil {
			subnet, err := subnets.Get(ctx, c.network, cfg.SubnetID).Extract()
			if err != nil {
				return nil, SafeError(err)
			}
			if subnet.NetworkID != network || subnet.EnableDHCP != *cfg.DHCPEnabled {
				return nil, &Error{Reason: "SubnetDHCPMismatch", Rejected: true}
			}
		}
		security := cfg.PortSecurityEnabled == nil || *cfg.PortSecurityEnabled
		groups := cfg.SecurityGroupIDs
		if groups == nil {
			groups = []string{}
		}
		opts := portCreateOpts{CreateOpts: ports.CreateOpts{Name: name, NetworkID: network, Description: OwnerKey + ":" + owner, SecurityGroups: &groups}, Security: security, Tags: cfg.PortTags}
		if cfg.SubnetID != "" || a.Spec.FixedIPAddress != "" {
			opts.FixedIPs = []ports.IP{{SubnetID: cfg.SubnetID, IPAddress: a.Spec.FixedIPAddress}}
		}
		p, err := ports.Create(ctx, c.network, opts).Extract()
		if err != nil {
			return nil, SafeError(err)
		}
		return portResource(p), nil
	case OSVolume, DiscoveryVolume:
		metadata := maps.Clone(t.VolumeMetadata)
		if metadata == nil {
			metadata = map[string]string{}
		}
		metadata[OwnerKey] = owner
		opts := volumes.CreateOpts{Name: name, Size: int(t.DiskGiB), VolumeType: t.VolumeType, AvailabilityZone: t.VolumeAvailabilityZone, Metadata: metadata}
		if kind == DiscoveryVolume {
			opts.ImageID = a.Status.ImageID
			opts.VolumeType = t.DiscoveryVolumeType
			opts.Size = max(int(t.DiscoveryVolumeMinGiB), int((a.Status.ISOSizeBytes+(1<<30)-1)/(1<<30)))
		}
		v, err := volumes.Create(ctx, c.storage, opts, nil).Extract()
		if err != nil {
			return nil, SafeError(err)
		}
		return volumeResource(v), nil
	case Server:
		flavor, err := c.flavorID(ctx, cfg)
		if err != nil {
			return nil, err
		}
		s, err := servers.Create(ctx, c.compute, servers.CreateOpts{Name: name, FlavorRef: flavor, Networks: []servers.Network{{Port: a.Status.PortID}},
			AvailabilityZone: cfg.AvailabilityZone, Metadata: map[string]string{OwnerKey: owner}, BlockDevice: BlockDevices(a)}, nil).Extract()
		if err != nil {
			return nil, SafeError(err)
		}
		return serverResource(s), nil
	case Image:
		visibility := images.ImageVisibilityPrivate
		i, err := images.Create(ctx, c.image, images.CreateOpts{ID: a.Status.ImageID, Name: name, DiskFormat: "iso", ContainerFormat: "bare", Visibility: &visibility,
			Properties: map[string]string{OwnerKey: owner, "agent_forge_sha256": a.Status.ISOSHA256}}).Extract()
		if err != nil {
			return nil, SafeError(err)
		}
		return imageResource(i), nil
	default:
		return nil, &Error{Reason: reasonInvalidResourceKind, Rejected: true}
	}
}
func (c *cloud) SetBootable(ctx context.Context, id, owner string) error {
	if _, err := c.Get(ctx, OSVolume, id, owner); err != nil {
		return err
	}
	return SafeError(volumes.SetBootable(ctx, c.storage, id, volumes.BootableOpts{Bootable: true}).ExtractErr())
}
func (c *cloud) UploadImage(ctx context.Context, id, owner string, data io.Reader) error {
	if _, err := c.Get(ctx, Image, id, owner); err != nil {
		return err
	}
	return SafeError(imagedata.Upload(ctx, c.image, id, data).ExtractErr())
}
func (c *cloud) Delete(ctx context.Context, kind Kind, id, owner string) error {
	r, err := c.Get(ctx, kind, id, owner)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if r.Attached || ((kind == OSVolume || kind == DiscoveryVolume) && r.State != "available" && r.State != "error") {
		return &Error{Reason: "ResourceStillAttachedOrBusy", Retryable: true, Rejected: true}
	}
	switch kind {
	case Port:
		err = ports.Delete(ctx, c.network, id).ExtractErr()
	case OSVolume, DiscoveryVolume:
		err = volumes.Delete(ctx, c.storage, id, volumes.DeleteOpts{}).ExtractErr()
	case Server:
		err = servers.Delete(ctx, c.compute, id).ExtractErr()
	case Image:
		err = images.Delete(ctx, c.image, id).ExtractErr()
	}
	if gophercloud.ResponseCodeIs(err, 404) {
		return nil
	}
	return SafeError(err)
}

// NewCachedFactory reuses authenticated clients until token expiry (at most five
// minutes). A Secret resourceVersion change immediately selects a new client.
// It never stores or logs a serialized Secret and bounds the number of entries.
func NewCachedFactory() Factory {
	var mu sync.Mutex
	cache := map[string]*cloud{}
	return func(ctx context.Context, cfg api.OpenStackPlacementSpec, secret *corev1.Secret) (Client, error) {
		key := secret.Namespace + "/" + secret.Name + "/" + string(secret.UID) + "/" + secret.ResourceVersion + "/" + cfg.Region + "/" + fmt.Sprint(cfg.APITimeoutSeconds)
		mu.Lock()
		if cached, ok := cache[key]; ok && time.Now().Before(cached.expires) {
			mu.Unlock()
			return cached, nil
		}
		mu.Unlock()
		provider, err := New(ctx, cfg, secret)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		for k, entry := range cache {
			if time.Now().After(entry.expires) || len(cache) >= 128 {
				delete(cache, k)
			}
		}
		cache[key] = provider.(*cloud)
		mu.Unlock()
		return provider, nil
	}
}
