# OpenStack setup and operations

All example names are fictional. The operator uses the Gophercloud Go SDK for
Keystone v3, Nova v2, Neutron v2, Cinder v3, and Glance v2. It never invokes the
OpenStack CLI and never reads `clouds.yaml`, process authentication variables,
or local kubeconfigs for provider configuration.

## Prerequisites

- HyperShift Agent platform, CAPI AgentMachines, Assisted Installer Agents, and
  an InfraEnv exposing a usable discovery ISO in the pool namespace.
- One infrastructure pool per NodePool demand. Do not configure overlapping
  OpenStack or vSphere pools for the same NodePool/control-plane namespace.
- A flavor, network, and Cinder types compatible with volume-backed CD-ROMs.
  Neutron must support port security and port descriptions; configured port tags
  additionally require the tag-on-create extension.
- A project-scoped application credential with the operations listed below.
- Network access from the operator to Keystone and catalog endpoints, and from
  the workers to the Assisted Service and their installation services.
- InfraEnv auto-approval and other automation must not approve or relabel these
  workers independently. Let Agent Forge verify the installation disk first.

Match `agent.labels` to the NodePool's Agent label selector. Set
`controlPlaneNamespace` to the namespace containing its CAPI objects. All object
references other than that explicit control-plane namespace are namespace-local.
Existing pools can use different Secrets, projects, regions, networks, and InfraEnvs.

## Authentication and TLS

Create a Kubernetes Secret through your normal secret-management system in the
pool namespace. Required keys are:

| Key | Meaning |
| --- | --- |
| `auth-url` | HTTPS Keystone v3 authentication URL |
| `project-id` | Expected authenticated project ID; checked against the token |
| `application-credential-id` | Project-scoped application credential ID |
| `application-credential-secret` | Application credential secret |
| `ca.crt` | Optional PEM CA bundle added to system roots |

No credential values belong in pool manifests, Git, status, events, or logs.
The operator requires HTTPS for catalog endpoints as well as authentication,
verifies server certificates, and does not follow authenticated HTTP redirects.
There is no insecure-TLS option. ISO download uses system certificate trust;
install a private InfraEnv image-service CA into the operator's system trust store
when needed. The Secret's `ca.crt` configures OpenStack API trust only.

Use a dedicated least-privilege credential scoped to a single project. Permit
reading the service catalog and relevant flavors/networks/subnets, listing and
getting project servers/ports/volumes/images, creating and deleting those owned
resources, setting the Cinder bootable flag, and uploading private Glance images.
Cinder must expose `os-vol-tenant-attr:tenant_id` to permit ownership verification.
Do not grant administrative cross-project roles or force-delete/reset-state powers.
Kubernetes RBAC allows the operator to read credential Secrets and manage its cache
ConfigMaps; restrict who may write pools, workers, Secrets, and cache records.
Credential rotation is supported within the same cloud/project/region. Changing
scope causes `CloudScopeChanged` and requires restoring the original access for
cleanup; it never authorizes deleting a same-named resource in a different project.

## Deployment and example

Use operator image `v1.3.0` or newer for OpenStack support. For development, build
an image from this working tree and make it available to your test cluster.
Apply all four CRDs before upgrading the controller. The Helm chart upgrade
instructions cover the separate CRD update required by Helm.

```sh
make manifests generate
make docker-build IMG=example-registry.invalid/agent-forge-operator:openstack-test
# After making that image available to your cluster:
make install
make deploy IMG=example-registry.invalid/agent-forge-operator:openstack-test
kubectl apply -f config/samples/agentforge_v1alpha1_openstackagentpool.yaml
```

These commands contact your selected cluster when run. They are not part of unit
testing. Preserve `--leader-elect` in the deployment. The new CRDs and generated
RBAC are included in `config/default` through Kustomize. No cloud endpoints,
credentials, or extra host binaries are added to the Deployment.

See the [complete example manifest](../config/samples/agentforge_v1alpha1_openstackagentpool.yaml)
and [API reference](openstack-api.md). The example enables port security.
To disable it, explicitly set `openstack.portSecurityEnabled: false` and leave
`securityGroupIDs` empty. The operator never disables it implicitly.

DHCP is a Neutron subnet property, not an individual port switch. Set `subnetID`
to select an existing subnet; optional `dhcpEnabled` requires that subnet's existing
setting to match. No shared subnet is modified. Pool-created workers request
Neutron-assigned addresses. An explicitly created worker may use its immutable
`spec.fixedIPAddress` with the selected subnet; a single address is intentionally
not copied to every worker in a pool.

## Permanent discovery media and boot limitations

The OS volume is blank, persistent, and marked bootable before Nova creation.
It is boot index **0**. The Cinder discovery volume is a secondary CD-ROM at boot
index **1**. Nova uses `source_type=volume`, `destination_type=volume`, and
`delete_on_termination=false` for both. There is no image-to-local mapping.

Firmware must fall back to the discovery CD-ROM when the primary disk is blank,
and prefer the installed OS disk on subsequent boots. Some Nova drivers or
firmware configurations do not support that fallback or Cinder-backed CD-ROMs.
The controller cannot infer firmware behavior from a successful API response.
Validate first boot, installation, and later reboots in a disposable canary pool.
If the blank bootable disk prevents discovery boot, select compatible deployment
settings; the operator will not make the ISO the root disk or orchestrate a
post-installation stop/start cycle.

The default buses are OS `virtio` and discovery `ide`. Alternative supported bus
values are configurable; configure `installationDisk` for the resulting OS device.
Approval requires exactly one eligible disk with that path and the OS Cinder
volume serial (full UUID or the standard virtio truncation of at least 20
characters). The disk's inventory ID is passed to Assisted Installer. A CD-ROM,
missing serial, wrong path, or ambiguous inventory blocks approval. Hypervisors
that do not expose a verifiable volume serial require investigation rather than
an unsafe fallback to an arbitrary disk.

The attached ISO is expected infrastructure in Ready state. It is never detached,
replaced, or rotated automatically, including after operator restarts or InfraEnv
ISO regeneration. New workers use the current ISO content; installed workers
keep their original volume and identity. Storage consumption includes one OS
volume plus one ISO volume per worker, and shared private Glance storage per
referenced ISO version. ISO volumes round actual image size up to GiB, respecting
`discoveryVolumeMinGiB`; they are not assumed to fit within 2 GiB.

## Security of retained media

Discovery media can contain bootstrap configuration, pull credentials, trust
material, or other sensitive provisioning data. It is not assumed to be harmless
or credential-free. An administrator or guest with access to the media may read it
for the entire worker lifetime. Restrict Nova console access, guest privileges,
Glance image downloads, Cinder attachments/snapshots, backups, and operator
filesystem access. Use appropriate encrypted storage types and retention policies.

ISO files are downloaded to private temporary files, hashed, and removed after
use. Source URLs and ISO contents are not persisted in OpenStackAgent status or
cache records and are not included in provider errors. Glance images are private,
scoped to the InfraEnv and cloud, and each worker has its own discovery volume.
Deleting a worker removes discovery storage under the Delete cleanup policy.
Retain intentionally retains the sensitive media too. A content change affects
future workers; replacing existing media requires a separately authorized worker
replacement through the normal CAPI lifecycle.

## Troubleshooting and recovery

Inspect `OpenStackAgent.status.phase`, its Ready condition, and recorded resource
IDs. Provider errors deliberately omit raw response bodies and URLs. Investigate
request failures using your cloud's authorized audit facilities rather than
expecting credentials or signed download URLs in operator logs.

| Condition/reason | Action |
| --- | --- |
| `CredentialsUnavailable`, `MissingCredentialKey` | Check the referenced Secret and required key names. |
| `InvalidCACertificate`, `InvalidAuthenticationURL` | Supply a valid PEM bundle and HTTPS Keystone endpoint. |
| `ProjectScopeMismatch`, `CloudScopeChanged` | Restore credentials for the expected project and original cloud scope. |
| `NetworkNotUnique`, `FlavorNotUnique` | Use an exact unique name or an explicit ID. |
| `SubnetDHCPMismatch` | Select a subnet with the requested network/DHCP configuration. |
| `ConfigurationRejected` | Check cloud policy, quotas, supported mappings, and requested placement. |
| `RateLimited`, `OpenStackRequestFailed` | Transient failures back off and retry; inspect service health. |
| `CreateOutcomeUnknown` | A POST may have succeeded. Inspect the recorded resource kind and exact UID-derived ownership. Do not blindly retry creation. |
| `InfrastructureResourceFailed`, `DiscoveryImageFailed` | Inspect the failed resource. No installed disk or instance is automatically replaced. |
| `DiscoveryImageChangedBeforeUpload` | Source bytes changed after pinning. Replace the uninstalled worker through CAPI so it obtains the new image identity. |
| `InstallationDiskNotVerified` | Check guest disk path, eligibility, and Cinder serial exposure. Approval stays blocked. |
| `AmbiguousAgentIdentity`, `AgentIdentityChanged`, `AgentBindingConflict` | Resolve duplicate/stale Agent records or an unexpected reservation; identities are not silently reassigned. |
| `AgentDiscoveryTimeout`, `AgentInstallationFailed` | Verify discovery boot and installation. Resources remain for diagnosis. |
| `AgentMachineStillPresent`, `MachineStillPresent`, `AgentStillBound` | Finish the CAPI drain/unbind/reclaim lifecycle before infrastructure deletion. |
| `ResourceStillAttachedOrBusy` | Wait for Nova/Cinder attachment release or investigate the service failure. Never force-delete or reset state to bypass this guard. |
| `ResourceOwnershipMismatch`, `ImageCacheIdentityChanged` | Restore valid ownership/bookkeeping or handle inventory manually; the operator does not assume ownership. |

If an ambiguous creation left no resource, a privileged operator may clear
`status.pendingCreate` through the status subresource **only after independently
establishing that no request remains in flight and no owned resource exists**.
Keep the existing identities intact. This exceptional recovery is deliberately
manual because absence from an eventually consistent listing is insufficient.
Do not delete and recreate an installed OpenStackAgent to fix a status issue.

A shared image is collected after its last worker releases it. No speculative
age-based deletion of active images occurs. Retained workers leave explicit
reference records; forced Kubernetes deletion can leave orphan references or
external resources requiring manual inventory review. See
[cleanup and recovery architecture](architecture.md).

## Tests

Normal CI uses synthetic Kubernetes objects and local mocked OpenStack APIs:

```sh
make test
make test-vcsim
make lint
# Compile-check the live test without any infrastructure access:
go test -tags=openstackintegration ./internal/openstack -run '^$'
```

The optional integration test operates on an **already installed canary worker**.
It checks that the selected Node's system UUID matches Nova, authorizes one soft
reboot, then requires the same Node to return Ready in CoreOS with a changed boot
ID while both original volumes and the same port/server remain. It provisions
nothing and performs no node cordoning. The operator itself does not access the
HostedCluster API; only this explicit test reads it for verification.

To opt in, explicitly supply both kubeconfig paths and both authorization flags:

```sh
go test -tags=openstackintegration ./internal/openstack \
  -run '^TestLivePersistentISOReboot$' -count=1 -timeout=20m -args \
  -openstack-live -allow-reboot \
  -management-kubeconfig=/path/to/explicit-management-config \
  -guest-kubeconfig=/path/to/explicit-guest-config \
  -worker-namespace=example-cluster -worker-name=example-worker \
  -node-name=example-node
```

Run this only where disruption of the selected canary is authorized. Build tags
and flags both gate access; it is not run automatically by CI. Live firmware boot
compatibility cannot be established by the mocked test suite.

## API sources

- [Nova compute API and block-device mappings](https://docs.openstack.org/api-ref/compute/)
- [Nova driver boot-order limitations](https://docs.openstack.org/nova/15.0.0/block_device_mapping.html)
- [Gophercloud v2](https://github.com/gophercloud/gophercloud)
- [Assisted Installer Agent API](https://pkg.go.dev/github.com/openshift/assisted-service/api/v1beta1)
