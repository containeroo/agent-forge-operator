# OpenStack API reference

API group/version: `agent-forge.containeroo.ch/v1alpha1`. Both resources are
namespace-scoped and have status subresources. Generated structural CRDs are in
`config/crd/bases`; `make manifests generate` regenerates them and DeepCopy code.
The existing vSphere API remains unchanged.

## OpenStackAgentPool (`osap`)

| Field under `spec` | Default / behavior |
| --- | --- |
| `hostedClusterRef.name` | Required local HostedCluster name; immutable. |
| `nodePoolRef.name` | Required local NodePool name; immutable. |
| `infraEnvRef.name` | Required local InfraEnv name; immutable. |
| `controlPlaneNamespace` | Required CAPI namespace; immutable. |
| `openstack.credentialsSecretRef.name` | Required Secret in the pool namespace. |
| `openstack.region` | Required catalog region. |
| `openstack.networkID` / `networkName` | Exactly one required; name resolution must be unique. |
| `openstack.flavorID` / `flavorName` | Exactly one required; name resolution must be unique. |
| `openstack.subnetID` | Optional existing subnet for port address allocation. |
| `openstack.dhcpEnabled` | Optional required DHCP setting of `subnetID`; validates, never mutates the subnet. |
| `openstack.portSecurityEnabled` | `true`; disabling requires no security groups. |
| `openstack.securityGroupIDs` | Optional groups on the dedicated port; empty list means no groups. |
| `openstack.portTags` | Optional Neutron tags; ownership remains in the reserved description. |
| `openstack.availabilityZone` | Optional Nova placement zone. |
| `openstack.apiTimeoutSeconds` | `60`, range 5–600; HTTP request timeout including uploads. |
| `template.diskGiB` | `120`, range 20–65536; blank persistent OS volume. |
| `template.volumeType` | Optional OS Cinder type. |
| `template.volumeAvailabilityZone` | Optional Cinder zone for both volumes. |
| `template.volumeMetadata` | Optional metadata; reserved `agent_forge_owner` cannot be overridden. |
| `template.discoveryVolumeMinGiB` | `2`; actual ISO size can increase this automatically. |
| `template.discoveryVolumeType` | Optional discovery Cinder type. |
| `template.osDiskBus` | `virtio`; also `scsi`, `sata`, `ide`. |
| `template.discoveryDiskBus` | `ide`; also `scsi`, `sata`. |
| `template.installationDisk` | `/dev/vda`; must match eligible inventory and OS volume serial. |
| `template.osVolumePolicy` | `Delete` or `Retain`; affects only OS storage. |
| `agent.role` | `worker`. |
| `agent.labels` | Required nonempty labels matching the NodePool's Agent selector. |
| `agent.approve` | `true`; approval follows successful disk verification. |
| `cleanupPolicy` | `Delete` or `Retain`; controls all external worker inventory. |
| `discoveryTimeoutSeconds` | `1800`, minimum 60; reports timeout without deleting the VM. |

Pool status contains `observedGeneration`, `desiredReplicas` (non-deleting matching
AgentMachines), `readyReplicas` (installed OpenStack workers), and conditions.
The pool controller polls demand; the shared AgentMachine watch dispatches creation.
Pool updates to mutable configuration apply to newly created workers only.

There is no ISO detach/eject setting, arbitrary Nova image-to-local mapping, or
OS disk boot-index override. OS index 0 and CD-ROM index 1 are invariants.
There is no per-port arbitrary metadata API in Neutron: use `portTags` for user
classification; the port description is reserved for ownership.

## OpenStackAgent (`osa`)

Normally created by the demand controller, not applied by users.
The entire `spec` is immutable:

- `poolRef.name`, `poolUID`: original pool identity.
- `agentMachineRef`: original demand name, namespace, and UID.
- `machineRef`: original CAPI owner reference, when present.
- `configuration`: pool-spec snapshot used for provisioning and cleanup.
- `fixedIPAddress`: optional per-worker Neutron fixed address; never a pool-wide
  repeated address. Standard pool-driven provisioning uses Neutron allocation.

Status tracks `portID`, `macAddress`, `osVolumeID`, `discoveryVolumeID`, `serverID`,
`agentRef`, `instanceCreatedAt`, and `phase`. Image state consists of `imageID`,
`imageCacheName`, `isoSHA256`, `isoSizeBytes`, and `infraEnvUID`. `scopeHash` records
cloud/project/region identity without copying credential values. `pendingCreate`
records an unresolved external POST. `conditions` and `observedGeneration` expose
progress and failure; no credentials, ISO contents, or signed download URLs appear.

Typical phases: CreatingNetwork, CreatingOSVolume, PreparingDiscoveryImage,
CreatingDiscoveryVolume, CreatingInstance, WaitingForDiscovery, WaitingForAgent,
WaitingForInstallation, Ready, Failed, RecoveringResource, Deleting. Ready requires
installation and correct binding, **not** media removal.

Both pool and worker use the `agent-forge.containeroo.ch/openstack` finalizer.
Cache ConfigMaps use the label
`agent-forge.containeroo.ch/openstack-image-cache: "true"`; their data keys are
worker UIDs. They are controller bookkeeping, not a user configuration API.
