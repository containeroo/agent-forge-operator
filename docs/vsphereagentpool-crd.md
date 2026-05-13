# VsphereAgentPool CRD

`VsphereAgentPool` is a namespace-scoped bridge between one Hypershift Agent `NodePool` and vSphere VM inventory.

HyperShift and CAPI remain authoritative. The operator reacts to `AgentMachine` objects that report `Ready=False` with `Reason=NoSuitableAgents` by ensuring enough matching Assisted Installer `Agent` objects can exist.

## Spec Fields

| Field | Required | Description |
| --- | --- | --- |
| `spec.hostedClusterRef.name` | yes | HostedCluster name in the same namespace as this CR. |
| `spec.nodePoolRef.name` | yes | Hypershift NodePool name in the same namespace as this CR. |
| `spec.infraEnvRef.name` | yes | Assisted Installer InfraEnv name in the same namespace. The InfraEnv must expose `status.isoDownloadURL`. |
| `spec.controlPlaneNamespace` | yes | Hosted control plane namespace containing the CAPI `AgentMachine` and `Machine` objects, for example `demo-demo`. |
| `spec.vsphere.credentialsSecretRef.name` | yes | Secret containing vSphere `server`, `username`, and `password`. Optional key: `insecure`. |
| `spec.vsphere.credentialsSecretRef.namespace` | no | Secret namespace. Defaults to the CR namespace. |
| `spec.vsphere.datacenter` | no | vSphere datacenter name. Defaults to `dc1`. |
| `spec.vsphere.datastoreCluster` | yes | Datastore cluster for VM disks, matching the existing module's `vsphere_datastore_cluster`. The operator passes this to vSphere as datastore-cluster placement, not as a concrete datastore. |
| `spec.vsphere.isoDatastore` | yes | Datastore for the discovery ISO, matching `vsphere_iso_datastore`. |
| `spec.vsphere.resourcePool` | yes | Resource pool path, for example `cluster/Resources`. |
| `spec.vsphere.folder` | no | VM folder path. Defaults logically to the hosted cluster name when empty. |
| `spec.vsphere.network` | yes | vSphere network attached to the VM NIC. |
| `spec.vsphere.vmTags` | no | Optional vSphere tag IDs to attach to created VMs. |
| `spec.vsphere.guestID` | no | Guest OS identifier. Defaults to `rhel9_64Guest`. |
| `spec.vsphere.scsiType` | no | SCSI controller type. Defaults to `pvscsi`. |
| `spec.vsphere.firmware` | no | VM firmware, `efi` or `bios`. Defaults to `efi`. |
| `spec.vsphere.networkAdapterType` | no | NIC adapter type. Defaults to `vmxnet3`. |
| `spec.vsphere.diskEagerlyScrub` | no | Enables eager disk scrubbing for the primary disk. Defaults to `false`. |
| `spec.template.namePrefix` | no | Prefix for operator-created VM names. Defaults logically to `<hostedCluster>-<agent.role>`. |
| `spec.template.numCPUs` | no | VM vCPU count. Defaults to `4`. |
| `spec.template.memoryMiB` | no | VM memory in MiB. Defaults to `16384`. |
| `spec.template.diskGiB` | no | Primary disk size in GiB. Defaults to `100`. |
| `spec.agent.role` | no | Value for `hypershift.openshift.io/nodepool-role`. Defaults to `worker`. |
| `spec.agent.labels` | yes | Labels required on discovered Agents. These should match the NodePool Agent selector. |
| `spec.agent.approve` | no | When true, patch matching Agents with `spec.approved=true`. Defaults to `true`. |
| `spec.iso.checkInterval` | no | How often to download and hash the InfraEnv ISO to detect changed bytes behind a stable URL. Defaults to `10m`. |
| `spec.iso.retainVersions` | no | Number of content-addressed ISO objects to keep in the datastore. Defaults to `2`. |
| `spec.iso.pathPrefix` | no | Datastore directory for cached ISO objects. Defaults to `agent-forge/<namespace>/<vsphereAgentPool>`. |

## Status Fields

| Field | Description |
| --- | --- |
| `status.observedGeneration` | Latest CR generation reconciled. |
| `status.agentMachines` | Non-deleting AgentMachines observed for `spec.nodePoolRef` in `spec.controlPlaneNamespace`. |
| `status.waitingAgentMachines` | AgentMachines in the hosted control plane namespace that currently report `Ready=False` and `Reason=NoSuitableAgents`. |
| `status.unreadyAgentMachines` | Observed AgentMachines whose `Ready` condition is not `True`, including machines waiting for suitable Agents and machines still installing. |
| `status.agentMachinesWithoutAgent` | Unready AgentMachines without an assigned Agent. Surplus unbound Agents are retained while this is non-zero. |
| `status.desiredReplicas` | Observed AgentMachine count. |
| `status.matchingAgents` | Agents in the CR namespace matching `spec.agent.labels`. |
| `status.boundAgents` | Matching Agents already bound to CAPI/HostedCluster. |
| `status.availableAgents` | Matching Agents not yet bound. |
| `status.ownedVMs` | VMs created or tracked by this CR, including vSphere BIOS UUID, instance UUID, primary MAC, AgentRef, MachineRef, and phase. The BIOS UUID and MAC are used to match discovered Agents to the VM that actually booted them before any hostname fallback. Phases include `Provisioning`, `Available`, `Bound`, `Released`, and `Orphaned`; orphaned VMs are tracked VMs whose Agent did not appear within the discovery grace period and are eligible for cleanup once current AgentMachine demand is satisfied. |
| `status.iso` | Active cached ISO URL, path, SHA256 digest, size, timestamps, force-refresh token, and retained history. |
| `status.plannedActions` | Latest planned or applied `CreateVM`, `DeleteVM`, `DeleteAgent`, `PatchAgent`, or `Noop` actions. |
| `status.conditions` | `Ready`, `AgentMachineDemandFound`, `InfraEnvAvailable`, `ISOReady`, `CapacitySatisfied`, and `VsphereReady`. |

The ISO cache is content-addressed as `<pathPrefix>/<sha256>.iso`. The operator
downloads and hashes the InfraEnv ISO when the cache is stale, reuses the active
datastore object when the digest is unchanged, and uploads a new object when the
bytes changed or the active datastore object is missing. To force an immediate
refresh, set annotation
`agent-forge.containeroo.ch/force-iso-refresh=<unique-value>` on the
`VsphereAgentPool`.

## Current Implementation Note

The controller includes the CRD, planner, status/condition/event handling, Agent patching, ISO cache reconciliation, and an injectable vSphere provider interface with unit tests. The default provider uses `govc`, which is included in the manager image, to upload cached InfraEnv ISOs, create/power-on VMs, read VM identity after creation, destroy owned VMs during cleanup or scale-down, and prune old ISO objects. For local development outside the image, set `GOVC_PATH` if `govc` is not at `/usr/local/bin/govc`.
