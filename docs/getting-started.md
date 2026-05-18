# Getting Started

This guide walks through a first `VsphereAgentPool` and active VM
reconciliation.

## Assumptions

You already have:

- An OpenShift management cluster with HyperShift and Assisted Installer
  resources.
- A hosted cluster that uses the Agent platform.
- A `NodePool` for the hosted cluster.
- CAPI `AgentMachine` and `Machine` objects rendered for that NodePool in the
  hosted control plane namespace.
- An `InfraEnv` in the hosted cluster namespace with
  `status.isoDownloadURL` populated.
- vSphere credentials that can upload the discovery ISO, create VMs, power on
  VMs, and destroy operator-owned VMs.

The examples use:

| Value                 | Meaning                                                                    |
| --------------------- | -------------------------------------------------------------------------- |
| `demo`                | Hosted cluster namespace and HostedCluster name.                           |
| `demo-worker`         | NodePool name and `VsphereAgentPool` name.                                 |
| `demo-demo`           | Hosted control plane namespace containing CAPI AgentMachines and Machines. |
| `vsphere-credentials` | Secret with vSphere credentials.                                           |

Adjust the names for your environment.

## 1. Install the Operator

Install CRDs from a release:

```sh
kubectl apply -f https://github.com/containeroo/agent-forge-operator/releases/download/v0.0.15/crds.yaml
```

Deploy the controller:

```sh
kubectl apply -k github.com/containeroo/agent-forge-operator//config/default?ref=v0.0.15
```

Check that the manager is running:

```sh
kubectl -n agent-forge-operator-system get deploy,pods
```

For a locally built image, use:

```sh
make docker-build docker-push IMG=<registry>/agent-forge-operator:<tag>
make deploy IMG=<registry>/agent-forge-operator:<tag>
```

## 2. Confirm AgentMachine Demand

The operator watches CAPI `AgentMachine` objects that HyperShift/CAPI created
for the NodePool. It creates VMs only when an AgentMachine reports
`Ready=False` with `Reason=NoSuitableAgents`.

List AgentMachines in the hosted control plane namespace:

```sh
kubectl -n demo-demo get agentmachines.capi-provider.agent-install.openshift.io
```

Inspect AgentMachines that belong to the NodePool:

```sh
kubectl -n demo-demo get agentmachines.capi-provider.agent-install.openshift.io -o yaml \
  | yq '.items[] | select(.metadata.annotations."hypershift.openshift.io/nodePool" == "demo/demo-worker") | .metadata.name'
```

If you do not use `yq`, inspect the YAML manually:

```sh
kubectl -n demo-demo get agentmachines.capi-provider.agent-install.openshift.io -o yaml
```

## 3. Confirm the InfraEnv

The `InfraEnv` must expose a discovery ISO URL:

```sh
kubectl -n demo get infraenv demo -o jsonpath='{.status.isoDownloadURL}{"\n"}'
```

If the value is empty, fix the InfraEnv before enabling active reconciliation.
The operator cannot create bootable discovery VMs without that ISO URL.

## 4. Create the vSphere Secret

Create a Secret in the same namespace as the `VsphereAgentPool`:

```sh
kubectl -n demo create secret generic vsphere-credentials \
  --from-literal=server='vcenter.example.com' \
  --from-literal=username='administrator@example.com' \
  --from-literal=password='<password>' \
  --from-literal=insecure='false'
```

Required keys:

| Key        | Description                                                       |
| ---------- | ----------------------------------------------------------------- |
| `server`   | vCenter server hostname or URL accepted by `govc`.                |
| `username` | vSphere username.                                                 |
| `password` | vSphere password.                                                 |
| `insecure` | Optional. Set to `true` to skip vCenter certificate verification. |

To keep the Secret in another namespace, set
`spec.vsphere.credentialsSecretRef.namespace`.

## 5. Create a VsphereAgentPool

```yaml
apiVersion: agent-forge.containeroo.ch/v1alpha1
kind: VsphereAgentPool
metadata:
  name: demo-worker
  namespace: demo
spec:
  hostedClusterRef:
    name: demo
  nodePoolRef:
    name: demo-worker
  infraEnvRef:
    name: demo
  controlPlaneNamespace: demo-demo
  vsphere:
    credentialsSecretRef:
      name: vsphere-credentials
    datacenter: dc1
    datastoreCluster: workload-datastore-cluster
    isoDatastore: iso-datastore
    resourcePool: cluster/Resources
    folder: demo
    network: VM Network
    guestID: rhel9_64Guest
    scsiType: pvscsi
    firmware: efi
    networkAdapterType: vmxnet3
  template:
    namePrefix: demo-worker
    numCPUs: 4
    memoryMiB: 16384
    diskGiB: 100
  agent:
    role: worker
    approve: true
    labels:
      agentclusterinstalls.extensions.hive.openshift.io/location: lab-a
      customer: example
      hypershift.openshift.io/nodepool-role: worker
  iso:
    checkInterval: 10m
    retainVersions: 2
    pathPrefix: agent-forge/demo/demo-worker
```

Apply it:

```sh
kubectl apply -f vsphereagentpool.yaml
```

You can also start from the sample:

```sh
kubectl apply -k config/samples/
```

## 6. Inspect the Plan

Check the resource summary:

```sh
kubectl -n demo get vsphereagentpool
kubectl -n demo describe vsphereagentpool demo-worker
```

Inspect status:

```sh
kubectl -n demo get vsphereagentpool demo-worker -o yaml
```

Useful status fields:

| Field                         | What to check                                                                                              |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `status.agentMachines`        | Non-deleting AgentMachines observed for this NodePool in `spec.controlPlaneNamespace`.                     |
| `status.waitingAgentMachines` | AgentMachines currently reporting `Ready=False` and `Reason=NoSuitableAgents`.                             |
| `status.unreadyAgentMachines` | Observed AgentMachines whose `Ready` condition is not `True`.                                              |
| `status.agentMachinesWithoutAgent` | Unready AgentMachines without an assigned Agent. Surplus unbound Agents are retained while this is non-zero. |
| `status.desiredReplicas`      | Observed AgentMachine count.                                                                              |
| `status.matchingAgents`       | Agents that already match `spec.agent.labels`.                                                             |
| `status.availableAgents`      | Matching Agents that are not yet bound to CAPI.                                                            |
| `status.ownedVMs[*].biosUUID` | vSphere BIOS UUID used to match discovered Agents to the VM that actually booted them.                     |
| `status.ownedVMs[*].macAddress` | Primary VM NIC MAC used as a fallback Agent-to-VM identity match.                                        |
| `status.ownedVMs[*].machineRef` | CAPI Machine paired with a bound or deleting VM. It is retained during Machine deletion until cleanup.    |
| `status.iso.path`             | Active content-addressed ISO datastore path used for new VMs.                                              |
| `status.iso.sha256`           | SHA256 digest of the active InfraEnv ISO bytes.                                                            |
| `status.iso.checkedAt`        | Last time the operator downloaded and hashed the ISO.                                                      |
| `status.plannedActions`       | Planned `CreateVM`, `DeleteVM`, `DeleteAgent`, `PatchAgent`, or `Noop` actions.                            |
| `status.conditions`           | Readiness, AgentMachine demand, InfraEnv availability, ISO cache state, and capacity state.                |

Production rollouts can set `spec.cleanupPolicy: Retain` to prevent automatic
deletion of stale owned VMs and unbound Agents while still allowing the operator
to create and prepare new capacity. The default `Delete` policy preserves the
normal automated cleanup behavior.

Check Events:

```sh
kubectl -n demo get events --field-selector involvedObject.name=demo-worker --sort-by=.lastTimestamp
```

Metrics are exposed on the controller manager metrics endpoint:

| Metric | Meaning |
| ------ | ------- |
| `agent_forge_vsphere_vm_operations_total` | VM create/delete attempts by operation and result. |
| `agent_forge_iso_operations_total` | ISO cache ensure/delete attempts by operation and result. |
| `agent_forge_pool_capacity` | Per-pool capacity gauges for desired, waiting, available, pending, and planned-create counts. |

## 7. Reconciliation Behavior

The operator will:

- Download and hash the InfraEnv discovery ISO when the cache is stale.
- Upload a content-addressed ISO object only when the bytes changed or the datastore object is missing.
- Create VMs when AgentMachines report `NoSuitableAgents` and demand exceeds
  available matching Agents.
- Record created VM identity from vSphere, including BIOS UUID, instance UUID,
  and primary MAC address.
- Power on created VMs.
- Match discovered Agents to owned VMs by BIOS UUID or MAC before assigning the
  Agent hostname.
- Patch matching Agents with labels, role, approval, and the VM-name hostname
  when configured.
- Delete owned VMs and stale unbound Agents during scale-down.

Force an immediate ISO refresh without changing the spec:

```sh
kubectl -n demo annotate vsphereagentpool demo-worker \
  agent-forge.containeroo.ch/force-iso-refresh="$(date -Iseconds)" \
  --overwrite
```

## 8. Troubleshooting

AgentMachine demand is not observed:

```sh
kubectl -n demo-demo get agentmachines.capi-provider.agent-install.openshift.io -o yaml
kubectl -n demo get vsphereagentpool demo-worker -o jsonpath='{.status.conditions[?(@.type=="AgentMachineDemandFound")]}{"\n"}'
```

InfraEnv ISO is unavailable:

```sh
kubectl -n demo get infraenv demo -o yaml
kubectl -n demo get vsphereagentpool demo-worker -o jsonpath='{.status.conditions[?(@.type=="InfraEnvAvailable")]}{"\n"}'
```

No Agents match:

```sh
kubectl -n demo get agents -o yaml
kubectl -n demo get vsphereagentpool demo-worker -o jsonpath='{.status.matchingAgents}{"\n"}'
```

VM names, Agent hostnames, or identities do not match:

```sh
kubectl -n demo get vsphereagentpool demo-worker \
  -o jsonpath='{range .status.ownedVMs[*]}{.name}{"\t"}{.biosUUID}{"\t"}{.macAddress}{"\t"}{.agentRef.name}{"\n"}{end}'

kubectl -n demo get agents.agent-install.openshift.io \
  -o custom-columns=NAME:.metadata.name,HOST:.spec.hostname,BIOS:.status.inventory.systemVendor.serialNumber,MAC:.status.inventory.interfaces[0].macAddress
```

The VM name in `status.ownedVMs[*].name` should match the Agent
`spec.hostname`. The BIOS UUID and MAC address should describe the same vSphere
VM. New VMs record this identity immediately after creation; adopted VMs get it
from the Agent inventory.

vSphere errors:

```sh
kubectl -n demo describe vsphereagentpool demo-worker
kubectl -n agent-forge-operator-system logs deploy/agent-forge-operator-controller-manager -c manager
```

## 9. Clean Up

Delete the bridge:

```sh
kubectl -n demo delete vsphereagentpool demo-worker
```

Uninstall the operator:

```sh
kubectl delete -k github.com/containeroo/agent-forge-operator//config/default?ref=v0.0.15
kubectl delete -f https://github.com/containeroo/agent-forge-operator/releases/download/v0.0.15/crds.yaml
```
