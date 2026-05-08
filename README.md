# agent-forge-operator

Agent Forge Operator bridges hosted-cluster autoscaling to VM capacity for
HyperShift Agent platform clusters on vSphere.

The hosted cluster autoscaler remains the source of truth. It scales the CAPI
`MachineSet` rendered for a HyperShift `NodePool`; Agent Forge watches that
`MachineSet` and ensures enough matching Assisted Installer `Agent` objects can
exist by creating or deleting vSphere VMs.

## What It Does

- Watches a namespace-scoped `VsphereAgentPool` custom resource.
- Discovers or follows the CAPI `MachineSet` for one HyperShift `NodePool`.
- Reads the `MachineSet.spec.replicas` value selected by the hosted cluster
  autoscaler.
- Creates vSphere VMs from an `InfraEnv` discovery ISO when more Agents are
  needed.
- Optionally approves matching Assisted Installer `Agent` objects.
- Deletes only owned VMs and stale unbound Agents when scale-down is allowed.
- Supports dry-run mode, status conditions, planned actions, and Kubernetes
  Events for operational visibility.

## Current Scope

Agent Forge is designed for OpenShift environments that use:

- HyperShift hosted clusters on the Agent platform.
- Assisted Installer `InfraEnv` and `Agent` resources.
- CAPI `MachineSet` resources rendered for hosted cluster NodePools.
- vSphere as the VM provider.

It does not replace the hosted cluster autoscaler and does not scale NodePools
directly. It reacts to the MachineSet demand that already exists.

## Installation

Install the latest release manifests:

```sh
kubectl apply -f https://github.com/containeroo/agent-forge-operator/releases/download/v0.0.5/crds.yaml
kubectl apply -k github.com/containeroo/agent-forge-operator//config/default?ref=v0.0.5
```

The published manager images are:

```text
ghcr.io/containeroo/agent-forge-operator:v0.0.5
containeroo/agent-forge-operator:v0.0.5
```

For a local image build:

```sh
make docker-build docker-push IMG=<registry>/agent-forge-operator:<tag>
make deploy IMG=<registry>/agent-forge-operator:<tag>
```

## Getting Started

Start with [docs/getting-started.md](docs/getting-started.md). It covers the
required cluster objects, vSphere Secret, a dry-run `VsphereAgentPool`, status
inspection, and switching from dry-run to active reconciliation.

For the complete CRD field contract and status model, see
[docs/vsphereagentpool-crd.md](docs/vsphereagentpool-crd.md).

## Example

```yaml
apiVersion: agentforge.containeroo.ch/v1alpha1
kind: VsphereAgentPool
metadata:
  name: demo-worker
  namespace: demo
spec:
  dryRun: true
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
  scaling:
    bufferAgents: 0
    maxProvisioning: 3
    deletePolicy: OwnedOnly
  iso:
    checkInterval: 10m
    retainVersions: 2
    pathPrefix: agent-forge/demo/demo-worker
```

The controller caches the InfraEnv discovery ISO by content digest. It downloads
and hashes the ISO at `spec.iso.checkInterval`, uploads a new `<sha256>.iso`
object only when the bytes changed or the datastore object is missing, and
inserts the active `status.iso.path` into every new VM. To force an immediate
refresh, annotate the CR with
`agentforge.containeroo.ch/force-iso-refresh=<unique-value>`.

## Development

Requirements:

- Go 1.26 or newer.
- `kubectl` or `oc`.
- Docker or another compatible container tool.
- Access to a Kubernetes or OpenShift cluster for deployment tests.

Common commands:

```sh
make test
make lint
make manifests
make crd
make deploy IMG=<registry>/agent-forge-operator:<tag>
```

Run locally against the active kubeconfig:

```sh
make install
make run
```

The controller uses `govc` for vSphere operations. The container image includes
`govc`; local `make run` expects `govc` at `/usr/local/bin/govc` unless
`GOVC_PATH` is set.

## Release

Releases are built by GoReleaser from pushed tags:

```sh
git tag v0.0.5
git push origin v0.0.5
```

The release workflow publishes multi-architecture images to GHCR and DockerHub
and attaches `crds.yaml` to the GitHub release.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
