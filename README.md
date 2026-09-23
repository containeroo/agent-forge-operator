# agent-forge-operator

Agent Forge Operator bridges hosted-cluster autoscaling to VM capacity for
HyperShift Agent platform clusters on vSphere.

HyperShift and CAPI remain the source of truth. Agent Forge watches the
`AgentMachine` objects rendered for a HyperShift `NodePool`; when they report
`Ready=False` with `Reason=NoSuitableAgents`, it creates vSphere VMs so matching
Assisted Installer `Agent` objects can appear.

## Discovery ISO lifecycle

Automatic ejection is **disabled by default**. Opt in each `VsphereAgentPool`
explicitly, starting with a canary pool:

```yaml
spec:
  iso:
    ejectAfterInstall: true
```

Omitting the field or setting it to `false` preserves the existing media behavior.
Enabling it includes existing installed workers in that pool. Disabling it stops
new ejections and does not reattach media; a previously recorded operation still
finishes, including its CD-lock confirmation. ISO caching and pruning remain
independent, so a pool without ejection enabled can still report ISO pruning errors.

For opted-in pools, the operator disconnects and ejects cached media when the matching
Assisted Installer `Agent` reports `Installed=True`. This condition covers both
`installed` and `added-to-existing-cluster`; assignment (`Bound`) is not sufficient.
Red Hat documents [removing discovery media after installation writes the OS and
reboots](https://docs.redhat.com/en/documentation/openshift_container_platform/4.20/html-single/installing_on_a_single_node/installing_on_a_single_node#install-sno-installing-with-assisted-installer_install-sno-installing-sno).
Waiting for installation completion is the operator's conservative automation policy.

Ejection verifies VM ownership and hardware identity and only touches
content-addressed ISO files in the pool's configured ISO datastore and path prefix.
It also handles existing installed workers. Unrelated media is left attached.
If the guest locks the CD door, the operator answers only the specific vSphere
CD-ROM disconnect lock question for its recorded disconnect operation; it does not enable
global automatic answers. Failures are retried and reported in the `ISOEjected`
condition (`Disabled`, `WaitingForInstallation`, `EjectionInProgress`, `Ejected`,
or `EjectionFailed`) and ISO operation metrics. The last successful `Ejected`
condition is preserved if the feature is subsequently disabled.
Before disconnecting, `status.isoEjection` records the VM UUID, ownership, datacenter,
device key, and exact ISO path. It checkpoints the question ID before answering.
After a restart, recovery resumes only that recorded operation, refuses changed
media or a different question, and clears the record only after success. Apply
updated CRDs together with the controller so these recovery fields are preserved.
Normal ISO retention removes the unused file
on a later refresh after every VM sharing it releases it.

Scale-down remains controlled by CAPI: drain, Agent unbind/reclaim, Machine deletion,
then VM and Agent cleanup. The operator does not start new ejections for a Machine already
being deleted or a released VM. A previously recorded ejection is finished first. Assisted Installer's automatic reclaim path
[downloads discovery boot artifacts to the node's disk and creates a boot entry](https://github.com/openshift/assisted-installer-agent/blob/master/src/commands/actions/download_boot_artifacts_cmd.go)
before rebooting; this path does not require the original virtual CD.
CAPI's deletion hook still waits for reclaim completion (or its failure state).
Environments that rely on manually booting discovery media to reuse hosts must
reattach an ISO for that workflow. Ejection does not bypass drain or deletion hooks.

## Documentation

User documentation is published on the containeroo website:

- [agent-forge-operator docs](https://containeroo.ch/docs/agent-forge-operator/)
- [Installation](https://containeroo.ch/docs/agent-forge-operator/installation/)
- [Get Started](https://containeroo.ch/docs/agent-forge-operator/get_started/)
- [API Reference](https://containeroo.ch/docs/agent-forge-operator/api_reference/)

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
make install
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

`make test-vcsim` exercises ISO caching and VM status/deletion against the vSphere
simulator. `make test-e2e` requires Docker and uses Kind to verify deployment,
metrics access, and a VsphereAgent status update. It installs minimal external
CRD fixtures for watches; it does not run HyperShift, CAPI, or Assisted Installer.
