# agent-forge-operator

Agent Forge Operator bridges hosted-cluster autoscaling to VM capacity for
HyperShift Agent platform clusters on vSphere.

HyperShift and CAPI remain the source of truth. Agent Forge watches the
`AgentMachine` objects rendered for a HyperShift `NodePool`; when they report
`Ready=False` with `Reason=NoSuitableAgents`, it creates vSphere VMs so matching
Assisted Installer `Agent` objects can appear.

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
