# Provider architecture

The management cluster remains the only Kubernetes API used by the operator.
HyperShift creates CAPI Machines and AgentMachines for a NodePool. CAPI handles
node drain, Agent reservation, binding, installation, and reclaim. Agent Forge
creates the infrastructure needed to satisfy `NoSuitableAgents` demand.

The shared AgentMachine controller matches the control-plane namespace and the
`hypershift.openshift.io/nodePool` annotation to exactly one infrastructure pool.
Ambiguous matches across **both** providers fail closed. An optimistic-lock
annotation claims demand for a pool UID, and the existing per-AgentMachine label
pins Agent selection. A claim cannot be stolen after a pool is replaced.

The existing vSphere CRDs, provider, planner, cleanup policies, and optional media
ejection remain available. Shared code covers InfraEnv readiness/version checking,
bounded ISO download and SHA-256 hashing, Agent selector pinning, and atomic
Agent labels/hostname/approval updates. The vSphere controller excludes Agents
identified as owned by OpenStack workers. No OpenStack worker uses vSphere's
ISO ejection or surplus-VM planner.

## OpenStack resources and ownership

The pool controller reports capacity and waits for child finalizers on deletion.
Worker creation is one-to-one with an unassigned AgentMachine reporting
`NoSuitableAgents`. Its UID determines the Kubernetes worker name. CAPI demand,
not a transient replica-count discrepancy, drives scale-down.

Each worker snapshots pool configuration, the pool UID, AgentMachine UID, and
CAPI Machine owner reference. The immutable snapshot permits cleanup after the
pool disappears and prevents later placement edits from moving installed VMs.
The Secret's contents may rotate; an authenticated cloud/project/region hash
prevents that rotation from redirecting existing resources into another scope.

External names derive from the worker UID. Nova and Cinder metadata and the
Neutron port description contain explicit ownership markers. Gets and deletes
verify ownership and project. No matching-by-name-only adoption is allowed.
A missing recorded resource is an error, never permission to rebuild a worker.

Provisioning advances through these observations:

1. Create a dedicated port and persist its ID and MAC address.
2. Create a blank Cinder OS volume; wait for availability and set bootable.
3. Download and hash the current InfraEnv ISO using the bounded streaming
   downloader. Persist its content identity before creating cache resources.
4. Acquire a shared Glance image reference; create a private `iso`/`bare` image
   with a deterministic UUID, upload if queued, and wait for `active`.
5. Create a per-worker Cinder volume from that image. Its size is the maximum of
   the configured minimum and the actual ISO bytes rounded up to GiB.
6. Create Nova with the existing port and both Cinder volumes. The OS disk is
   `disk`, boot index **0**, and defaults to `virtio`; the discovery device is
   `cdrom`, boot index **1**, and defaults to `ide`. Both map volume to volume
   with `delete_on_termination=false`.
7. Match exactly one discovered Agent by any inventory NIC MAC, or Nova UUID in
   DMI product UUID/serial number. Hostnames and IP addresses are not identities.
8. Verify the chosen installation disk's path, eligibility, and Cinder serial;
   set `spec.installation_disk_id` in the same optimistic-lock patch as approval
   and the AgentMachine selection label. CAPI binds and installs the worker.
9. Report Ready when the correct AgentMachine is bound and `Installed=True`.
   Reconciliation observes the server and Agent; it does not refresh the ISO,
   change power state, rebuild Nova, or remove media.

No hosted-cluster credentials, node cordoning, taint changes, automatic media
rotation, or post-installation power cycle are part of this controller.

## Recovery and concurrency

Every potentially non-idempotent POST is preceded by a durable `pendingCreate`
record. Returned IDs are persisted before advancing. After an ambiguous response
or restart, reconciliation searches for a unique exact name and owner match.
It does **not** replay POST when absence could be eventual consistency. The
`CreateOutcomeUnknown` condition requests investigation if no resource appears.
Definitively rejected requests clear the journal and can be retried after correction.
Glance supports caller-supplied UUIDs, so image creation is naturally idempotent.

The image cache key hashes authenticated scope, InfraEnv UID, and ISO SHA-256.
A namespace-scoped ConfigMap tracks worker UID references; its own UID owns the
Glance image. All acquisition/release changes use resourceVersion checks.
The final releaser first persists a `Deleting` tombstone, excluding new references,
then deletes the owned image and the ConfigMap. A finalizer retry resumes collection.
Worker references remain until their discovery volumes have been removed. Before
any discovery-volume or instance creation, an obsolete queued image reference may
also be released during snapshot recovery.
Images are not shared across unrelated InfraEnvs, namespaces, or cloud scopes.
Every new worker hashes the source ISO, but existing active images avoid repeated
Glance uploads. The downloaded file is kept through the identity checkpoints and
uploaded in the same reconciliation, so the hashed and uploaded bytes are identical.
If a restart loses the file, the controller observes Glance before downloading:
active images are reused and saving/importing images are left to finish. A queued
image whose replacement download differs triggers `RefreshingDiscoveryImage`.
That phase is a durable journal: release the obsolete reference, finish any last
reference GC, clear the old identity, then download and upload one fresh snapshot.
The journal survives errors and restarts, and blocks refresh if a discovery volume,
instance, Agent, or unresolved resource-creation request exists. The port and OS
volume are preserved. Running workers never enter this recovery path.
There is no credential-bearing URL in cache status or ConfigMaps.

Use the shipped deployment's leader election. Independent workers can progress
concurrently; shared cache updates and demand claims use Kubernetes optimistic
locking. The SDK applies request deadlines and cancellation. Its global request
limiter returns a retryable result instead of sleeping. Transient failures use
controller-runtime exponential backoff; asynchronous states requeue normally.
Authenticated clients are cached for at most five minutes and never beyond token
expiry, with Secret resourceVersion changes invalidating the cache immediately.

## Deletion

Deleting a worker or pool does not bypass CAPI. Under `cleanupPolicy: Delete`:

1. Wait for the original AgentMachine and original CAPI Machine to disappear.
2. Refuse cleanup while a matching Agent is still reserved or bound. Unapprove
   unbound Agents with an optimistic lock to exclude new reservations.
3. Delete Nova and observe its absence.
4. Wait until Cinder confirms the discovery volume is unattached; delete it and
   observe absence. Never force deletion or reset Cinder state.
5. Delete the unattached OS volume unless `osVolumePolicy: Retain`.
6. Delete the unbound owned Neutron port.
7. Release the Glance reference and collect the image only when unreferenced.
8. Delete the matching unbound Agent and remove the worker finalizer.

`cleanupPolicy: Retain` deliberately leaves all external resources and the cache
reference as a retention record, while allowing Kubernetes bookkeeping deletion.
`template.osVolumePolicy: Retain` retains only OS storage; discovery media is still
cleaned up. These policies are captured when the worker is created.
Do not remove finalizers or edit cache ConfigMaps to bypass failed cleanup:
forced deletion can orphan infrastructure and its sensitive bootstrap material.
There is no background sweep that deletes resources lacking ownership evidence.
