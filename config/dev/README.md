# Development RBAC

Use `role-gateway.example.yaml` with a platform-managed Envoy Gateway when
testing active health checks for individual Java backends. Replace the
`catalog` instance prefix, Gateway reference, listener name and route
hostnames, then apply it in the instance namespace. Verify both routes
report Accepted and ResolvedRefs for that parent listener and the current
generation. Check that the BackendTrafficPolicy reports Accepted for the
same Gateway. The example expects a plaintext Java listener on port 9200. It
does not grant the operator permissions to manage the Gateway or routes.
Restrict the listener to authorized development clients. Both rules
forward all paths, including Java administrative endpoints. Verify a
successful read through each role route on every proxy replica before
exposing a restarted proxy or declaring a new fleet reachable. Envoy can use a slower probe cadence
for clusters that have never carried traffic. [Envoy health-check intervals](https://www.envoyproxy.io/docs/envoy/v1.40.0/api-v3/config/core/v3/health_check.proto.html)

Enable `--development-emergency-failback` to opt into retirement of
temporary emergency Pods. It requires leader election,
`--development-release-gated-pods`, `--development-observe-endpoints`,
the qualified role probes, and current namespaced outage evidence that
explicitly clears all unavailable zones. The development Role grants
`create` on `pods/eviction`.

When a degraded search bootstrap survives, clearing the outage can also
authorize one regular search replacement in the bootstrap AZ. Include
positive `fitByRole.search` evidence for that AZ and its current capacity
profile. The instance also needs an available rolling surge slot and
search warm-up slot. Once regular warm endpoints meet all configured
targets, the controller can evict the bootstrap and then the temporary Pod.

Enable `--development-search-scale-down` for reductions to `replicas`
or `warmSpare` in an existing search zone.
Use leader election, gate release, EndpointSlice observation and
`--development-warmup-image`. Configure the search pool for strict
warm-up and set `spec.availability.maxConcurrentRetirements` to at least
one. The same namespaced Role and EndpointSlice binding cover this path.
The controller records the selected UID in `status.scaleDownRetirement`
and uses the Eviction API. A rollout, retained rollback fleet, visible
disruption or unqualified regular search endpoint holds retirement.
If the original cold-bootstrap Pod is still serving, the controller can
select it first after a reduction. The other strict-warm endpoints must
meet every remaining zonal target and the regional serving floor.
To drain search from a zone, keep its entry and set `replicas`,
`warmSpare`, and `minServing` to zero. Give the destination zones positive
targets. The controller keeps the source's existing positive PDB floor
until strict-warm search endpoints meet the destination targets. This
path retires Ready source endpoints. Unhealthy Pods left in the source
zone can hold the move until disruption recovery clears them.
After an explicit eviction denial, the controller rechecks the Pod and
sets `status.scaleDownRetryAfter` when it can release the claim. Before
rolling back to a manager that predates this feature, let the current
manager finish or release any existing scale-down claim.

The optional `--development-observe-endpoints` flag checks the stable
Services and EndpointSlices for all configured roles with requested Pods.
Apply `endpoint-observation-rbac.yaml` separately in the watched namespace
to give the operator ServiceAccount EndpointSlice list access.
`Available=True` means all observed roles met their configured zonal and
regional floors, with at least one Ready endpoint per role, after excluding
Pods marked for disruption. A verified shortfall reports `False`. Missing
or conflicting evidence, disabled observation and active operations
report `Unknown`. An instance with zero requested serving Pods reports
`False`. Spec or storage errors have their own reasons.

The operator normally samples at one-minute intervals and records the
sample time and a rotating role detail in `status.lastEndpointObservation`.
The detail's result uses the same floors as `Available`. Its Ready counts
include Pods marked for disruption. The status records both sampled fleet
IDs. During reconciliation, a fleet change or a sample age of two
minutes invalidates a positive observation. Check the sample timestamp
when reconciliation is delayed.
Each observation uses Pod zone labels, with a shared five-second time
budget and limits of 512 Pods or endpoints and 128 slices per role.
Use Java and edge observations separately for process health and traffic.

For an isolated plaintext development runtime, `--development-service-probe-image` accepts the exact digest-pinned Java image and requires gated Pod creation. Normal reconciliation records one historical role-Service sample per minute in `status.lastServiceProbe`, rotating between configured roles. A successful sample matches one stats response and valid lease to an owned Running/Ready Pod, then rechecks the Pod and Service identities and versions. `Unavailable` replaces the prior sample when the observation fails. The default development Role already grants the required namespaced Pod and Service reads. This HTTP response is unauthenticated and needs an isolated namespace and network. A single sample does not establish all backend health, actual AZ, cache warmth, ingress or ALB convergence, or permission to promote. HTTP TLS and security-configured instances are not probed by this path; old status can remain if sampling stops.

The external S3 repository ConfigMap supplies public bucket, region and optional endpoint settings. The endpoint can be an HTTP(S) URL or a host and optional port; URL user information, query strings, fragments and invalid hosts or ports are rejected before they can be pinned. The endpoint path is public too: do not put credentials or tokens in it. `pathStyleAccess` parsing errors do not echo the supplied value. The operator does not create the bucket or establish provider compatibility.

This Kustomize overlay describes permissions for the development manager, which records status and creates stable role Services. Change the example namespace for a disposable development environment and supply the same value to `--watch-namespace`. This is not an installation package. Deployment manifests, workload permissions and API-server authorization tests remain part of M01 and M04.

For development only, `--development-create-gated-pods` enables direct Pod creation. It creates a PDB for each role and zone, waits for each budget to be observed, then persists an intent and creates or recovers a single owned Pod. `--development-release-gated-pods` enables gate release when no other instance Pod is unbound. Both flags are off by default. Set positive CPU and memory requests and a JVM heap. A spec change during an outstanding intent stops that intent for recovery. The development path supports opt-in HTTP and peer TLS and requires an isolated environment. Application security and production rollout qualification remain open. [Managed HTTP TLS](#managed-development-http-tls)

If you enable OCI plugin installation, apply `config/dev/plugin-staging-rbac.yaml` in the watched namespace before starting the operator. The separate Role adds ConfigMap creation for immutable plugin request and policy snapshots. Without it, plugin-enabled Pods remain pending creation after the controller records their intent. [OCI plugin setup](../../docs/design/oci-plugins.md#development-installer)

The development path accepts a same-namespace immutable Secret containing the `opensearch.keystore` binary, mounted read-only only into OpenSearch. The shell requires an unencrypted keystore. The controller checks and records the Secret UID before creation and gate release, without recording its contents. Kubernetes mounts the Secret by name, so deletion and recreation after the UID check can still change what the kubelet mounts. The development Role's `get` permission permits reading Secrets throughout its namespace; enforce an appropriate namespace trust boundary. Rotation, reference authorization, stronger identity binding and Kubernetes startup qualification remain unfinished.

The development Pod uses the image's `health-probe` executable to check local Java readiness, the expected node name and a valid local lease with a five-second margin. Build the Java image with the accompanying Go build stage so that executable exists. This does not establish peer reachability or strict cache warmth.

### Managed development HTTP TLS

Set `--development-http-tls-image` and `--development-peer-dns-image`
to the same qualified runtime digest and set `--development-cluster-domain`.
Build the image with the HTTP TLS Java provider and the bundled health
probe. Configure TLS before the instance's first Pod creation.

```yaml
spec:
  tls:
    http:
      source: Secret
      certificateRef:
        name: http-leaf
      trustBundleRef:
        name: http-roots
        key: ca.crt
```

Create both Secrets in the instance namespace with `immutable: true`.
The certificate Secret uses `kubernetes.io/tls`. Its certificate needs
`serverAuth` and SAN coverage for the generated Pod DNS names. For one
certificate shared by this instance, use a wildcard such as
`*.peer-<hash>.<namespace>.svc.<cluster-domain>`. `<hash>` is the first
20 lowercase hexadecimal characters of SHA-256 over the instance UID.
The operator can wait for the referenced Secrets while the certificate
is issued. Bring the CA chain in the trust Secret and keep CA private
keys outside the Pod.

The controller pins both Secret UIDs and the public file hashes before
creation. The API rejects a change of HTTP TLS mode after provisioning
starts. Java verifies the mounted certificate bytes and key pair at
startup. The kubelet probe dials the Pod IP and verifies the pinned CA
bundle and the Pod DNS identity. A new Secret UID holds further Pod
creation and release. The kubelet resolves Secrets by name. Runtime
verification binds the mounted certificate and CA bytes to the baseline.
This path provides HTTPS server identity. Basic Auth activation, live
TLS rotation, and Gateway backend TLS qualification still need separate
support.

For development warm-on-join, use the qualified Java image with the
warm-up observation overlay and set `--development-warmup-image` to its
digest. The controller reserves one search warm-up slot before releasing
a Pod. A zero `maxConcurrentSearchWarmups` holds release. It clears the
slot after two matching observations that the same JVM's worker has
finished, or after that Pod disappears. A readiness timeout alone keeps
the slot occupied. A worker that finishes in a degraded, failed or
timed-out state can release its slot. Strict search readiness still
requires successful completion without a timeout.

With managed HTTP TLS enabled, the observer connects to the Pod IP and
verifies the per-Pod DNS name using the pinned CA Secret. The qualified
Java image must report its process incarnation in both readiness and
stats. TLS or identity failures leave the slot unchanged. The controller
caps operator-started warm-ups at one. Unplanned JVM restarts can still
start additional work, and completion describes the worker's result at
the time of observation. [Warm-up verification](../../docs/implementation/verification.md#strict-search-warm-up-over-https)

The development `status.zoneCapacity` sample reports each role/AZ’s desired replicas, spare target and `minServing`, along with active, unbound, terminating and terminal Pod counts, Kubernetes Ready count, desired Ready shortfall, and whether the Ready count is below `minServing`. `zoneCapacityObservedAt` and `zoneCapacityObservedGeneration` identify the last successful sample; an unchanged sample is refreshed at most once per minute while reconciliation runs. The AZ is taken from the operator’s Pod label, not verified against a Node. A shortfall does not identify an AZ failure, a traffic outage or cache warmth. Requested spare warmth remains `Unknown` and old samples can remain in status if observation stops.

PDBs use the configured zonal `minServing` and Kubernetes Pod readiness. They do not enforce a role-wide floor across zones or prove a search cache is warm. Provider-forced disruption can also exceed a PDB. Changes to a budget's owner or selector are reported as conflicts instead of being overwritten; obsolete budgets are not yet garbage-collected independently of the instance.

### Provisioning holds and retry

The development pending timeout defaults to ten minutes and can be changed with `--development-pending-timeout`. It begins when release is recorded. On expiry the controller persists a withdrawal intent and deletes only the still-unbound Pod using UID and resource-version preconditions. Once the Pod is gone or terminating, the controller records a hold for its role and AZ and frees the active slot for other groups. A Pod observed bound before withdrawal is retained. One pending Pod can still cause multiple provider launches, and withdrawal cannot undo nodes already created.

When several role/AZ groups have a desired Pod deficit, the development controller first chooses among groups whose Kubernetes Ready count is below `minServing`, then uses deterministic role/AZ ordering. Blocked and rate-limited groups remain excluded. This is a creation priority based on Pod status, not a guarantee of Java eligibility, warm cache, traffic routing or PDB admission. It does not create surge above a group’s desired Pod count to compensate for existing unready Pods.

The development controller also limits gate-release intents per role and AZ, across fleets. `--development-release-limit` defaults to 32 releases and accepts 1–64; `--development-release-window` defaults to ten minutes and accepts one second through one hour. A release reserves budget in instance status before the gate is removed, including when the API result is ambiguous. A `ProvisioningRateLimited` condition reports a held gate. History retains up to 256 releases across the instance for one hour so increasing the window does not erase recent attempts; a full history holds further releases until space expires. Future-dated or malformed history fails closed. These settings also limit initial scale-out and disaster recovery; select them for the deployment and keep controller clocks synchronized. Before creating a Pod, the controller skips rate-limited groups when another deficient group can proceed. An already-held Pod still occupies the single instance-wide pending slot, so a later budget change or a scheduling failure can delay other groups. This limits operator release attempts, not the number of nodes a provider might launch for one request or actions by other workloads.

For all released Pods, binding does not release the provisioning slot until the Pod reports Running and Ready. Plugin-enabled Pods must first report a successful installer init container. A bound Pod that has not completed installation within the development pending timeout records `PluginInstallTimeout` and remains in place. The controller moves the hold to that role and AZ, allowing other groups to progress. It continues observing the same Pod and clears its group hold only after the required initialization succeeds and the Pod becomes Running and Ready. If it disappears, the hold becomes `PodGone` and requires the explicit retry procedure below. This limit cannot cancel provider launches already underway.

After initialization, the controller uses the same timeout for a separate startup phase. A Pod that remains unready records `StartupTimeout` and moves to a role/AZ hold while it may recover. A terminal Pod records `PodTerminal`, remains available for inspection, and is moved to a role/AZ hold so other groups can proceed. Readiness of the same Pod clears a startup timeout. Retrying a terminal group requires the Pod name to be absent and an explicit operation as described below. If a terminal search Pod still owns the warm-up slot, that slot remains held until its existing recovery rules clear it. Readiness here is a Kubernetes observation, not proof of cache warmth or durable application health. The release budget also limits repeated replacements after a Pod has reached Ready, but serial startup and the rate limit can delay capacity restoration.

After correcting the platform issue, an isolated development installation can enable `--development-retry-provisioning` and submit a `RetryProvisioning` operation with `instanceRef`, `provisioningRetry.podUID` set to the blocked attempt's UID, and `provisioningRetry.acknowledgeNewAttempt: true`. The operation waits for that Pod name to be absent, serializes with other instance operations, and clears only the matching `PodGone`, `PodTerminating`, or `PodTerminal` hold. Its finalizer finishes a claimed retry even if the operation is deleted; keep the flag enabled until in-flight retries finish. It clears only the acknowledged Pod’s hold. The controller then applies its normal profile, fit and pending-demand checks to any new Pod. The acknowledgement authorizes a new attempt; it is not proof that the platform issue is fixed. An existing hold without role and AZ identity remains instance-wide. Bound Pods stuck in startup, DNS preflight or installation are observed in their role/AZ holds; legacy attempts lacking role and AZ remain instance-wide. Holds caused by identity conflicts or invalid state need a separate recovery procedure, which is not implemented.

An isolated development installation can opt into `ApplyClusterSettings` with `--development-settings-image` set to the exact runtime image digest. The current qualified operation accepts only `serverless.bucket` set to `auto` or `off`, or removal of that key. It uses the owned ingest Service over unauthenticated HTTP; TLS and security providers are not supported by this path. The namespace must prevent other writers from changing operator-managed keys. The operation persists the prior value and Service UID before writing, checks read-back after a failed or successful write, and holds on a conflicting value. A `Persisted` operation keeps the instance claim because fleet-wide effect verification and claim release are not implemented. This is not a production administration interface.
