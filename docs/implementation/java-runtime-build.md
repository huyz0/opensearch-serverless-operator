# Java runtime bundle development

`tools/prepare_java_runtime.py` prepares a fresh checkout from the pinned Git
commit, applies the warm-up, peer DNS, web-identity, local lease and route
process observation overlays and the writer admission gate, then builds with
the peer-TLS source. Clean offline
bundle builds passed on Windows with JDK 25 and a populated Gradle cache.
The optional `--stage` copies the JARs and hash manifest to
`runtime/staged-bundle`. The local Kubernetes drills used a test Dockerfile
with those inputs and a prebuilt Linux readiness probe. The repository
`runtime/Dockerfile` also built the refreshed staged bundle with
digest-pinned Temurin 25 and Go 1.23.12 Linux base images.
See [runtime preparation](../../runtime/README.md) for the command and limits.

## Local lease observation

`java/patches/local-lease-observation.patch` makes `/_serverless/stats`
report validity and remaining time from the same local lease publication.
The previous remaining-time field came from the cached membership list,
which can lag successful local renewals. The response still applies the
Java clock-skew margin to validity. Before the first successful publication
the expiry fields are absent, so lease-aware readiness remains closed.
The fields and their units stay compatible with the Go probe.

`java/patches/route-process-observation.patch` puts the existing
`/_serverless/ready` process incarnation in the local
`/_serverless/stats` response. The rollout probe can compare that
identity and lease from one response even when the Gateway selects
a different backend on each request.

## Writer admission at shutdown

`java/patches/writer-admission-shutdown.patch` closes writer admission
after the reconciliation loop and scheduler stop. The gate also covers
direct writer acquisition and metadata-driven reopen. The JVM waits
for admitted operations to finish before its existing handoff runs.
The gate is local to the current JVM and remains closed for the rest of
that process's lifetime.

The Java test pauses an already admitted writer open. While the shutdown
thread waits for that open, another acquisition cannot create a shard head.
After the open settles, recovery sync cannot open a second shard even
when its durable head names the same JVM. Live writer retirement and
durable per-shard receipts are separate from this shutdown gate.

On Windows with Temurin 25 and the offline Gradle cache, the preparation
script applied the final overlay to a fresh checkout and built the
Basic Auth bundle. The bundle manifest matched the overlay hash.
`:serverless:testkit:test --tests '*ActivationResourceCleanupTests'`
passed all four tests from those sources.

### Writer handoff results

The Java shutdown report records each observed open or hibernated writer,
its shard identity and term, the durable head result, and local cleanup
completion. WAL references come from the head written by the successful
conditional update. Giving up a bucket member while a sibling keeps
the shared head has its own result. A late failure deleting an assignment
hint leaves the confirmed head result intact.

Pass `-Dserverless.writer_handoff.persist_on_shutdown=true` to the JVM
to retain a report after an orderly shutdown. The setting defaults to `false`.
Java attempts a create-only receipt under `writer-handoffs/` in the
configured primary store. The key binds the node ID and ephemeral ID.
A retry with identical results succeeds, and a conflicting report leaves
the first receipt intact. Capture `node` and `node_ephemeral_id` from the
target JVM's `/_serverless/stats` response. The Java reader requires those
two values. `process_incarnation` still identifies the readiness snapshot.
Each opted-in process can leave one receipt. Automated retention and
cleanup are pending.

A receipt describes the heads observed by the shutdown pass. A crash
before the write can leave no receipt. Automatic index retirement still
needs a complete ownership inventory, reconciliation that reads the
receipt, and evidence that successors accept writes.

On Windows with Temurin 25, 47 Java tests across the writer, fencing,
scheduler, and bucket suites passed against a local filesystem store.
A fresh pinned Basic Auth bundle included the final overlay hash and
passed the four focused report tests. Those cover open and hibernated
writers, refusal on a changed head, retention of WAL, a bucket member
whose sibling retains the head, and cleanup failure after the head
update.

The next pinned Basic Auth build included the receipt overlay. Eleven
filesystem tests passed, including an HTTP write followed by shutdown
and independent receipt readback using the IDs captured from stats.
One S3 conformance test passed against a fresh SeaweedFS fixture. It
verified a repeat write, a conflicting create, and lookup by the exact
process IDs. Run it with `:serverless:testkit:s3Test --tests
'*SeaweedBlobContainerConformanceTests.testWriterHandoffReceiptCannotBeReplaced'`
with a local SeaweedFS endpoint on port 8333. The shared Envoy fixture
had exhausted its volume slots. The successful rerun used an empty
SeaweedFS server. The S3 test exercises the store adapter. Shutdown and
post-exit readback were exercised on the filesystem.

### Authenticated writer receipt readback

On a surviving JVM, set `serverless.writer_handoff.reader` to the account
that may call `GET /_serverless/writer_handoffs/{node_id}/{ephemeral_id}`.
Java registers the route only when an identity plugin and reader are
configured. With the Basic Auth plugin, anonymous and invalid credentials
receive `401`. Other authenticated accounts receive `403`. The reader
setting controls access to the receipt route. It leaves the Basic Auth
account's permissions on other APIs as configured by the authentication
and authorization plugins.

For a managed instance, use `ServerlessBasicAuth` and HTTP TLS. Approve
the image containing the receipt overlays with
`--development-runtime-settings-image`. Set the reader name in the
survivor role's `runtime.settings`. Enable
`serverless.writer_handoff.persist_on_shutdown` in the ingest role's
`runtime.settings` with the string value `"true"`. Keep passwords in
the credential Secret. A standalone deployment must configure TLS before
using Basic Auth for receipt reads.

Use the outgoing node's exact `node` and `node_ephemeral_id` from direct
stats. The v1 response includes the same IDs, shard identity and term,
the outgoing JVM's observed result, and its local cleanup status. The Go
client requires direct, certificate-verified HTTPS, refuses redirects,
and validates the response identity and shard fields. Only the endpoint's
typed `404` means the receipt is absent in that survivor's store. An
unreadable receipt returns `503`. The caller must choose a survivor of
the same instance and store. The receipt can contain an unconfirmed
result and does not qualify a successor.

A fresh pinned Basic Auth build passed six Java regressions covering the
receipt store, shutdown readback, and the handler's account check. The
Java HTTP fixture also checked a missing or corrupt receipt and route
registration without an identity plugin. The Go tests exercised the
client over HTTPS and checked unsafe transport, redirects, wrong process
IDs and malformed responses. The operator package suite and plan check
passed. The Java and Go tests used separate fixtures. The controller
uses this HTTPS client after confirming Pod absence.

### Durable writer head inventory

Use the same authenticated reader to call
`GET /_serverless/writer_heads/{node_id}/{ephemeral_id}` on a survivor
bound to the same store. The scan walks physical `shards/` registers.
It includes heads with no local shard open, and records a shared bucket
head once under its bucket ID. A head that names the node and has no
ephemeral ID is reported as ambiguous legacy ownership.

The endpoint examines up to 32 registers per request. Start without
`after`, then pass each `next_after` value as `after` until `next_after`
is `null`. A page can contain no matching heads and still have a next
cursor. Invalid or unreadable registers fail the page. The Go reader
checks the process and cursor on each response and discards partial
results if any page fails. Call it with a context that bounds the walk.

The scan is read-only and pages can observe different points in time.
For a retirement decision, stop new acquisitions on the exact target
before starting the walk. A bucket head describes shared ownership.
Use independent evidence for its members, WAL replay, and successor
write readiness.

The pinned Basic Auth build passed six filesystem-backed Java tests
covering direct inventory, malformed ownership, zero-byte registers,
authentication, and compatibility of the regular head decoder. The
SeaweedFS S3 conformance test resumed across three pages and returned
the matching shard and bucket while excluding a different incarnation.
The Go package suite passed, including an empty intermediate page and
rejection of missing or invalid continuation fields.

### Prepare writer retirement

On a managed ingest JVM with durable shutdown receipts enabled, configure
`serverless.writer_handoff.prepare_user` with the account authorized to
prepare retirement. The setting is available in the qualified ingest
`runtime.settings` catalog. The Java handler requires the secure HTTP
transport and an authenticated caller with that exact name.

Persist the target's node ID, ephemeral ID, and operation ID before
sending a management request directly to the target over verified HTTPS.
The sequence for an ingest-only JVM is:

1. Send `POST /_serverless/writer_withdrawal/{node_id}/{ephemeral_id}/{operation_id}`.
   Java reserves the operation and drops client readiness with state
   `writer_traffic_withdrawn`. The regular lease renewal publishes a marker
   for that incarnation. The withdrawal POST and retirement GET return
   `503` until Java can read back the live lease with the marker. Retry
   the exact operation. Already-open writers keep serving peer requests.
2. Wait for the exact Pod to be non-ready and for the index Service's
   EndpointSlices to exclude it or explicitly mark it non-serving. Check
   that qualified surviving endpoints cover the zonal and regional floors.
   With Envoy in front, also verify the current proxy configuration and
   traffic before closing admission. Verify that the surviving Java peers
   have observed the withdrawal marker. A durable lease read proves
   publication, while peers can still route using cached membership.
3. Send `POST /_serverless/writer_retirement/{node_id}/{ephemeral_id}/{operation_id}`
   for that same reservation. Java requires the exact live lease marker,
   then stops new writer acquisitions and waits for the admitted work to
   finish. Start the durable inventory after observing `closed` for this
   pair. A hibernated writer that still owns its exact head can reopen at
   the same term until shutdown closes that path too.

`GET` on the retirement path returns `open`, `withdrawn`, `closing`, or
`closed`. A different JVM or competing operation receives `409`. Prepare
also returns `409` before the withdrawal reservation exists. Withdrawal
and closure remain in effect for the JVM's lifetime. After an ambiguous
POST, retain the reservation and retry the same process and operation.
A GET may observe the prior state while an earlier request is still queued.

The Kubernetes observer checks the pinned stable index Service and exact
Pod UID. It treats a lingering Ready or possibly serving endpoint as a
hold, even when only its IP matches. The observer does not establish
external proxy or Java peer convergence. Controller dispatch, Envoy
verification for this path, and successor write verification are still
pending.

The `writer-retirement-routing.patch` overlay excludes a marked lease
from new shard and bucket placement, load-based handoff and takeover
ranking. A withdrawing JVM also steers cold document, bulk and upsert
writes to an eligible peer. Existing writer heads still route by exact
owner, and client APIs still report the ingest role. Before prepare, a
stale or older peer can still steer work to the withdrawing JVM while
admission remains open. Automatic retirement needs marker-aware runtimes
on every peer that can steer writes and evidence of their current routing
view.

The local HTTPS and Basic Auth test confirmed that withdrawal, status and
prepare return `503` while the marker is absent, then succeed after the
lease renewal. In a three-node filesystem-backed test, a coordinator
ranked the victim first before withdrawal. Its later view steered the
first write to the surviving writer. Bulk and upsert sent to the
withdrawing node also landed on the survivor. After admission closed,
an already-open shard still acknowledged a write through its recorded
owner. The broader Java run covered activation, bucket placement, role
separation, update and forwarding regressions.

The clean pinned Basic Auth bundle passed seven Java tests. The HTTPS
fixture used the configured store-backed account, refused the bootstrap
admin and stale process IDs, and rejected a conflicting operation.
The plaintext test verified that admission stayed open. The concurrent
test held an admitted writer open while preparation and shutdown waited,
and verified that a later acquisition could not create a head. The
Go package suite passed, including TLS response identity and state checks.

The withdrawal build also passed seven Java tests. The Java HTTPS fixture
observed readiness `503`, liveness `200`, and a successful write through
an existing writer after preparation. The Go suite rejected stale or
ambiguous endpoint evidence and missing survivor capacity. In a separate
Kubernetes 1.32.0 kind drill with test Pods, the index EndpointSlice marked
the running victim `ready=false` and `serving=false`, the survivor stayed
ready, and the headless peer EndpointSlice kept the victim published.
Both the victim UID and index Service UID stayed fixed.

### Observe writer withdrawal on peers

Configure `serverless.writer_handoff.reader` on each live search and ingest
JVM that shares the primary store, including preview and retained fleets.
The operator can send the reader credential over direct Pod HTTPS to
`GET /_serverless/writer_withdrawal_view/{observer_node_id}/{observer_ephemeral_id}/{writer_node_id}/{writer_ephemeral_id}`.
The endpoint answers from the observer's cached placement view. A positive
response identifies both Java processes and includes the remaining lease
time for the observed marker. A missing, expired, or different writer
incarnation returns `withdrawn: false`. A stale observer incarnation
receives `409`. A delayed refresh cannot restore assignment eligibility
for an exact process during its already-observed withdrawal lease.

The Go observer reads the namespace's owned runtime Pod cohort, checks
each peer's identity through its verified TLS channel, and rereads the
instance and Pod cohort before accepting the sample. It holds when any
peer is missing, stale, changed, or cannot establish a live marker. The
shortest sampled lease bounds the observation deadline. Before sending
prepare, verify the same reservation directly on the victim and check
Kubernetes and Envoy routing. The observer assumes exclusive primary-store
use by the managed instance. Automatic dispatch and successor verification
remain pending.

The clean pinned Basic Auth bundle passed 25 Java tests. The HTTPS test
returned false while the writer's marker was already in the store and
the observer still held its old snapshot. After refresh it returned true
for the exact writer incarnation. The membership race test completed an
old refresh after a newer one and kept the writer ineligible. A replacement
with the same node ID and a new ephemeral ID remained eligible. The Go
suite and Kubernetes API-server cohort regression passed.

### Guard writer preparation with Envoy evidence

The controller preparation executor takes an already persisted reservation.
It checks the exact victim's Java state over direct HTTPS, observes the
peer cohort, and requires the stable index Service to keep qualified
survivors above its floors. The Envoy evidence binds the index HTTPRoute,
HTTPS Gateway listener, frontend Service and replica versions, backend
EndpointSlices, victim identity and surviving Pod versions. A platform
collector checks active route rules, EDS and healthy endpoints, and sends
authenticated stats requests through each serving replica. The controller
reads the current Gateway and frontend EndpointSlices independently, and
checks the reservation and routing again before dispatch.

The supported selector route uses a literal HTTPS index hostname and a
single root routing rule to the stable index Service. The caller must
bind the production listener and verified per-replica tunnels. The
controller's scoped Kubernetes reads include the Gateway and its frontend
Service and EndpointSlices in their respective namespaces. Its edge
sample covers the designated Envoy frontend.

The executor uses
`POST /_serverless/writer_retirement/{node_id}/{ephemeral_id}/{operation_id}/prepare_before/{deadline_millis}`.
It subtracts one second from the shortest evidence deadline to allow for
clock skew. Java tests the resulting wall-clock deadline inside the
admission reservation lock. A queued request that expires leaves admission
open. If the POST response is lost, a later attempt reads the exact
reservation and adopts `closing` or `closed` without sending another POST.
The controller and Java hosts must keep their clocks within the allowance.

The clean Basic Auth bundle passed 24 Java tests, including an expired
prepare request against a live reservation followed by a successful timed
request. The Go tests rejected a stale Envoy route rule and evidence for an
earlier EndpointSlice, an earlier writer, or a serving proxy cohort that had
gained a replica. A read-only Kind drill verified live RDS, EDS, healthy
backends and authenticated HTTPS through one Envoy Gateway 1.8.6 replica.
It used the already off-route green ingest Pod from the rollback fixture
and an older Java image that exposed process incarnation for this drill.
The drill exercised the collector and left writer admission untouched.

### Persist writer retirement before dispatch

The development operation handler accepts an immutable `RetireWriter`
target with the Pod name and UID. It adds a finalizer, claims the instance,
and records the JVM node and ephemeral IDs, container ID, instance generation,
and routing identities. A separate status update authorizes withdrawal for
those identities for a 30-second window. Deleting the operation before
authorization releases its claim. The controller fences older instance snapshots before removing the
finalizer so a delayed claim cannot name a deleted operation.

The direct HTTPS withdrawal helper reads the exact JVM's retirement
state first. If it is open, the helper qualifies remaining index capacity
against zone and regional floors and rereads the Service, EndpointSlices,
Pods, and operation reservation before sending withdrawal. If Java
already published withdrawal for that operation, the helper returns the
observed state. The operation saves the confirmation time before it can
invoke guarded preparation. Admission closure holds the claim for
successor verification.

The helper sends
`POST /_serverless/writer_withdrawal/{node_id}/{ephemeral_id}/{operation_id}/withdraw_before/{deadline_millis}`.
The deadline is one second earlier than the earliest evidence expiry or
recorded cutoff, allowing for clock skew. Java checks it under the lock
that reserves the exact process and changes readiness. An expired request leaves an
open writer ready and leaves its membership lease eligible for new
assignments. A request for an already published withdrawal returns that
state for the same operation. If the operation is still active after the
window, the controller must read `open` from that JVM after the window
and requalify the same writer before recording a fresh deadline.
Controller and Java hosts must keep their clocks within the allowance.

The pinned Basic Auth bundle passed 53 Java tests across the authentication,
activation, forwarding, and hibernation suites. A lock-contention test
queued withdrawal while the deadline was live and held the lock until
it expired. The HTTPS test verified that an expired request preserved
readiness and assignment eligibility. The Kubernetes API-server regression
verified that the controller records a new deadline only after it reads
the exact writer as open past the previous window.

#### Enable the development writer retirement drill

Run the manager with leader election, endpoint observation and the exact
digest-pinned service-probe, managed HTTPS and Basic Auth image. Mount this
JSON file and pass its path to `--development-writer-retirement-config`.

```json
{
  "instance": "catalog",
  "httpRoute": "catalog-index",
  "gatewayNamespace": "operator-development",
  "gatewayName": "catalog-gateway",
  "gatewayListener": "https",
  "frontendNamespace": "envoy-gateway-system",
  "frontendService": "catalog-envoy",
  "indexURL": "https://index.example.internal",
  "edgeCredential": "index-rollout-probe",
  "readerCredential": "writer-evidence-reader",
  "preparerCredential": "writer-retirement-preparer",
  "platformKey": "<base64 Ed25519 public key>",
  "enableEviction": true
}
```

The HTTPRoute must have a literal hostname, one HTTPS parent listener
on port 443, and one root rule to the stable index Service. Gateway API
may default that rule to a single `PathPrefix /` match. Grant the
operator namespaced reads of the route, Gateway and frontend Service
and EndpointSlices. Move the Gateway read rule to the configured Gateway
namespace if the Gateway and HTTPRoute are in separate namespaces.
[Example RBAC](../../config/dev/writer-retirement-rbac.example.yaml)

Use three distinct immutable Secrets of type
`serverless.opensearch.org/rollout-probe` with `username` and `password`.
The Java reader must be configured on the search and index JVMs. The
preparer is an index JVM account with `serverless.writer_handoff.prepare_user`.
The edge account is used through Envoy by the platform observer.
Retirement binds deployment routing and credential UIDs. Exact Java
state readback uses the original preparer Secret independently of the
current route, so a route change cannot turn recovery into a new mutation.
On deletion after the deadline, an exact `open` response permits the
controller to cancel and release the claim.

`enableEviction` defaults to false. To exercise automatic eviction, set
`spec.pools.ingest.replaceOnExit: true` on the instance before it creates
the target Pods. Those Pods use `restartPolicy: Never`, so a JVM exit
requires replacing the Pod. Kubernetes can replace the caller's
resource-version precondition when evicting an unready Pod under
`IfHealthyBudget`. The Pod UID precondition still applies. Requiring a
single OpenSearch lifetime per Pod UID keeps the eviction from targeting
a restarted JVM.
[Kubernetes Eviction API implementation](https://github.com/kubernetes/kubernetes/blob/v1.32.3/pkg/registry/core/pod/storage/eviction.go#L215-L275)

```yaml
apiVersion: serverless.opensearch.org/v1alpha1
kind: OpenSearchServerlessOperation
metadata:
  name: retire-index-writer
  namespace: operator-development
spec:
  action: RetireWriter
  instanceRef:
    name: catalog
    uid: "<instance UID>"
  writerRetirement:
    podName: "<serving index Pod name>"
    podUID: "<serving index Pod UID>"
```

After the operation records `status.writerRetirement.withdrawnAt`,
collect the Envoy evidence from a trusted platform environment. Use the
same route, Gateway, Service and edge credential configured in the
manager. Map every serving Envoy Pod UID to its verified loopback admin
and HTTPS tunnels, using the [observer tunnel format](development-rollout.md#collect-envoy-proxy-evidence).
The observer verifies direct Java HTTPS against the Pod DNS identity.
If the platform environment needs Pod port-forwards, pass
`--writer-pod-tunnels-file` with an optional JSON map from Java Pod UID
to a verified loopback tunnel that reaches the Pod IP.

Envoy Gateway can retain the withdrawn Pod in EDS. For that case, attach
a route-scoped `BackendTrafficPolicy` with
`spec.healthCheck.panicThreshold: 0` to the index HTTPRoute. The observer
requires the exact victim to be ineligible in EDS, `DRAINING` in the
active cluster, and panic routing disabled in the active configuration.
Only qualified survivors may be healthy. The collector still probes the
actual authenticated HTTPS route on each serving replica.

```sh
rollout-proxy-observer --writer-withdrawal \
  --namespace operator-development --operation retire-index-writer \
  --writer-instance catalog --writer-http-route catalog-index \
  --writer-gateway-namespace operator-development --writer-gateway catalog-gateway \
  --writer-gateway-listener https --gateway-namespace envoy-gateway-system \
  --gateway-service catalog-envoy --tunnels-file envoy-tunnels.json \
  --route-url https://index.example.internal --route-ca-file edge-ca.pem \
  --route-auth-secret index-rollout-probe --writer-reader-secret writer-evidence-reader \
  --writer-pod-ca-file java-ca.pem --writer-cluster-domain cluster.local \
  | rollout-evidence-publisher --writer-withdrawal \
      --namespace operator-development --operation retire-index-writer \
      --signing-key-file platform-ed25519.pem
```

The publisher checks the journal and signs into
`writer-withdrawal-<operation UID hash>`. It requires operation and
instance reads and ConfigMap get, create and update in the instance
namespace. The observer needs the same operation and instance reads,
plus Pod and Service reads, EndpointSlice listing, route and Gateway
reads, and access to the edge and Java reader Secrets. Keep the signing
key in the platform publisher. Run the
commands again if the observation expires or any verified object version
changes. The operator verifies the report against the current Envoy
replicas, Kubernetes routing and Java peer evidence.

#### Run the writer evidence publisher as a Job

Build the observer and publisher images from their separate targets.

```sh
docker build --target rollout-proxy-observer -t opensearch-rollout-observer:dev .
docker build --target rollout-evidence-publisher -t opensearch-rollout-publisher:dev .
```

Make the publisher image available to the cluster before the drill.
The publisher Job waits for a report ConfigMap in the trusted platform
namespace. It reads the named ConfigMap through the API once per second
for up to two minutes. This lets you start the Pod before collecting
the observation, which expires after 30 seconds. The signing key stays
in a Secret in the platform namespace. Keep workloads that can create
Pods out of that namespace.

Pre-create the output ConfigMap in the instance namespace using the
exact retirement operation UID. Preserve any already-published
evidence. The instance Role grants get and update on the named output
and get on the named instance and operation. The platform Role grants
get on the named input only.

```sh
INSTANCE_NS=operator-development
PLATFORM_NS=operator-platform
OPERATION=retire-index-writer
OPERATION_UID=$(kubectl -n "$INSTANCE_NS" get opensearchserverlessoperation "$OPERATION" \
  -o jsonpath='{.metadata.uid}')
if test -n "$OPERATION_UID"; then
  EVIDENCE_NAME=writer-withdrawal-$(printf '%s' "$OPERATION_UID" | sha256sum | cut -c1-24)
  kubectl -n "$INSTANCE_NS" create configmap "$EVIDENCE_NAME"
fi
kubectl -n "$PLATFORM_NS" create secret generic writer-withdrawal-signing-key \
  --from-file=key.pem=platform-ed25519.pem
```

Use the same Ed25519 key configured for this retirement path. Render
the example Job and RBAC with the correct namespaces, instance name,
operation name, output ConfigMap name and publisher image digest.
Choose a fresh input ConfigMap name for each attempt and set it in
both the platform Role and the Job argument. Apply the rendered RBAC
and start the rendered Job before creating its input.

```sh
JOB=$(kubectl create -f publisher-job.yaml -o jsonpath='{.metadata.name}')
kubectl -n "$PLATFORM_NS" wait --for=create pod -l "job-name=$JOB" --timeout=30s
kubectl -n "$PLATFORM_NS" wait --for=condition=Ready pod -l "job-name=$JOB" --timeout=90s
```

With the Pod ready and the Envoy withdrawal in progress, run the
observer from the trusted environment with verified tunnels and
redirect stdout to `report.json`. A containerized observer needs
those tunnels in its own network namespace. Immediately create the
named input and wait for the publisher to complete. Set `REPORT_NAME`
to the same fresh input name used in the Job and Role.

```sh
kubectl -n "$PLATFORM_NS" create configmap "$REPORT_NAME" --from-file=report.json
kubectl -n "$PLATFORM_NS" wait --for=condition=Complete "job/$JOB" --timeout=40s
```

If the attempt times out, check the Job log, then start a fresh
Job and collect a new report. The controller independently verifies
the signed report before proceeding. The CLI also accepts a file
with `--evidence-file` or reads the report from stdin.

[Publisher RBAC example](../../config/dev/writer-withdrawal-publisher-rbac.example.yaml) ·
[Publisher Job example](../../config/dev/writer-withdrawal-publisher-job.example.yaml)

#### Verify live writer retirement through Envoy

Use a disposable instance with two index JVMs, a search JVM and an
index with one independent shard. Seed one document on the writer
selected for retirement. Set up the automatic eviction path, let it
withdraw the writer from the index route, and start the staged
publisher Job. Leave the exact target Pod running and the Job input
empty until the verifier starts.

```sh
python tools/verify_live_writer_retirement.py \
  --kubeconfig "$KUBECONFIG" --namespace "$INSTANCE_NS" --operation "$OPERATION" \
  --publisher-job "$PLATFORM_NS/$JOB" --platform-public-key platform-ed25519.pub.pem \
  --index-url "$INDEX_URL" --search-url "$SEARCH_URL" --ca edge-ca.pem \
  --username "$EDGE_USER" --password-file edge-password.txt \
  --index writer-single-shard --seed-id seed --seed-message seed-before-retirement
```

The verifier prints when it is waiting for the publisher. In another
terminal, capture the observer report and submit it to the staged Job.
Keep other writers off this test index until the verifier finishes.
If the route names only resolve inside Kubernetes, port-forward the
Envoy HTTPS listener and pass `--envoy-tunnel 127.0.0.1:10443`.
The verifier still sends each route's hostname and checks it against
the certificate. The command writes the verification record as JSON.
It uses OpenSSL to verify the signed report, checks that the exact
Pod UID disappears after the operator arms eviction, then sends one
write through the index route. Both client routes use the specified
CA and Basic Auth. The verifier checks that search returns the seed
and the new document, and that the terminal journal names the
successor that acknowledged the new write on the same shard.

#### Record durable ownership before eviction

After the manager persists admission closure, it reads all physical shard
and bucket ownership pages through an authenticated surviving index JVM
on the pinned primary store. It checks the exact victim's closed
admission and the observer's JVM, lease and serving endpoint identities
around the scan. The journal stores the observer Pod UID and JVM IDs
along with the inventory in `status.writerRetirement.headInventory`.

An incomplete scan, a legacy node-ID-only owner, or an index head without
an index UUID leaves the inventory pending. The development journal
accepts up to 512 exact heads and 384 KiB of encoded head entries.
Each scan attempt is bounded by 30 seconds and the observer's lease
window. The `HeadInventoryReady` condition distinguishes a timeout,
limit failure, and unavailable evidence. The controller leaves the claim
in place until it records a complete scan, which can be an empty list.
The paginated read has no common store timestamp. If a head is released
before its page is read, it may be absent from that scan.

#### Evict the withdrawn writer

With automatic eviction enabled, the manager commits
`status.writerRetirement.evictionArmed` before dispatch. It rescans the
closed writer's heads and compares the physical keys and terms against the
recorded inventory. A difference sets `WriterHeadsChanged` and holds the
operation for explicit recovery. The controller checks the surviving Java
peer leases, index serving floors, signed Envoy proof and the matching zonal
PDB before submitting `policy/v1` Eviction. Republish the Envoy observation
after admission closes if the last statement expires or its versions
change. Keep operator-owned PDBs protected from concurrent external edits
or deletion. Kubernetes allows eviction when no PDB matches.

The controller checks the original Pod UID after every attempt, including
an ambiguous API response. It waits for a terminating Pod and records
`status.writerRetirement.victimGone` when the original UID is absent.
The operation retains its claim and finalizer through successor
verification. Deleting an armed operation before the outcome is recorded
freezes further dispatch and keeps the claim. If an eviction might
already have reached the API server, the controller continues reading
the exact Pod UID. It waits through termination and records absence
only when the original UID is gone. Read-only receipt and successor
checks can then finish while the operation is deleting.
When automatic eviction is enabled, the controller scans the exact
writer's open heads before arming traffic withdrawal. An observed
shared bucket sets `UnsupportedWriterHeads`. An incomplete scan sets
`WriterHeadPreflightPending`. The reservation stays unarmed and the
operator can cancel it before the writer stops serving.

The open-writer scan is a read-only preflight. The writer can acquire
heads while it serves, so the controller still takes a complete
inventory after closing admission. That inventory holds shared bucket
heads before eviction because their successor evidence is unavailable.

#### Record the writer release receipt

After it confirms the Pod's absence, the manager reads the exact retired
JVM's receipt through the scoped Java reader account on a serving index
Pod. It checks the pinned store, stable index endpoints and the
observer's JVM lease around the read. The manager stores matching
outcomes in `status.writerRetirement.releaseReceipt`. Each entry refers
to an ordinal in the immutable head inventory.

`WriterReleaseReady=True` means the receipt accounts for every recorded
independent shard at its original index UUID, shard number and primary
term. `RELEASED_WITH_WAL` still requires successor replay evidence.
An unreadable receipt keeps the operation pending. A confirmed missing
receipt can use [WAL recovery evidence](#recover-a-writer-without-a-receipt).
For a valid partial receipt, the controller retains the recorded
releases. Each omitted independent shard needs a verified successor
write with settled predecessor WAL. A newer-term write remains required
for every shard. Shared bucket heads need evidence for the bucket and
its members.

#### Observe a successor write

The managed ingest runtime exposes
`GET /_serverless/writer_successor/{node_id}/{ephemeral_id}` to the
configured writer handoff reader over HTTPS. Pass `index`,
`index_uuid`, `shard` and `previous_term` for one independent shard.
The exact process returns its newer active term after a foreground
index, delete or bulk write has completed its WAL and local write path.
Opening and replaying a shard alone leave the evidence pending. Java
checks that the shard is still started and unfenced and reads the
durable head to verify its UUID, term and exact owner.

A typed `writer_successor_pending` HTTP 404 means the selected JVM
could not prove a current newer-term write. The Go reader distinguishes
that state from an unavailable or unsupported endpoint and checks the
returned process, shard and term. This read never writes to the index.

After a complete release receipt is recorded, the controller samples
ready index endpoints in the pinned serving fleet. It checks the Pod
and JVM identities, the JVM lease, the stable index Service and serving
floors, and the pinned store and operation before saving observations.
Each entry in `status.writerRetirement.successors` records the inventory
ordinal, Pod UID, JVM, observed term and observation time. A bounded
scan resumes across reconciles until every head has a recorded write.
An idle shard can keep `WriterSuccessorReady=False` until real traffic
reaches it.

Once all heads have evidence, the manager records `Succeeded` before
releasing the instance claim and removing the finalizer. A new manager
can recover that cleanup from the saved outcome. An altered route
binding holds verification until the original binding is restored.

#### Recover a writer without a receipt

After the original Pod UID is gone, an authenticated and qualified
receipt read can report that the exact JVM left no receipt. The
controller journals this observation, retries the receipt, and can
recover from repeated absence using the complete independent-shard
inventory. Each head needs a verified newer-term foreground write on
a surviving index JVM and an explicit `predecessor_wal_settled=true`
from Java. Java obtains this from the durable physical head of the same
index UUID. The head carries predecessor WAL references until replay and
publication settle. A successor write alone can precede settlement.
Java clears these references using the exact index UUID, owner JVM and
term. The bounded settlement retry covers storage errors and concurrent
head changes. A different writer, index or term stops the retry.

The operation records this result as `WALRecoveryVerified` and retains
the receipt-absence timestamp. The recovery path requires at least one
inventoried head. The same WAL proof can account for omitted entries in
a valid partial receipt. The operation retains those receipt entries
alongside successor evidence. An unreadable receipt, a shared bucket
head, or a deleted index UUID still holds the claim.

### Hand off hibernated writers

Prepared retirement allows a forwarded write to wake a locally tracked
hibernated writer. Java reads the durable head and matches the index UUID,
node ID, ephemeral ID and term. The process must still have a valid lease.
If the head moved, the wake cannot claim it again. Read or open failures
keep the hibernation record available for retry or shutdown accounting.
A demand-driven wake after preparation requires available local capacity
and cannot evict another writer to make room.

Once withdrawal starts, new idle releases of hibernated writers wait for
shutdown handoff. The prepare barrier settles releases already admitted.
Shutdown blocks both acquisitions and owned wakes, waits for work already
admitted, and releases the observed hibernated heads with the existing WAL
handoff. The successor must open the shard at a newer term and prove that
it can accept and read back writes.

The clean pinned Basic Auth bundle passed 23 Java regression tests and two
SeaweedFS 4.53 S3 tests. The same two-node handoff scenario passed on the
filesystem and on S3. A forwarded write woke the prepared owner without
changing its term. After shutdown handoff the survivor acquired a newer
term, acknowledged a write, and read all four documents written before,
during and after retirement. The tests also covered retry after failed
head reads, refusal to reacquire a released head, and refusal to evict
another writer at capacity. The Go suite and plan check passed. Kubernetes
dispatch and automated successor verification remain pending.

#### Keep advisory assignment cleanup out of shutdown

The SIGTERM hook releases the fenced shard heads of open and hibernated
writers before releasing membership and writing the process receipt.
When Java has prepared the JVM for retirement, live peers can clean up
the departed node's hints and prune empty rollup entries. Ordinary
shutdown and idle eviction still remove hints so a normal JVM restart
can reuse its node ID. The metadata plane checks each hint against the
authoritative head before treating it as an assignment. The receipt's
`local_cleanup_complete` field reports local writer cleanup. Object
store reads and fenced updates can still consume the termination grace
period when the store is slow.

Prepared retirement expires the exact process lease and attempts to
remove its member index entry. It retains the expired register for WAL
readback and skips the conditional-delete capability probe. The
ordinary membership release path continues to use the configured probe
and reclaim an eligible register.

In a second Kind drill, the shutdown hook was waiting on the same S3
delete future at 8 and 20 seconds. The head had already been released.
Kubelet's 30-second grace elapsed during advisory assignment deletion
and containerd recorded exit 137. Both documents remained readable
after the surviving writer took over at term 2.

The assignment-only patch moved the stall to physical lease cleanup.
The JVM waited on the capability probe's S3 bulk delete after it had
already expired and unenrolled the writer's lease. The Pod again
exited with status 137 and published no receipt.

## Experimental S3 web identity overlay

`java/patches/s3-web-identity-setting.patch` adds the single non-secret
`s3.client.default.identity_token_file` JVM property to the shell's forwarded
settings. Verify it against the pinned commit with
`python tools/verify_java_s3_identity_patch.py --source-repo <repository>`.
Build the bundle after applying the patch. With `OPERATOR_JAVA_RUNTIME_LIB`
pointing to that bundle, run `go test -tags java_integration ./internal/java
-run TestShellUsesProjectedS3WebIdentity`. On Windows with JDK 25, the shell
sent the synthetic token and role to a local STS protocol fixture and used its
temporary credentials to sign a request to a local S3 fixture. After a local
token-file update, it also requested fresh credentials and signed an S3 request
with them; this passed three consecutive runs. The S3 fixture does not store
data. This does not verify a cloud role, a real bucket, Kubernetes atomic
token projection or admission. The operator's experimental
`WorkloadIdentity` path requires explicit qualification of an image containing
this overlay.

An additional opt-in `TestWebIdentityS3WriteAndReplacement` passed with a
disposable SeaweedFS gateway and the patched shell on Windows outside the
filesystem sandbox. A local proxy required synthetic STS credentials, and the
test wrote a document, stopped the writer, changed the token file and read the
document from a replacement process. The proxy checks credential identifiers,
not cryptographic signatures. Set `OPERATOR_JAVA_RUNTIME_LIB` and
`OPERATOR_SEAWEED_EXE` and run `go test -tags java_integration ./internal/java
-run TestWebIdentityS3WriteAndReplacement`. This is not AWS, a Kubernetes
cluster, multi-node failover or conditional-write conformance.

The same fixture denied S3 access after the writer acquired a lease. After the
12-second test lease expired, the Go lease-aware serving check rejected the
writer and later accepted it when access returned. It also rejected a fresh
process with an STS-denied synthetic token across five one-second samples.
This is not evidence of Kubernetes endpoint propagation, a search node, or
real AWS authorization behavior.

`tools/package-java.gradle` adds an `operatorRuntimeBundle` task to a checkout of the Java shell. The task collects the shell JAR and its resolved runtime classpath under `serverless/shell/build/operator-runtime/lib` and fails on duplicate filenames. The shell's process-test harness uses the main runtime classpath and excludes the test framework.

The development smoke run used Java source commit `e71d42331a3ec82ec01109a5bb42d6a9a9162a5c`, Gradle 9.7.1 and Temurin JDK 25 on Windows. `:serverless:shell:classes` and `:serverless:shell:operatorRuntimeBundle` succeeded. A JVM launched with the resulting `lib/*` classpath, the module-open and Netty flags from `NodeProcess`, an isolated filesystem store, and the `ingest` role returned HTTP 200 with `ready=true` from `/_serverless/ready`.

This establishes a local startup check. A production image still needs a Linux build, pinned base image and JDK, explicit launch contract and writable paths, security provider, JVM shutdown behavior, S3 identity, plugin loading, and multi-node recovery tests. The current source checkout is an archived copy and its build reports that Git metadata is absent. Produce release provenance from the original commit and build environment rather than treating the archive's build metadata as a source identity. [Java runtime requirements](../research/java-serverless-runtime.md) · [M03 tasks](foundation.md#m03--java-runtime-and-primary-store)

`tools/java-role-smoke.ps1` starts separate ingest and search JVMs against a fresh local filesystem store, creates an index, writes one document and queries it through the search role. This passed on Windows outside the filesystem sandbox. The sandboxed run reached an access-denied error on a store blob during the write. The script terminates its processes after the check and retains its working directory for inspection.

The script also forcibly stops the first ingest process, starts a replacement with a fresh local home, retries an additional write and checks the original acknowledged document through the search process. That recovery sequence passed on the same Windows filesystem store outside the sandbox. It does not establish S3, Kubernetes, or sustained-load recovery behavior.

When `OPERATOR_GO_EXE` points to Go, the smoke script also runs `TestLiveReadinessAndSettings` against the ingest process. This passed against the pinned shell and confirmed that the Go adapter parses readiness and persistent-settings responses, writes `serverless.bucket=off`, reads it back and removes it. The test establishes persistence of that isolated setting, rather than effective application on every node.

`tools/java-s3-smoke.ps1` passed against a local SeaweedFS 4.53 S3 gateway on Windows. It created a disposable bucket and keystore, started an S3-backed ingest shell, created an index, retried until its shard accepted a write, and retrieved the document. This checks one local S3-compatible implementation. Conditional-write conformance, production S3, identity refresh and multi-node S3 recovery require further tests.

The same script subsequently stopped the ingest process, started a replacement with a fresh local home and recovered the acknowledged document from the S3-backed store. This is a single-node process-replacement check with SeaweedFS and does not establish concurrent multi-node failover or production S3 behavior.

## Proposed warm-up observation overlay

`java/patches/warmup-observation.patch` applies to pinned commit `e71d42331a3ec82ec01109a5bb42d6a9a9162a5c`. Apply it to a clean extracted checkout with `git apply --check <absolute-patch-path>` followed by `git apply <absolute-patch-path>`, then build the runtime bundle from that checkout. `python tools/verify_java_warmup_patch.py --source-repo <repository-containing-commit>` independently checks the patch against that commit; it does not alter the supplied repository. The current workspace's Java branch is divergent and was not modified.

The overlay adds a random per-node-process incarnation and a background warm-up result to `/_serverless/ready`. `running` remains distinct from traffic readiness even after its deadline; `completed`, `degraded`, and `failed` indicate that the warm-up worker exited. No peers, failed peers, failed reader opens, or store-read failures make the result degraded. `warmup_timed_out` records the readiness deadline separately. Completion does not prove cache blocks remain resident, full workload coverage, peer reachability, or future performance, and the initial no-peer bootstrap cannot be declared warm from this signal.

The patched shell's `SearchWarmupTests` passed on Windows with Gradle 9.7.1 and Temurin JDK 25, including a direct timeout-state test and no-peer degradation. The extracted checkout's shell and testkit formatting checks passed after formatting that disposable checkout; the original baseline had unrelated formatting failures, which are not part of this overlay. A local filesystem-store split-role smoke test with warm-on-join enabled confirmed the actual HTTP response reports a degraded finished warm-up with no search peers. It also indexed, searched and replaced an ingest process. The development operator can opt in with an explicitly qualified image digest and a single durable warm-up slot. These checks do not qualify a Kubernetes image, real-cluster crash/restart slot accounting, strict warm readiness, or cloud traffic.

The warm-on-join smoke also ran the Go qualification adapter against both a
no-peer degraded process (rejected) and a second search process warmed from
the first (accepted). The same run indexed and searched a document and
recovered after ingest replacement. In an additional same-home search JVM
restart, the process incarnation changed, readiness became green, but warm-up
finished degraded; the qualification adapter rejected it. A previous attempt
that expected this restart to qualify timed out after 90 seconds. Diagnostics
showed the restarted process had a new node ID while the old unexpired search
lease still advertised the same transport address. In a separate run, waiting
until the survivor no longer listed the old lease before restarting resulted in
completed warm-up and a successful Go qualification. With targeted warm-copy
debug logging enabled, two immediate-restart runs recorded a failed peer request
for the old node ID while that old lease was still visible. The Java code marks
warm-up degraded when a peer fails, establishing the mechanism in those runs;
the log does not identify the underlying transport exception. This does not
establish a safe fixed delay. The smoke supports a controlled comparison using
`OPERATOR_TEST_WAIT_LEASE_EXPIRY=1` and correlation using
`OPERATOR_TEST_DEBUG_WARM_COPY=1`. These local filesystem tests do not establish
cache residency, S3 or Kubernetes behavior.

The experimental warm-up overlay now starts peer-failure retries at five
second intervals before the configured warm-on-join maximum wait. An attempt
already in progress can finish after that time; a queued retry checks the
window again when it starts and does not copy if the window has closed. The worker
reports `running` between attempts and reports `completed` only after a clean
attempt; exhausted peer failures remain `degraded`. No-peer, shard-open and
store-read failures still prevent successful qualification. In strict mode,
an attempt with no search peer is also retried within the same window, allowing
a deliberately degraded bootstrap peer to appear. Retries stop when
draining is observed. Existing Java warm-up tests passed, and local smoke runs
covered recovery after the stale lease expired and a short retry budget that
ended degraded while the lease remained. This is an experimental overlay, not
a released Java change or proof under Kubernetes, cloud faults or sustained
peer failure.

A Java test starts a strict process before any search peer, observes the worker
remain running and unready, then introduces a peer and observes completion
and search-role advertisement. It passed three iterations.

Boundary tests also invoke a delayed retry after its deadline and a retry on a
draining node. Both terminate degraded without beginning a readiness transition;
the tests passed three iterations. They simulate the callback state rather than
saturating the actual executor.

The overlay also has an opt-in
`serverless.search.require_warmup_completion=true` Java setting. With
warm-on-join enabled, it holds process readiness, local reader use and search
role advertisement until completion without timeout; it remains false by
default. Java tests cover completed, degraded, timeout and drain cases. A local
smoke run confirmed a peer-warmed strict process qualifies, while a degraded
restart reports HTTP 503 and omits the search role from its lease. The operator
enables this setting only for strict successors in an explicitly selected
development configuration with one degraded bootstrap. Actual Kubernetes
traffic, bootstrap recovery, peer reachability and cache residency remain
unqualified.

The Go health probe accepts `-require-warmup-completion` for search. It checks
the process incarnation, completed non-timeout warm-up, serving state and
lease; the option fails for ingest and does not claim current cache residency.
In a local Java smoke, this mode rejected the ready-but-degraded first search
process and accepted a peer-warmed process. The development Pod template does
selects this option for strict development successors, while its explicitly
degraded bootstrap uses serving readiness. Kubernetes readiness, actual traffic
and recovery behavior remain unqualified.

## Experimental peer TLS provider

`java/patches/peer-dns-advertisement.patch` is a separate overlay for the same pinned commit. It adds `serverless.transport.advertise_host`: when set to a DNS hostname, Java writes that hostname and the actual bound transport port in its membership lease. When unset, the existing bound-address behavior is preserved. Invalid host syntax fails startup. Apply it alongside the warm-up overlay before building and running the TLS integration tests. `python tools/export-peer-dns-patch.py --source-repo <repository-containing-commit>` checks the original files against the pinned commit, verifies the patch applies and reproduces the tested source, and exports the patch.

The local DNS integration test wrote a lease naming `localhost` and forwarded a write over mTLS using a certificate with a `localhost` DNS SAN and no IP SAN. This shows the tested Java peer path resolves and verifies the advertised DNS name. The development direct-Pod controller creates an owner-checked, per-instance headless Service with `publishNotReadyAddresses` and gives each newly created Pod a deterministic hostname and subdomain. With `--development-peer-dns-image` set to the exact qualified image digest and `--development-cluster-domain` set to the cluster DNS domain, it supplies that Pod's full name as the Java advertised host. The domain is included in the durable Pod revision; a changed domain or existing Pod with a different recorded domain holds reconciliation for explicit migration. Kubernetes 1.32 API-server tests verify the stored fields, the Java argument, conflict rejection and the pending-intent guard. The test API server does not run DNS or EndpointSlice controllers. Pods created by older development builds without these fields require explicit recovery rather than silent adoption. The operator does not yet issue certificates or independently verify DNS record ownership. DNS availability and caching, cross-version peers, and lease reattachment also require qualification. The operator must not treat an arbitrary supplied hostname as proof of Pod identity. This development option alone does not enable TLS.

The qualified image must also include the current `health-probe` executable. In peer DNS mode the Pod runs it as an init container before Java, with an explicit CPU, memory and ephemeral-storage request included in fit accounting. The preflight polls for up to 90 seconds and succeeds only when the DNS name has exactly one distinct address, equal to `POD_IP`; duplicate identical records are allowed. It rejects multiple addresses, including dual-stack answers. Admission checks protect the init container and its inputs. This is a point-in-time local DNS observation, not continuing proof of DNS ownership or later resolution. After binding, the controller retains the provisioning slot until the preflight succeeds. If it has not completed within the configured development pending timeout after the controller observes binding, the controller records `PeerDNSPreflightTimeout` and holds new automatic provisioning; it does not delete the bound Pod. If that same Pod's preflight later succeeds, the hold clears. If it is removed, the existing explicit retry operation can acknowledge the resulting gone/terminating hold once its other guards pass. Kubernetes may restart a failed init container while the hold is active. Real kubelet, EndpointSlice, DNS caching and dual-stack behavior remain untested.

`java/peer-tls` is an opt-in classpath plugin for the pinned shell, not a production-ready security integration. Pass an absolute `-Doperator.peerTlsSource=<operator-root>/java/peer-tls/src/main/java` to the Gradle invocation using `tools/package-java.gradle`; the runtime bundle then includes the provider. Configure the class name `org.opensearch.serverless.operator.PeerTlsPlugin` in `serverless.plugins` and supply absolute paths via `serverless.peer_tls.certificate`, `serverless.peer_tls.private_key`, and `serverless.peer_tls.trust`. The PEM private key must be unencrypted PKCS#8, RSA PKCS#1, or EC SEC1 and supplied through a protected mount. The provider reads the inputs at startup, checks the leaf's current validity and verifies that the private key matches it, and uses those same bytes to build both TLS contexts. It requires a client certificate, sets endpoint identity verification for outgoing peer connections, and refuses plaintext transport, disabled hostname verification, or TLS dual mode. Use a dedicated peer CA: any client certificate chaining to that CA and accepted by the TLS implementation can connect; the provider does not authorize individual peer identities or actions. It does not secure HTTP, validate the actual mounted Secret revision, or implement rotation or hot reload.

For development deployments, `--development-peer-tls-image` must match the qualified peer-DNS image. `spec.tls.transport` can reference a same-namespace TLS Secret and trust Secret, including mutable issuer-managed sources. Before creating a Pod, the controller stages an immutable Secret and records its UID. It validates the key pair, certificate chain, client and server usages and the planned Pod DNS SAN at the current time and one hour later.

Once committed, the snapshot supplies peer TLS for new, replacement and held Pods even if an issuer Secret changes or disappears. A change to the configured reference, a deleted or replaced snapshot, or a certificate that fails the validity check holds provisioning. Deleting an issuer Secret does not revoke the pinned certificate. If the issuer refreshes the source before the snapshot is created, the controller retires the provisional Pod intent and replans from the new source once it verifies that no instance Pod exists. If the snapshot was already created, the controller can recover its UID from the recorded baseline, including after retirement of the original Pod intent.

The Java provider checks the mounted leaf and trust bytes against the recorded public hashes and verifies the private key matches the leaf. Kubernetes resolves Secret volumes by name, so the controller's prechecks and the Java content check cannot prove the mounted Secret UID during a delete-and-recreate race. Namespace authorization and admission policies must protect the snapshots. The development blue/green workflow activates new peer material at JVM startup. [Peer TLS staging procedure](development-rollout.md#stage-a-peer-tls-rotation)

Generate disposable fixtures in a new directory with `go run ./tools/peer-tls-fixtures <new-directory>`. With the same init script and source property above, add `-Doperator.peerTlsTestSource=<operator-root>/java/peer-tls/src/test/java` and `-Doperator.tlsFixtures=<fixture-directory>`, then run `:serverless:testkit:operatorPeerTlsTest`. On the pinned checkout, Gradle 9.7.1 and JDK 25 on Windows, this passed a forwarded write, rejection of a wrong server IP identity and an untrusted server, and TLS 1.2 handshakes accepting a trusted client and rejecting absent or untrusted client certificates. Fresh handshakes exercised old and new leaves during overlapping trust, followed by rejection of old trust after removal. It also checks unsafe transport options, mismatched keys, and loading RSA, EC and Ed25519 key pairs, including RSA PKCS#1 and EC SEC1, and rejection of mismatched keys in those formats. A separate local filesystem-store smoke run with `tools/java-role-smoke.ps1 -PeerTlsDirectory <fixture-directory>` passed indexing, search and an ingest replacement using the packaged provider. The local Gradle run required `-Djapicmp.compare.version=3.0.0` for offline configuration, and both Java tests needed execution outside the filesystem sandbox. The tests do not establish live fleet rotation, existing connection revocation, Kubernetes mounts, or cloud behavior.

An additional experimental classpath plugin, `org.opensearch.serverless.operator.HttpTlsPlugin`, provides a separately configured HTTPS certificate and unencrypted PKCS#8, RSA PKCS#1 or EC SEC1 key using `serverless.http_tls.certificate` and `serverless.http_tls.private_key`. Set `serverless.http_tls.require_client_certificate=true` and `serverless.http_tls.client_trust` to require certificates from a separate HTTP client CA. The plugin rejects a plaintext `http.type` override. It does not provide user authentication or authorization. The local Java integration test starts both HTTP and peer providers, confirms an HTTPS response with a trusted client, rejects absent and untrusted clients when required, and checks that a client rejects the wrong HTTPS server identity. It also checks a handshake without a client certificate when client certificates are not required. It does not qualify the external ingress, management access, Kubernetes or rotation.

The `health-probe` executable accepts a CA and expected DNS or IP
identity for HTTPS readiness. `--tls-ca-sha256` checks the mounted
bundle against the controller's pinned bytes. The managed Pod template
supplies these flags and the Java provider's expected certificate hash.
The probe also supports optional paired client-certificate flags.
`tools/java-https-probe-smoke.ps1` exercises those flags against a Java
runtime with HTTP client-certificate enforcement. Managed client
certificates and automatic leaf or CA rotation remain open.
[Managed HTTP TLS](../../config/dev/README.md#managed-development-http-tls)
