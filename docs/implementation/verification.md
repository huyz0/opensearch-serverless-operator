# Verification and evidence

## Strict search warm-up over HTTPS

On kind with Kubernetes 1.32.2 and a supplied SeaweedFS S3 bucket, the
HTTPS-enabled instance started one ingest Pod and one bootstrap search
Pod. The fixture wrote a document through ingest and read it through the
existing search Pod. It then scaled search to two replicas. The strict
successor became Ready, served the same document and released its
warm-up slot after the controller recorded matching completion samples
for its JVM. The completion had no timeout. A separate kubelet probe
Pod running the final health-probe binary verified the live strict JVM
over HTTPS with the pinned CA and Pod DNS identity.

Tests reject a ready response from a worker that is still running or
has timed out, a missing or mismatched stats incarnation, a wrong SAN,
and an identical CA recreated under a new Secret UID. The Kubernetes
retirement regression holds eviction when lease evidence expires and
allows it after fresh evidence. These checks establish process-local
worker completion and admission. Cache residency, authenticated
management and multi-AZ failback over HTTPS require separate proof.

## Managed HTTP TLS readiness

On kind with Kubernetes 1.32.2, the controller created one search and
one ingest Pod with an external SeaweedFS S3 bucket. A separate client
Pod verified each ready JVM over HTTPS, using the Pod IP for transport
and its managed DNS name for certificate verification. Both JVMs rejected
plaintext and a client using the wrong expected server name. Recreating
the trust Secret with identical bytes produced a new UID. The controller
held scale-up while both original JVMs continued serving verified HTTPS.

The cluster API rejected both enabling and disabling HTTP TLS after the
first creation intent. The Java runtime refused startup with a mismatched certificate hash.
Go tests check the readiness probe's trust hash, prevent plaintext
transport overrides, and reject sidecar aliases of the certificate
Secret through volumes, projections and environment references. The
cluster drill covers the HTTP-only TLS channel. Managed client
authentication, rollover and validation at the Gateway
still require qualification.

## Managed Basic Auth startup

Build the pinned Java runtime with `tools/prepare_java_runtime.py` using
`--build --basic-auth`, and include the produced auth JAR on the image's
classpath. The qualification uses the same image digest for the peer DNS,
HTTP TLS, Basic Auth and strict warm-up operator flags. Enable
`ServerlessBasicAuth` with an immutable Secret containing a generated
OpenSearch keystore and matching bootstrap password file. Use a
precreated S3 remote-store bucket. The operator takes the repository
settings by reference.

In Kubernetes 1.32.0 kind, the readiness command verified the Pod DNS
certificate and rejected anonymous health requests. Ingest wrote to
the precreated SeaweedFS bucket, and both original search and the new
strict search Pod served the document. The controller cleared the
strict warm-up slot after authenticated completion samples. The Pods
rejected plaintext HTTP and an incorrect TLS identity. Recreating the
credential Secret under the same name blocked further Pod creation while
the running Pods retained their original credentials and served requests.
The standard `runtime/Dockerfile` built from the fresh pinned Java bundle
produced a Linux image with manifest digest
`sha256:9b235b7c4b965d9e0a421ea7f934e4be0d6661ae16e4ae45570993643be967fc`.

A Linux image test at UID 10001 substituted a different valid keystore
and password behind the source mount. The entrypoint exited before
starting Java. The approved bundle and the ordinary keystore mount both
reached Java. `go test ./api/... ./cmd/... ./internal/... ./pkg/...`
covers staging, admission protection and direct controller probes.
`TestAPIServerValidatesInstanceAndStatus` with the `envtest` tag
verified that Kubernetes rejects enabling or disabling this startup mode
after Pod creation starts and rejects removing it after the baseline is
pinned.
This drill serves traffic directly over Pod HTTPS. Edge promotion and
rollback with authentication still require verification.

## Operator account changes

Enable `--development-basic-auth-operations` on the leader-elected
operator and run the [managed Basic Auth](#managed-basic-auth-startup)
runtime against a precreated SeaweedFS bucket. Use two immutable
`serverless.opensearch.org/account-password` Secrets and apply one
`ApplySecurityConfiguration` action for creation, rotation and removal.
The kind run observed HTTP 401 for the old password after rotation and
for the final password after removal. The three operations succeeded on
one ingest and two search processes with unchanged Pod UIDs and no
container restart. A request with the earlier revision returned
`Conflict` and left the active revision unchanged. A reference to an
ordinary `Opaque` Secret returned `InputsUnavailable` without acquiring
an instance claim.

Run `go test ./internal/controller -run '^TestSecurityAccountOperationRecovery$' -count=1 -v`.
The fault fixture commits the simulated password change but loses the
write response. Reconciliation verifies the result, records the revision
on the instance, and completes from the receipt after a controller
restart. A reserved write without a dispatch, a replaced Secret UID, and
a missing JVM each hold verification without replaying the mutation. A
deletion request in the unresolved dispatch case retains the finalizer.
These fault tests exercise controller recovery. The kind run verifies
the healthy-store Java path. It leaves multi-writer behavior, external
writer fencing, and partitioned revocation unqualified.

## Scale changes and Pod revision continuity

Kubernetes 1.32.0 kind ran one search and one ingest Pod using the
previous count-dependent operator and the managed HTTPS and Basic Auth
Java runtime on a precreated SeaweedFS bucket. The in-place operator
upgrade recorded the old revision in `status.templateRevisionAliases`
while both original Pods were still serving. Increasing search from one
to two preserved both original Pod UIDs. The added search Pod used the
old fleet's literal label, warmed strictly, and read the seeded document.
Both preview roles then reached Ready with three Pods on the normalized
revision while the serving fleet kept its original label.

The Go regression tests also exercise spare counts and per-zone serving
floors, preserve zone identity and JVM heap changes in the template
revision, and hold a source role whose mixed older labels cannot be
verified. The API-server rollout regression passed.

## Strict search scale-down

Kubernetes 1.32.0 Kind ran two search and two index Pods against a
precreated SeaweedFS bucket. The fixture replaced the cold-bootstrap
search Pod before the trial and verified two strict-warm search Pods.
Lowering the search reserve from one to zero preserved both Pod UIDs
while the voluntary retirement limit was zero. Setting the limit to one
while the planned survivor reported `DisruptionTarget=True` also held
retirement.

After clearing the signal, a test admission policy denied the real
Eviction request. The watch recorded the victim UID claim, then observed
the claim clear and `status.scaleDownRetryAfter` appear while both Pod
UIDs remained. The controller retried after the policy was removed and
the delay expired. It retired the recorded UID and cleared the claim
after the Pod disappeared. The original strict search survivor, both
index Pods, and both stable Services kept their UIDs. The index Service
acknowledged writes before and after retirement, and the search Service
returned both documents afterward.

The operator ServiceAccount could create `pods/eviction` in the test
namespace and had no Node list permission. The Go and real API-server
suites passed. The controller unit test covers overlapping PDBs and
substitution of a foreign PDB. The drill used plaintext transport and one
Kind zone with a positive search target.

### Retire the original search bootstrap

A second Kubernetes 1.32.0 Kind drill began with one recorded bootstrap
and one strict-warm search Pod. Both stayed in service while the requested
search replicas plus reserve totaled two, even with a positive retirement
budget. After the reserve was reduced to zero, the watch showed a
retirement claim for the recorded bootstrap UID. The operator evicted that
Pod and cleared both the retirement and bootstrap claims. The strict
survivor kept its Pod UID and JVM incarnation. Both index Pods and stable
Services kept their UIDs. Search found the writes acknowledged before and
after retirement. The fixture needed no manual eviction.

The Go and API-server suites passed. Unit cases prevent a same-name Pod
with a different UID from inheriting the bootstrap exemption and hold
when a Kubernetes Ready regular Pod fails Java qualification. Additional
planner cases cover a missing zonal target, a reserve shortfall, and the
regional serving floor. The live drill used one Kind zone.

### Move search capacity between zones

Two Kubernetes 1.32.0 Kind nodes carried different zone labels. Search
began with one Pod per zone and index stayed in the original zone. The
fixture cordoned the destination, set the source search target to zero,
and raised the destination to a replica plus a reserve. The destination
replacement stayed pending, and the source Pod stayed Ready. Its PDB
still had `minAvailable: 1` and zero allowed disruptions at the observed
generation. The API server rejected a dry-run eviction for the source UID.

After uncordoning, the replacement completed strict Java warm-up. The
source PDB kept its UID and reached a zero floor. The operator claimed and
retired the original source UID. The two destination search Pods were
strict-warm. The original destination JVM, both index Pods and the stable
Services kept their identities. The index route acknowledged a write
before the move and another after it. The search Service and both
destination Pods returned both documents. The periodic health sample
returned the instance to `Available=True`, and both claims were clear.

The Go and real API-server suites passed. The source zone entry remained
in the spec with a zero target. This test covered a planned move on
labelled Kind nodes with shared placement. Unhealthy source cleanup,
deleting the zone entry, and writer retirement are separate cases.

### Index writer recovery after PDB-governed eviction

Kubernetes 1.32.0 Kind ran two Java index Pods and two search Pods
against a precreated SeaweedFS remote store. The fixture identified the
active writer of a single test shard from its write acknowledgement,
local held-shard stats, and the durable shard head. It reduced the
requested index count to one and submitted a UID-preconditioned Eviction
for that writer through the existing PDB.

The surviving Java process opened the same shard at term 2. The durable
head and the local writer agreed on the new owner, and the survivor
acknowledged same-shard writes at that term. Of 100 client attempts, 98
returned HTTP 201, one returned HTTP 421 `not_the_writer` while the shard
had no owner, and one received no HTTP response. The search Service
returned all 98 acknowledged load documents and the seed document. The
captured logs supplied no per-shard shutdown receipt, so the drill
establishes ownership and data recovery after eviction. It leaves the
exact publication path and time of release unverified.

The fixture submitted the eviction and used plaintext on one Kind node.
Automatic writer retirement still needs a complete pre-eviction
ownership and capacity check and a way to observe interrupted handoff
before authorizing another retirement.
[Writer handoff contract](../design/reconciliation.md#index-writer-handoff-evidence)

## Serving revision after total Pod loss

The kind drill upgraded a serving HTTPS and Basic Auth instance with the
older literal revision. After scaling up search, it observed both role
pins in `status.servingRevisionByRole`. It stopped the operator, deleted
both search Pods, verified the role was empty, and restarted the operator.
Both replacement Pods became Ready with the old revision label and each
searched for the document previously written to the precreated SeaweedFS
bucket. The ingest Pod kept its original UID. After recovery, the three
green preview Pods became Ready on their separate normalized revision.

The Go tests reject adoption from a gated or foreign Pod, preserve an
existing compatible pin when the cohort is empty, and hold when rollout
uses an incompatible pinned template. API-server tests verify that a
first creation reservation establishes no pin and that ordinary promotion,
rollback and weighted promotion persist their expected fleet and revision.

## Authenticated Envoy writer retirement

A fresh namespace on Kubernetes 1.32.0 Kind used Envoy Gateway 1.8.6,
two index JVMs, one search JVM and a precreated SeaweedFS S3 bucket.
The Basic Auth Java runtime ran by digest. Both client routes used
verified TLS, and Envoy used backend TLS to Java. Both hostnames
rejected missing and invalid credentials before retirement. After
eviction they still returned HTTP 401 for anonymous requests.

1. The index route acknowledged `seed` on the single shard at primary
   term 1 and sequence number 0. The drill identified the owning JVM
   and targeted its exact Pod UID.
2. The manager recorded Java withdrawal and held at
   `AdmissionClosurePending` with no evidence ConfigMap. Envoy retained
   the victim in EDS as unhealthy and reported it as `DRAINING` in the
   active cluster. The fixture's route-scoped policy disabled panic
   routing, which was verified in the active Envoy configuration.
3. The observer verified EDS, active host health and authenticated
   requests through the serving Envoy replica. The publisher wrote the
   signed observation. The same operation recorded
   `WriterAdmissionClosed`.
4. The drill submitted a manual `policy/v1` Eviction with Pod UID and
   resource version preconditions. The API admitted it under the
   index PodDisruptionBudget, and the old Pod disappeared. The
   existing surviving JVM acknowledged `after` on the same shard at
   primary term 2 and sequence number 1 through the index route. Search
   returned both `seed` and `after`.

The optional shutdown receipt returned HTTP 404. The test verified
takeover through the authenticated write and search responses. The
manager's development path stops at admission closure and retains the
claim. This drill used one node and one Envoy replica.
[Configure and publish writer withdrawal evidence](java-runtime-build.md#enable-the-development-writer-retirement-drill)

### Namespaced writer evidence publisher Job

The observer and publisher compile as separate static Linux AMD64
binaries. The publisher accepts a mounted report and applies the same
parser and input limit as stdin. The regression test verifies file
selection and rejects an oversized input.

A fresh isolated Kubernetes 1.32 API server accepted the Job in a
server-side dry run. A request impersonating the platform publisher
ServiceAccount, with its normal groups, updated the pre-created
evidence ConfigMap in the instance namespace. The API denied
ConfigMap creation, updates to unrelated ConfigMaps, Secret reads,
Node listing and access to an unrelated instance. The isolated API
server exercised manifest admission and authorization.

The mounted-file integration test used the same Role and a journal
persisted through the real status subresource. Under the platform
ServiceAccount identity, the publisher updated the original ConfigMap
and produced a signature accepted by the operator's evidence verifier.
Publishing an earlier report preserved the ConfigMap resource version.
This test used synthetic Envoy observations. The native run used the
same assertions with Windows-specific fixture cleanup and passed.
Verify includes the Linux envtest case. At this stage the live Envoy
drill used the standalone tools. The packaged Job run is recorded below.

The signed writer publisher regression also simulates two ConfigMap
races. A competing create stores an older signed report, and the retry
publishes the newer report. A competing update stores a newer signed
report, and the retry verifies and preserves it.

The staged Job can start before the observer captures the report.
It polls for one named input ConfigMap through the Kubernetes API.
A regression covers a missing ConfigMap, an empty ConfigMap and a
populated report. A Kubernetes 1.32 API server admitted the Job in a
server-side dry run. Using the publisher ServiceAccount identity,
the staged path signed the synthetic report in the pre-created output
ConfigMap. An unrelated input read was denied, and a subsequent older
file-based report left the output unchanged. The assertions passed with
the Windows envtest assets and separate fixture process cleanup.
The Kind API was unreachable for a kubelet and Envoy run.
[Publisher Job procedure](java-runtime-build.md#run-the-writer-evidence-publisher-as-a-job)

#### Local staged publisher image

The Linux AMD64 fixture image pins manifest
`sha256:7ac767f5edddbb36a7986c1cfa8414cc1081010c04e20621ab0a11015cc8e5eb`.
Its config runs as UID and GID 65532. The archive verifier checked
the manifest and both blob checksums. A second readback checked the
uncompressed layer digest, executable mode, static publisher bytes
and CA bundle. The workspace also contains a copy of the example Job
with the local digest filled in. The Dockerfile target build returned
HTTP 500 from Docker Desktop. The direct import also received HTTP
500 before containerd could be reached. Neither Kind API passed the
readiness check before the restart described below.

### Graceful shutdown and receipt readback

The final pinned Java image
`sha256:e6c7a5a7ab768bb402e64ac87a92fd3a130bae6afdaef23a4da6b46b8365296c`
repeated the signed withdrawal and manual PDB-aware eviction in a fresh
Kind namespace with a separate SeaweedFS bucket. The Pod kept the default
30-second grace period. Containerd recorded exit 143 **1.41 seconds**
after SIGTERM and no force kill.

The HTTPS Basic Auth receipt request through Envoy search returned HTTP
200 for the exact retired JVM. It recorded shard 0 of the seeded index
as `RELEASED` at primary term 1 with local cleanup complete. The
existing survivor acknowledged a write through the index route on the
same shard at term 2. Search returned the seed and successor documents.
The earlier unpatched run and the assignment-only patch each hit the
30-second deadline in separate optional S3 delete paths. Automatic
eviction and successor qualification remain outside this drill.
[Shutdown implementation](java-runtime-build.md#keep-advisory-assignment-cleanup-out-of-shutdown)

### Durable ownership before eviction

In a fresh Kind namespace, the final manager and pinned Java image
waited at `AdmissionClosurePending` with no head inventory. After the
signed Envoy evidence arrived and Java closed admission, the manager
recorded `HeadInventoryReady=True` and bound the scan to the exact
surviving index Pod and JVM.

The journal's index UUID, shard 0 and primary term 1 matched a separate
TLS and Basic Auth read of the durable `writer-single-shard` head.
Direct process and Kubernetes readback confirmed that the original
writer Pod and JVM were still running. An earlier fixture recorded two
heads. Its entire inventory survived a rollout to the final manager
image. The API-server regression also verified that a failed scan does
not persist a partial result and that the operation retains its claim.
[Ownership journal](java-runtime-build.md#record-durable-ownership-before-eviction)

### Automatic eviction and successor write

The fresh `operator-writer-auto-eviction` namespace used Kubernetes
1.32.0 Kind, Envoy Gateway 1.8.6, two index JVMs, one search JVM and a
separate SeaweedFS bucket. The manager ran by platform manifest digest
`sha256:a19bd9eef0932472d430d3040d72c16c60f612377fd55dec5630b681acd48229`.
Both index Pods had `restartPolicy: Never`, and the Java image matched
the graceful shutdown drill above.

1. The exact writer stayed running at `AdmissionClosurePending` before
   the platform published signed proof from the live Envoy replica.
2. After publication, the manager closed admission, recorded the durable
   head, armed eviction and confirmed the original Pod UID was gone.
   The test did not issue eviction or deletion.
3. Containerd recorded exit 143 approximately 1.40 seconds after
   SIGTERM, within the 30-second grace period. The authenticated Java
   receipt returned HTTP 200 and reported `RELEASED` for shard 0 at
   term 1.
4. The original survivor acknowledged `after-auto-eviction` on the same
   index and shard at term 2 through the verified TLS and Basic Auth
   index route. The authenticated search route returned both `seed`
   and `after-auto-eviction`.

The API-server regression also confirmed that a stale UID is rejected
when evicting an unready same-name replacement. The operation remains
claimed pending automated successor qualification.
[Eviction checks](java-runtime-build.md#evict-the-withdrawn-writer)

### Controller receipt readback after upgrade

The receipt-readback manager
`sha256:8b48e01f510bb4b3548ed95d0e178c84f8661ea5ee25b2d4ef579572ae1e5c5c`
resumed the same operation in the automatic-eviction fixture. It read
the receipt over direct Java HTTPS and Basic Auth through the original
surviving index JVM. The API status matched the independent receipt for
the same index UUID, shard 0 and term 1, recorded `RELEASED` with local
cleanup complete and set `WriterReleaseReady=True`. The operation
remained `Running` and held the original instance claim.

The API-server journal test covered a missing receipt, retry and
retention across a new reconciler. The receipt matching unit test
rejected a new index UUID, a different term, an unconfirmed outcome and
a member result for a bucket head.
[Receipt journal](java-runtime-build.md#record-the-writer-release-receipt)

### Java successor write evidence

The pinned Java overlay built the Basic Auth runtime bundle and
passed `ServerlessAuthTests.testWriterSuccessorRequiresANewerForegroundWrite`
on Windows with Temurin JDK 25. The translog file-descriptor test was
skipped because it requires `/proc/self/fd`. The HTTPS test returned pending for a writer already
open at term 2. A foreground index request changed the exact reader
response to HTTP 200 at term 2. The test covered anonymous and
wrong-account requests, a wrong JVM, a wrong UUID and the same term.

The Go adapter tests exercised TLS, Basic Auth, request identity, strict
newer-term validation and typed pending status. Controller-driven
successor qualification still requires an end-to-end drill.
[Successor endpoint](java-runtime-build.md#observe-a-successor-write)

### Automatic writer retirement completion

The fresh namespace `operator-writer-successor` used one Kubernetes
1.32.0 Kind node, Envoy Gateway 1.8.6, two index JVMs, one search JVM
and a separate SeaweedFS S3 bucket. The manager and Java platform
digests were
`sha256:5c7a52250e1ea487e8a3db9806fb43a3014c345335b7f2b8a5c0c79f9859324f`
and `sha256:da6ef620d0276aeb5145a0745c01f0f25eb961636b90e7e7444fd13ee395ead4`.

1. The platform published signed evidence from the live Envoy replica.
   The controller closed admission, recorded the exact shard, performed
   the PDB-aware eviction and read back the release receipt. The victim
   exited in 1.40 seconds of its 30-second grace period.
2. Two reads eight seconds apart confirmed the original claim was
   still held and `WriterSuccessorReady=False`. Direct authenticated
   Java HTTPS returned `writer_successor_pending`.
3. A write through the verified TLS and Basic Auth index route reached
   the original survivor on the same shard at primary term 2. The
   authenticated search route returned both documents. The scoped Java
   endpoint returned the exact writer identity and term.
4. The operation recorded the survivor against the original inventory
   entry, set `WriterSuccessorReady=True` and `Succeeded`, and cleared
   the original instance claim.

An upgrade to the final manager
`sha256:68beba1717b244ca323e4686e8d4cd2933d9c781f5f18ac790a029ef1740350e`
preserved the exact successor entry and completed operation. The instance
claim and retirement finalizer were absent after restart.

The Go suite and plan check passed. The Kubernetes API-server test
covered the shared bucket hold, same-term rejection, continued
read-only verification after deletion, and claim release from the
persisted success record with the backend disabled.

#### Open writer head preflight

Before arming withdrawal for automatic eviction, the manager now
reads the exact open writer's head inventory over HTTPS with the
scoped Basic Auth reader. The scan is bounded by the writer lease,
the capacity evidence window and a 30-second cap. The HTTPS adapter
test accepted an independent shard, rejected a shared bucket and
treated an incomplete page as unverified.

The real API-server journal test injected both preflight holds.
The claim and unarmed reservation remained recorded, and the backend
received zero withdrawal requests. Once the gate cleared, the
reconciler journaled the withdrawal authorization before dispatch.
The open scan does not replace the complete post-closure inventory.
The current Kind API timed out, so the change was not exercised on a
live Envoy writer in this pass.
[Writer preflight and eviction](java-runtime-build.md#evict-the-withdrawn-writer)

#### Live signature and route recheck

A readback from the same Kind fixture independently verified the
published ConfigMap's Ed25519 signature against the fixture public key.
The signed observation identified the same operation, victim Pod UID
and index route and covered its one serving Envoy replica. Kubernetes
still reported the operation as Succeeded, the exact victim UID was
absent, and the instance claim was clear.

The original successor had acknowledged the post-eviction write at
term 2. By the recheck, a different index JVM acknowledged
`retirement-live-recheck-d067105f` on the same shard at term 3. The
Envoy search route returned both documents with their expected
payloads. Both routes used certificate verification and Basic Auth.
The recheck evidence is in
`tools/bin/oss-writer-successor/live-retirement-recheck.json`.

#### Reusable staged publisher drill

The verifier checks staged Job completion, validates the Ed25519
signature, and compares the signed body with the Job input and
withdrawal journal. It confirms the exact Pod UID is gone before
sending the controlled successor write. It checks the seed and
new document through search and ties the write receipt to the
successor recorded for the original shard.

A replay used the recorded Kind operation and route responses
with a staged Job stub. OpenSSL verified a freshly signed copy
of the recorded report and rejected a modified copy. The local
HTTPS fixture confirmed that both routes retain their SNI and
Host names through a tunnel. It rejected an untrusted hostname
and incorrect Basic Auth. The original Kind API failed its etcd
readiness check and timed out on operation readback. The alternate
Kind API closed the connection. The live staged-Job drill remains open.
[Live writer drill](java-runtime-build.md#verify-live-writer-retirement-through-envoy)

#### Live staged publisher and writer retirement

After Docker Desktop restarted, containerd imported the Linux AMD64
publisher archive and CRI resolved its exact manifest digest. The
single Kind node had no free Pod slots. Its kubelet limit was raised
from 110 to 128 within its existing /24 Pod CIDR. The original
kubelet config was backed up and existing Pods were preserved.

A fresh `operator-writer-staged` namespace used Kubernetes 1.32.0,
Envoy Gateway 1.8.6, two index JVMs, one search JVM and a separate
SeaweedFS bucket. It used the same pinned manager and Java images as
the automatic writer retirement completion drill.

1. The original writer acknowledged the seed for shard 0 at primary
   term 1. After withdrawal, the operation waited at
   `AdmissionClosurePending` with the original Pod still running.
2. The publisher Job started before the Envoy observation. It ran under
   `writer-withdrawal-publisher`, and the kubelet reported the pinned
   publisher image and exit code 0. The verifier independently checked
   the Ed25519 signature and matched the signed payload to the live
   Envoy report delivered through the Job's input ConfigMap.
3. The operator armed eviction and the exact original Pod UID
   disappeared. The journal recorded `RELEASED` with JVM cleanup
   complete. The drill issued no eviction or Pod deletion.
4. The survivor acknowledged the new document through the index route
   at primary term 2 on shard 0. The search route returned the original
   seed and the exact new document with their expected values. Both
   requests used verified TLS and Basic Auth.
5. The journal recorded the same successor JVM and term, marked the
   retirement `Succeeded` and released its instance claim.

The verifier record and an independent Job and operation API readback
are in `tools/bin/oss-writer-staged-live/live-staged-result.json` and
`tools/bin/oss-writer-staged-live/live-staged-api-readback.json`.

#### Deletion during ambiguous eviction

The operation API-server regression injected an ambiguous eviction
response and deleted the operation before Pod absence was journaled.
The controller kept the claim and issued only readback while the
original UID remained. After readback confirmed absence, it recorded
the receipt and a newer-term successor write, released the claim and
removed the finalizer. The fixture recorded one dispatch attempt.

The Go readback test covered a terminating original Pod, a same-name
replacement, a missing Pod and an unavailable API. The real API-server
eviction regression also passed its PDB rejection, lost-response and
stale UID checks.

[Successor verification contract](java-runtime-build.md#observe-a-successor-write)

### Writer recovery without a shutdown receipt

The Java Basic Auth runtime bundle built on Temurin JDK 25. The HTTPS
successor test verified the durable WAL settlement field.
`NodeLogTakeoverIntegrationTests` exercised recovery of unpublished
writes through two takeovers. The Go reader rejected a newer-term response with an
omitted or false settlement field when recovery required it.

The operation API-server test recorded a missing receipt, held the
claim through an observed newer-term write with unsettled WAL, and
completed only with positive settlement evidence. It recovered claim
release from persisted status. The ordinary receipt path also passed.

In the `operator-writer-wal-recovery` Kind fixture, the test interrupted
the exact admission-closed JVM with SIGKILL after inventory was
recorded. The JVM exited with code 137. The test removed the failed
Pod with UID and resource-version preconditions, then enabled automatic
eviction so the manager could recover the existing operation from
readback. The read through the authenticated search route returned
`writer_handoff_receipt_missing`. The claim remained held until the
original surviving JVM accepted a term-3 write through Envoy over
verified TLS and Basic Auth. Search returned the term-1 seed and the
new write. The operation recorded `WALRecoveryVerified` and
`predecessorWALSettled=true`, reached `Succeeded` and released its
claim. No release receipt appeared. The victim had already published
the seed before the crash.

The follow-up Java tests passed with the exact index and JVM guards
on the settlement CAS. Wrong UUID and JVM checks preserved the WAL
references. A one-shot storage failure while clearing predecessor
references retried and settled after unpublished writes were replayed.

The stricter `operator-writer-wal-unpublished` fixture ran manager
`sha256:257a2a6ed9163d993d51c9168c3fd31e6df881691f6bef0216c7870263c5c5ea`
and Java
`sha256:91d29697abeabd41592c83117081c7c0dea231c531b98001e52e36fc9c23d025`.
After the baseline document was published, the exact admission-closed
writer acknowledged another document at term 1, sequence 1, with WAL
durability and refresh disabled. The S3 manifest ETag and SHA-256 did
not change from before that acknowledgment through the JVM's SIGKILL
and exit 137. The exact JVM's receipt remained absent.

After Pod removal, the controller held the claim until the original
survivor accepted a write at term 3 through the verified TLS and Basic
Auth index route. Search returned the baseline, unpublished and
successor documents. The journal recorded `predecessorWALSettled=true`
and `WALRecoveryVerified`, reached `Succeeded` and released the claim.
Seven focused Java tests passed with zero skips. The Go suite, API-server
journal regression and plan check passed.

[Recovery contract](java-runtime-build.md#recover-a-writer-without-a-receipt)

### S3 access loss before writer eviction

The `operator-writer-store-outage` fixture ran on Kubernetes 1.32.0 in
Kind with Envoy, a dedicated SeaweedFS S3 bucket, and the Java image
`sha256:91d29697abeabd41592c83117081c7c0dea231c531b98001e52e36fc9c23d025`.
The operator image built from the current controller was imported and
resolved through CRI as
`sha256:2c2a31bbd3cb73679445419975d79237486cad724ecf57cd45359fe18db8750a`.

With automatic eviction disabled, the fixture submitted `RetireWriter`,
published signed Envoy withdrawal evidence, and verified the durable
inventory. The closed writer then acknowledged a write at term 1 and
sequence 1 through its WAL. The S3 segment manifest ETag and hash stayed
unchanged across that acknowledgment.

The drill removed the fixture S3 Service's backing endpoint by changing
its selector. The probe confirmed approximately 43 seconds without a
connection to the store. While the Service was unreachable, the fixture
enabled automatic eviction and restarted the manager. The operation
armed eviction and reported `WriterEvictionPending`. The exact writer
Pod was still present without a deletion timestamp. The same operation
retained the instance claim and its original inventory. Restoring the
selector returned the probe to HTTP 200 against the same S3 process.

The fixture refreshed signed Envoy evidence. The operator then evicted
the original writer and recorded its release receipt. The original
survivor accepted a term-2 write through the Envoy HTTPS index route
with certificate verification and Basic Auth. The corresponding search
route returned the published seed, the WAL-only document, and the new
write. The journal identified the exact survivor, reported `Succeeded`,
and released the claim.

Use `tools/verify_writer_store_outage.ps1` with the explicit Kind
kubeconfig and the prepared fixture directory. The directory needs the
signed writer fixture, its TLS probe Pod, and the verified WAL-only
acknowledgment saved as `unpublished-ack-proof.json`. Run the script once
to inject the outage and restore the Service, refresh signed Envoy
evidence, then run it with `-VerifyRecovery`. The two stages write
`store-outage-hold.json` and `store-outage-result.json` in that directory.
This drill uses a connection failure before eviction.

### S3 throttling and access loss after eviction dispatch

Use separate signed writer fixtures in `operator-writer-s3-faults`
and `operator-writer-s3-post-dispatch`. Label the SeaweedFS Pod
`app: s3-backend` and apply `tools/s3-fault-proxy/fixture.yaml`
in each namespace. Keep the
repository endpoint `http://s3:8333`. Build `./tools/s3-fault-proxy` for
the Kind node's OS and architecture, place the binary `s3-fault` beside
the fixture Dockerfile in the image build directory, and import
`oss-s3-fault-proxy:local` into the node. The proxy forwards to the
original store in healthy mode. Throttling returns S3 `503 SlowDown`
responses. Disconnect mode closes active request connections, including
requests on reused connections. Proxy logs record the client IP and
method without recording signed headers or object keys.

Prepare each journal and WAL-only acknowledgment as in the pre-eviction
drill. Run `tools/verify_writer_store_outage.ps1` with `-Fault Throttle`
to test throttling and restore S3 access. Run it with
`-Fault Throttle -VerifyRecovery` to check the newer-term write and all
acknowledged documents through Envoy. The fixture records
`store-throttle-hold.json` and `store-throttle-result.json`.

In the separate post-dispatch fixture, let the initial Envoy report age
past its 30-second window before enabling eviction. Start the script
with `-Fault AfterDispatch`. Wait until it writes `watching-eviction.txt`,
then refresh the signed Envoy evidence. The watcher starts the
disconnect after observing the exact Pod's deletion timestamp or
journaled absence. To start after the Pod is already gone, run the
same command while the armed operation still awaits successor proof.
The script checks that the survivor's S3 requests failed and that the
operator kept the claim with the victim gone. Run the
script with `-Fault AfterDispatch -VerifyRecovery` after it restores the
proxy. This stage verifies the exact successor and reads all three
documents back through Envoy. The fixture records
`store-outage-hold.json` and `store-outage-result.json`.

In the throttling fixture, the proxy returned `503 SlowDown` for about
42 seconds and recorded 68 requests from the surviving index Pod. The
operator held the armed eviction during throttling. After access was
restored, an independent Java scan found no heads owned by the retiring
JVM. The original journal still contained the term-1 head, so the
operator stayed at `WriterHeadsChanged` with the original Pod and claim
intact. The surviving JVM acknowledged a term-4 write through Envoy
with verified TLS and Basic Auth. Envoy search returned the published
seed, the WAL-only document, and the successor write. Data-serving
recovery passed. Automatic retirement remained held and requires
explicit recovery when the durable inventory changes.

In the post-dispatch fixture, the operator evicted the exact original
writer and journaled its absence and release receipt. Before a successor
write, the proxy disconnected S3 requests for about 15 seconds. It
recorded two disconnected requests from the original surviving index
Pod. The operation stayed at `SuccessorWritesPending` with the claim
held and no successor proof. After the proxy restored access, the
survivor acknowledged a term-2 write through the Envoy HTTPS index
route with certificate verification and Basic Auth. The search route
returned the published seed, the WAL-only document, and the successor
write. The operation reached `Succeeded` and released the claim.

Both runs used the Kind, operator, and Java versions from the
pre-eviction test. The fault proxy image resolved in Kind as
`sha256:1588f9df0d92f13ccc964dc106fefbfd5dd3bbd77ec9f906347b20d6277acde2`.
The sustained post-dispatch outage started after the release receipt
was already recorded. The interval between eviction acceptance and
receipt recording still lacks a complete outage run.

### Partial writer receipt recovery

The API-server regression loaded a saved receipt that covered one of
two independent shard heads. A successor write for the covered head
was recorded. A newer-term write for the omitted head kept the
instance claim held until the JVM supplied explicit predecessor WAL
settlement. The controller preserved the original receipt, recorded
both successor writes, and released the claim after persisting
`Succeeded`. The Go unit tests covered selecting the WAL check per
shard and rejecting duplicate receipt entries. The existing complete
and missing receipt journal regressions also passed.

The Java integration test
`WriterHibernationTests.testPartialShutdownReceiptAllowsTakeoverOfTheUnconfirmedShard`
used the filesystem-backed store. It failed publication for one shard
during graceful shutdown and verified the runtime's durable report
contained one `RELEASED` row and one `UNCONFIRMED` row. The unconfirmed
shard still had no published segment manifest when the original writer
closed. A fresh JVM recovered both prior writes, accepted new writes
at newer terms and cleared the unconfirmed shard's predecessor WAL
references. No production Java behavior changed in this test overlay.

The manager image was built and imported into Kind by digest
`sha256:764a190a363beedd9a9c328b4d41e3b36439cb9a071cfed094e3712a3ad44999`.
The one-node cluster was at its 110-Pod limit and no new standalone
Kind fixture ran. The Java and Kubernetes API-server tests provide
separate evidence for receipt generation, data recovery and claim
handling.
[Recovery contract](java-runtime-build.md#recover-a-writer-without-a-receipt)

## Authenticated Envoy blue/green rollout

### Exact platform manifest and authenticated routes

The archive-based alias fix was verified in the fresh
`operator-envoy-task` namespace on the one-node Kubernetes 1.32.0 Kind
cluster with Envoy Gateway 1.8.6. The helper imported the existing
manager through its bare digest using the local-tag fallback, and the
Java runtime directly by digest. Containerd created the aliases from
archive content. A new curl fixture repository had no local CRI entry
before import. A Pod then started from its exact digest with
`imagePullPolicy: Never`. The curl OCI index digest was rejected. The
six importer tests include a digest alias that points at a different
manifest.

The wrong rollout credential held both production selectors at
`initial`. After correcting it, promotion moved search and index to
`green`, and rollback restored both to `initial`. The search selector
advanced before index during promotion. At each completed transition,
authenticated HTTPS requests identified both expected Java roles and
fleets. Index acknowledged a write on green and another after rollback.
Search returned both documents after rollback. Both operations recorded
`Succeeded` and released the instance claim. Both stable Service UIDs
match the rollout journals. Their ClusterIPs matched the snapshot taken
during promotion.

After rollback, both routes returned HTTP 401 for anonymous and
wrong-password requests and rejected an invalid TLS hostname. Envoy's
live configuration used the expected Java CA for two search endpoints
and one index endpoint. Both fleets used the same pinned Java image
and an externally created SeaweedFS bucket. The Go suite, the API-server
rollout conflict regression, and the plan check passed.

### Earlier exact platform run

A fresh one-node Kubernetes 1.32.0 Kind cluster ran Envoy Gateway 1.8.6,
a manager built from the workspace, and one digest-pinned Java build in
both fleets. SeaweedFS served a bucket created outside the operator. The
fixture enabled strict search warm-up and waited for Ready candidate Pods.

Docker could find the local BuildKit image's `linux/amd64` manifest via
its tag. Docker rejected a direct export by the nested manifest digest.
The import helper's `repository:tag@sha256:...` form registered the
requested manifest under its repository-qualified digest. A separate
Pod ran that exact digest with `imagePullPolicy: Never`. Supplying the
OCI index digest with the same tag failed before export. The importer
unit tests also cover a tag change between inspect and export.

Authenticated requests through both Envoy hostnames identified the Java
role and fleet in each phase. Both client routes returned HTTP 401 for
anonymous and wrong-password requests in all three phases. After
rollback, both rejected an invalid TLS hostname. The active Envoy
configuration used the expected Java CA for both search endpoints and
the index endpoint.

| Phase | Search fleet | Index fleet | Data check |
| --- | --- | --- | --- |
| Baseline | `initial` | `initial` | Both client routes identified their expected Java role. |
| Promotion | `green` | `green` | Index acknowledged `secure-promote` and search returned it. |
| Rollback | `initial` | `initial` | Index acknowledged `secure-rollback` and search returned it, the green write, and the seed. |

Both rollout operations recorded `Succeeded` and released the instance
claim. Both stable Services kept their original UIDs and ClusterIPs.
The Go package suite, `TestRolloutStatusAPIServerConflict`, the importer
and Envoy backend-trust unit tests, and `python tools/verify_plan.py`
passed. This test exercised same-image fleet switching on one Kind node
with one Envoy replica.

### Rerun with direct digest import

For a locally built image, use the platform manifest reported by Docker
when pinning the Pod image. The importer accepts the bare
`repository@sha256:…` reference even when Docker can access that manifest
only through a local tag. It searches tags in that repository, matches the
requested platform digest and verifies the imported containerd alias.
A multi-platform index digest is unsuitable for
this single-platform import. [Docker image inspect](https://docs.docker.com/reference/cli/docker/image/inspect/) · [Docker image save](https://docs.docker.com/reference/cli/docker/image/save/)

```powershell
$image = 'oss-envoy-current:rollout'
$digest = docker image inspect --platform linux/amd64 --format '{{.Descriptor.Digest}}' $image
python tools/import_kind_images.py --node oss-envoy-rerun-control-plane --platform linux/amd64 "oss-envoy-current@$digest"
```

The local-tag fallback rerun used a fresh namespace on the existing
Kubernetes 1.32.0 Kind node and Envoy Gateway 1.8.6. Docker could not
resolve the rebuilt manager by its bare platform digest. After import,
CRI resolved that digest and the manager started with `imagePullPolicy:
Never`. The pinned Java image also imported by its bare digest. With
Docker API 1.48 forced, the direct Java digest import still passed. The
curl OCI index digest was rejected.

Authenticated HTTPS requests identified the Java search and ingest Pods in
`initial`, then `green` after promotion, then `initial` after rollback.
Index acknowledged a write in each phase. Search returned all three
documents after rollback. Both operations succeeded and released the
instance claim. Both stable Services retained their UIDs and ClusterIPs.
After rollback, both routes returned HTTP 401 for missing or invalid
credentials and rejected the wrong TLS hostname. The live Envoy config
showed the expected Java CA for two search endpoints and one index
endpoint. The five importer tests, three backend trust tests, Go suite,
rollout API-server regression and plan check passed. This was a
selector-based rollout with one Kind node and one Envoy replica.

### Earlier direct digest run

On 2026-11-25, a fresh one-node Kubernetes 1.32.0 Kind cluster ran Envoy
Gateway 1.8.6, a manager built from the current workspace, and the same
digest-pinned Java image in both fleets. SeaweedFS supplied an externally
created remote-store bucket.

Before the fix, Docker omitted the repository reference when exporting by
digest, leaving the importer unable to verify the requested image. With the
fix, the importer registered a previously absent manager manifest by its
exact digest and resolved it through CRI. A Pod ran the image with
`imagePullPolicy: Never`. Importing the curl multi-platform index digest
reported that the single-platform export could not preserve that digest.
The four original tag imports resolved through CRI by both tag and their
resulting platform manifest digests.

Both HTTPS hostnames reached their intended Java role in `initial`,
`green` after promotion, and `initial` after rollback. Each phase
acknowledged a document through the index route and returned it through
the search route. After rollback, search returned all three documents.
Both rollout operations succeeded and released the instance claim. The
search and index Services kept their original UIDs and ClusterIPs.

Both restored routes returned HTTP 401 for anonymous and wrong-password
requests and rejected the wrong TLS hostname. The current route and
backend TLS conditions named the expected Envoy Gateway. The active Envoy
configuration used the expected Java CA for both search endpoints and
the index endpoint. The Go suite, the rollout status API-server regression,
the Envoy backend-trust unit tests and the plan check passed. This rerun
covered selector changes on one node and one Envoy replica.

### Earlier authenticated runs

Kubernetes 1.32.0 kind and Envoy Gateway 1.8.6 exercised the standard
Java runtime image with managed HTTPS, Basic Auth and a precreated
SeaweedFS remote-store bucket. Each role used a separate Gateway hostname
and stable Service. The Gateway terminated client TLS and validated runtime
TLS using a CA and backend name from an accepted `BackendTLSPolicy`.
The operator used a dedicated ordinary account for both role probes.

On Windows, the image import helper streams the single-platform Docker
archive directly into the kind node's `k8s.io` containerd namespace. The
helper checks both the Docker export and containerd import exit codes. It
also registers the repository-qualified digest of each imported manifest,
so Pods pinned by digest can resolve the image locally without retagging
it in the fixture or reaching a registry. Before reporting success, it
checks the imported tag and digest through the node's CRI image service.

```powershell
python tools/import_kind_images.py --node oss-https-control-plane --platform linux/amd64 oss-https-runtime:auth-standard oss-https-operator:tls-rollout-rerun oss-failback-s3:local curlimages/curl:8.12.1
```

The 2026-11-24 rerun used an operator built from the current workspace
and the Java manifest digest
`sha256:9b235b7c4b965d9e0a421ea7f934e4be0d6661ae16e4ae45570993643be967fc`.
All four images imported successfully. The Java Pods started from the
repository-qualified digest created by the helper. A missing image failed
with both subprocess exit codes. Both fleets used the same Java image.
With the wrong rollout probe password, both production selectors stayed
on `initial`. After correcting the credential, `Promote` converged both
routes on `green`. `Rollback` returned both to `initial`. On each fleet,
the test verified the JVM and role through the client route, wrote a
document through index, and found it through search. After rollback,
search returned both documents. Both Service UIDs were preserved.
Each HTTPRoute and backend TLS policy was accepted. Both restored routes
returned HTTP 401 for anonymous requests and an invalid password. Both
rejected a client connection with the wrong TLS hostname.

A subsequent run with the current operator (`oss-https-operator:envoy-verify`)
repeated the full switch and confirmed that both Service UIDs and ClusterIPs
were preserved. Both operations succeeded and released the instance claim.
Kubelet events showed local images for Java, the operator, the remote-store
fixture and the curl probe. The updated importer resolved all four tags and
digests through CRI on a second test node and failed on an absent image.
After rollback, the active Envoy configuration showed the expected upstream
CA for two search endpoints and one index endpoint. Search returned the
writes acknowledged by green index and restored index. The Go suite and
`TestRolloutStatusAPIServerConflict` passed on the same workspace.

An earlier prepared rollback stayed on `green` while a retained search Pod
reported degraded warm-up, even though Kubernetes marked it Ready.
The test evicted that Pod through a UID-qualified Eviction API request,
leaving the strictly warmed retained Pod at the configured search floor.
The prepared rollback then completed, released its claim and restored
search to two Ready Pods. In the import-fix rerun, the fixture replaced
the degraded bootstrap Pod before staging green and waited for both
retained search Pods to be Ready in strict mode. Promotion and rollback
then completed and released their claims through the normal operation path.

`go test ./api/... ./cmd/... ./internal/... ./pkg/...`, the
`TestRolloutStatusAPIServerConflict` envtest regression, and
`python tools/verify_plan.py` passed. This drill used one AZ, one Envoy
replica and selector switches with ready EndpointSlices. Traffic under
load requires separate qualification.

### Blue/green JVM startup settings

The development startup catalog was enabled for the digest-pinned Java
image in Kubernetes 1.32.0 Kind with Envoy Gateway 1.8.6, TLS and Basic
Auth. Both initial JVM roles used UTC. The initial search JVMs used a
four-minute warm-up limit. Green used Asia/Tokyo and the explicit G1
flag, and only green search received the five-minute limit. The test
read the ordered arguments from the running Java processes. A candidate
that set the search warm-up limit on ingest was held before preview Pod
creation, and both authenticated routes continued serving.

The controller used the hardened operator build
`sha256:8934d6980458c7457c339c01a95f6a674097aadcb166a0672f1379855d4f6b55`
for promotion and rollback. The tests checked both role routes and
stable Service UIDs. Both fleets acknowledged an index write. Search
returned both writes after rollback. A UID-qualified eviction in each
serving fleet produced a Ready search replacement with the same
revision and startup arguments. The Go and Kubernetes API-server suites
passed. Unit tests cover initial settings persisted with
the first Pod intent, implicit and explicit candidate validation, and
rejection of Java option overrides supplied through the container
environment. A separate Docker smoke test ran the updated entrypoint
with a fake Java executable and verified that it cleared all three
JVM option environment variables while preserving the literal argument.

The rollout profile used one AZ and selector routing. A follow-up
plaintext drill confirmed that the pinned initial settings survived a
direct spec edit and a Ready Pod replacement. It also reproduced the
missing lease seen in the original supplementary fixture. [Remote-store
capacity recovery](#plaintext-readiness-after-remote-store-capacity-exhaustion)

### Plaintext readiness after remote-store capacity exhaustion

Kind Kubernetes 1.32.0 ran the pinned Java image
`sha256:9b235b7c4b965d9e0a421ea7f934e4be0d6661ae16e4ae45570993643be967fc`
with the same operator and a disposable SeaweedFS 4.53 S3 gateway. The
first instance's index and search Pods became Ready. Its startup pin
remained UTC across a direct spec edit and a UID-qualified eviction,
and the replacement Java process used the original revision and UTC.

The gateway started with `-volume.max=8` and a 64 MB volume size limit.
The first bucket's writes allocated seven slots and the second bucket
took the last slot. S3 accepted creation of a third bucket, but the
volume server could assign no writable volumes for its first writes.
The third index Pod's stats reported `valid_now: true` with zero known
nodes and no `expires_in_millis`. The kubelet probe failed with
`local lease observation is incomplete`, and the role EndpointSlice
marked that endpoint unready. Java's internal pre-first-renewal flag
alone is insufficient for traffic admission.

Adding a volume server to the existing store restored the lease without
replacing the index Pod or restarting its JVM. Its EndpointSlice became
ready, search reached Ready, and a write through the index Service was
returned by the search Service. The updated Go probe now reports
`local lease expiry is missing from the membership snapshot` for this
response. The health-probe regression exercises that exact JSON shape
and still requires the expiry. The live drill used the preceding probe
binary to reproduce the original symptom.

### Managed HTTP leaf rotation

The API-server regression stages a replacement leaf under the current CA.
It rejects a replacement without the Gateway backend DNS name and a retained
leaf that expires before the configured rollback window and startup margin.
It also advances the controller clock after staging to verify that promotion
rechecks the remaining lifetime. Both fleets mount their own immutable Secret.
Direct TLS probes reject the other fleet's same-CA leaf, and a same-name Secret
with a new UID cannot reconstruct the rotated fleet's startup material.

The live selector drill used one AZ, one Envoy Gateway 1.8.6 replica and
Kubernetes 1.32.0 kind. Both fleets used Java image manifest
`sha256:9b235b7c4b965d9e0a421ea7f934e4be0d6661ae16e4ae45570993643be967fc`.
The candidate used a new key and leaf signed under the original backend CA.
For each transition the ordinary Basic Auth request through Envoy identified
the JVM and the role. A fresh direct TLS handshake to each identified JVM
then captured its leaf fingerprint. Search and index served the replacement
leaf on green and the original leaf after rollback to initial. Both index
writes were acknowledged and search returned both documents after rollback.
The Gateway used the same CA and backend name throughout. Both backend TLS
policies were accepted after rollback. Both routes returned 401 for missing
or incorrect credentials and rejected a client TLS hostname mismatch.

### Managed HTTP CA rollover

TLS input validation rejects Secrets containing a malformed certificate
block, a PEM header, or an indented certificate marker anywhere in the
leaf chain or CA bundle. A valid certificate later in the PEM cannot
make the Secret eligible for a pin. This applies to HTTP and peer TLS
inputs. The Kubernetes API-server test submitted a
malformed CA block followed by the valid root in a candidate Secret.
Reconciliation reported `PreviewCertificateUnqualified` and left the
candidate and its certificate pin unset.

The Kubernetes API-server regression rejects a new-only bundle while
the retained fleet still serves under the original CA. It pins an overlap
bundle under a different Secret key, verifies the two Pod volume sources,
and negotiates actual TLS connections with old and new CA leaves. A
direct probe rejects the wrong leaf even when the CA bundle trusts it.
The regression verifies rollback qualification and stages a new-only
bundle using the active leaf after modeling original fleet retirement.

Signed platform evidence tests reject missing, mismatched and unfenced
backend trust approvals. The approval remains required after a CA change
even when later Pod trust pins agree. The weighted rollback check selects
the displaced backend's trust bundle when the stable selectors never moved.

In a one-AZ kind cluster, the live selector drill updated Envoy Gateway's
backend trust from the original CA to the overlap bundle. OpenSSL verified
both leaves with the overlap and rejected the new leaf under the original
CA alone. With Basic Auth, both production routes reached Java JVMs serving
the new CA leaf on promotion and the original leaf on rollback. Index
acknowledged writes on both fleets, and search found both documents after
rollback. Gateway trust remained on the overlap bundle for the drill.

A follow-up on one-AZ kind with Envoy Gateway 1.8.6 verified full removal
of the original HTTP CA. After the original fleet's rollback window
closed, the platform fixture completed signed retirement, checked the
evicted JVMs had stopped and removed the original CA and leaf Secrets.
Envoy switched to the replacement root alone. OpenSSL accepted the
replacement leaf and rejected the original leaf with that bundle.

With Envoy on the replacement root, both routes promoted to the fleet
mounting the new-only bundle, rolled back to the overlap fleet, and
then promoted again. Each transition used HTTPS and Basic Auth and
verified the serving fleet and leaf on both routes. Index acknowledged
writes and search returned all four documents after signed retirement
of the overlap fleet and deletion of its trust Secret. All three
remaining Pods mounted the exact replacement bundle. Both routes
rejected anonymous requests and invalid Basic Auth with HTTP 401.

An independent check of the single serving Envoy proxy followed the
active endpoint TLS sockets in the overlap and replacement-only
configurations. The final snapshot verified the replacement root alone
for both search endpoints and the index endpoint. The fixture's signed
approvals used a separate cluster-level trust check.

### Promoted preview cleanup

The Kubernetes API-server regression holds detach while a stable selector
disagrees with the committed promotion. After both selectors match, a fresh
reconciler removes the two archived preview aliases. It preserves the stable
Services, Gateway backends, serving Pod and PDB, leaf pin and rollback target.
A replacement preview Service with a different UID is reported and left in
place. Staging a third fleet is held while capacity is retained, including
after a rollback that leaves the detached fleet behind.

The live selector drill on one-AZ kind removed both green preview aliases
after promotion, then restarted the corrected operator. Both authenticated
Envoy routes still reached green, and a request to stage amber was held.
Rollback through the stable Services restored the original leaf on both
roles. Index acknowledged another write, and search still found the
document indexed on green. The live drill used the selector path. The
API-server regression also verified preservation of the Gateway backend
Services during alias cleanup.

### Displaced fleet retirement

The API-server regression rejects retirement evidence during the rollback
window and holds retirement when the signer is untrusted, evidence is absent,
or a displaced Pod object remains. It finds an owned Pod even when its
instance label is missing. After rollback before preview cleanup, the
formerly serving candidate stays on the signed retirement path. Accepted
evidence leaves a fleet identity record, preserves the serving leaf pin and
allows the next immutable same-CA leaf to stage.

The two-phase retirement regression races an account operation claim against
the reservation in both orders. It checks that selector and weighted rollback
hold after reservation, that a withdrawn provisioning attempt can still be
retried, and that a completion signed for another reservation is rejected.
It changes a reserved Pod's fleet label and verifies that the pinned UID
still holds retirement until the Pod is removed.

The API-server regression also exercises a displaced fleet with different
search and index Pod revisions. It rejects swapped role evidence and a late
search Pod carrying the index revision. Retirement preserves both literal
revision pins after promotion or rollback, including when preview cleanup
follows rollback. A separate test verifies that a signed shared-revision
receipt still completes an existing reservation.

The live run used Kubernetes 1.32.0 kind and one Envoy Gateway 1.8.6
replica in one AZ. It promoted green, detached its preview aliases, and
waited for the original rollback window to expire. The platform fixture
serialized its operations, lowered only the displaced fleet's PDB floors,
and evicted its three Pods with UID preconditions. After confirming their
JVM containers had stopped in the node runtime, it published the signed
retirement receipt. The operator cleared the original rollback target.

The next promotion sent both TLS and Basic Auth routes to amber and both
Java roles served the third leaf. Rollback restored green and its leaf on
both routes. Search found the writes acknowledged through the index route
on the first promotion, second promotion and second rollback. This live
run used selector routing and platform-controlled drain and shutdown.

The reservation rerun recorded the three displaced Pod UIDs before starting
drain. A rollback and an account change both reported
`FleetRetirementReserved`. The same reservation survived an operator restart,
and authenticated requests through both Envoy routes still reached green
while the reservation was active. The operator ServiceAccount could list
Pods and read ConfigMaps in its namespace. It lacked permission to list
Nodes. A signed completion for the persisted reservation released the lock
after the displaced JVMs stopped. Amber then promoted and rolled back to
green through both selector-based routes. Each Java role presented the
expected leaf, and search returned all three indexed documents after rollback.

## Authenticated weighted rollout evidence

The Go suite passed with HTTPS observer coverage for the accepted account,
anonymous denial, a wrong password and an unbound probe URL. The test
collects 500 HTTPS responses and checks that the Envoy admin client never
receives the Authorization header. Planner tests hold a prepared step when
one proxy lacks successful incumbent samples. They also cover removing an
unhealthy fleet while the retained fleet still answers authenticated probes.

`TestWeightRouteConditionalPatchAPIServer` passed with the generated
operator CRD and a fixture HTTPRoute CRD. It covers signed evidence for a
protected route, rejects a foreign hostname and a recreated Secret, and
checks that replacement between observation and the final Secret read
holds the weight write. Restart after an ambiguous write and rollback of
an interrupted promotion also passed. `TestRolloutStatusAPIServerConflict`
passed in the same run.

### Live weighted promotion and rollback

The kind drill used Kubernetes 1.32.0, Envoy Gateway 1.8.6 with two serving
replicas, six Java Pods and a precreated SeaweedFS S3 bucket. Clients
verified the Gateway certificate. Envoy validated JVM certificates using
accepted `BackendTLSPolicy` resources on all four fleet Services. The
observer used the ordinary account, loopback tunnels for each Envoy Pod,
and separate read-only Kubernetes credentials. The platform publisher
signed the reports and fleet approvals.

Search moved to 1%, 10%, 45% and 100% green before index moved to 1%,
20% and 100% green. The manager Pod was replaced after search reached 1%
and before index moved. The new manager resumed promotion. Rollback
readmitted `initial` at 1% and then restored it to 100% for each role.
Both operations finished as `Succeeded` and released their claims. Both
stable Service UIDs and ClusterIPs stayed the same.

Both Envoy replicas returned HTTP 401 for anonymous clients and invalid
credentials on each role before promotion, on green and after rollback.
The promoted index route acknowledged a write, and restored search
returned that document and the one written before promotion. All 1,999
continuous authenticated document reads returned HTTP 200. The 90 signed
role reports captured 100,800 successful process identity probes with zero
failures in the published sampling windows. The collector also discarded
observations when the route or active configuration changed mid-sample.
The drill used one AZ and two fleets. Multi-AZ and cloud behavior require
separate qualification.

## Basic Auth reload across search and ingest

Run `go test -tags java_integration ./internal/plugins -run '^TestInstalledShellAuthPluginManagesAccounts$' -count=1 -v`
with JDK 25, `OPERATOR_TEST_AUTH_HTTPS=1`, `OPERATOR_TEST_AUTH_COHORT=1`,
and `OPERATOR_JAVA_RUNTIME_LIB` and `OPERATOR_AUTH_PLUGIN_JAR` set to
the staged runtime library and pinned auth JAR. The bundle needs the
experimental HTTP TLS provider and the process incarnation field in
`/_serverless/stats`. By default the fixture uses a shared filesystem
store. To exercise S3, set `OPERATOR_AUTH_S3_ENDPOINT` to a disposable
loopback HTTP gateway and `OPERATOR_AUTH_S3_BUCKET` to a bucket created
in that gateway before running the test.

The fixture installs the real auth plugin through an OCI protocol
fixture. It authenticates the old password on both role JVMs, uses the
HTTPS account API on ingest to rotate and delete the account, and checks
both established and fresh HTTPS connections. It pins a five-minute
positive cache TTL and requires both JVMs to enforce the change within
eight seconds. The authenticated JVM identities are sampled around the
changes and must stay the same.

To exercise the exec probe as well, build `./cmd/health-probe` and set
`OPERATOR_AUTH_HEALTH_PROBE` to the absolute binary path. The live fixture
verified that both ingest and search rejected the wrong password with
HTTP 401 and that the correct password passed the readiness and lease
checks. This run used a shared filesystem store. Unit tests verify that
the probe preserves the exact password file bytes, scopes credentials to
health reads on the expected HTTPS endpoint, rejects a mismatched server
identity, and leaves redirects unfollowed. Kubernetes Pod rendering and
credential delivery are covered in the [startup drill](#managed-basic-auth-startup).

The filesystem run verified both JVMs within 837 ms of the rotation
acknowledgement and 554 ms of the deletion acknowledgement. With the
precreated SeaweedFS 4.53 bucket, those observations took 847 ms and
424 ms. These are observations on a healthy local store. Kubernetes
delivery, retained fleet enforcement, store partitions, native peer
authorization and general policy reload require separate proof.

The same S3 fixture runs `UserAdmin.VerifyAccount` for creation, rotation
and rejection after removal on both JVMs. It also checks record absence
through the ingest account API. The TLS unit tests reject an anonymous
success, a generic 401, a 503, a changed process incarnation and a changed
connection. If either credential still behaves contrary to the requested
change, the observer reports the result as unverified.

`go test -tags envtest ./internal/controller -run '^TestAccountCohortOnAPIServer$' -count=1 -v`
checks the cohort adapter against the Kubernetes API. The fixture
supplies application observations and verifies that the adapter includes
unready preview Pods and a terminating retained Pod marked `Failed`
after node loss while its JVM is still recorded as running. It holds
when another Pod appears, a container restarts or an owned Pod loses
its instance label. The direct endpoint TLS test accepts the matching
DNS and IP identities and verifies a wrong SAN prevents any HTTP
request. The Java HTTPS fixture separately exercises learning the JVM
incarnation during credential observation. These fixtures cover the
inventory, TLS identity and Java application boundaries independently.

Setting `OPERATOR_TEST_AUTH_HTTPS=1` for
`TestInstalledShellAuthPluginManagesAccounts` also enables the experimental
HTTP TLS provider and exercises the operator account client against the real
Java endpoint. The local run passed account creation, rotation, deletion and
untrusted-certificate rejection on one JVM. It is not Kubernetes, peer TLS or
fleet-wide revocation evidence.

`go test -tags java_integration ./internal/plugins -run
TestInstalledShellAuthPluginManagesAccounts` passed with
`OPERATOR_JAVA_RUNTIME_LIB` and `OPERATOR_AUTH_PLUGIN_JAR` pointing to the
pinned staged shell and auth JAR. It installs the real shell auth plugin from
a local OCI protocol fixture and exercises account creation, rotation and
deletion in one JVM over loopback HTTP. It does not test Kubernetes, HTTPS,
multi-node revocation or a real OCI registry in that run.

Run `go test -tags registry_integration ./internal/plugins -run
TestDistributionRegistryPluginInstall` on Linux with
`OPERATOR_REGISTRY_BINARY` and `OPERATOR_JAVA_RUNTIME_LIB` pointing to the
registry binary and pinned shell bundle. A local run passed with the upstream
[Distribution 3.1.2 release](https://github.com/distribution/distribution/releases/tag/v3.1.2),
archive SHA-256 `40df2224d410f72ae425c3371873b078bbdbda3b8b612be9571f0e6751f3acc8`,
TLS, Basic authentication and a compiled fixture plugin loaded by Java after
the registry stopped. The test builds and invokes the installer executable
with `-registry-ca`; wrong credentials, an untrusted CA and a fresh install
during outage were rejected. No kubelet or production plugin was involved.

### Live OCI installation and registry outage

Kubernetes 1.32.0 kind ran the operator, installer init container, pinned Java
shell and a private Distribution 3.1.2 registry. The registry used TLS and
Basic authentication. The Java shell used a precreated SeaweedFS bucket.
The fixture approved a digest-pinned REST plugin for search only. Search
served its endpoint, and ingest had no installer and returned the shell's
HTTP 501 response for the absent endpoint.

The base operator Role denied plugin snapshot creation. After the optional
`config/dev/plugin-staging-rbac.yaml` Role was applied, the controller
created the owned immutable request and policy ConfigMaps. Its service account
still could not patch ConfigMaps. The kubelet ran the installer as UID 10001.
The live Java containers could read the installed JAR and could not write
the plugin directory or see the registry credentials file.

The test removed the registry Service endpoints and scaled search from one
to two. The new Pod's init container failed before Java started. The
controller recorded `PluginInstallTimeout` for its exact Pod UID and
`search/zone-a`. The original Pod still served the plugin endpoint.
After the endpoints returned, the same pending Pod completed init, became
Ready and served the endpoint. The controller cleared its provisioning hold.
A UID-qualified eviction then caused a fresh replacement to fetch and load
the same artifact and serve the endpoint.

The run used these manifest digests:

| Input | SHA-256 |
| --- | --- |
| Operator image | `a6ecefab244216f20fc82d1509408eebad71e7b868598c871c7d99ea10c47979` |
| Installer fixture image | `50183e32651ca8fa56847f4d2300459dacf5f059790cf369b45b07b79c43a72e` |
| Java runtime image | `9b235b7c4b965d9e0a421ea7f934e4be0d6661ae16e4ae45570993643be967fc` |
| OCI fixture plugin | `0b4d1fa11d37d2e883be54fcf98224d7ecfce891bfe67251708e22256eba8c7d` |

The scoped Go suites, `TestPluginStagingAndGateReleaseOnAPIServer` with
`-tags envtest`, and `python tools/verify_plan.py` passed. The live drill
used a single AZ, a synthetic plugin and a locally built installer fixture
image. Production plugins, registry token authentication, signature
verification, and the Secret name-reuse race remain open.

#### Shared plugin ConfigMaps

The API-server test verified that a second Pod with identical plugin inputs
recorded the same immutable request and policy ConfigMap UIDs. The controller
rejected a same-name snapshot after its recorded UID changed. Unit coverage
checks that an exact legacy pair still works for its original Pod and rejects
using it for another Pod.

The Kind drill repeated scale-out during a registry endpoint outage, registry
recovery and a PDB-governed eviction with operator image digest
`sha256:c8880d588bbbf0b16f5f4f8b49f339c9d07bc12b40c1c6d11b0658f81710136d`.
The initial search Pod, the recovered scale-out Pod and the replacement shared
both ConfigMap UIDs. The instance had exactly two owned plugin snapshots.
All three JVMs served the fixture plugin after their own init container
finished. The run used the same installer and runtime images as above and a
new build of the same synthetic plugin.

#### Blue/green plugin-version rollout

On one-AZ Kind with Kubernetes 1.32.0, Envoy Gateway 1.8.6 and private
Distribution 3.1.2, the fixture published three artifact builds for
the same Java image. An individually approved third artifact, whose fleet transition
was absent from the approval policy, was held before candidate Pods were
created. The approved candidate pinned both fleet sets and its search and
index JVMs served the v2 fixture endpoint while production served v1.

With verified TLS and Basic Auth, both Envoy routes served v2 after
promotion and v1 after rollback. The operations preserved the stable
role Service UIDs. Index acknowledged writes on both fleets and search
returned both writes after rollback. Each serving fleet also replaced
a search Pod through a UID-qualified eviction. Its replacement loaded
the correct plugin artifact and retained the fleet revision.

The manager was updated during the drill. After rollback, the fixture
switched to a new immutable approval ConfigMap and repeated replacement
with the final operator. The new v1 Pod still used the original pinned
install-policy ConfigMap. Both authenticated Envoy routes served v1.
The final operator image digest was
`sha256:a6010a48227600ca3e7a3830698329cca08283e4ca44651914f70c5df310cb55`.

The Go regression verifies that a source edit in the same update as a
candidate cannot backfill a plugin set absent from the source Pods. A
matching plugin annotation with no installer is rejected. The regression
also covers inherited sets and an approved explicit removal. The live
fixture used a synthetic REST plugin without plugin-specific shared data
format changes. This
evidence covers selector routing on a single AZ. Plugin-specific wire and
storage compatibility and weighted plugin rollouts need separate evidence.

### Other local and API-server checks

The opt-in `java_integration` plugin test now invokes a REST handler supplied
by a compiled OCI fixture plugin after the registry fixture has stopped and a
completed installation has been retried. It passed with the pinned local Java
bundle. This is a local protocol and plugin fixture, not a real registry,
kubelet, fresh replacement volume or production plugin qualification.

`tools/java-s3-mixed-smoke.ps1` passed the same-image candidate-owner and
recovery scenario against a local SeaweedFS 4.53 S3-compatible gateway. The
gateway and bucket were disposable; this does not qualify AWS S3 or IAM.

The optional `OPERATOR_TEST_CANDIDATE_OWNS=1` extension to the mixed-process
role smoke test passed locally. The shell's shard API reported the candidate
node and unchanged term immediately before and after the verified write; the
original ingest endpoint read the document before and after candidate shutdown.
The run used same-image JVMs and a filesystem store, not S3 or Kubernetes.

The opt-in `OPERATOR_TEST_MIXED_FLEET=1` role smoke scenario passed with two
same-image ingest JVMs and a search JVM on a local filesystem store. A write
submitted to the candidate endpoint remained readable through the original
endpoint after candidate shutdown. The request may have been forwarded to the
existing shard owner; this is not candidate ownership or mixed-version proof.

With `OPERATOR_TEST_WARM_ON_JOIN=1`, the local role smoke script invokes
`TestLiveCandidateSearchObservation` after a second Java search process has
completed warm-up. The test qualifies that real JVM using synthetic Pod and
candidate metadata. A patched-shell run passed, including the script's
search restart and ingest-replacement checks. It is not a Kubernetes or
blue/green traffic test.

With `SERVERLESS_TEST_EXPECT_WARMUP=1` and `OPERATOR_GO_EXE` configured, the
local `tools/java-role-smoke.ps1` also invokes
`TestLiveCandidateIngestObservation` against the ingest JVM. It uses fake
Kubernetes Pod metadata and does not demonstrate a Kubernetes or blue/green
deployment. The local patched-shell run passed along with the script's
separate-role write, search and ingest-replacement checks.

The opt-in `TestDedicatedHostsAcrossNamespacesWithScheduler` also compares
regular-sidecar CPU fit with the Kubernetes 1.32 scheduler at an exact boundary
and 10 millicores over it. The Pod includes a restartable init sidecar, an
installer and RuntimeClass overhead; a pre-bound synthetic agent represents a
reservation. This does not exercise live DaemonSet changes, a kubelet or a
cloud provisioner.

The opt-in `TestSidecarAdmissionWithAPIServerDefaults` uses a Kubernetes 1.32
API server to create a gated Pod with an explicit sidecar environment and
probe. It accepts the API's probe defaults and rejects an altered environment
in a copied response. It does not run an admission webhook, kubelet or sidecar
process.

The development security account client has local HTTPS fixture coverage for
account reads and mutations, TLS and input rejection, redirects and a lost
response after a mutation. The last case returns an unknown outcome; it does
not prove that all transports or upstream intermediaries avoid retries. The
pinned staged Java shell passed four selected `:serverless:testkit:test` tests:
`ServerlessAuthTests.testAnAccountCreatedThroughTheEndpointCanUseTheShell`,
`testACredentialRemovedElsewhereStopsWorkingWhenTheCacheExpires`,
`testAnAccountChangedElsewhereStopsWorkingWithinASecond`, and
`CredentialInvalidationTests.testAnAmbiguousPasswordChangeEvictsTheOldCredential`.
These use an in-process node, local storage and test clocks, not separate JVMs,
Kubernetes or an external HTTPS connection.

Run `python tools/verify_plan.py` from the operator directory. This checks task IDs, dependency references, cycles and coverage references. It does not prove the task descriptions or product behavior are correct.

Run `python tools/test_verify_plan.py` to exercise the checker against a valid plan, an unknown dependency, a dependency cycle and a missing coverage mapping in temporary copies.

Run `python tools/check_contract_examples.py` with `kubectl` installed to render both draft CRDs and examples locally. This parses YAML and checks resource presence. It does not perform Kubernetes schema validation, validate the examples against the schemas, or prove CEL behavior.

Run `python tools/test_contract_examples.py` to verify that malformed fixture YAML fails the offline renderer using a temporary copy.

Use unit and deterministic fault tests for planner decisions and retry behavior. Use envtest or a real API server for schemas, admission, status, ownership and conflicts. Use a disposable cluster with kubelet and controllers for scheduling, PDBs, readiness and deletion. Use the real Java image and store for leases, persistence, plugin loading and recovery. Run provider-specific EKS and GKE tests for provisioning, maintenance and client traffic. [Envtest boundaries](https://book.kubebuilder.io/reference/envtest)

The opt-in `go test -tags envtest ./internal/controller -run TestAPIServerValidatesInstanceAndStatus` test installs the generated development CRDs into a disposable API server. On Linux set `KUBEBUILDER_ASSETS` to a compatible envtest binary directory. It has passed with Kubernetes 1.32.0 in Ubuntu for a valid instance and status update, rejection of a zonal floor above replicas, operation-spec immutability, competing operation claims using stale resource versions and recovery of the winning claim, stale Pod deletion preconditions, guarded creation and retry, preservation of other scheduling gates, rejection of stale Pod identity, unbound withdrawal, and preserving the pinned remote-store identity after a repository ConfigMap changes. The development settings and provisioning-retry executors use the claim primitive and release their claims after recording a terminal outcome. The repository check invokes reconciliation directly; it does not test the manager's watch. The test does not cover the complete API, a binding race, scheduler, PDB controller or cloud behavior.

For each task, record source and image revisions, commands, environment, expected and observed outcomes, negative cases and missing evidence. A failed or unavailable environment remains unverified. Keep logs free of credentials. An implementation claim requires evidence for each acceptance criterion.

Kubernetes 1.32.0 API-server tests accepted a Pod rendered with the development plugin installer and exercised persisted staging intent, immutable snapshots, gate release and replacement identity detection. Unit tests cover the installer-only credential mount, effective init-container resource requests, failed-set cleanup and retry after publication. A Linux subprocess test invokes the installer CLI against a local HTTPS registry protocol fixture with Basic authentication. It checks trust and credential failures, blob digest rejection, successful installation and retry after the registry stops. The kind drill supplies the kubelet and Java loading evidence for the synthetic plugin. [Live OCI installation](#live-oci-installation-and-registry-outage)

The same API-server test exercises the first Service reconciliation: the controller persists initial role fleet intent before creating the two stable Services, verifies their role selectors, and reports an externally changed selector without replacing it. Envtest does not run operator-created backends. EndpointSlice convergence, real traffic, RBAC admission, and coordination with an active promotion or a concurrent Service deletion remain unverified; fleet-changing operation execution must not be enabled until that coordination is implemented.

The controller also has an unwired per-role Service switch primitive that uses
JSON Patch tests for UID, resource version and the complete expected selector.
The API-server test injects a concurrent selector update and a lost successful
patch response, and verifies recovery and rejection of a same-name Service with
a new UID. Role Service checks reject publishing unready addresses, headless
Services and node-local internal traffic. This does not qualify the target fleet
or prove EndpointSlice convergence, actual traffic or promotion recovery.

An unwired fleet planner sequences search before ingest and requires fresh
qualification and route evidence for each previously converged role before
advancing or completing. Unit tests exercise a lost patch response, a candidate
losing qualification and a changed Service version. An unwired status helper
persists both role intents and records planner observations using optimistic
locking. A Kubernetes 1.32 API-server test verifies status field preservation,
rejection of a stale write and the precision of a recorded observation time;
it also reads two actual Services and holds a switch plan when the other role's
selector changes. The test supplies a synthetic instance and candidate evidence
and does not run the operation manager.
The helper does not create candidates or execute promotion. For mixed rollback, the
planner can adopt a role already on the target without patching its Service.
The adapter allows adoption only for a rollback with the expected mapping.
Fresh route evidence after the recorded adoption time and qualified candidate
evidence are still required. Unit tests cover both mixed-mapping directions and
an external selector change. A fake-client test re-reads progress as a restarted
reconciler; end-to-end recovery with a live selector switch remains untested.

An unwired route observer combines Kubernetes endpoint observations with an
independent external-route result, requiring matching Service UID and version,
fresh post-switch observations and a qualified responding Pod among the ready
endpoints. Unit tests inject the external result and fake Kubernetes objects.
An unwired HTTPS probe has also been tested against a local TLS server and fake
Kubernetes objects, including rejection of redirects and changed Service
versions. These tests do not establish production authentication, actual zone
placement, ingress or ALB convergence, or real client traffic.

Instance reconciliation records a BlueGreen candidate and creates separate
candidate search and ingest ClusterIP Services, then pins their UIDs. A Kubernetes
1.32 API-server test verifies this reconciliation, restart after recording the
candidate, and rejection of a conflicting selector without rewriting it. It
also exercises normal reconciliation for a same-image candidate, including
serving and candidate PDB creation, a serving-capacity hold and scheduling-gate
release. The test writes PDB status itself; envtest does not run the disruption
controller, scheduler, kubelet or Java, restrict network access or establish
preview traffic. Candidate changes and removal remain held until retirement
exists; a restored unchanged template can be rebased after lifecycle and
identity checks.

Removing a candidate request withdraws still-gated, unbound candidate Pods via
the persisted retired-creation path. The API-server test covers that withdrawal
and retention of the serving Pod; a unit test confirms the gated-withdrawal
path holds if the candidate gate has already been released. Running candidate
retirement and tombstone cleanup remain unimplemented.

The same API-server test exercises a released but unbound candidate's recorded
cancellation, withdrawal and tombstone. It injects an API binding between the
withdrawal read and DELETE, verifies the resource-version conflict preserves
the bound Pod, and injects a lost response after a successful DELETE to verify
tombstone recovery. The binding is issued by the test rather than a running
scheduler.

The API-server test also exercises persisted preview Service retirement,
rejects a replacement UID and a concurrent Service version change, and
recovers from a successful delete whose response is lost. It does not exercise
ingress withdrawal or established client connections.

The API-server test verifies that an empty retired candidate can be replaced
by a differently named fleet, a delayed gated Pod is withdrawn, and a
same-name replacement preview Service is held. Retired fleet history remains
bounded at 64 and has no garbage collection; running Pod retirement remains
unimplemented.

The API-server test also pins and deletes a retired candidate PDB while
preserving the serving PDB. It rejects a concurrent edit and a replacement UID,
and recovers from a lost deletion response. The namespace-wide PDB scan is
paginated and holds beyond 8,192 objects. Envtest does not run the disruption
controller or prove eviction behavior.

Candidate capacity is sampled separately by role and zone, with Kubernetes
Ready counts distinct from unknown warmth. The API-server test verifies an
unbound candidate appears in status. A fake-client test rotates the historical
development search qualification sample to a candidate Pod. Neither observation
is used as a promotion or disruption decision.

An unwired development observer can collect a complete candidate search
warm-up sample, bounded by an observation window and checked again against the
Pod set. Unit tests use fake Kubernetes objects and local HTTP fixtures to
reject a timed-out warm-up even when readiness is green. This is not evidence
of actual Node AZ, retained cache contents, all production capacity, or a real
Java-in-Kubernetes run.

With development endpoint observation and strict warm-up enabled, the operator
also records a historical serving-search sample across the configured zones.
The sample combines process-local qualification with stable Service and
EndpointSlice checks; failures replace prior success. Local HTTP and fake-client
tests exercise timeout and routing conflicts, while the API-server test checks
status persistence. It is not used for eviction and does not establish actual
Node AZ, cache residency, peer reachability or ingress health.

The opt-in development ingest diagnostic checks local role, lease and stable
JVM incarnation alongside stable Service endpoints. A local patched-shell
smoke run exercised the process check, and a fake-client/local-HTTP test
rejected a wrong role. This is not a writer takeover or real Kubernetes Pod
test, and it is not used to authorize retirement.

Lease-aware serving and route probes require the requested local role from
stats. A local pinned-shell smoke run checked an ingest-only process, indexed
and searched a document, and wrote after replacing the ingest process. This
does not verify peer lease contents or writer takeover in Kubernetes.

An opt-in development EndpointSlice observer verifies the stable Service UID and selector,
slice owner and controller, port, ready endpoint Pod identity, caller-qualified
Pod UID, Pod IP and role/fleet/zone labels, and per-zone endpoint minima. It
rejects unknown readiness, potentially serving terminating endpoints, stale
Pod identities and a Service changed during observation. Unit tests and an
API-server test with synthetic EndpointSlices pass, including status refresh
through normal reconciliation. The API server does not run the EndpointSlice
controller, kube-proxy, ingress or Java. The separate optional
`config/dev/endpoint-observation-rbac.yaml` grants namespaced list access; the
default role does not. Sampling is capped at five seconds, 512 Pods/endpoints
and 128 EndpointSlices and records an unavailable result above those bounds.
It cannot alone prove actual node AZ, cache warmth, traffic convergence or an
atomic view and is not used to authorize promotion.

### Instance endpoint availability

The development observer sets `Available` from both stable role
Services and their EndpointSlices. Its result reflects configured zonal
and role-wide serving floors and excludes disruption-marked Ready Pods.
The rotating detail keeps raw Ready counts and reports the same floor
result as the aggregate for its role.

The API-server test covers Ready, a verified zero-endpoint shortfall and
recovery through ordinary reconciliation. Unit tests cover an empty zone
with no configured minimum, a same-generation change to the other role's
fleet and positive evidence expiring before workload reconciliation.
If a Pod, Service or EndpointSlice version changes during the read,
the development sampler retries that role once within the original
five-second window. An unverified sample schedules a new check after
10 seconds. Verified coverage and confirmed floor shortfalls use the
one-minute interval. The regression test injects both a single change
and repeated changes. Lifecycle actions still require independently
verified endpoint evidence.

Before the change, 15 steady-state observations across 70 seconds
all reported `True`. The transient unverified result during initial
install was not reproduced in that steady-state sample.

The secure-install Kind fixture ran manager image
`sha256:ffdb4bd5ffdf7d4d9d75e6b73cdff2fcce420dec87171265e94c46f4211e3a74`.
An empty test EndpointSlice with unrecognized ownership made the next
sample report `Unknown`. After the test removed the slice, the next
sample reported `True` exactly 10 seconds later. Both Ready role Pods
retained their UIDs. A TLS and Basic Auth write to the stable index
Service was returned by search. The local evidence is in
`tools/bin/oss-secure-install-fixture/endpoint-retry-result.json`.

Inspect `status.lastEndpointObservation.observedAt` and
`observedServingFleets` for the observation time and serving scope.

The Kubernetes 1.32.0 Kind drill ran four Java Pods against a supplied
SeaweedFS bucket. Both role Services met their floors and reported
`Available=True`. A conflicting index selector reported `Unknown`.
Restoring it recovered availability. A synthetic disruption on a Ready
index Pod reduced the verified index floor and reported `False`.
Clearing the signal restored `True`, with the same four Pod UIDs and
both Service UIDs and ClusterIPs. A subsequent synthetic mismatch in
the stored index fleet forced a fresh sample without a change in spec
generation. The Go and full real API-server suites passed.

With the opt-in development flags, the API-server test also checks that a role/AZ Pod creation intent is persisted before creation, a retry recovers the same Pod UID, an altered fleet label blocks recovery without clearing the intent, and an unbound Pod prevents further development creates. It verifies that gate-release intent is persisted before gate removal and cleared only after read-back. Unit tests cover rejection of stale Pod versions and changes to the gate, placement, runtime, resources and data mount. Envtest does not exercise a mutating admission webhook or establish scheduling, kubelet startup, runtime credentials, security, application readiness, or traffic.

Development Pod validation also compares the complete readiness probe, including
period, timeout and thresholds. Unit tests reject changed timing or failure
thresholds, and the API-server reconciliation test passes with Kubernetes
defaulting. This protects the configured probe from admission changes; it does
not prove kubelet timing, endpoint withdrawal or native Java peer routing.

The same API-server test uses an injected clock to expire an unbound development Pod, verifies that withdrawal intent precedes deletion and that the resulting hold prevents another Pod, and checks that an already-bound Pod is retained. It exercises an acknowledged retry, rejection when the old Pod name exists, restart after the hold reset, and deletion of a claimed retry operation. It does not reproduce a concurrent scheduler binding during the delete request or validate Karpenter or GKE behavior. Provider-level node limits and automatic recovery are not implemented by this path.

The API-server test also checks that the development controller persists PDB intent, creates a role/AZ selector, holds Pod creation until a matching PDB generation is observed, and reports a changed selector without overwriting it. The test writes PDB status itself because envtest does not run the disruption controller. The failback case reads the persisted proposal to check that it converges, rejects a stale retirement claim, resumes with a new reconciler, and verifies that eviction counts the original victim in the PDB before cleanup.

### Namespaced Pod disruption signal

The API-server rollout suite marks a Ready search Pod as a disruption
target. The regenerated CRD preserves the per-role and per-zone count
alongside the Ready count. The Pod stops contributing to the source
fleet's promotion floor. Index qualification remains available, and
clearing the condition lets the same search Pod qualify again.

The suite injects a disruption status change after the source has been
qualified for promotion and after regular capacity has been qualified
for failback. The controller rechecks Pod identities and versions and
holds the selector write or the eviction in those cases. The normal
failback flow resumes after the signal clears. This exercises reads and
writes against the Kubernetes API server. It does not measure drain lead
time or provider behavior.

The provisioning tests exercise the regional and zonal floors independently.
The next eligible creation goes to the marked search Pod's group in either
case. A cleared condition restores normal order, and a marked group that
already meets its replica target gets no extra Pod. The planner test also
checks that when two eligible zones can restore the same regional floor,
the zone below its own floor gets the next slot.

In Kubernetes 1.32.0 Kind, two labeled workers hosted the search and index
roles on separate zones. The test marked the Ready search Pod and raised
the declared search reserve and index replica count together while the
operator was stopped. On restart, search took the single available
provisioning slot. The original Pod remained Ready with its disruption
condition set. The new search JVM reported completed warm-up before the
additional index Pod took the next slot. The projected zonal shortfall
cleared while status still counted the original disruption target, and
search returned the document stored through the index Service. The
operator's ServiceAccount could not get, list or watch Nodes.

With two Ready search Pods and a zonal PDB floor of one, the Kubernetes
Eviction API admitted the UID-qualified eviction of the marked Pod. The
stable search Service returned the stored document after deletion. The
warm survivor kept its UID, and the next search Pod also completed strict
warm-up. The search and index Pods shared their respective zone workers.
This drill used a synthetic disruption signal and a single Pod eviction.
Provider drain lead time and recovery from loss of the zone worker remain
separate qualification cases. The full Go and API-server suites passed.

### Serving recovery takes priority over a pending preview

The API-server suite signals disruption while a preview is waiting to be
created, while its Pod is scheduling-gated, and after its gate release and
warm-up slot are reserved. The controller retires the pending identity,
withdraws only the still-gated Pod, and gives the next slot to serving
replacement after the old serving Pod disappears. With a release limit of
one, confirmed gated deletion returns the rate-limit reservation.

The test also removes the gate before withdrawal can complete. The
operator continues tracking the exact released UID and keeps its charge.
When the fixture subsequently cancels the preview, the controller reuses
the retired identity and releases the warm-up slot after deletion. A
separate case checks that preview admission leaves the final cleanup entry
available in the bounded history.

For an already released preview, the API-server test verifies that a
serving disruption queues withdrawal before the pending timeout. An
unbound preview yields its provisioning and warm-up slots while retaining
the release charge. Recovery of the serving fleet or a scheduler binding
between intent and deletion cancels the withdrawal. If a pending timeout
was recorded first, a newly observed serving shortfall changes the
withdrawal to serving recovery before a shared role/AZ hold is created.
With a release limit of one, the serving replacement waits for the prior
charge to expire. The lifecycle package also injects a binding during
the conditional delete and verifies that Kubernetes rejects the stale
resource version. Kubelet warm-up timing and cloud autoscaler reactions
require live proof.

### Local indexing failback

A three-node kind cluster running Kubernetes 1.32.2 exercised two labeled
zones, with two hosts in the surviving zone. The staged Java image used
SeaweedFS 4.53 as its S3 primary store. The fixture created the bucket and
supplied it through `spec.storage.repositoryRef`. The operator ran with
namespaced permissions, dedicated-host placement, gate release, endpoint
observation, emergency failback and `RetryProvisioning` enabled.

The index Service acknowledged a write before the test cordoned the
first zone's node and terminated its Java Pod. The scheduler rejected the
replacement in the cordoned zone, then the operator withdrew it and
started one emergency Pod on the second host in the surviving zone.
After the test uncordoned the first node and acknowledged the blocked
attempt, the operator brought a regular Pod back in that zone. It kept
the emergency Pod until the platform fixture explicitly cleared the
outage. Kubernetes then began terminating the recorded victim and the
operator cleared the retirement intent once it disappeared. The Service
retained two regular endpoints and returned the original document after
retirement. A repeat with one unchanged controller build returned HTTP
200 on all 95 sampled reads, including samples before, during and after
the kubelet's termination event. The operator ServiceAccount could create
evictions in its namespace, could not evict in `kube-system`, and could
not list Nodes. The test used a controlled loss with graceful termination,
one indexing role, and direct in-cluster Service access.

### Local strict search failback

A four-node kind cluster running Kubernetes 1.32.2 used the same staged
Java and SeaweedFS 4.53 images. Dedicated placement separated indexing
from search. The test indexed a document through the index Service and
found it through the search Service. It cordoned the node in the first
zone and deleted the degraded bootstrap search Pod. The surviving strict
search Pod continued serving while the controller withdrew an unschedulable
regular replacement and started one temporary Pod on a spare host in the
surviving zone.

The temporary Pod reported completed warm-up without timing out and had
one local reader. After the test reopened the first zone and acknowledged
the blocked attempt, its regular replacement also reported completed
strict warm-up and one local reader. The controller evicted the temporary
Pod after explicit outage clearance. The original query returned the
indexed document with one successful shard after retirement. All 105 HTTP
samples taken after the simulated loss, spanning the temporary Pod's
termination, returned 200. Some Pods briefly lost readiness due to the
lease probe during the drill. The fixture observed one shard and used the
in-cluster Service. It provides no claim about whole-cache residency,
abrupt AZ loss, ingress or cloud provisioners.

### Local failback with surviving degraded bootstrap

A five-node Kubernetes 1.32.2 kind drill lost the zone running the only
strict search Pod. The degraded bootstrap survived. After scheduling a
temporary strict Pod and recovering the failed zone, the test cleared the
outage but reported zero additional fit in the bootstrap AZ. The operator
kept all three search Pods. Positive fit for the same capacity profile
then enabled one strict regular replacement on another host in the
bootstrap AZ. The replacement reported completed warm-up. Kubernetes
terminated the bootstrap at 00:57:25 UTC and the emergency Pod at 00:57:50
UTC. The two remaining strict Pods each reported one local reader and the
stable search Service returned the indexed document.

The first traffic sample returned 104 HTTP 200 responses and 21 attempts
with no HTTP response while the surviving bootstrap was the remaining
capacity and the emergency Pod was starting. A second sample spanning the
emergency retirement returned 45 HTTP 200 responses and 20 attempts with
no HTTP response. Pod events recorded failures of the local lease check
in both windows. The drill verifies the bounded retirement sequence and
data recovery. A follow-up in steady state reproduced connection failures
while both JVMs answered directly, reported `valid_now: true`, and returned
negative `expires_in_millis`. In the pinned Java source, `StatsHandler`
reads validity from the locally published lease but gets remaining time
from the cached membership snapshot. The probes correctly reject an
expired remaining-time value.

#### Repeat with local lease reporting fixed

The Java overlay now reports both fields from one successful local lease
publication. The repeat used the same five-node kind topology, a separate
indexing role and the external SeaweedFS 4.53 bucket. The operator again
honored the no-fit hold, then retired the degraded bootstrap and emergency
Pod in order. The indexed document was still returned after retirement.
The stable search Service returned HTTP 200 on all 155 samples from the
simulated loss through both retirement events. In 45 subsequent seconds
of steady-state sampling, all 45 searches returned HTTP 200 and the two
strict Pods reported at least 19 seconds of remaining lease time in all
90 direct observations. The S3 denial integration
test separately confirmed that the running JVM reports an expired lease
and the probe rejects it before accepting the writer again after access
returns. This covers the local Service, a controlled zone loss and one
shard. Abrupt infrastructure loss, ALB and ingress paths need separate
qualification.

### Local ingress during search AZ failback

A five-node kind cluster used Traefik 3.6.2 with two Ready replicas on
different AZ labels. Its Kubernetes Ingress provider discovered backend
Pod addresses through EndpointSlices. Two hostname rules sent search and
index traffic to the operator's stable role Services. Direct stats through
the two hostnames identified one search process and one indexing process.
The test wrote one document through the index route and queried it through
the search route. The operator ServiceAccount lacked permission to patch
Ingress resources. [Traefik EndpointSlice provider implementation](https://github.com/traefik/traefik/blob/v3.6.2/pkg/provider/kubernetes/ingress/kubernetes.go#L583-L615)

The drill cordoned the worker hosting one strict search Pod and one
ingress replica, then deleted both Pods gracefully. The remaining ingress
replica continued to route traffic. After the failed search zone recovered,
the operator retired the degraded bootstrap and temporary search Pod in
order, and both routes read back the indexed document. All 155 sampled
search requests and all 155 sampled index-route reads returned HTTP 200.
The sample window began immediately before workload deletion and covered
both retirements. Traefik logged 404 and 503 responses during initial
startup, before the indexed document was available. The fixture used
plain HTTP through an in-cluster ingress Service. It exercised a controlled
workload interruption on one host. ALB target registration, TLS, hard node
loss, and provider AZ failure require separate qualification.

### Local abrupt loss behind ingress

The five-node kind drill put one strict search Pod and one Traefik replica
on the only worker labeled in the failed AZ. The fixture paused that
worker abruptly and immediately published a namespaced outage report. The
operator started a temporary search Pod in the surviving AZ while the old
Pod retained its UID and node binding. The temporary Pod completed strict
warm-up in about 49 seconds. Kubernetes still reported the paused worker
and its endpoints Ready in the sample around 48 seconds. The next sample,
around 50 seconds, showed Node Ready `Unknown` and both the search and
ingress endpoints for that worker marked unready. The fixture resumed the
worker after about 59 seconds. This is a local node freeze, with the outage
reported by the fixture. After the worker resumed, the regular Pod
became Ready again and the operator completed both retirements. The
search route returned the indexed document afterward.

Of 150 requests per route, 18 searches and 11 index-route reads timed
out before an HTTP response. The last timed-out search sample began
around 49 seconds after the pause, and the last timed-out index sample
began around 44 seconds after it. The other 132 searches and 139
index-route reads returned HTTP 200. The test used a one-second client
timeout through the ingress Service. The outage report accelerated Pod
placement while Kubernetes endpoint convergence still governed routing.
Provider notification and ALB health-check timing remain unverified.

### Three-zone search and index outage

The Kubernetes 1.32.2 Kind fixture ran one dedicated Java search Pod and
one dedicated index Pod in each of three labeled zones. SeaweedFS held the
remote-store bucket on the control-plane host outside those zones.
The fixture paused both workers labeled zone-c while clients ran
single-shard searches and unique document writes through the stable role
Services. It published namespaced outage and fit evidence. The operator
had no Node list permission.

The first run held without fit evidence. With fit, search consumed the
single temporary slot in zone-b and the shared cap held back index.
Raising the cap allowed index to serve but changed its Pod template
label, leaving a mixed revision during failback. The controller now
excludes availability policy from that hash and records reproducible
legacy labels before subsequent policy edits.

The repeat upgraded the controller while the six regular Pods kept their
UIDs. Both temporary Pods inherited the regular revision despite the cap
edit. Search reported completed strict warm-up about 59 seconds after
the pause. A direct request to it returned the seed document in 44 ms.
The JVM reported zero local shard readers and zero cache activity before
and after that request. This establishes that the Pod could answer the
query. Local cache residency remains unverified.

The temporary index Pod became Ready after about 90 seconds. It
acknowledged a write while zone-c was still offline, and the search
Service returned it. After the two workers resumed, the fixture
published outage clearance. Both temporary Pods retired and the six
regular Pods kept their UIDs. Failback finished about 42 seconds after
the workers resumed. Search returned all 180 acknowledged load writes
from the repeat and the separately verified outage write.

| Stable Service | Attempts while zone-c was offline | Successful | No HTTP response within 2 s | Successful p95 / p99 |
| --- | ---: | ---: | ---: | ---: |
| Search | 181 | 164 | 17 (9.4%) | 24 / 37 ms |
| Index | 114 | 99 | 15 (13.2%) | 286 / 765 ms |

The client made no retries and issued one sequential stream per role.
Peak observed successful starts per second were four for search and two
for index. The workload was limited by the client and does not establish
maximum throughput. During the outage, each zone-b PDB allowed one
eviction and zone-a and zone-c allowed zero. From the observed
placements, a further loss of zone-b would leave one Pod per role in
zone-a. Loss of zone-a would leave two per role in zone-b. Either is
below the regional floor of three. A second zone loss was not injected.
The run used direct HTTP Services, so provider and edge health timing
remain open. The Go and API-server suites and plan validation passed.

### Active ingress health during abrupt loss

Two local runs sent paired requests through the ingress Service and
through HAProxy 3.2 in front of the same two Traefik replicas. HAProxy
resolved ingress Pod IPs from a headless Service and checked `/ping`
every second with a 600 ms check timeout and a threshold of two failures.
It used no Kubernetes API credential. Both paths ran during one freeze of
the worker hosting a Traefik replica and a strict Java search Pod. Test
clients allowed one second per request and used no retries. [HAProxy HTTP health checks](https://docs.haproxy.org/3.2/configuration.html#4-option%20httpchk)

HAProxy marked the frozen ingress replica down about 2.2 seconds after
the pause. Kubernetes still marked the worker Ready until about 51 seconds
after the pause. The surviving Traefik access log showed canceled requests
to the frozen Java IP after edge ejection, including a request that started
about 45 seconds after the pause. The ingress target check alone could not
remove that stale Java destination.

The second run stored samples in a Kubernetes Job so the worker freeze
could not interrupt collection. HAProxy marked the frozen ingress
replica down in 2.3 seconds. Kubernetes marked the worker NotReady in
48.7 seconds.

| Route | Through ingress Service | Through checked edge |
| --- | ---: | ---: |
| Search read timeouts | 16 of 95 | 12 of 95 |
| Index-route read timeouts | 13 of 95 | 1 of 95 |

The other samples returned HTTP 200. The sole checked index-route
timeout began before edge ejection. At least 11 checked search
timeouts began after ejection, with the last starting about 46
seconds after the freeze. The surviving ingress forwarded a search
to the frozen Java address after HAProxy had marked the ingress
replica down. The restored search fleet had two Ready strict Pods,
and both routes could read the document through the checked edge.
This was a local, plain HTTP, read-only test. ALB target timing,
existing client connections, and write outcomes remain unverified.

### Active Java endpoint health in Envoy Gateway

The follow-up used Kubernetes 1.32.2 and Envoy Gateway 1.8.6 in kind.
Two checked and two control HTTPRoutes referenced the same two operator
Services through the same Envoy replica. The checked routes used
individual Service endpoints, a one-second HTTP check with a 600 ms
timeout and thresholds of two, and panic routing disabled. The control
routes relied on Kubernetes' endpoint view. The operator ServiceAccount
could patch neither HTTPRoutes nor BackendTrafficPolicies. The HTTPRoutes
resolved the real Services, and a stats request through each host
confirmed the intended Java role. [Role Gateway example](../../config/dev/role-gateway.example.yaml)

During cold start, the initial probes failed before the first successful
checks. Direct Java calls returned HTTP 200 while the checked search
route returned HTTP 503 with `no healthy upstream`. The first search
endpoint became eligible about two minutes after it appeared in Envoy.
The health checker used its longer no-traffic cadence before the
cluster served any requests. Run a client read through each proxy and
role before admitting traffic after proxy startup or route recreation.
[Envoy no-traffic interval](https://www.envoyproxy.io/docs/envoy/v1.40.0/api-v3/config/core/v3/health_check.proto.html)

The first outage comparison had no checked search timeouts and 17
control search timeouts out of 95. The frozen search endpoint had yet to
pass its startup checks, so this run confirms isolation of an ineligible
address and does not measure active ejection.

The strict repeat started with two strict Ready Java search Pods, both
healthy in Envoy. After the failed-zone worker froze, Envoy observed its
Java endpoint as unhealthy within 3.0 seconds. Kubernetes marked the
worker NotReady after 48.0 seconds. During the freeze, after Envoy
detected the unhealthy endpoint, the control route still selected it
34 times and the checked route selected it zero times.

| Read requests in the strict repeat | Control timeouts | Checked timeouts |
| --- | ---: | ---: |
| Search with fresh client connections | 12 of 95 | 0 of 95 |
| Search with client connection reuse enabled | 23 of 95 | 3 of 95 |
| Index route with fresh client connections | 0 of 95 | 0 of 95 |

The three checked timeouts began before ejection. After ejection, one
checked downstream connection carried 90 requests. Counts use Envoy's
structured access records for the probe Pod. The concurrent text output
interleaved some client rows, so the displayed curl summary alone is
incomplete. Each timeout appears in Envoy as a client disconnect with
no HTTP response. Both checked routes returned the indexed document
after failback, with two Ready strict search Pods. The final example
passed server-side dry run and the listener-scoped routes and policy
reported Accepted for their current generations.

The drill used plaintext HTTP, one Envoy replica and no configured
request retries. It froze a Java search worker while the Gateway and
index backend stayed up. Production TLS, ALB health checks, multi-replica
Gateway recovery, fleet selector promotion, and writes during loss need
separate qualification.

### JVM identity during blue/green route verification

The rollout now joins the HTTPS response's incarnation and lease to the
directly qualified Pod UID. Direct role checks require the same
incarnation in readiness and stats before marking a process eligible for
rollout. Ordinary qualification still accepts older stats responses.
Promotion rechecks the capability on the retained source Pods before
each selector write. The test fixtures exercise a same-name, same-UID
response with a different JVM, a response with no incarnation, and a
source-process hold before either Service changes.

Run `go test -tags envtest ./internal/controller -run
TestRolloutStatusAPIServerConflict` with `KUBEBUILDER_ASSETS`,
`OPERATOR_JAVA_RUNTIME_LIB` and `OPERATOR_ROLLOUT_S3_BINARY` set to the
envtest binaries, the rebuilt runtime bundle and the SeaweedFS binary.
The fixture uses six Java processes and a disposable bucket. It
supplies Pod status, EndpointSlices and platform conclusions, and uses
a local TLS reverse proxy. A real Kubernetes Gateway rollout and
provider load balancers need separate verification. [Development rollout](development-rollout.md)

The API-server test passed with the rebuilt Java runtime and SeaweedFS
4.53. The promoted ingest route acknowledged a green-owned write.
Restored search returned that document. Blue ingest acknowledged another
write 30.016 seconds after green process loss, when the shard ownership
term advanced from 1 to 2. This is one local measurement with client
retries. All checked-in Go packages and the plan validator also passed.

### Cross-role evidence before a selector switch

Unit tests change a search Pod and, separately, a search EndpointSlice
while indexing is being qualified. The executor drops the changed
evidence before planning. Planner tests reject lease deadlines that have
elapsed even when the observation is inside the normal age limit.

The API-server regression changes the search EndpointSlice after search
converges and while the retained source is being checked. It requires a
hold with indexing still on blue. A separate mutation of the indexing
EndpointSlice after both route probes holds the final fleet commit.
Changing the instance during the final recheck also holds the selector
write. The next stable observation is eligible to progress.
The API-server test passed with six Java processes, the local TLS proxy
and SeaweedFS. It verified that the write acknowledged by green remained
searchable after rollback. The full Go package suite and plan validator
passed.

### Blue/green behind a two-replica Gateway

The kind drill used Kubernetes 1.32.2, Envoy Gateway 1.8.6, the pinned
Java bundle with process identities, and a fresh SeaweedFS bucket. It ran
the real controller and publisher. A test CA authenticated the two
HTTPS role routes. The fixture checked actual Node zones, Pod versions,
direct JVM identities, leases, and warm search readiness before signing
rollout evidence. Both fleets used the same image and store.

The scheduler rejected preview Pods in two groups while their available
dedicated hosts were occupied. The operator withdrew the unbound Pods
and held retries. After the test supplied enough hosts, two explicit
`RetryProvisioning` operations allowed green to reach three Ready Pods.
The two fleets used six hosts with one OpenSearch Pod per host.

The promotion switched search before indexing. Both Gateway replicas
reported green JVM identities on both routes and a write through the
promoted index route was acknowledged to the write-ahead log. Rollback
restored the original JVM responders and both proxies returned the exact
documents written before and after promotion. Both production Services
kept their original UIDs and ClusterIPs throughout.

The client opened fresh HTTPS connections and sampled all four
proxy/role paths about once per second. Each path returned 123 responses
with HTTP 200 and two with HTTP 503, one during promotion and one during
rollback. Envoy recorded `no_healthy_upstream` for all eight failures.
This drill measured selector-based cutover and per-endpoint health
checks. Prequalified per-fleet route switching, connection reuse across
cutover, and AWS ALB remain separate qualification work.

Rollback restored both selectors and committed the serving fleet, then
held the operation claim because enabling `spec.rollout` had changed the
template hash even though the Java Pod configuration was the same.
Restoring the test instance's original rollout policy released the claim
and let the operation report success.

The revision fix excludes rollout policy from template hashing and
preserves verified legacy labels. The API-server regression starts with
the revision of an instance that had no rollout policy, then exercises
rollback with blue/green still enabled. The reconciler releases the
instance claim and marks the operation successful. Unit tests cover
preserving a previously issued legacy label after policy edits and
rejecting that label after an image change.
[Revision label migration](development-rollout.md#revision-labels-on-existing-fleets)

### Rollback with blue/green left enabled

A seven-node kind cluster repeated the drill using Kubernetes 1.32.2,
Envoy Gateway 1.8.6, and the same digest-pinned Java build and external
S3-compatible store. The instance started without a rollout policy.
Both production routes initially returned 503 while the upstream Pods
were Ready. Recreating the fixture HTTPRoutes restored traffic before
seeding or sampling. The drill did not isolate the startup failure.

After enabling blue/green, all six Java Pods had the same template
revision. Both promotion and rollback reported `Succeeded`. Rollback
cleared the instance operation claim and returned both role selectors
to the initial fleet while `spec.rollout.strategy` stayed `BlueGreen`.
Both proxies returned the expected JVM identities for both roles after
each cutover. Search after rollback returned the exact two documents
acknowledged before and after promotion. Both role Services retained
their UIDs and ClusterIPs. The second run again measured 492 HTTP 200
responses and eight HTTP 503 responses across the 500 fresh HTTPS reads.

### Envoy Gateway backend weights and health admission

The seven-node drill also tested a header-matched route for each role
through the same two Envoy Gateway 1.8.6 replicas. Each route pointed to
separate initial and green Services backed by the Ready Java Pods.
The test patched HTTPRoute weights directly and left the production
rules unchanged.

At 100/0, each proxy reported one route cluster per role, with two
search endpoints and one ingest endpoint. The metrics counted only the
positive-weight fleet's endpoints, despite all six Pods being Ready.
After switching search directly to 0/100, both proxies returned 503
for all 50 immediate search probes. The 50 control probes for ingest
stayed on the original fleet and returned 200. Once both rules
settled at 0/100, all 100 probes reached green. At 80/20, all 400 probes
returned 200, with 19 to 23 of 100 requests reaching green on each
proxy and role combination.

With both weights positive, each proxy counted four healthy search
endpoints and two healthy ingest endpoints. Changing 80/20 to 20/80
one role at a time produced 1,200 HTTP 200 responses across the
baseline and both transition samples. About 80% of requests reached
green on each changed role, while ingest stayed near 20% until its
own change.

For another round trip, each role moved from 100/0 to 99/1. The
fixture waited for both proxies to report both fleets' expected
healthy endpoint counts before moving that role to 0/100. Rollback
used the same sequence in reverse. All 1,720 sampled requests returned
200, and the 100 requests sampled for each role after its full cutover
reached the intended fleet. The first positive weight exposes a small
share of client requests to the joining fleet.

Gateway API specifies that zero weight receives zero requests. Proxy
health admission at that weight depends on the implementation. These
tests used fresh HTTPS requests without concurrent faults and patched
test routes directly. Operator-driven weight changes, durable route
claims, connection reuse, and an ALB cutover remain unverified.
[Gateway API traffic splitting](https://gateway-api.sigs.k8s.io/guides/user-guides/traffic-splitting/)

### Weighted planner and conditional route patch

The planner tests cover initial admission with both two and three
fleets, rejection of a direct zero-to-full switch, missing proxy
evidence, stale revisions and JVM incarnations, and samples that
disagree with configured weights. They also cover recovery from a
patch whose outcome must be read back and removal of an unhealthy
canary while the verified incumbent retains traffic.

`go test -tags envtest ./internal/controller -run '^TestWeightRouteConditionalPatchAPIServer$' -count=1`
passed. With a test HTTPRoute schema, the API server applied the
weight-only patch and rejected another after a hostname edit landed
between the final read and the patch. The edit and prior weights
were preserved. This test covers Kubernetes compare-and-set behavior.
The separate Envoy drill covers data-plane behavior for manually
applied weights. The planner and adapter are available for controller
integration.

### Per-fleet Gateway backend Service lifecycle

The API-server regression enables `spec.rollout.gatewayBackends` with
an initial and a candidate fleet. It checks that the controller
persists all four role and fleet intents before creating the Services,
then replaces the reconciler after losing a successful create response.
The new reconciler pins the original UID. The test checks the selectors
on each backend, retains the original UID and ClusterIP when provisioning
is disabled, and verifies a deleted pinned backend remains unavailable
even after a same-name Service is created.
The stable search Service keeps its UID, ClusterIP and selector.
`go test -tags envtest ./internal/controller -run '^TestRolloutStatusAPIServerConflict$/gateway_backend_lifecycle' -count=1` passed.

### Signed evidence for weighted routes

`go test -tags envtest ./internal/controller -run '^TestWeightRouteConditionalPatchAPIServer$' -count=1 -v` passed. The test publisher rejects a
report before the operation claims its instance. After the claim it
publishes two signed replica reports through the API server, and the
reader and planner accept the observed 99/1 split. Changing the route
version prevents republication of the old evidence and clears it from
the planner's input. Unit tests reject expired reports, missing or
duplicate replicas, a different route or weight distribution, tampering,
and signatures from the selector evidence domain.

The fixture supplies the proxy inventory and probe results. Collection
from real serving proxies remains unqualified.

### Weighted operation journal recovery

The API-server fixture exercises two role routes and four pinned
backend Services. It records the schedule in operation status, then
lets Kubernetes apply the first weight patch and discards the response.
A fresh executor reads back the target and records its observation time.
It waits for evidence after that time before advancing the journal.
An edit that changes the ingest route version during search
qualification also holds the pending write. The test completes the
search schedule before the ingest schedule. The journal builder reads the
approved routes, verifies their initial 100/0 weights, and pins the current
backend Service UIDs and source and candidate revisions. The test checks
these pins against the real API objects before running the schedule.

The fixture invokes the weighted executor with injected qualification.
Qualification against real proxies remains open. The publisher test
checks that a source fleet approval
requires a claimed weighted journal and uses a separate ConfigMap,
preserving the target approval.

### Weighted promotion commit

The API-server test continues through finalization after both weighted
routes converge. Missing fresh route evidence holds before changing
either stable Service. The operator then switches one stable Service
per pass and keeps its UID. With both selectors switched, missing
EndpointSlices still prevent the serving fleet commit.

After the fixture supplies matching EndpointSlices, the operator
commits both serving roles and retains the route and Service UIDs,
route spec digests, and both fleet revisions for weighted rollback.
The pins are stored on the instance so rollback can read them without
the promotion object. A later pass reports operation success before
releasing the claim. The test reruns finalization after release
and verifies that selector rollback rejects the weighted target.
The fixture supplies qualification and EndpointSlices. Live Gateway
and Java verification of the weighted commit remains open.

### Weighted rollback recovery

The API-server fixture interrupts promotion after Kubernetes applies
99/1 search weights and the patch response is lost. Rollback reads that
distribution, records a reverse search step, and adopts the unchanged
ingest route. A second pass transfers the claim. Both routes require
fresh qualification before the operator commits the restored fleet.

A second fixture completes promotion, releases the claim, and deletes
the promotion operation. Rollback uses the retained route, backend and
revision pins on the instance. It admits the original fleet at 1% for
each role before restoring full weight. Both cases return the stable
Services to the original fleet and release the rollback claim. These
tests use injected proxy observations and fixture EndpointSlices.

### Weighted reconciliation dispatch

The API-server fixture drives the normal operation reconcile entry
point. It holds an explicit weighted promotion without route
configuration, then claims the instance and persists the weighted
journal when the routes and candidate evidence are available. Both
rollback fixtures inherit weighted mode and use the reconcile entry
point for handoff, execution and finalization. Existing selector
rollout tests still pass. This fixture injects qualification and
EndpointSlices. It does not run a controller-manager deployment or
collect reports from real Gateway replicas.

### Live weighted Gateway promotion and rollback

The seven-node kind fixture ran Kubernetes 1.32.2, two Envoy Gateway
1.8.6 proxies, the pinned OpenSearch Java bundle, and an externally
provisioned SeaweedFS S3 bucket. It deployed the operator with the
weighted route flags and a namespaced ServiceAccount. API access
checks allowed it to patch the approved search route and denied its
requests to patch an unapproved route or read Nodes.

The fixture verifier inventoried both serving proxies, then read their
local Envoy admin cluster state and sampled each HTTPS path separately.
It compared the active endpoints with Pod IPs and JVM incarnations,
rechecked the HTTPRoute resource versions and proxy inventory, and
published signed reports. The verifier also supplied separate signed
role and AZ evidence. This fixture uses the same Java image in both fleets
with development HTTP and security settings.
[Envoy admin cluster status](https://www.envoyproxy.io/docs/envoy/latest/operations/admin)

The controller moved each role from 100/0 through 99/1 to 0/100.
The fixture restarted the operator Deployment with search admission
recorded and the ingest schedule still at its first step. The replacement
manager completed the same operation. Rollback used the reverse
sequence, restored both stable role mappings and production routes to
the original fleet, and released its claim. Both operations reported
`Succeeded`. Search on the restored route returned both acknowledged
documents, including the write made through the promoted indexing route.

Four independent HTTP/1.1 client loops reused HTTPS connections to
each proxy and role, sampling about four times per second for 91
seconds. All 1,356 document reads returned 200 with no transport errors.
The signed reports contained another 60,480 successful process-identity
probes across the observed weight stages. These larger bursts only
covered windows where the proxy endpoint set matched the route.
The live fixture used fresh, signed observations supplied by a test
verifier. Production proxy collection, ALB, faulted Gateway replicas,
and arbitrary rainbow weight schedules remain open.

### Configurable weighted promotion stages

The API-server fixture supplies candidate weights of 10% and 45%
for search and 20% for ingest. It checks the persisted role schedules,
including the automatic 1% and 100% steps, and observes each route
patch before proceeding. At the 10% step, the planner holds when a
replica reports the previous 99/1 traffic split. It also holds when
traffic looks correct but a proxy lacks active configuration evidence.
The signing tests reject stale configured weights even when a report
names the current HTTPRoute resource version.

The follow-up repeated the live drill on a fresh seven-node kind
cluster with the same Java image, S3 fixture and two Envoy Gateway
1.8.6 proxies. The test verifier read the active Envoy Endpoint
Discovery Service assignment before and after each sample. It mapped
the addresses in each weighted locality to the fleet Pod identities
and used the locality weights to verify the exact configured split.
Both proxies independently rejected a claimed 99/1 baseline while
their active configuration still served 100/0.
[Envoy endpoint configuration and locality weights](https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/endpoint/v3/endpoint_components.proto)

The restarted manager finished search at 1%, 10%, 45% and 100%
before moving indexing through 1%, 20% and 100%. Both operations
returned `Succeeded` and released their claims. Rollback restored
both roles to the original fleet, and the restored search route
returned the writes acknowledged before and after promotion.
All 2,121 continuous HTTPS document reads returned HTTP 200.
Signed reports captured 67,200 successful identity probes during
windows where both proxies had the expected configuration. The test
used a disposable verifier and two fleets.

### Reusable Envoy proxy collector

The Go tests serve typed Envoy admin responses and send 500 real HTTPS
requests through a loopback probe target. The collector retains the
observed 80/20 configuration and counts 400 source and 100 candidate
responses with matching JVM identities. With the endpoint set unchanged,
it rejects the older 81/19 split. It also rejects a route version change
between the initial and final inventory reads. The live Envoy EDS dump
omitted `version_info` and `last_updated`. The collector compares the
full endpoint assignment before and after sampling and uses the xDS
version and timestamp when Envoy supplies them. The regression test
omits both fields and verifies that a changed assignment invalidates
the report even when endpoint identities and weights match.

The Kubernetes client fixture verifies that both ready EndpointSlice
targets must have tunnels and that a changed Pod resource version
invalidates an earlier signed approval.

The maintained binary drove the same weighted rollout on a fresh
seven-node kind cluster with two Envoy Gateway 1.8.6 replicas, six Java
Pods and a SeaweedFS S3 fixture. Its service account could read the
HTTPRoutes and list the Gateway EndpointSlices. Kubernetes denied it
route and ConfigMap writes, Node reads and creation of Pod port
forwards. The platform fixture established the tunnels, and the
separate publisher supplied signed fleet approvals and proxy reports.

An early observer run held on the omitted EDS fields. The drill resumed
the same promotion with the rebuilt binary. The manager restarted
after search reached 1% and before indexing weights moved. Search
continued through 10%, 45% and 100%. Indexing advanced through 1%,
20% and 100%. Rollback restored both original fleets. Both operations
reported `Succeeded` and released the claim. The stable Service UIDs
and ClusterIPs stayed the same, and the restored search route returned
both acknowledged writes.

All 1,577 client reads during the resumed run returned HTTP 200.
Another 765 reads returned HTTP 200 during the initial window before
the resume. Across 76 published proxy reports, the observer recorded
85,120 successful HTTPS identity probes and zero probe failures.
The drill covered two fleets on kind. Cloud Gateway and ALB behavior
require separate qualification.

### Persistent cluster settings and claim recovery

The opt-in `ApplyClusterSettings` API-server test injects a lost response
after a successful write and verifies read-back before release of the
instance claim. A new reconciler completes the release with the settings
feature disabled. A second operation acquires the instance, and replay
of the completed operation leaves the new claim in place. The test
deletes the second operation between read-back and submission of the
write. It verifies cancellation, finalizer removal and claim release
with no additional write. A conflict before dispatch produces a terminal
result and releases the claim without changing the observed value.

An injected timeout leaves an earlier write running while the operation
is deleted. The controller retains the claim and submits no second write
while read-back still shows the base value. It releases the claim after
the delayed commit becomes visible. A separate injection delays the
instance claim patch until deletion completes. The API server rejects
that stale patch after the controller fences the instance resource
version, and the instance is left without a claim.

Unit tests check admin Service ownership, role, selector and resource
version. A Kubernetes 1.32.2 kind drill ran the final controller with
separate Java search and ingest Pods and a precreated SeaweedFS S3
bucket referenced through `spec.storage.repositoryRef`. The controller
set `serverless.bucket=off` through the ingest Service and released its
claim. After the ingest Pod was replaced, the new process read back
`off` from storage. A second operation removed the setting and released
its claim. An unsupported value was rejected before acquiring a claim.
Both writes reported `Persisted=True` and `VerifiedEffective=Unknown`.
Authenticated transport, settings behavior during retained-fleet
rollback and concurrent external writers require separate qualification.

The API-server test creates an immutable test Secret, checks its UID is recorded in the Pod creation intent, and admits the corresponding Pod volume. Unit tests reject mutable or empty Secrets and a recreated Secret UID. The test Secret is not a real keystore and envtest does not mount it; the earlier standalone Java/SeaweedFS smoke test used a real keystore outside Kubernetes. WSL's Docker CLI reports that Docker Desktop integration is unavailable, so a real local Kubernetes workload has not been run.

The development peer-DNS path persists whether preflight is required through Pod creation, gate release and provisioning status. Unit and Kubernetes 1.32 API-server tests exercise a bound Pod waiting for its DNS init container, a timeout hold, retention of the bound Pod and automatic recovery when that same preflight later reports successful termination. The API-server test injects Pod status; it does not run a kubelet or DNS, and it does not validate node or autoscaler behavior.

### Peer TLS snapshot recovery

The opt-in peer TLS path validates each planned Pod DNS identity and
records the immutable snapshot UID before creating a Pod. The unit
tests cover snapshot recovery after the original Pod intent was retired,
a missing issuer Secret, changed references, a mismatched snapshot UID,
insufficient certificate validity and modified or aliased mounts.

The Kubernetes 1.32 API-server test refreshes the source before the
initial snapshot exists and checks that the controller records a fresh
intent using the new source version. It updates the issuer Secret again
while the first Pod is held and verifies gate release. A later attempt
creates a new Pod with the committed snapshot after replacing the
issuer's leaf Secret with invalid material under a new UID. Deleting
the snapshot or recreating it under the same name and a different UID
keeps the replacement Pod gated. The API-server fixture does not mount
volumes or run Java. Earlier Java integration tests verify that changed
leaf or trust material fails the startup pin checks.

### Peer TLS rollout staging

The Kubernetes 1.32 API-server fixture rejected a peer TLS candidate
without a finite rollback window. It observed the snapshot
intent before Secret creation and the immutable UID before the preview
Services. An issuer update before Secret creation replanned the candidate
without changing its fleet name.

The fixture qualified a same-CA leaf renewal and rejected a new-CA leaf
while the serving fleet trusted only the original CA. It also rejected
a leaf that expired inside the configured rollback window. With a modeled
serving-fleet switch between stages, it qualified the overlap bundle,
then the new leaf, then the new-only bundle. It rejected pruning while
the serving leaf still needed the old CA. The fixture also checked both
rollback trust directions and held preview Pod admission after the
serving snapshot disappeared. No Java processes or native peer
handshakes ran in this fixture.

### Live Java peer TLS rotation

The selector rollout drill used one-AZ kind with Kubernetes 1.32.0 and
Envoy Gateway 1.8.6. The Java image manifest digest was
`sha256:9b235b7c4b965d9e0a421ea7f934e4be0d6661ae16e4ae45570993643be967fc`.
Envoy kept the same independent HTTP backend CA throughout the peer
rotation.

The controller rejected a new peer leaf while the serving fleet
trusted only the original peer CA. The next candidate used the old leaf
with both CAs. Both production roles promoted to that fleet and
continued to serve HTTPS with Basic Auth. The platform retired the
initial fleet after checking the reserved Pod UIDs and confirming that
the JVM containers had stopped.

With the new leaf on the candidate, both production roles promoted,
rolled back to the fleet with the old leaf, and promoted again. Fresh
TLS 1.2 probes exercised every cross-fleet pair in both directions.
An independent probe of the mixed-leaf cohort pinned both leaf hashes
and checked the six Pod UIDs, IPs, container IDs and Java process
incarnations before and after eighteen handshakes.

Two separate writes went directly to the preview ingest JVM over
HTTPS. Each write receipt named the serving ingest JVM that actually
owned the shard. The second write crossed CA generations. Those
receipts verify Java request forwarding in addition to the fresh
handshakes.

For the new-only promotion, the fixture rechecked Pod and Java
process identities after the handshake matrix and signed approvals
only for tested target processes. Both Envoy routes reached the
new-only fleet and verified an authenticated index write and search
after the selector change.

After signed retirement of the last overlap fleet, all three serving
Pods mounted the exact replacement-only CA bundle. Each accepted
fresh peer connections from the other two Pods and rejected both
the retired client certificate and a connection without a client
certificate with a remote TLS alert. Index acknowledged another
write after retirement, and search returned all eight verification
documents. Both routes returned HTTP 401 for anonymous and wrong-password
requests. The stable Service UIDs were preserved, and the active Envoy
endpoint TLS configuration still used the original HTTP backend CA.

### Live cert-manager peer issuance

On one-AZ kind with Kubernetes 1.32.0 and cert-manager 1.20.5, the
operator created a Certificate for the allowed namespaced CA Issuer.
The CertificateRequest was approved, and Java booted from the
immutable snapshot of the issued material. The operator ServiceAccount
could get and create Certificates and get CertificateRequests. It
could not read Issuers or ClusterIssuers, patch Certificates, or list
CertificateRequests.

The controller refused a second valid Issuer outside the allowlist
without creating a Certificate. It also refused a different valid
leaf from the same CA when the mutable Secret's leaf did not match
the current CertificateRequest. Both checks left the serving
snapshot intact.

The fixture stopped the cert-manager controller and requested renewal.
The candidate stayed unstaged. A replacement search Pod reached Ready
using the original snapshot, and both protected Envoy routes remained
usable. After cert-manager restarted, the same source Secret UID held
a new leaf and public key. The controller staged that revision in a
separate immutable snapshot and kept the original pin for the
serving fleet.

Fresh native TLS probes verified both leaf fingerprints during the
transition. A write sent to the preview ingest JVM was forwarded to
the serving shard owner over Java peer transport. Both Envoy routes
promoted to the renewed certificate fleet and rolled back to the
original fleet with verified HTTPS and Basic Auth. Index acknowledged
a write on each fleet, and search returned those writes and the
native forwarded write after rollback. Both routes kept their Service
UIDs and returned HTTP 401 for an invalid password.

### Live cert-manager HTTP issuance

On one-AZ kind with Kubernetes 1.32.0, Envoy Gateway 1.8.6 and
cert-manager 1.20.5, the Java fleet started from an operator-owned
immutable HTTP leaf snapshot. Envoy accepted both HTTPRoutes and
BackendTLSPolicies, with a separate DNS identity for each role
Service. The running Envoy configuration contained the expected CA
for all two search endpoints and the index endpoint. The fixture used
an independently configured edge certificate, a precreated SeaweedFS
bucket and the same digest-pinned Java runtime for both fleets.

With cert-manager stopped, a requested renewal stayed unstaged. A
search Pod evicted by UID was replaced using the original snapshot,
and authenticated requests to both routes succeeded during the
outage. After cert-manager restarted, the same mutable Secret UID
held a new leaf and key. Green pinned it in a new immutable Secret
while the serving fleet retained its original snapshot.

Promotion moved both routes to green. Rollback returned both to the
original fleet and certificate. Each transition verified the role and
fleet through Envoy, checked the JVM leaf fingerprint, wrote a document
through index and found it through search. Search returned both
documents after rollback. Both production Service UIDs were preserved,
and both routes returned HTTP 401 for missing and incorrect credentials.

The Go package suite, Kubernetes API-server suite with `-tags envtest`,
and `python tools/verify_plan.py` passed. The single-AZ drill exercised
the same CA throughout and sampled requests at rollout checkpoints.

### TLS snapshot and probe tests

`go test -tags envtest ./internal/tlsconfig -run TestAPIServerImmutableTLSSnapshotRecovery` has passed against a disposable Kubernetes 1.32.0 API server. It checks that a staged TLS Secret is immutable and that the same snapshot is recovered after the source changes. Unit tests cover source-version changes, incorrect key material, required identities and a minimum validity window. They do not prove a Java TLS provider, Secret volume mounting, certificate issuance, rotation or peer/client handshakes.

Local Go HTTPS tests also check a fresh verified server connection, an mTLS test server, wrong identities, missing client credentials and old/new CA overlap and removal. These test the operator's HTTP probe against a Go server, not the Java shell or an actual certificate rollout.

### Placement and readiness fixtures

Capacity-profile tests cover same-namespace immutable profile resolution, zone and expiry rejection, hard placement, and fit with an injected sidecar. The API-server test admits a profiled Pod and exercises its gate-release path with synthetic profile data. It does not run the scheduler or establish that platform measurements, node shapes, taints, DaemonSets, or actual binding match the profile.

The local split-role Java smoke test at pinned revision `e71d42331a3ec82ec01109a5bb42d6a9a9162a5c` exercised the readiness and local-lease adapter against a real ingest process, then verified indexing, search and replacement on a filesystem store. API-server admission and unit tests cover the development exec probe and fail-closed lease parsing. The probe executable has not been run by a kubelet, and lease validity does not prove peer reachability or cache warmth.

The development warm-up slot is persisted before gate release in the API-server test. Unit tests simulate readiness timeout, worker completion, container restart and a new process incarnation before the slot is released. These use a fake observer and do not establish network identity, concurrent Kubernetes updates, autonomous JVM restart limits, strict warmness or cloud performance.

The development zone-capacity sample also reports its observation time and
generation, Pod lifecycle counts, Ready shortfall and whether the Kubernetes
Ready count is below the configured zonal minimum. Unit tests exercise stale
Pod phases, termination, incomplete observations and timestamp refresh. The
counts use operator Pod labels and do not verify Node AZ, traffic or warmth.

### Ready capacity for the warm reserve

Per-zone `warmSpare` adds desired serving Pods beyond `replicas`.
The status shows how many requested reserve Pods lack even Ready
capacity in `warmReserveReadyShortfall`. The operator counts regular
Ready Pods toward replicas first and excludes temporary and
disruption-marked Pods from the reserve calculation. A positive
shortfall yields `InsufficientReadyPods`. With zero shortfall,
Java warmth remains `Unknown` for a requested reserve.

Unit tests exercise an emergency Pod that is also marked for disruption,
an independent index reserve, and a raw Ready count that already meets
target while the projected reserve is short. API-server tests cover
the status fields, combined capacity validation and creation-time
autoscaler annotation. GKE Cluster Autoscaler and Karpenter spare
retention require separate integration tests. Safe scale-down after
reducing the configured reserve still needs controller support.

A Kubernetes 1.32.0 Kind drill ran two Java Pods per role with one
replica and one reserve requested. A synthetic disruption signal on a
Ready Pod raised the affected role's reserve shortfall to one while
both of its Pods still counted as Kubernetes Ready. The other role
kept its own reserve status. Clearing each signal cleared its shortfall
and returned the status to `Unknown`. All four Pod UIDs were preserved.
The Go and full real API-server suites passed.

## Release evidence

Record the exact operator and Java images, Kubernetes and cloud versions, store, security provider, plugin artifacts, workload, observed latency and errors, and thresholds. Include recovery and rollback traces and the conditions under which a feature is supported.

### Versioned secure development package

The packaging command rendered `v0.1.0-dev.0` and `v0.1.0-dev.1` for
`oss-release-check`. The versions used different manager digests and
identical CRD bytes, Java image, and DNS domain. The packaging tests
verified manifest checksums, custom namespace and ConfigMap references,
digest-only inputs, and refusal to overwrite an existing output.

Kubernetes 1.32.0 Kind accepted the first package. Both CRDs were
already established, and the package created the namespace and started
its manager Deployment. The manager retained namespace-only access.
The new Java workload failed to reach both Ready roles within the
initial wait. A subsequent read hit an API timeout while sustained
memory pressure affected the local Docker environment. Writes, upgrade
and rollback through this package remain unverified. A repeat read
confirmed that the manager intermittently failed its leader lease
renewal on API timeouts. The original package subsequently created
both role Pods and Kubernetes reported both Ready. The manager was
still crash-looping and the install check timed out waiting for
`Available`. The API TLS handshake timed out before it could start
the direct readiness probe. A container-runtime retry also produced
no route response within the observation window. No package transition
was applied. The local input packages and fixture are under
`tools/bin/oss-release-check-*`.
[Package usage](../../config/operator-dev-secure/README.md#package-a-versioned-development-install)

#### Signed install inventory

The local `v0.1.0-dev.1` package was rendered and signed with an
ephemeral Ed25519 key. Its unsigned inventory and manifests were
byte-for-byte identical to the original candidate package. The verifier
used the separately supplied public key, validated the signature and
recomputed the two manifest hashes. The test rejected a changed
manager manifest and rejected a rewritten inventory that contained the
changed manifest's new hash. Registry image signing, SBOM publication
and release publication remain separate gates.
[Package signing and verification](../../config/operator-dev-secure/README.md#package-a-versioned-development-install)

#### Digest-pinned SPDX attachments

Syft 1.57.2 generated SPDX 2.3 from two offline Linux AMD64 OCI
fixtures. The manager fixture contained the built Go operator
binary. The Java fixture contained the staged ANTLR runtime JAR.
The packager verified each platform manifest and all referenced
blobs before scanning. It then checked Syft's reported manifest
digest. The verifier authenticated the signed inventory and both
SPDX files. A one-byte edit to the Java SPDX file was rejected.
Regression checks cover a missing manifest, corrupted layer and
a scanner result for a different digest.

The release fixture's API remained unavailable and the local Docker
image query did not return. Both SPDX scans used the fixture archives.
Scanning and publishing the actual release images remain open.
[Packager procedure](../../config/operator-dev-secure/README.md#package-a-versioned-development-install)

#### Manager image-only package preflight

The verifier checked the signature and manifest hashes of both
`v0.1.0-dev.0` and `v0.1.0-dev.1` signed with the same fixture key.
The actual package pair passed in both directions. The previous
manager manifest matched the desired manifest after substituting its
digest-pinned manager image. Regression tests rejected changes to the
previous package's CRD bytes, changes to the Java image, and a
separately signed change to the manager Deployment replica count.
This verifies package differences before Kubernetes applies them.
[Manager image-only procedure](../../config/operator-dev-secure/README.md#package-a-versioned-development-install)

#### Packaged API-server transition

The release job runs the signed-package transition test against a
temporary Kubernetes API server and etcd. It pins the Linux AMD64
envtest archive by SHA-512 and uses the accompanying kubectl binary.
[Envtest v1.32.0](https://github.com/kubernetes-sigs/controller-tools/releases/tag/envtest-v1.32.0)

To run the test on Linux or WSL, set `KUBEBUILDER_ASSETS` to the directory
with the envtest binaries and put its kubectl on `PATH`. The test
also uses Python 3 and OpenSSL 3.

```sh
export PATH="$KUBEBUILDER_ASSETS:$PATH"
go test -mod=readonly -count=1 -tags envtest ./tools/releasecheck
```

The test installs the CRDs and original signed package, verifies
manager EndpointSlice access and denied Node access, and records
instance state through the Kubernetes status subresource. It applies
the signed manager image change and reverts it, requiring the same
Deployment UID and arguments, stored CRD versions and instance state.
The native Windows API server accepted all apply and state assertions.
The harness reported failure during cleanup because its shutdown signal
is unsupported on Windows. The two fixture processes were stopped
explicitly. The repository test targets Linux and WSL and registers
cleanup before starting the control plane. The local Linux test binary
built successfully, but its resource-constrained WSL run timed out
starting etcd. The hosted CI job has yet to run. Java readiness and
authenticated write/search checks use the separate Kind drill.

#### Read-only manager transition observation

The observer reads the Deployment, Pod, Lease, instance, role
Services and EndpointSlices in the package namespace. It verifies the
signed package before the reads. It checks current leadership, a
fresh availability sample and whether the role EndpointSlices match
the eligible Ready Pod UIDs and addresses. It then compares the
recorded Java Pod and Service identities before and after a manager
image change. Regression checks reject a stale Lease or availability
sample, a recreated Service, a stale endpoint Pod UID, an incorrect
endpoint IP and a missing Ready endpoint.

The Verify workflow now runs the packaging and observation tests
in a dedicated Ubuntu 24.04 job. The step uses fixture signing
keys generated during the tests and requires no cluster or registry
credentials. Both test commands and the workflow YAML check passed
locally. The hosted job has yet to run.
[Verification workflow](../../.github/workflows/verify.yaml)

In the Kind release namespace, the observer verified the original
package and stopped at the Deployment check because it had no Ready
replica. A successful live pre/post observation and authenticated
round trip still require a healthy manager. A later direct read of
the two role EndpointSlice sets timed out during the Kubernetes API
TLS handshake. The observer's EndpointSlice checks have regression
coverage. The live validation remains pending.

### Endpoint observation telemetry

The endpoint sampler records one duration in the manager's metrics
registry for each completed attempt. The only application label is
the outcome. The regression test gathers the registry after a
successful retry, repeated Pod version changes, recovery and a failed
status patch. It confirms that the retry and the waiting interval
produce no duplicate observations and that a failed patch produces
only the failure outcome. The resource-constrained Kind release fixture did not
run the new image or exercise a live metrics scrape.
[Telemetry in the runbook](day-two.md#endpoint-observation-telemetry)

### Namespace-scoped incident diagnosis

In the Kind release namespace, a tokenless fixture ServiceAccount
bound only to the optional diagnostic Role read the manager Deployment,
Pod, leader Lease and events. It also read the instance, operations and
both stable Services and EndpointSlices. The manager had no Ready
replica. The same Lease renewal was 109 seconds old on the first read
and 177 seconds old on the second, against a 15-second duration. The
second read included the account's normal Kubernetes groups. The
recorded endpoint floor shortfall and zero Ready
endpoints on both roles were visible without Node access. Kubernetes
denied Node and Secret reads, Pod logs and eviction, Service mutation,
and Pod listing in a different namespace. The exercise identified the
stalled manager. It did not restore Java readiness or exercise the
provider-specific recovery steps.
[Development runbook](day-two.md)

### Development manager install

The repository Dockerfile built a Linux AMD64 manager with Go 1.23.12
and ran it as UID 65532. Kubernetes 1.32.0 Kind installed both generated
CRDs and the Kustomize Deployment, Role, RoleBinding and ServiceAccount.
The manager passed its health probes with a read-only root filesystem.
Its ServiceAccount could not list Nodes or Pods in another namespace.

The instance example used the pinned Java manifest
`sha256:9b235b7c4b965d9e0a421ea7f934e4be0d6661ae16e4ae45570993643be967fc`
and an externally created SeaweedFS S3 bucket. The manager created the
search and index Pods and their stable Services. Test writes through the
index Service were searchable through the search Service before and
after the manager image changes. [Development install](../../config/operator-dev/README.md)

The Kustomize image override ran the manager at
`sha256:4df99f6df848174a4da5930ffdf6694504eb4c3bb6d5081028e074675ca86743`.
Reapplying the original overlay restored
`sha256:52e9467f514c12bf3cd472650fd9c927984e13093da3cfec4fd158cf6c63a40c`.
The test waited for the expected manager Pod to hold the leader lease
before writing on each image. Both Java Pod UIDs and both stable Service
UIDs and ClusterIPs were preserved. Search returned all five test
documents. The two manager images used the same binary and differed in
image metadata. Cross-version CRD and runtime compatibility, signed
release publication and cloud installation remain separate gates.

### Secure development install

A fresh Kubernetes 1.32.0 Kind cluster applied the shipped CRDs and
`config/operator-dev-secure`. The manager ran from
`sha256:764a190a363beedd9a9c328b4d41e3b36439cb9a071cfed094e3712a3ad44999`
and Java from
`sha256:91d29697abeabd41592c83117081c7c0dea231c531b98001e52e36fc9c23d025`.
The test supplied an external SeaweedFS bucket, the immutable startup
Secret and a certificate with both Service names and the Pod DNS
wildcard. The instance created no Java Pods before the TLS inputs were
available. The manager ServiceAccount could list EndpointSlices in its
namespace. The Kubernetes API denied its Node list request.

Once the role Services qualified, both returned Ready over verified
HTTPS with Basic Auth and HTTP 401 for an anonymous call. A client
using a different TLS hostname failed verification. The index Service
acknowledged a document at primary term 1 and the search Service
returned it. The controller briefly reported `RoleEndpointsUnverified`
during resampling. The final observation recorded
`Available=True` with `ReadyRoleEndpointsObserved`.
[Secure development install](../../config/operator-dev-secure/README.md)

### Cross-build manager and additive schema upgrade

Kubernetes 1.32.0 Kind ran the earlier manager manifest
`sha256:e323f40b32609ddf8d352871cebb8a460122b3648f1ce5d740e5d9cbfefdf2a4`
and the packaged manager manifest
`sha256:52e9467f514c12bf3cd472650fd9c927984e13093da3cfec4fd158cf6c63a40c`.
The earlier CRD artifact was unavailable. The fixture used the current
schema with only `status.lastEndpointObservation.observedServingFleets`
removed to test that additive change. Both managers ran the same pinned
Java image against the same supplied SeaweedFS bucket.

The earlier binary created the initial search and index Pods. The
fixture applied the current instance CRD in place, retaining its UID
and the stored version `v1alpha1`. After acquiring the leader lease,
the packaged manager reconciled a new search reserve and persisted
`Available=True` with the two fleet IDs. The fixture then reapplied the
earlier manager overlay and kept the updated CRD. The earlier binary
reacquired leadership, resumed its historical availability reporting
and reconciled a new index reserve. All existing Java Pod UIDs, both
Service UIDs and ClusterIPs, and the instance UID were preserved.
The search Service returned documents indexed during all three phases.

This evidence covers the two local builds and the selected additive
status field within `v1alpha1`. Production release upgrades, stored
version conversion and Java image compatibility remain separate gates.

# Strict development bootstrap

`TestStrictBootstrapIntentOnAPIServer` covers the development `Strict` /
`AllowOneDegraded` sequence: the bootstrap name and sequence are persisted
before creation, its UID is pinned, the next Pod intent uses strict mode, and
an edited bootstrap mode holds reconciliation. It also drives the normal
reconciler through PDB observation, scheduling-gate release, a degraded
warm-up slot, and creation and gate release of a strict successor with its Java
and kubelet probe settings. PDB status, binding, Pod status and
Java observations are supplied by the test; no scheduler, kubelet or Java
process runs in envtest. Separate
Java tests cover a strict worker retrying until a peer appears. A degraded
bootstrap remains PDB-ready for its lifetime, and automatic recovery of a
strict Pod that exhausts its retry window is not implemented.

# Reusable replacement planning

The `pkg/podlifecycle` replacement planner proposes one action at a time for a
role, fleet and zone. It checks owned, fresh Pod observations, target-revision
eligibility, serving floors, pending creation reservations and surge limits.
It holds an eligible retirement when that would reduce serving capacity below
the group's desired count. Unit tests exercise stale and foreign observations,
pending Pods and reservations, cross-zone isolation and degraded capacity.
The claimed-replacement recheck evaluates the exact victim with the caller's
hold removed from a private snapshot. Tests cover another victim sorting
first, a competing claim, loss of qualification, termination, stale
observations and a changed victim identity. The Verify workflow inspects the
lifecycle package's transitive imports for dependencies on the operator's API,
commands or internal packages. A worker test exercises the public capacity
planner through creation and convergence.

`TestWithdrawUnboundBindingRaceOnAPIServer` injects a real API-server Pod
binding between the executor's GET and DELETE. The server rejects the stale
resource-version precondition, and the test observes the same bound, nondeleting
Pod afterward. This verifies the API race guard; it does not exercise a real
scheduler, kubelet, or cloud provisioner.

`TestEvictionPDBAndLostResponseOnAPIServer` verifies PDB denial preserves a
Pod's observed Ready condition, then injects a lost response after an admitted
eviction. A reread observes termination, a repeated eviction is rejected,
and the stale UID cannot evict a same-name replacement. Envtest has no PDB
controller or kubelet: the test supplies PDB status and force-completes the
Pod deletion for the name-reuse check. It does not prove application serving,
drain, or complete controller recovery.

`internal/java.QualifySearch` combines a completed, non-timeout warm-up signal
from the patched shell with local serving and lease checks, bracketed by the
same process incarnation. Unit tests cover running, degraded, failed,
unrequested, timed-out, restarted and invalid-lease cases. The result does not
prove cache residency or guarantee future health. The caller must bind observations to an exact Pod and container,
recheck those identities, and limit evidence age. Real Java restart, workload
warmth and cloud disruption qualification remain outstanding.

The controller's `qualifySearchPod` adapter brackets an injected direct-Pod
probe with API reads and checks the instance owner, Pod UID, role/fleet/zone,
readiness, container identity, restart count and resource version. Unit tests
reject a changed fleet, restarted container and invalid process incarnation.
Its fake-client test is not proof of API-server races, endpoint authentication
or current cache residency.
The development-only caller now probes the Pod IP directly with bounded HTTP
requests, no proxy and no redirects; it requires the explicitly selected image
and rejects configured HTTP TLS or security. Unit tests exercise the direct
probe and a lease expiring during the final warm-up read. This plaintext path
is not production authentication and does not authorize eviction.
When development gated Pods and the matching warm-up image are enabled, the
controller samples at most one Pod per minute and stores one historical result
in `status.lastSearchQualification`. The API-server test verifies this status
survives a round trip. This field is not a warm-reserve count or current-health
guarantee, and is not consumed by disruption decisions.

### Local Java search activity

The historical sample includes `localSearchActivity` when the stats and
warm-up observations identify the same JVM and the reader and cache
counters are complete. The same HTTP requests supply these diagnostics.
Unit tests cover a verified zero reader count, missing identity, a
different JVM, and missing, malformed or negative counters. The API-server
round trip preserved the optional object and its explicit zero fields.

A Kubernetes 1.32.0 Kind drill ran the pinned Java image on a fresh
SeaweedFS bucket. One search Pod bootstrapped and the second completed
strict warm-up. The sample for the second Pod matched its UID, container
ID and JVM incarnation. The CRD and direct JVM stats both showed zero
local readers, hits, misses and bytes fetched. The requested search
reserve remained `Unknown`. The Go suite and focused real API-server
rollout regression passed.

The local split-role Java smoke additionally checks that a search process with
no warm-up peer reports degraded and fails qualification, while a second
search process can complete warm-up from a live peer and qualify. This uses a
local filesystem store and does not prove cache residency, production S3 or
Kubernetes scheduling and network behavior.
Restarting the warmed search JVM with the same local home yielded a new
incarnation and degraded warm-up despite green readiness; the qualification
adapter rejected it. A comparison restart after the old lease disappeared
completed and passed qualification. Targeted warm-copy logs in two immediate
restart runs identify a failed peer request to the old node ID. The Java code
marks warm-up degraded on peer failure, but the precise transport exception and
any generally safe delay remain unverified.

This planner is not yet wired into the controller. The caller must supply a
complete snapshot and application-specific eligibility, serialize and persist
operation intent, recheck immediately before effects, and use Kubernetes
eviction and PDB protections. These unit tests do not demonstrate a working
certificate rotation or rollout in a Kubernetes cluster.


The unwired `internal/rollout` role-switch planner has unit coverage for claim,
Service identity, candidate qualification, ambiguous patch recovery and route
evidence. Its inputs are caller assertions. It does not create a preview fleet,
patch a Service, validate a live Java process, or prove promotion or rollback.

The local API-server test also drives the unwired role-switch adapter against a
real Service and a synthetic candidate-qualification record. It checks that a
Service resource-version change invalidates a planned patch and that a fresh
plan can patch and read back the target selector. This does not validate a
candidate fleet, a concurrent claim change, endpoint convergence or traffic.

The opt-in development Service sample uses a single Java stats response and
brackets it with Service and Pod API reads. Unit tests use an HTTP transport
fixture; an API-server test uses synthetic HTTP with actual Service and Pod
objects through normal reconciliation. The plaintext response is unauthenticated and does not qualify an
external route, all endpoints or cache warmth.
