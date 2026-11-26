# Task handoff

Writer retirement now accepts a complete pre-eviction scan that only
omits heads the closed writer no longer owns. The original inventory
stays in the journal, and new or changed heads still block eviction.
The Kind run reproduced the smaller inventory after 48 seconds of S3
throttling. The original Pod was then evicted and its first receipt PUT
triggered an S3 outage. The operator held the claim with
`ReceiptUnavailable` until the store recovered. The original survivor
accepted a term-3 write through Envoy with verified TLS and Basic Auth,
and search returned all acknowledged documents. The operation recorded
settled predecessor WAL and confirmed receipt absence, succeeded, and
released the claim. Go tests and vet, 30 shuffled writer test runs,
PowerShell parsing, and the plan check passed.
[Receipt-window verification](verification.md#s3-outage-during-shutdown-receipt-publication)

The first-instance guide covers secure setup, a write and search check,
different warm-spare counts by role/AZ, and selector promotion and
rollback. `kubectl get` displays instance availability, fleet intent,
and the claimed operation. Operation rows show phase and condition
reason. The read-only preview shows desired capacity and the last Ready
sample, then reads both live stable Service selectors for a rollout.
It reports differences between the live selectors and recorded intent.

Local Go and Python checks, CRD regeneration, and the plan check passed.
The Kubernetes 1.32 controller and Pod lifecycle suites passed on the
local Windows envtest fixture. The controller test requested Kubernetes
Table output to verify the new columns and their values. The preview
tests used fixtures for split selectors, stale capacity, and rollback.
This pass did not repeat a live Java or Envoy drill or a first-user trial.

Two signed Kind writer fixtures exercised S3 throttling and a disconnect
after eviction. Under `503 SlowDown`, the operator held the armed
eviction. After restoration, the survivor served a term-4 write through
Envoy and search returned every acknowledged document. The original
retirement stayed at `WriterHeadsChanged` because the durable scan no
longer listed the journaled head for the retiring JVM.

In the second fixture, the operator held the claim during a 15-second
S3 disconnect after the original writer was gone and its release
receipt was recorded. The original survivor served a term-2 write after
restoration. Envoy search returned the two prior acknowledged writes
and the successor write. Both routes used verified TLS and Basic Auth.
The operation succeeded and released its claim. PowerShell parsing,
the proxy build and vet, and the plan check passed.
[S3 fault verification](verification.md#s3-throttling-and-access-loss-after-eviction-dispatch)

The live writer-retirement S3 outage drill passed in Kind. The manager
held an armed eviction and kept the operation claim while the S3
Service was unreachable for about 43 seconds. After restoring the
Service and refreshing signed Envoy evidence, the operator evicted
the exact writer. The surviving JVM acknowledged a term-2 write
through Envoy with verified TLS and Basic Auth. Search returned the
published seed, the WAL-only document, and the successor write. The
operation succeeded and released its claim. The plan and PowerShell
parse checks passed. The drill faulted access before eviction dispatch.
[S3 outage verification](verification.md#s3-access-loss-before-writer-eviction)

The code quality review is complete. The fixes cover controller and
Pod lifecycle boundaries, operation naming and journal handling, CRD
validation and upgrade compatibility, and regression coverage for
retries, conflicting updates, and recovery after a restart.

Writer head inventory now saves the next observer position when a Pod
cannot complete a scan. The initial inventory and the pre-eviction
rescan share the position. Complete scans still require both lease
observations, the pinned credential and store, stable index routing,
and the operation claim.

The regressions cover observer rotation and wraparound, persistence
after an incomplete scan, and a failed rescan that advances the
position without dispatching eviction.
The Go suite, vet, 30 shuffled writer regressions, Kubernetes 1.32
controller suite, plan checks, and CRD regeneration checks passed. The
live writer handoff was outside this run.

Receipt readback tries up to four qualified index Pods per reconcile,
with an individual deadline for the Pod and Java reads. The operation
status preserves the next observer offset on failure. The shared route,
store, lease, and claim checks still gate each accepted receipt or
confirmed missing receipt.

The regression resumes past four unavailable observers to reach a
healthy endpoint. The API-server fixture checks that the offset survives
reconciliation and that observer failure does not record absence.
The Go suite, vet, 30 shuffled writer regressions, Kubernetes 1.32
controller suite, plan checks, and CRD regeneration checks passed. The
live writer handoff was outside this run.

Post-eviction receipt and successor verification share a final check of
the store, retired Pod absence, route and credential binding, index
endpoints, and claim. The earliest observed lease bounds this check
before new evidence can be recorded. A confirmed missing receipt with
an explicitly empty head inventory can now finish with `NoHeadsToTransfer`.

The regression covers persisting the missing receipt observation,
completing the empty inventory without fabricated proof, and releasing
the claim in a later reconcile. An expired observation is rejected
before the final checks.
The Go suite, vet, plan checks, 25 shuffled writer regressions, and the
Kubernetes 1.32 controller suite passed. No live handoff was repeated.

Writer eviction now checks its qualified Envoy objects and report,
credential, Java Pod cohort, index Service and EndpointSlice versions,
and operation claim after the PDB read. The same Pod UID and resource
version continue to guard dispatch. Envoy observation is now separate
from Java preparation.

The focused fixtures simulate late changes to survivor Pods, slices,
the index Service, Envoy evidence, and the claim. Each change blocks the
eviction call. Current evidence submits the exact victim preconditions.
The Go suite, vet, plan checks, 25 shuffled writer regressions, and the
Kubernetes 1.32 controller and Pod lifecycle suites passed. This run did
not repeat the live Envoy and Java handoff drill.

Weighted rollout now rechecks both HTTPRoute versions and pinned backend
Services after the final evidence reads. Stable Service handoff and
commit also recheck the stable Services and any required EndpointSlices
in that window. Route patch, ambiguous write read-back, and finalization
share the route version check.

The API-server regression changes the ingest route's metadata during
final search evidence revalidation. The search route patch and stable
Service handoff both require a fresh observation after that edit.
The Go suite, vet, plan checks, and Kubernetes 1.32 controller suite
passed. The regression changed API-server objects and did not send live
Gateway traffic.

Weighted evidence now carries a recheck for the signed proxy ConfigMap,
the signed platform ConfigMaps, and the retained Pod cohorts. The
operator runs it with the credential and operation claim checks before
a route patch, convergence record, stable Service handoff, or fleet
commit. An already applied route change can still be recorded by
observing its target weights.

The API-server fixtures cover a changed proxy report version and a
failed final evidence check on the route, convergence, and Service paths.
The Go suite, vet, plan checks, and Kubernetes 1.32 controller suite
passed. Live Envoy traffic was outside this run.

The selector planner is separate from its status writes. The operator
checks the plan again after the final Service read, so a lease or route
observation expiring during that read cannot authorize the write.
Weighted promotion and rollback recheck freshness before HTTPRoute
patches, stable Service handoff, and serving-fleet commit.

The regression simulates a Service read advancing the clock to the
evidence deadline and confirms that the controller sends no patch. The
HTTPRoute test verifies a refused patch leaves the object version intact.
The Go suite, vet, plan checks, 30 shuffled focused runs, and the
Kubernetes 1.32 controller suite passed. Live traffic was outside this run.

Selector rollout now rechecks the ConfigMap UID and resource version of
each signed platform report before the planner can use its candidate or
route evidence. Weighted rollout makes the same check when collecting
fleet qualification. Selector promotion rechecks the source Pod cohort
before the final claim and evidence age checks.

The API-server race fixture updates a signed report's ConfigMap after
qualification. The step must hold and leave the selector on its source
fleet, then allow a fresh observation to qualify a later attempt.
The Go suite, vet, plan checks, 25 shuffled focused runs, and the
Kubernetes 1.32 controller suite passed. Live Envoy traffic was outside
this run.

Service endpoint verification is separate from availability status
sampling. The snapshot rejects shared backend addresses, host-network
or still-gated Pods, and conflicting Ready conditions. IPv4 and IPv6
entries for one Pod count as one replica. The preview fixture uses
distinct IPs for distinct Pods.

The Go suite, vet, plan checks, 25 shuffled endpoint regressions, and
the Kubernetes 1.32 controller suite passed. Duplicate address and
dual-stack coverage used synthetic endpoint fixtures. The live ingress
and load-balancer drills were outside this run.

Released provisioning Pods now have a shared identity check for active
attempts and isolated group holds. It checks the controller reference,
Pod UID, recorded role and zone, and absence of the placement gate.
Pending-timeout and outage withdrawals require their qualified resource
version. Unknown withdrawal reasons hold the attempt. Bound DNS,
plugin, and readiness observation share the phase deadline logic.

The Go suite, vet, plan checks, 30 shuffled provisioning regressions, and
the Kubernetes 1.32 controller and Pod lifecycle suites passed. The
simulated race changes the Pod zone and resource version between reads
and confirms that the controller sends no delete. The API-server suite
covers binding, startup holds, and outage withdrawal with labelled Pods.

The reusable Pod lifecycle adapter verifies identity, the full controller
reference, and the intent annotation on both new create responses and
recovered creates. It releases an existing scheduling gate only while
the Pod is unbound. The operator now uses its creation identity check
before interpreting a missing gate as a recovered release or acting on a
pending withdrawal. A changed Pod keeps the intent for investigation.

Unit regressions cover changed responses, already-bound Pods, and gate
recovery with altered UID, intent annotation, or revision. The Go suite,
vet, plan checks, 30 shuffled focused runs, and the Kubernetes 1.32
controller and Pod lifecycle suites passed. The dependency check found
no operator API or internal package imports in the reusable package.

Pod admission now checks the complete controller reference and reports
changed operator annotations by name. Placement and runtime checks are
separate. The operator rejects host networking, changed restart policy,
unexpected mounts over the Java data directory, and updates to its
disk-backed `emptyDir`. Configured Java limits survive admission, and
defaulted memory limits must exceed the fixed heap.

The Go suite, vet, plan checks, 20 shuffled admission regressions, and
the Kubernetes 1.32 controller suite passed. The focused tests exercise
compatible defaults, rejected mutations, and unrelated sidecar
injection. The mutation fixtures simulate admission changes without a
live webhook or cloud scheduler.

Temporary Pod admission now records the failback report name and UID in
the same status update as the creation intent. Removing emergency policy
stops new temporary admissions. Failback and bootstrap replacement can
use the retained report while the retirement budget permits it. The
report must match the current generation and original ConfigMap UID. The
operator still watches the report and clears its pin when cleanup ends.
Emergency reports with duplicate fields or ambiguous zone fit are
rejected before placement or retirement can use them.

An upgraded instance can backfill the pin while its configured report
matches the UID on an existing owned temporary Pod. If the policy was
already removed before that checkpoint, the evidence source must be
restored before automated cleanup can be authorized.

The Go suite, vet, 30 shuffled emergency regressions, plan checks,
and generated artifact checks passed. The Kubernetes 1.32 suite passed
for the policy change. The focused API-server regression exercised the
legacy backfill, generation and report identity checks, eviction with
a PDB, and removal of the pin after the exact victim disappeared.

Serving protection and replacement planning now accept a mixed-revision
serving cohort when the labels resolve to the same fleet template. An
existing literal serving pin still governs new Pod creation. Without a
pin, a mixed cohort of verified owned Pods adopts the canonical revision.
The ingest failback regression exercises intent recording, a PDB denial,
and a lost eviction response with a mixed cohort. A label verified for a
different template still holds failback.

The Go suite, vet, plan checks, 30 shuffled focused regressions, and the
Kubernetes 1.32 controller suite passed. The final focused API-server and
unit checks passed. The mixed-revision eviction fixture uses a fake
eviction response and did not run a provider drain.

The OCI plugin schema now checks plugin names, SHA-256 digest references,
and role values in both the instance and blue/green candidate. The Go
API, resolver, and installer share the check for valid filesystem names
and the bundled `repository-s3` name. Duplicate selections and OCI
policy approval continue to be checked in Go.

The Go suite, vet, plan checks, and Kubernetes 1.32 admission tests
passed. The upgrade test verifies that an existing mutable tag still
allows status updates and can be replaced with a digest reference.

The manager watch audit moved instance watch registration and mapping
out of the main reconciler. ConfigMap events now match capacity profiles,
configured and pinned OCI policies, and referenced plugin snapshots.
The lookup deduplicates references and requires a namespace. It uses
the existing ConfigMap watch and namespaced RBAC.

The Go suite, vet, plan checks, 40 shuffled index tests, and the
Kubernetes 1.32 manager-watch and rollout tests passed. The real
manager test confirmed that a new profile ConfigMap matches its
instance and that the unowned retired-Pod watch still matches.

Disruption budget reconciliation now plans desired floors separately
from API updates. For a retiring search zone, lowering a floor requires
fresh destination capacity evidence tied to the budget UID and resource
version seen in planning. Creates and updates re-read the instance before
the effect and stop on a changed instance or new operation claim.

The Go suite, vet, plan checks, 30 shuffled budget regressions, and
Kubernetes 1.32 provisioning and rollout tests passed. The regressions
cover a newly positive floor, a raised positive floor, and a claim
appearing after intent recording. Provider drain behavior was outside
this run.

Role Service and HTTPS edge probes now share the Pod and Service checks
used to qualify serving evidence. A Ready Pod marked for disruption or
still carrying the placement gate cannot confirm convergence. Capacity
planning uses one regular-ready count for placement and preview, while
tracking occupied slots and an existing search bootstrap separately.

The Go suite, vet, 30 shuffled focused runs, and plan checks passed. A
fixture now clears the scheduling gate before marking a bound Pod Ready.
The Kubernetes 1.32 rollout and provisioning API-server tests passed
against the corrected tree. The probe regressions used HTTP and HTTPS
fixtures. Live Envoy failover was outside this run.

The startup credential audit applied the keystore mount and access
checks to both keystore-only and Basic Auth Pods. They reject Secret
aliases, extra mounts, shadowing mounts and shared process access.
The controller rechecks the source UID immediately before creating
a gated Pod or releasing its gate. Kubernetes still mounts the Secret
by name, so name reuse after release remains a platform trust boundary
for keystore-only runtimes.

The Go suite, vet, plan checks, 30 shuffled focused runs, and the
Kubernetes 1.32 controller suite passed. A regression injected a
different keystore UID at the final read and confirmed that the
real API-server Pod retained its scheduling gate. The same test
then released the gate with the original Secret.

The primary-store audit found that kubelet could expand dollar syntax
in managed S3 arguments after the operator pinned the literal values.
Pod rendering now escapes every argument except the operator's exact
`network.host` Pod IP reference. Runtime-qualified fleets compare the
complete store argument set with the pin, including endpoint omission.
Templates whose managed values contain no dollars keep the same tokens.

The full Go suite, vet, plan checks, 30 shuffled focused runs, and the
Kubernetes 1.32 controller suite passed. A disposable Pod on the local
Kind cluster used arguments generated by the Go renderer. Kubelet
passed the literal S3 endpoint and expanded only the bind address.
The Pod was removed after verification. The test did not contact S3.

The cluster-settings audit separated claim handling from dispatch and
read-back. A possible write keeps its finalizer when the original
instance disappears, is replaced, or loses the claim. Legacy Prepared
records follow the same rule. The settings backend uses the shared
Service identity check. Read-back rejects null values, conflicting
duplicate fields, and a missing persistent projection.

The Go suite, vet, plan checks, and 30 shuffled focused runs passed.
The Kubernetes 1.32 controller suite verified that the unresolved
operation survives claim loss and recovers after the original claim
returns. A disposable Java ingest JVM verified setting, reading, and
removing the qualified key on a local filesystem store. This run did
not exercise S3 or fleet-wide effectiveness.

The account audit moved both indexed-record reads inside the Pod and
instance observation window. A JVM joining during the final record
read invalidates the evidence. The reconciler separates claim handling
from the prepared and writing phases, and shares account-name checks
with the HTTPS client. Tests cover a cohort change before dispatch and
accept a newly observed process after dispatch only when the original
cohort still verifies.

The Go suite, vet, 30 shuffled account test runs, plan checks, and the
Kubernetes 1.32 controller suite passed. The API-server regression adds
a Pod during the final record read and rejects the resulting evidence.
The real search and ingest JVMs observed password rotation and deletion
over existing HTTPS connections on a filesystem store. The first JVM
run found a duplicate TLS provider JAR in the staged fixture. The
successful rerun used a disposable classpath with one provider. The
test now reports JVM exit during startup immediately. This run did not
exercise S3 or store partitions.

The placement audit separated gate recovery from release qualification.
An expired release intent now settles before current inputs are resolved.
Withdrawal is recorded before the controller deletes a still-gated Pod
by UID and resource version. A release that raced withdrawal becomes a
tracked provisioning attempt. Ambiguous outcomes keep their rate-budget
reservation. Final qualification rechecks both the gate deadline and
the capacity profile expiry. Applying dedicated placement initializes a
missing selector map and leaves the Pod untouched on conflict.

The Go suite, vet, formatting, plan checks, and 40 shuffled targeted
runs passed. The Kubernetes 1.32 controller suite passed with the local
Windows process cleanup overlay. Its regression withdrew an expired
replacement and cleared the warm-up slot while the pinned TLS snapshot
was unavailable. Provider autoscaling was outside this run.

HTTP and peer TLS qualification now checks continuous certificate-chain
coverage through the required window. The validator follows successive
valid paths across expiry boundaries and rejects a gap even if both
window endpoints qualify. Snapshot, startup, and rotation checks use
the same validator. New peer snapshot intents record the PEM digest for
the full chain, as HTTP snapshot intents already did. Existing UID-pinned
peer snapshots still recover with their original public pins. An older
provisional intent recovers from its exact recorded source versions.

The regenerated instance CRD publishes the optional peer chain digest.
The Go suite, vet, formatting, 30 shuffled TLS test runs, and plan checks
passed. The TLS snapshot and controller suites passed against Kubernetes
1.32 envtest with the local Windows process cleanup overlay. Tests cover
overlapping and disjoint valid paths, different PEM chains with the same
leaf, and recovery with and without the new digest. Live Java and Gateway
TLS rotation were outside this run.

The preview Service audit separated candidate recording, observation,
and retirement from Service creation and deletion. The child phases
preserve the optimistic status patches and existing operation guards.
Service creation, endpoint qualification, Gateway backends, and
preview cleanup now share the same selector and identity checks where
they pin a Service. Role Service shape validation rejects external IPs
and NodePorts. A Service with either setting is held for correction.

The Go suite, vet, formatting, and plan checks passed. The controller
suite passed against Kubernetes 1.32 envtest with the Windows process
cleanup overlay. An API-server regression verified that reconciliation
rejects an external IP without rewriting the stable Service. The live
Envoy deployment was outside this run.

The Pod lifecycle audit made search scale-down planning linear in the
number of Pods and zone policies. Victim ordering remains zone, name, and
UID. Revision replacement now rechecks the exact claimed Pod against
current capacity and other retirement claims. Kubernetes gate release,
withdrawal, and eviction share one live identity and version guard.
Both delete paths apply UID and resource-version preconditions.

The worker consumer test now exercises planning before and after capacity
converges. The Verify workflow checks transitive lifecycle dependencies for
operator-specific imports. `go list -mod=readonly -deps ./pkg/podlifecycle/...`,
the Go suite, vet, 40 shuffled lifecycle test runs, and the plan checks
passed. Both the controller and lifecycle adapter suites passed with
Kubernetes 1.32 envtest and the local Windows process cleanup overlay.
This pass used API-server fixtures for Pod binding and eviction races.

The OCI installer now reads each descriptor once with the same literal
property handling as the Java loader. It rejects unknown properties
before publication. Its installation deadline covers ZIP extraction
and the final publication check. Cancellation removes partial stages,
and extraction errors retain their underlying cause.

The Go suite, vet, and 25 shuffled plugin regression runs passed.
The real Java shell loaded the fixture from a local registry protocol
fixture and served its REST endpoint. That test also reused the
completed set after stopping the registry fixture.

The rollout evidence audit found two cases where stale observations
could survive a check. The observer now snapshots maps and slices
before probing, and the publisher preserves a newer valid signed
report across HTTPRoute versions. Weighted reports must postdate or
match their inventory snapshot. The observer also normalizes loopback
addresses before checking tunnel uniqueness.

The observer reuses connections across its eight workers. The
wrong-password regression uses a small probe set while the successful
weighted cases still exercise the production sample count. The rollout
package passed 50 shuffled repetitions. The Go suite, vet, targeted
Kubernetes API-server rollout and retirement regressions, and plan
checks passed. This pass did not rerun the live Envoy drill.

The runtime audit found that kubelet could substitute environment
references in custom JVM arguments and rollout qualification could
ignore an extra launcher argument. The Pod renderer escapes custom
dollars while retaining the managed Pod IP reference. Fleet
qualification checks the exact custom argument order, balanced heap
tokens, and the set of supported managed keys. Admission still verifies
the complete managed values. Tests cover unexpected JVM options,
duplicate settings, and missing managed keys.

API validation and launcher rendering now use one heap parser. It
accepts quantities such as 1.5Gi that resolve to exact bytes and rejects
a fractional byte. A disposable live Kind Pod recorded literal custom
dollars and the expanded managed address, then was removed.
The Go suite, vet, plan checks, and controller API-server suite
passed. The workload scheduler scenario requires a separate scheduler
binary and was skipped in the native run.

The instance API audit added admission checks for S3 credential modes,
nonempty trust keys, supported pools, warm-readiness modes, rollout
strategy, and availability budgets. Go validation now groups checks by
configuration area. The Kubernetes upgrade test verified that a stored
invalid credential combination can still update status, accept unrelated
spec changes, and be repaired. A change that leaves the combination
invalid is rejected.

Instance startup, gate release, emergency failback, and search
scale-down now share the primary-store identity conversion and check.
S3 endpoint tests used descriptive case names and cover both populated
and empty URL fragments.

The controller and Pod lifecycle API-server suites passed on the
Kubernetes 1.32 fixtures. The expanded upgrade test passed separately
after the primary-store refactor. The generation and plan checks passed.

The first quality audit aligned operation admission with the supported
Go action shapes. New invalid requests fail at creation. The operation
spec stays immutable. A Kubernetes 1.32 upgrade fixture confirmed that
an existing operation with a missing payload, an empty patch, or an
unknown action can still update status and clear its finalizer.

The shared operation reader and claim helpers now have operation-wide
names. Claim release checks the operation UID as well as the Kubernetes
resource version. The capacity planner validates the snapshot and counts
active Pods in one pass before choosing a group. Unit tests cover an
invalid later policy and a duplicate Pod identity across groups.

The controller and Pod lifecycle API-server suites passed using native
Windows envtest with a local process cleanup overlay. Verify now checks
generated CRDs and runs both API-server suites. The Linux-only release
API-server tests remain covered by CI. The broader quality audit is
still in progress.

The structure refactor splits gated Pod recovery and execution, capacity
observation and planning, template rendering, and admission validation.
The instance recovers stale intents before resolving new credentials.
Rollout, account operations, and writer retirement use dedicated Pod
HTTP and fleet observation components. Writer retirement derives an
internal action from its persisted journal and retains fresh identity
checks before effects. Neither CRD changed. API deep copies now come
from controller-gen v0.17.3, and Verify checks for drift.

The Go suite, vet, formatting, plan checks, and generated-file check
passed. The focused Kubernetes API-server suite passed for provisioning
recovery, rollout conflicts, credential changes, and writer retirement.
The native Windows envtest run used a local process cleanup overlay.
The earlier live Envoy drill used the pre-refactor manager.

The live staged publisher drill passed after the authorized Docker
Desktop restart. The new Job published signed evidence from the running
Envoy replica, the exact writer Pod was evicted, and the successor
acknowledged a term 2 write on the same shard through the authenticated
index route. Authenticated search returned the seed and the new write.
The operation succeeded and released its claim. The Kind kubelet limit
was raised from 110 to 128 to fit the fresh fixture with existing Pods
in place. Its original config was saved inside the control-plane node.
[Live writer retirement](verification.md#live-staged-publisher-and-writer-retirement)

The fresh staged-Job Envoy drill is blocked by the local runtime.
The writer Kind API timed out, and the alternate Kind API reset
its connection. The running Ubuntu distro shares the WSL VM,
which had 36 to 37 MiB of available memory out of about 32 GiB.
Memory pressure stalled all runnable work for roughly 78 percent
of the sampled 10-second window. Ubuntu had no separate
container runtime. The local fixtures were left running.
[Publisher image verification](verification.md#local-staged-publisher-image)

A local Linux AMD64 publisher image is staged in
`tools/bin/writer-evidence-release/publisher-staged-drill.oci.tar`.
The archive manifest, config, layer, executable and root filesystem
checksums passed. The matching image reference and sample Job are in
the same directory. The Dockerfile build returned HTTP 500 from Docker
Desktop, so the local fixture image was assembled from the staged
static publisher and a local CA bundle. Docker Desktop also returned
HTTP 500 during the import attempt. The image remains staged locally.
[Publisher image verification](verification.md#local-staged-publisher-image)

The live writer drill now has a reusable verifier. It ties the
staged Job and Ed25519 report to the original Pod UID, checks
eviction before submitting a write, and compares both route
responses with the terminal retirement journal. Replay with the
saved Kind responses passed. A localhost TLS fixture verified
SNI, hostname checking and Basic Auth. The staged Job still
needs a live Envoy run. The original Kind API returned a failed
etcd readiness check, and the alternate API closed its connection.
[Live writer drill](java-runtime-build.md#verify-live-writer-retirement-through-envoy)

The publisher Job now starts before the observer captures the
30-second report. It reads a single named platform ConfigMap through
the API. The API-server test admitted the Job, verified the signed
output under the publisher identity and denied an unrelated input
read. The Go suite passed. The Kind API was unreachable for a fresh
Envoy run with this Job.
[Publisher verification](verification.md#namespaced-writer-evidence-publisher-job)

Signed evidence publishers now reread the ConfigMap after a concurrent
create or update. The writer regression verifies that both races retain
the newest signed report. The Kind API was unreachable for a fresh
end-to-end run.
[Publisher verification](verification.md#namespaced-writer-evidence-publisher-job)

Automatic writer retirement now checks for unsupported shared
bucket heads while the exact writer is open. A shared bucket or
incomplete scan leaves the reservation unarmed. The HTTPS adapter
and real API-server journal regressions passed. The post-closure
inventory still gates eviction. A live run with this change remains
open because the Kind API timed out.
[Open writer head preflight](verification.md#open-writer-head-preflight)

The mounted-file writer publisher now has a real API-server test. The
test uses the example publisher Role, persisted withdrawal state and a
synthetic Envoy report. It verifies the signed update and confirms that
an older report leaves the ConfigMap unchanged. The native assertions
passed and the Linux test is included in Verify. The publisher Job
still needs a live kubelet and Envoy run.
[Publisher verification](verification.md#namespaced-writer-evidence-publisher-job)

The Envoy observer and evidence publisher now have separate image
targets. A one-shot writer publisher Job accepts an observer report
from a mounted file and updates only its pre-created evidence
ConfigMap. Its signing Secret stays in the platform namespace.
The Linux builds, Go suite and plan checks passed. A temporary API
server admitted the Job and verified the exact scoped permissions.
Building the container images and running the Job through Envoy
remain open.
[Writer evidence publisher Job](verification.md#namespaced-writer-evidence-publisher-job)

The secure development package can include both OCI image SPDX
documents. The packager checks the pinned manifests, config and layer
blobs and identifies the image Syft scanned. The signed inventory
covers both SPDX checksums. Syft generated both fixture
attachments. Package verification rejected a changed attachment.
The release image scans and publication still require access to the
actual release artifacts.
[Digest-pinned SPDX attachments](verification.md#digest-pinned-spdx-attachments)

A new envtest release check applies the signed install, manager image
upgrade and rollback on an isolated Kubernetes API server. The native
Windows run passed the admission, RBAC and persistence assertions.
The Windows shutdown signal was unsupported, so the fixture processes
were stopped separately. The Linux binary compiled and local WSL
startup timed out on etcd. Verify now runs the test with a checksum-
pinned Linux bundle. Plan and workflow syntax checks passed. A live
Java upgrade and authenticated write/search still require the Kind
drill.
[Packaged API-server transition](verification.md#packaged-api-server-transition)

The manager transition observer now compares each role's live
EndpointSlices against the current eligible Java Pod UIDs and IPs.
The regression checks reject stale targets, misplaced IPs and
missing endpoints even with a fresh availability condition. The
Verify workflow runs the signed package and observer tests as a
separate job. The live upgrade and rollback remain open.
[Read-only transition observation](verification.md#read-only-manager-transition-observation)

Endpoint observation now exports a duration histogram with a bounded
outcome label. It distinguishes verified capacity, a verified floor
shortfall, unverifiable endpoints and a status patch failure.
Controller regression tests verify the counts and label set.
The local release manager was still crash-looping, so a live scrape
and the packaged upgrade and rollback remain open.
[Endpoint telemetry](verification.md#endpoint-observation-telemetry)

The read-only transition observer checks the signed package against
a live manager Pod and current leader Lease. It records the Java Pod
and stable Service identities for comparison across upgrade and
rollback. The Kind check correctly rejected the unhealthy manager
before producing a baseline. The observer regressions and plan
checks passed. The live rollout still waits on fixture recovery.
[Read-only transition evidence](verification.md#read-only-manager-transition-observation)

The signed package verifier now compares both releases before a
manager image-only transition. It verifies both signatures and
manifest hashes, holds CRDs and Java fixed, and rejects other manager
manifest changes. The exact local release pair passed verification
in both directions. Five packaging tests and the plan checks passed.
[Transition preflight](verification.md#manager-image-only-package-preflight)

The development day-two runbook covers manager and endpoint health,
provisioning, primary store, warm-up, routing, policy and trust. In the
Kind release namespace, the optional diagnostic reader identified the
expired Lease and zero Ready endpoints without Node, Secret, log,
mutation or cross-namespace access. The runbook link check, plan check
and plan-verifier tests passed. The release upgrade and rollback still
depend on a healthy test cluster.
[Namespace-scoped incident diagnosis](verification.md#namespace-scoped-incident-diagnosis)

The development packager now supports a detached release inventory
signature. The verifier checks the trusted signature before hashing
both manifests. OpenSSL 3.5.8 verified a signed copy of the exact local
candidate package. Tests detected a changed manifest and an attempt to
rewrite the signed inventory after the change. The packager tests and
plan checks passed.
[Signed install inventory](verification.md#signed-install-inventory)

The secure development release command packages both CRDs, the
namespaced manager and a checksum inventory with exact Java and
manager digests. The packaging tests and plan checks passed. Kind
accepted the package in a fresh namespace and started the manager.
Java readiness and the remaining release drill hit API and probe
timeouts under sustained host memory pressure. The two packages
share the same CRDs and Java digest, and the isolated fixture includes
a manager upgrade and rollback check for when the cluster recovers.
[Release verification](verification.md#versioned-secure-development-package)

The development availability observer retries a racing endpoint read
once within its five-second window and checks an unverified result
again after 10 seconds. The controller regression covers a single
change, repeated changes and recovery. The secure-install Kind fixture
recovered from an unowned test EndpointSlice after 10 seconds with the
same Ready Pod UIDs, then confirmed authenticated index and search.
The Go suite, `TestAPIServerValidatesInstanceAndStatus` under envtest,
and the plan verifier and its tests passed.
[Endpoint availability evidence](verification.md#instance-endpoint-availability)

The live Kind retirement recheck independently verified the published
Envoy evidence signature and confirmed the exact victim UID was gone
and the claim was clear. The original successor wrote at term 2 during
retirement. On recheck, the current owner wrote at term 3 through the
TLS and Basic Auth index route, and Envoy search returned both writes.
The rollout tests and plan check passed.
[Live retirement recheck](verification.md#live-signature-and-route-recheck)

The secure development install now has a Kustomize overlay and an
instance example for BYO HTTP TLS and startup Basic Auth. A separate
Kubernetes 1.32 Kind cluster installed the overlay by digest and
served an authenticated write and search result through the stable
role Services. The namespaced manager could list EndpointSlices and
could not list Nodes. Gateway routing, peer mTLS and cloud
qualification retain their separate test procedures.
[Secure install evidence](verification.md#secure-development-install)

A Java filesystem-store integration test now generates a real partial
shutdown receipt by failing one shard's publish. The successor recovers
the unpublished write and settles predecessor WAL after taking both
shards at newer terms. The complete, missing and partial receipt
API-server journal regressions and Go suite passed. The manager was
imported by digest, and Kind remained at its 110-Pod limit.
[Partial receipt verification](verification.md#partial-writer-receipt-recovery)

The controller now completes independent-shard retirement with a
valid partial receipt. It preserves the receipt and requires durable
predecessor WAL settlement plus a newer-term write for each omitted
head. The API-server journal test rejected an omitted head's newer
write without settlement and released the claim after proof was
recorded. The complete and missing receipt cases, Go suite and plan
check passed. The mixed path has API-server and adapter coverage.
[Partial receipt verification](verification.md#partial-writer-receipt-recovery)

Writer retirement recovered a missing receipt in Kind after the exact
admission-closed JVM exited on SIGKILL. The claim stayed held until an
authenticated Envoy index write reached the survivor at a newer term
with settled predecessor WAL. With the final runtime, a second live
drill preserved an acknowledged WAL-only document whose S3 commit
manifest stayed unchanged through the crash. Search returned that
document, the published baseline and the successor write. The Java
settlement CAS matches the index UUID and JVM identity and retries
transient store errors. Java, Go, API-server and plan checks passed.
[WAL recovery verification](verification.md#writer-recovery-without-a-shutdown-receipt)

Deleting a retirement operation after an ambiguous eviction now
continues exact Pod readback without dispatching eviction again.
Confirmed absence enables receipt and successor verification during
deletion. The Go suite, operation journal regression, Kubernetes
eviction regression and plan check passed.
[Ambiguous eviction verification](verification.md#deletion-during-ambiguous-eviction)

Automatic writer retirement now completes after newer-term write
evidence for every inventoried independent shard. The live Kind drill
held the exact claim after eviction and before the successor write.
An authenticated write through the Envoy index route advanced the
shard to term 2, and the search route returned the seed and successor
documents. The manager persisted the exact successor and cleared the
claim. The final manager preserved completion after restart. The Go
suite, API-server journal test and plan check passed. The development
controller holds shared bucket inventories before eviction.
[End-to-end successor verification](verification.md#automatic-writer-retirement-completion)

The pinned Java runtime now exposes read-only proof of a newer-term
foreground write from the exact active index JVM. Its HTTPS regression
held an open shard pending until a real write completed and rejected
an old term, a different process and a recreated index UUID. The WAL
takeover test, runtime bundle build, Go adapter suite and plan check
passed. The manager still needs to collect per-head proof and finish
the operation.
[Successor write evidence](verification.md#java-successor-write-evidence)

The controller journals release evidence after eviction. The existing
live Kind fixture resumed after a manager upgrade and recorded
`RELEASED` for the original shard at term 1. The scoped Java read
matched the independent HTTPS Basic Auth receipt and retained the
original operation claim. The Go suite, writer journal API-server
regression and plan check passed. Shared bucket accounting, interrupted
handoff recovery and automated successor proof remain open.
[Live receipt journal](verification.md#controller-receipt-readback-after-upgrade)

The opt-in manager automatically evicted the exact retired index Pod
in Kind. It waited for signed Envoy evidence, journaled the durable shard
head, and confirmed Pod absence after its PDB-aware eviction. The
original surviving index JVM wrote the same shard at term 2 through
Envoy with verified TLS and Basic Auth. The search route returned both
documents and the Java receipt confirmed release at term 1. The Go
suite, API-server regressions and plan check passed. The manager keeps
the operation claim after eviction while automated successor
qualification remains pending.
[Automatic writer eviction](verification.md#automatic-eviction-and-successor-write)

After signed writer admission closure, the manager journals the
paginated physical head scan from an authenticated surviving index
JVM. It checks the pinned store, survivor lease and endpoint versions,
and exact victim before recording evidence. The fresh final Kind
fixture matched an independent TLS and Basic Auth read of the exact
seed shard while the victim remained running. An earlier two-head
journal survived a manager upgrade. The Go and Kubernetes API-server
journal tests passed. The manager holds the retirement claim until
later eviction and successor verification are implemented.
[Durable ownership journal](java-runtime-build.md#record-durable-ownership-before-eviction)

The prepared writer now defers S3 assignment-hint cleanup and avoids
the optional conditional-delete capability probe during lease release.
The exact shard heads are still released and the exact lease expired.
The final pinned Basic Auth image repeated the signed Envoy drill and
the manually submitted PDB-aware eviction. Java exited 1.41 seconds
after SIGTERM with status 143 and published an authenticated receipt
for shard 0 at term 1. The original survivor wrote at term 2 and search
returned both documents. The final pinned build passed 70 targeted
Java tests. The Go suite and plan check passed. Manager-driven eviction
and successor qualification remain pending.
[Graceful writer retirement](verification.md#graceful-shutdown-and-receipt-readback)

The signed writer evidence pipeline passed a live Kind drill through
Envoy Gateway 1.8.6 with verified TLS and Basic Auth. The manager
waited for the signed observation before closing admission. A manual
UID- and resourceVersion-qualified Eviction removed the writer. The
existing survivor acknowledged a write to the same shard at primary
term 2, and search returned the seed and successor writes. The
observer accepts Gateway API's default root match and an exact
draining victim with panic routing disabled. The shutdown receipt
returned HTTP 404. Automatic eviction and successor verification
remain open. The Go suite, Kubernetes API-server regressions and
plan check passed.
[Live retirement drill](verification.md#authenticated-envoy-writer-retirement)

The manager now supports opt-in development writer withdrawal and
preparation for one named instance. It binds the current Envoy route,
listener and frontend, three immutable credentials, and the exact JVM.
Before closing admission it rechecks the full signed Envoy report and
uses the final evidence deadline. Exact-state readback is independent
of the route. The API-server regression verified safe cancellation
after the deadline with a simulated open JVM, and kept the claim when
withdrawal may have happened. The Go suite and plan check passed.
Live manager dispatch, automated signed publication, eviction and
successor write verification remain pending.
[Configure the development drill](java-runtime-build.md#enable-the-development-writer-retirement-drill)

Writer withdrawal now uses a deadline enforced inside Java's reservation
lock. The operation records a 30-second dispatch window. A fresh read of
the exact JVM as open after expiry allows requalification and a new
recorded window. The pinned Basic Auth bundle passed 53 targeted Java
tests, including queued withdrawal expiry and authenticated HTTPS
readback. The Go suite, Kubernetes API-server regression, and plan check
passed. Manager enablement and recovery after deletion of an armed
operation still require the runtime adapter.
[Timed writer withdrawal](java-runtime-build.md#persist-writer-retirement-before-dispatch)

The writer retirement handler now journals an exact Pod, container and
JVM before recording withdrawal authorization. Operations release an
unarmed claim if qualification fails. Deletion fences an in-flight
claim before removing the finalizer. An authorized operation retains
its claim across ambiguous withdrawal and preparation replies and
through admission closure. The direct HTTPS withdrawal helper checks
index serving floors and rereads Kubernetes state before dispatch.
The API-server regression verified the claim fence, release and retry,
and recovery with a simulated Java backend and a new reconciler on
each pass. The Go suite and plan check passed. Manager enablement,
the signed Envoy publication flow, and Kubernetes eviction and
successor verification remain pending.
[Writer retirement journal](java-runtime-build.md#persist-writer-retirement-before-dispatch)

The Kind importer now creates digest aliases from archive content in
containerd and verifies each alias before reporting CRI success. A Pod
started by a newly imported digest with image pulls disabled. A fresh
namespace drill promoted and rolled back both Envoy routes with verified
TLS and Basic Auth. Search returned the writes acknowledged by the green
and restored index Pods. Six importer tests, three Envoy trust tests,
the Go suite, the rollout API-server regression, and the plan check
passed.
[Authenticated Envoy drill](verification.md#exact-platform-manifest-and-authenticated-routes)

A prepared Java writer can reopen a hibernated shard at the term it still
owns. It checks the exact durable owner and keeps the hibernation record
on read or open failure. The shutdown barrier blocks new wakes and waits
for admitted ones. In two-node filesystem and SeaweedFS tests, forwarded
writes continued before shutdown and the successor acknowledged a write at
a newer term and read all four documents. The pinned Basic Auth build
passed 23 Java regressions and two S3 tests. The Go suite and plan check
passed. Automatic dispatch and successor verification in Kubernetes are
still pending.
[Hibernated writer handoff](java-runtime-build.md#hand-off-hibernated-writers)

The guarded writer preparation executor checks the persisted reservation,
exact victim, Java peer cohort, stable index Service and signed Envoy
evidence before calling timed Java prepare. It rereads the frontend proxy
cohort and checks that the active index route and EDS assignment leave only
qualified survivors. Java checks the deadline inside the admission lock.
The clean Basic Auth bundle passed 24 Java tests. The Go suite and the
Kubernetes API-server rollout regression passed. The Envoy collector
passed a read-only one-replica Kind drill with an already off-route ingest
Pod. The manager still needs durable retirement dispatch, a platform
publisher for production retirement and automated successor verification.
[Guarded preparation](java-runtime-build.md#guard-writer-preparation-with-envoy-evidence)

The Java writer withdrawal view reports what each peer's placement cache
sees over authenticated Pod HTTPS. Completing an older refresh after a
newer one cannot restore eligibility for an already-observed withdrawal.
The Go helper checks the owned search and ingest cohort, including preview
and retained Pods, and rejects changed identities or expired observations.
The clean pinned bundle passed 25 Java tests. The Go suite and the cohort
API-server regression passed.
[Peer withdrawal evidence](java-runtime-build.md#observe-writer-withdrawal-on-peers)

The Java runtime now publishes a writer withdrawal marker through its
lease timer. The withdrawal and preparation endpoints verify the exact
incarnation's live lease before reporting withdrawal or closing admission.
Peers that have observed the marker route cold writes to surviving
writers, including bulk and upsert. Already-open writers remain reachable
by their recorded owner. Hibernated writers still need a handoff path.
The broader Java run passed 37 tests. A clean `--build --basic-auth`
bundle recorded the final overlay digest and passed four targeted tests.
The Go package suite and `python tools/verify_plan.py` passed.
[Writer retirement routing](java-runtime-build.md#prepare-writer-retirement)

Writer retirement now has a withdrawal phase that changes client readiness
without closing writer admission or dropping peer discovery. Java refuses
prepare until the exact operation reserves withdrawal. The Kubernetes
observer checks the stable index Service, the victim UID and IP, and the
survivor floors. A Kind drill verified the readiness split.
[Writer traffic withdrawal](java-runtime-build.md#prepare-writer-retirement)

Java now exposes HTTPS-only writer retirement preparation for a configured
account. The exact target JVM reserves the first operation, blocks new
writer opens, and reports closure after previously admitted opens finish.
The Go client verifies the response's JVM and operation identity. Seven
Java tests, the Go package suite, and the plan check passed. The
controller still needs to persist and dispatch the operation and verify
successor writer readiness before completing retirement.
[Prepare writer retirement](java-runtime-build.md#prepare-writer-retirement)

The authenticated writer inventory reads physical shard and bucket
registers directly from the primary store. It finds heads absent from
JVM-local state and reports legacy node-ID-only heads as ambiguous.
The Go client walks all pages and discards partial results on error.
The pinned Basic Auth build passed six local Java tests and the
SeaweedFS pagination test. The Go package suite and plan check passed.
Retirement uses this scan after admission closes and still needs
successor replay and write evidence.
[Durable writer head inventory](java-runtime-build.md#durable-writer-head-inventory)

The pinned Java runtime now serves exact-process writer handoff receipts
to a configured account. The Go client requires verified HTTPS and
distinguishes absence from unreadable or invalid evidence. Managed startup
requires Basic Auth and HTTP TLS when configuring a receipt reader.
The clean Basic Auth bundle, six Java regressions, Go package tests and
plan check passed. Controller integration, complete ownership inventory,
and live index retirement remain open.
[Readback contract and tests](java-runtime-build.md#authenticated-writer-receipt-readback)

The pinned Java runtime can now save an opt-in, create-only shutdown
receipt under the exact Java node and ephemeral IDs. Node stats expose
both IDs. The filesystem tests read the receipt after the writer exits,
and SeaweedFS verified immutable create and retry through the S3 adapter.
The fresh Basic Auth bundle and all twelve targeted tests passed.
At that stage the receipt was only available to Java's internal reader.
[Java receipt evidence](java-runtime-build.md#writer-handoff-results)

The Kind importer now accepts a tag with an exact platform manifest
digest for local OCI index builds. A Pod started from that digest with
image pulling disabled. On a fresh Kind cluster, Envoy verified TLS and
Basic Auth on both search and index through `initial → green → initial`.
Both transitions verified index writes and search reads. Rollback
preserved the green write and both Service identities. The Go, API-server,
importer and backend-trust checks passed.
[Kind and Envoy evidence](verification.md#exact-platform-manifest-and-authenticated-routes)

Java shutdown now reports per-shard writer handoff results. A confirmed
release records whether the exact applied head still references WAL.
Closing a bucket member while a sibling keeps the head has a separate
result. A post-CAS cleanup failure preserves the durable release evidence.
[Writer handoff results](java-runtime-build.md#writer-handoff-results)

The pinned Java shutdown path now closes writer admission before
handoff and waits for admitted acquisitions and reopens. The regression
held an open in flight and checked that a second request could neither
claim a new head nor reopen a durable writer owned by the same JVM.
A fresh offline build included Basic Auth and passed the four Java
regression tests. This gate covers individual writer operations. Live retirement,
per-shard receipts, and coordination of an entire split remain open.
[Java writer admission](java-runtime-build.md#writer-admission-at-shutdown)

A Kind writer drill observed a durable transfer of the same test shard
from term 1 to term 2 after a UID-qualified eviction. Search recovered
the seed and all 98 acknowledged load documents. The 100-attempt run
recorded one HTTP 421 while the shard was unowned and one request
without an HTTP response. The result qualifies a single controlled
handoff and identifies the need for complete Java ownership and
retirement evidence before automatic index scale-down.
[Writer drill](verification.md#index-writer-recovery-after-pdb-governed-eviction)

Search capacity can move out of a zone whose entry remains at zero.
During a two-node Kind move, the source PDB held its positive floor and
denied a dry-run eviction while the destination was short. The operator
retired the source UID after strict destination warm-up. Both search
and index passed data checks, and the Go and real API-server suites passed.
[Zonal move verification](verification.md#move-search-capacity-between-zones)

Search scale-down can now retire the recorded cold-bootstrap UID when
strict-warm endpoints alone cover the reduced zonal and regional targets.
Kind kept the bootstrap at the original reserve target and evicted it
after the reserve was removed. The strict JVM, both index Pods, and both
Services retained their identities. The Go and real API-server suites
passed. [Bootstrap retirement verification](verification.md#retire-the-original-search-bootstrap)

The development controller now uses the reusable scale-down planner for
strict-warm search Pods in existing zones with positive targets. It
records one UID, rechecks live Java and Kubernetes evidence, and uses
the namespaced Eviction API. A confirmed denial releases the claim with
a persisted retry time. Pending or ambiguous evictions keep their UID
reservation. A Kind drill verified the zero-budget and disruption holds,
admission denial and retry, and an index write and search read on either
side of retirement. The Go and real API-server suites passed. Normal
index retirement and removal of a zone still need controller integration.
[Scale-down verification](verification.md#strict-search-scale-down)

The Kind importer now accepts a local image's exact manifest digest and
reports when a single-platform export drops the requested index digest.
A Pod started from the imported manifest with pulling disabled. A fresh
Envoy drill verified search and index over TLS and Basic Auth on
`initial → green → initial`. Both stable Service UIDs and ClusterIPs
survived, and restored search found writes from all three phases. Both
operations released their claims. The Go and rollout API-server checks
passed. [Rerun evidence](verification.md#rerun-with-direct-digest-import)

A Kind drill upgraded between two distinct manager binaries using an
in-place additive `v1alpha1` CRD update. The packaged manager added a
search reserve and persisted both observed fleet IDs. After rollback
with the newer CRD in place, the earlier manager added an index reserve.
All pre-existing Java Pods and both stable Services kept their
identities. Search returned writes from all three phases. The test
projected the one known schema change from the current CRD.
[Cross-build verification](verification.md#cross-build-manager-and-additive-schema-upgrade)

A repository Dockerfile now builds the non-root manager. The development
Kustomize overlay installs the Deployment and reuses namespaced RBAC.
On Kind, the example started both Java roles against an externally
created S3 bucket. A digest-pinned manager upgrade and overlay rollback
both transferred the leader lease. Both workload Pod UIDs and Service
identities were preserved, and search returned all five indexed
documents. The test used one manager binary across both image versions.
[Development manager verification](verification.md#development-manager-install)

The development operator now reports instance availability from both
role Services and their Kubernetes EndpointSlices. It scopes the
sample to the serving fleet IDs and checks its age. Kind verified
healthy floors, an index selector conflict, a disruption-marked Ready
index endpoint, and recovery with the same Pods and Services. A
mismatched sampled fleet forced a fresh observation at the same
generation. The Go and real API-server suites passed.
[Instance availability verification](verification.md#instance-endpoint-availability)

Zone status now reports a Ready capacity shortfall for each requested
search and index reserve. It counts regular Pods without a disruption
signal toward replicas before counting reserve capacity. In Kind,
marking a Ready Pod in either role showed a one-Pod reserve shortfall
for that role. Clearing the synthetic signal cleared the shortfall
while all four Java Pods kept their UIDs. The Go and real API-server
suites passed. [Reserve status verification](verification.md#ready-capacity-for-the-warm-reserve)

The historical search qualification now records local shard readers and
block cache activity when Java's readiness and stats identify the same
JVM. The Kind drill matched the recorded Pod and JVM identities and
observed completed warm-up with explicit zero counters on an empty S3
fixture. The warm reserve stayed `Unknown`. The Go suite and real
API-server rollout regression passed.
[Search activity verification](verification.md#local-java-search-activity)

Emergency capacity now shares its AZ limit across search and index.
Availability policy edits preserve the Pod revision after the operator
checkpoints compatible legacy labels. In three-zone Kind, scale-out
held with no fit and again at the shared cap. With fit and a higher cap,
both temporary roles served while two workers were frozen. Both retired
after explicit outage clearance, and search returned all 180 acknowledged
load writes and the explicit outage write. The Go and API-server suites
passed. The run recorded 17 search and 15 index timeouts and confirmed
that a completed Java warm-up can coexist with zero local readers.
[Three-zone outage verification](verification.md#three-zone-search-and-index-outage)

Regular serving provisioning now discounts Ready disruption targets when
ordering regional and zonal capacity work. The two-zone Kind drill gave
search the next slot, confirmed strict JVM warm-up, and then admitted index
scale-out. A PDB-governed eviction preserved the searchable document and
the warm survivor's UID. The operator restored the second warm search Pod
with namespaced permissions. The test used a synthetic Pod signal and the
Go and API-server suites passed.
[Pod disruption verification](verification.md#namespaced-pod-disruption-signal)

The Kind importer resolves a bare platform manifest digest through a local
tag in the same repository when Docker cannot resolve the digest directly.
It checks the imported digest and CRI lookup. A fresh namespace on Kind
with Envoy Gateway 1.8.6 ran the rebuilt manager pinned by digest with
`imagePullPolicy: Never`. Both authenticated HTTPS routes served green
after promotion and returned to `initial` after rollback. Search returned
the three documents acknowledged through index across the test. Both
stable Services retained their UIDs and ClusterIPs. The restored proxy
used the expected upstream CA for both roles. Importer tests, the Go suite
and the API-server rollout regression passed.
[Authenticated Envoy verification](verification.md#rerun-with-direct-digest-import)

The plaintext readiness follow-up reproduced the missing expiry by
exhausting the disposable SeaweedFS volume slots. The affected endpoint
remained unready until a volume server was added. The original JVM
then published a lease, and the index and search Services passed a
write/read check. The Go probe now reports a missing expiry explicitly.
A separate Ready Pod replacement reproduced the original pinned UTC
settings after a direct spec edit. [Capacity recovery verification](verification.md#plaintext-readiness-after-remote-store-capacity-exhaustion)

The development operator now accepts qualified JVM options, time-zone
properties and a search warm-up limit. Blue/green can stage common and
role-specific startup settings on the same Java image. A Kind drill
verified both Envoy routes through promotion and rollback, including
search replacements with fleet-specific settings and reads of both
indexed writes. The Go and API-server suites passed.
[Startup configuration](../design/runtime-configuration.md#stage-startup-settings-in-bluegreen) · [Live verification](verification.md#bluegreen-jvm-startup-settings)

Blue/green can stage a different OCI plugin set on the qualified Java
image after the administrator approves coexistence and rollback for the
exact role sets. The controller witnesses the source Pods before pinning
their inputs. A Kind drill promoted v2 and rolled back to v1 on both
authenticated Envoy routes. Each fleet replaced a search Pod using its
own plugin version, and search returned writes from both fleets.
Replacing an approval policy preserved the install policy used by a
new Pod in the restored fleet.
[Plugin rollout setup](../design/oci-plugins.md#stage-a-bluegreen-plugin-change) · [Live verification](verification.md#bluegreen-plugin-version-rollout)

Pods of one instance now reuse immutable OCI request and policy ConfigMaps
when their inputs match. Existing per-Pod intents remain valid on upgrade.
The API-server test rejected a recreated snapshot after UID pinning. The
live Kind drill verified that scale-out, registry retry and a replacement
shared the original pair and left exactly two owned plugin ConfigMaps.
[Shared snapshot verification](verification.md#shared-plugin-configmaps)

The OCI installer passed a live kind drill with a private TLS and Basic Auth
Distribution registry. Search installed and served its approved plugin, while
ingest had no installer. Losing registry endpoints isolated the new search
Pod in a role/AZ provisioning hold. The same Pod recovered and served the
plugin after endpoints returned. A later eviction triggered a fresh install.
The kubelet drill used a synthetic plugin and a locally built installer
fixture image. The focused Go and API-server tests and plan verifier passed.
[Live OCI verification](verification.md#live-oci-installation-and-registry-outage)

A released preview can now yield to serving recovery while the preview
Pod is unbound. The controller persists its withdrawal decision and
rechecks serving capacity before deleting the exact Pod version. A
scheduler binding or restored serving capacity cancels the withdrawal.
Released demand keeps its rate-limit charge. [Preview recovery verification](verification.md#serving-recovery-takes-priority-over-a-pending-preview)

Preview provisioning now yields to serving recovery when a disruption
signal or readiness loss leaves serving capacity below target. The
controller retires a pending creation and conditionally withdraws still-gated
preview Pods. A confirmed gated deletion returns the release reservation.
A released preview stays tracked by UID and retains its charge. The
API-server tests cover the race, later cancellation and the cleanup history
limit. [Preview recovery verification](verification.md#serving-recovery-takes-priority-over-a-pending-preview)

The namespaced controller reports disruption-marked Pods by role and zone
even while Kubernetes still reports them Ready. Promotion and failback
revalidate Pod versions before making a new change. The API-server tests
inject a disruption during each action and verify a hold, then allow the
normal path after the condition clears. The signal alone leaves readiness
and replica counts to the existing lifecycle.
[Pod disruption verification](verification.md#namespaced-pod-disruption-signal)

Weighted promotion and rollback passed with TLS and Basic Auth on two
Envoy Gateway replicas in kind. The manager restarted after search
reached 1% and resumed before index moved. Both operations released the
claim, and both stable Service UIDs and ClusterIPs were preserved.
Restored search returned the write acknowledged by green index. All 1,999
continuous authenticated reads returned HTTP 200. This run covered one AZ
and two fleets. [Live weighted verification](verification.md#live-weighted-promotion-and-rollback)

Authenticated weighted rollout now binds each signed proxy report to the
approved HTTPS role URL, HTTPRoute version and ordinary account Secret.
Every serving proxy must produce successful authenticated samples from each
retained fleet before a weight change. The controller rechecks the Secret
before writing weights or finalizing. The Go suite and API-server tests
passed, including a Secret replacement between evidence collection and the
weight write.
[Weighted evidence verification](verification.md#authenticated-weighted-rollout-evidence)

The operator now records the serving revision per role and fleet. A
kind drill stopped the controller, deleted all search Pods, and restarted
the controller. Both replacements kept the pre-upgrade fleet label and
read the document in the external SeaweedFS store. Ingest kept its UID.
The API-server tests cover revision pins through rollout commits and
verify that a pending first creation leaves the serving revision unset.
[Role recovery verification](verification.md#serving-revision-after-total-pod-loss)

Per-zone replica, warm spare and minimum serving counts leave the Pod
template revision stable. Live kind verification upgraded an existing
HTTPS and Basic Auth instance, recorded its old label, scaled search
from one to two without replacing the original Pods, and staged both
preview roles. The API-server rollout regression passed. An older fleet
already mixed across unrecoverable revisions still holds rollout
qualification. The upgrade must record its existing alias before a
capacity change makes the old hash unreproducible.
[Scale and revision verification](verification.md#scale-changes-and-pod-revision-continuity)

The authenticated blue/green drill passed through Envoy Gateway 1.8.6
with a fresh operator build. `tools/import_kind_images.py` loaded the
Java, operator and fixture images directly into kind's containerd. Both
stable Services stayed on `initial` with an invalid probe credential,
moved to `green` on promotion and returned to `initial` on rollback.
Both routes verified client and backend TLS and rejected invalid Basic
Auth. The index route accepted writes on both fleets, and search returned
both documents after rollback. Both Service UIDs stayed the same.
Rollback held until the test evicted the degraded retained search Pod.
The remaining strict Pod met the configured floor, and search recovered
to two Ready Pods after the switch. The Go suite and the API-server
rollout regression passed.
[Authenticated Envoy verification](verification.md#authenticated-envoy-bluegreen-rollout)

The development `ApplySecurityConfiguration` handler now creates, rotates
and removes one application account at a time. A kind run against an
externally created SeaweedFS bucket verified the three operations on the
same ingest and two search JVMs, and rejected a stale revision. The
operator pins the Secret and JVM identities, reserves one dispatch, and
persists the revision receipt before releasing the instance claim. Fault
tests covered a lost write response, a lost dispatch, operation deletion,
Secret recreation and a missing JVM. The development contract requires
exclusive account writes per backing store. Fleet fencing, recovery for
an unresolved write, and authorization policy reload remain open.
[Account operation verification](verification.md#operator-account-changes)

Managed development Pods now start with `ServerlessBasicAuth` over HTTPS.
The controller pins the startup Secret and the runtime verifies the
mounted files before Java starts. The kubelet and the controller require
anonymous health requests to return HTTP 401. A kind run with an externally
created SeaweedFS bucket brought up ingest and search, wrote a document,
and admitted a strict search replica after authenticated warm-up. Deleting
and recreating the same Secret name blocked further scale-up. A non-root
image test rejected a different valid keystore and password before Java
ran. Bootstrap credential rotation and authorization policy reload still
need implementation and qualification.
[Managed Basic Auth verification](verification.md#managed-basic-auth-startup)

Strict search warm-up now works with managed HTTP TLS. The controller
uses the pinned CA and Pod DNS name for direct probes. Strict HTTPS
readiness and controller qualification require the readiness and stats
responses to identify the same JVM. A live kind scale-out tested
completion, slot release and a search against an externally supplied
SeaweedFS S3 store. The final probe binary also passed a kubelet exec
check against the strict JVM. The failback gate now checks Java lease
deadlines immediately before eviction.
[Strict HTTPS warm-up verification](verification.md#strict-search-warm-up-over-https)

Managed development HTTP TLS now pins immutable certificate and CA
Secrets before Pod creation. The Java provider checks the certificate
bytes and the readiness probe checks the CA and expected Pod DNS name.
Two real kind JVMs passed verified HTTPS, wrong-identity rejection and
plaintext rejection with an external SeaweedFS bucket. A same-name CA
Secret with a new UID held scale-up while existing endpoints stayed
healthy. The development blue/green path can stage a same-CA HTTP leaf
for one candidate fleet and retain the original leaf for rollback.
The API-server regression verifies the certificate identities and expiry
gates. A live Envoy selector drill verified the new and restored Java
leaf fingerprints for both search and index. HTTP CA rollover now supports
an overlap bundle with signed platform trust evidence. A live selector
drill verified promotion and rollback through both authenticated routes.
The live drill also verified pruning through signed retirement of both
prior fleets. Envoy and all three serving Pods used the replacement root
alone, and both authenticated routes passed promotion and rollback with
that Envoy trust bundle.
The shared TLS validator now rejects malformed PEM anywhere in a leaf
chain or CA bundle. With Kubernetes 1.32 envtest, an otherwise qualified
HTTP candidate with malformed content before the valid CA was held
before staging. The operator Go packages and API-server fixtures passed.

```sh
go test -tags envtest -count=1 ./api/... ./cmd/... ./internal/... ./pkg/... ./tools/peer-tls-fixtures
```

[Managed HTTP TLS verification](verification.md#managed-http-tls-readiness)
[Managed HTTP leaf rotation](verification.md#managed-http-leaf-rotation)
[Managed HTTP CA rollover](verification.md#managed-http-ca-rollover)

Peer TLS replacement now uses the committed immutable snapshot through
issuer updates or source deletion. Kubernetes 1.32 envtest verified
replanning when renewal supersedes an unstaged source and creation
using the original snapshot after a failed renewal. It held the
replacement Pod's scheduling gate when the snapshot was missing or
recreated under another UID. Unit coverage includes recovery after an
unrecorded snapshot create and rejects aliased Pod volumes for the
snapshot. [Peer TLS verification](verification.md#peer-tls-snapshot-recovery)

Development blue/green now accepts a candidate peer TLS leaf and trust
pair. It pins a separate immutable snapshot, revalidates mutual trust
before preview Pods can start and before traffic changes, and keeps
each retained fleet's pins until signed retirement. The API-server
fixture passed staging and mutual trust checks for the overlap sequence,
including issuer renewal before snapshot creation. The one-AZ kind drill
verified native Java forwarding with different peer leaves, promotion
and rollback on both authenticated Envoy routes, and retired-client
rejection after the new-only cutover.
[Peer TLS staging](development-rollout.md#stage-a-peer-tls-rotation)
[Peer TLS rollout verification](verification.md#peer-tls-rollout-staging)
[Live Java peer TLS rotation](verification.md#live-java-peer-tls-rotation)

Peer TLS can request and renew a certificate from an approved
namespaced cert-manager Issuer. The development profile uses one
Certificate per instance and checks the approved CertificateRequest
before snapshotting its output. The kind drill exercised issuer
outage, Pod replacement, renewal with a new key, native Java
forwarding, and Envoy promotion and rollback.
[Configure cert-manager peer issuance](development-rollout.md#issue-peer-tls-certificates-with-cert-manager)
[Live issuance verification](verification.md#live-cert-manager-peer-issuance)

HTTP now supports opt-in issuance from a separate approved namespaced
Issuer. Cert-manager renews the source Secret, and
`httpRenewFromIssuer` stages the new leaf for blue/green. Each fleet
keeps its immutable leaf. Envoy can verify the two role Service DNS
names against the Issuer CA. A live drill passed issuer outage,
renewal, promotion, and rollback on both authenticated Envoy routes.
[Configure cert-manager HTTP issuance](development-rollout.md#issue-http-tls-certificates-with-cert-manager)
[Live HTTP issuance verification](verification.md#live-cert-manager-http-issuance)

Removing the candidate stanza after a committed promotion now archives
its preview aliases for UID-checked cleanup. The serving revision and
TLS pin stay available for Pod replacement. The rollback target and
its stable and Gateway Service identities remain available. New staging
holds while a previous fleet still reserves rollback or detached capacity.
[Promoted preview verification](verification.md#promoted-preview-cleanup)

Signed fleet retirement reserves the instance before the platform drains the
displaced fleet. The reservation excludes other operation claims and survives
operator restart. The platform supplies traffic drain and JVM fencing, and the
controller requires an empty displaced Pod inventory and a signed completion
receipt bound to the reservation. The live leaf and Gateway backends stay in
place.
[Fleet retirement procedure](development-rollout.md#confirm-fleet-retirement)

The Kubernetes Basic Auth cohort adapter checks all observable owned
Pods, including preview and terminating rollback Pods. It binds HTTPS
observations to Pod UIDs and JVM incarnations, then rereads the instance
and Pod inventory. The API-server test exercises membership changes,
container restart and label drift. The real Java test verifies learning
a process incarnation over HTTPS. Wiring the adapter into a security
operation and fencing newly admitted processes remain open.
[Basic Auth cohort verification](verification.md#basic-auth-reload-across-search-and-ingest)

The reusable Basic Auth observer checks account acceptance and rejection
through a direct verified HTTPS endpoint. It requires the same JVM
incarnation and connection throughout the observation, and verifies that
the provider rejects anonymous requests. TLS unit tests cover stale
processes, connection replacement and generic error responses. The Java
and SeaweedFS fixture exercises the observer on both roles, including
independent read-back of the removed account record.
[Basic Auth cohort verification](verification.md#basic-auth-reload-across-search-and-ingest)

The built-in auth plugin passed live password rotation and account
deletion on separate ingest and search JVMs with verified HTTPS and
unchanged process incarnations. The tests passed on both a filesystem
store and a supplied SeaweedFS 4.53 bucket. They pin a five-minute
positive cache TTL and check existing and fresh connections after the
change. This qualifies healthy-store Basic Auth behavior for the local
fixture. Kubernetes activation, authorization policy and a revocation
bound under partition remain open.
[Basic Auth cohort verification](verification.md#basic-auth-reload-across-search-and-ingest)

The development cluster-settings operation now records its outcome
before releasing the instance claim. It records `Writing` before one
PUT and uses read-back for recovery after a timeout or restart. The
API-server regression covers deletion while a write may still commit
and rejection of a stale claim after deletion. A live kind and
SeaweedFS S3 drill verified set, read-back from a replacement ingest
Pod, removal and rejection of an unsupported value. An unresolved
write retains the claim. Fleet-wide effectiveness, authenticated admin
transport and behavior with retained rollback fleets still need proof.
[Cluster settings verification](verification.md#persistent-cluster-settings-and-claim-recovery)

The revision fix passed the Go and API-server controller suites and a
live two-replica Envoy Gateway drill. Promotion and rollback both
succeeded. Rollback released the claim with blue/green enabled, both
role Services kept their UIDs and addresses, and both writes remained
searchable. The controller preserves verified legacy revision pins
across rollout policy edits. Eight of 500 reads received HTTP 503
during selector convergence, matching the original drill.
[Live rollback after the revision fix](verification.md#rollback-with-bluegreen-left-enabled)

M07.06 has a per-role weight planner and a conditional HTTPRoute adapter.
The planner limits initial admission, ties participating fleets to their
pinned revisions, and checks active endpoint identities and sampled
weights at each expected proxy. A replay can record the observed route
write, then must obtain separate convergence evidence. The adapter
changes only weight fields and checks the route UID, spec scope and
resource version. The instance controller now provisions opt-in
per-fleet Services and pins their UIDs before a platform-managed route
uses their names. The signed evidence publisher now checks a claimed
promotion or rollback and the live HTTPRoute version before publishing
proxy observations. The weighted executor persists both role schedules
before a write and recovers a lost patch response by reading the
HTTPRoute. A recovered write still waits for fresh proxy evidence.
The weighted promotion builder pins deployment-approved routes and
their serving and candidate fleets. The live qualification adapter
checks each remaining fleet's JVMs, serving floors, disruption budgets
and separate platform approval before using signed proxy observations.
The weighted commit requalifies both production routes, synchronizes the
stable Services, and verifies their qualified endpoints before recording
the serving fleet. It pins the old fleet for weighted rollback and
records operation success before releasing the claim. The weighted
rollback executor can recover an interrupted promotion or a committed
one whose operation was deleted. It persists the reverse schedule
before transferring the claim. Manager dispatch now supports an opt-in
`WeightedGateway` promotion on two deployment-approved HTTPRoutes.
Rollback inherits the predecessor's weighted routing. A seven-node kind
drill exercised both operations with real Envoy replicas, signed test
observations and a manager restart. All 1,356 continuous client reads
returned 200 and both writes remained visible after rollback.
The repository now provides a read-only Envoy proxy observer that uses
the serving Service inventory and private per-replica tunnels. The
maintained CLI drove promotion and rollback on kind under namespaced
read-only access. The manager resumed after its restart, both writes
remained searchable, and all 1,577 client reads in the resumed run
returned HTTP 200. Cloud qualification remains open.
[Live observer evidence](verification.md#reusable-envoy-proxy-collector)
Weighted promotions can also specify strictly increasing intermediate
percentages per role. When the same fleets stay active, each proxy must
report its active configuration as well as its sampled distribution.
The follow-up Envoy drill verified distinct search and ingest
schedules, manager restart and rollback. All 2,121 continuous client
reads returned 200. Rollback derives its sequence from the live weights.
[Weight planner and route patch tests](verification.md#weighted-planner-and-conditional-route-patch)
[Per-fleet Gateway Services](development-rollout.md#per-fleet-gateway-services)
[Signed proxy evidence](development-rollout.md#signed-proxy-evidence-for-weighted-routes)
[Envoy proxy collector](development-rollout.md#collect-envoy-proxy-evidence)
[Weighted journal recovery](verification.md#weighted-operation-journal-recovery)
[Weighted promotion commit](verification.md#weighted-promotion-commit)
[Weighted rollback recovery](verification.md#weighted-rollback-recovery)
[Manager configuration](development-rollout.md#enable-weighted-bluegreen-execution)
[Live weighted promotion and rollback](verification.md#live-weighted-gateway-promotion-and-rollback)
[Configurable weighted promotion stages](verification.md#configurable-weighted-promotion-stages)

On separate test rules, Envoy Gateway 1.8.6 omitted the zero-weight
fleet from active health checks. A direct weight flip returned 503.
A 1% admission step followed by a health check on both proxies allowed
a full promotion and rollback across both roles with 1,720 HTTP 200
responses. The test changed the route weights manually. Operator
qualification needs fresh evidence of the expected endpoint identities
at each proxy, guarded route writes, and restart recovery.
[Gateway weights and health admission](verification.md#envoy-gateway-backend-weights-and-health-admission)

The rollout executor revalidates both roles' Pod sets and the EndpointSlice
versions used by applied routes after the source and disruption-budget
checks. It rereads the Services, instance and operation before advancing.
The earliest sampled Java lease deadline further bounds candidate and
route evidence. [Cross-role evidence](verification.md#cross-role-evidence-before-a-selector-switch)

Development rollout binds the HTTPS stats response to the directly
qualified JVM incarnation for the responding Pod UID. The runtime build
adds the identity to the same response as the lease. Legacy stats remain
valid for ordinary probes. Before promotion changes either selector, the
controller also checks the retained source Pods for incarnation support and
a usable lease. The Linux tests for all Go packages and the six-process
API-server and SeaweedFS test passed. The plan validator passed.
[JVM identity during blue/green route verification](verification.md#jvm-identity-during-bluegreen-route-verification)

The development Envoy Gateway example routes to the stable role Services
and checks discovered Java Pod addresses directly. It passed server-side
dry run and was applied to the single-replica kind Gateway. In a matched
worker freeze with both strict search addresses initially healthy, Envoy
excluded the failed address in 3.0 seconds. Checked versus control search
had zero versus 12 timeouts on fresh connections and three versus 23
with connection reuse enabled, each out of 95. All checked timeouts
began before ejection. Both checked hosts could read the document after
failback. A cold proxy can temporarily return 503 while its first checks
converge. ALB, production TLS, Gateway replicas and write outcomes remain
unverified. [Active Java endpoint health in Envoy Gateway](verification.md#active-java-endpoint-health-in-envoy-gateway)

The paired kind drill measured active ingress health separately from Java
backend convergence. HAProxy dropped the frozen ingress Pod in 2.3
seconds. Over 95 attempts per path, checked versus direct ingress produced
12 versus 16 search read timeouts and one versus 13 index-route read
timeouts. The surviving ingress still selected the frozen Java Pod after
edge ejection. Both routes read the document after recovery. The next
local check needs per-Java-endpoint health, with endpoint membership
still scoped to the production Service and each role. ALB behavior
remains unverified. [Active ingress health during abrupt loss](verification.md#active-ingress-health-during-abrupt-loss)

Fresh outage evidence now holds new placements in the reported zones and
can withdraw an already released but unbound Pod before its timeout. The
API-server test confirms the withdrawal releases the slot without a
persistent group hold, and clearing the report before deletion cancels
that withdrawal. The Go suite and plan check passed. In a local abrupt
node freeze, the emergency Pod reached Ready in about 49 seconds while
the original Pod remained bound. Kubernetes convergence took about 50
seconds. The ingress sample recorded 18 search and 11 index-route timeouts
out of 150 attempts each. The original document was searchable after
failback. [Local abrupt loss behind ingress](verification.md#local-abrupt-loss-behind-ingress)

Host-based search and index routing passed a five-node local Traefik 3.6.2
drill. The fixture removed the ingress replica and strict search Pod on
the same cordoned worker, then recovered and retired the degraded bootstrap
and emergency search Pods. All 155 search and 155 index-route read samples
returned HTTP 200 across that window. Both routes read back the document,
and the operator ServiceAccount had no Ingress patch permission. The
example Ingress passed a server-side dry run and the plan check passed.
The fixture covered plain HTTP and graceful Pod loss.
[Local ingress during search AZ failback](verification.md#local-ingress-during-search-az-failback)

The pinned Java build now includes `local-lease-observation.patch`.
Lease validity and expiry come from the same locally published lease,
rather than mixing the local validity check with cached membership expiry.
The refreshed staged bundle passed all 141 JAR hash checks and built with
the repository Dockerfile. The real S3 denial fixture confirmed an expired
lease was rejected and eligibility returned after store access recovered.
The five-node search repeat reported HTTP 200 on all 155 samples across the
simulated outage and both retirements. Steady-state follow-up returned 45
HTTP 200 responses and 90 positive local lease observations. The Go suite
and plan check passed. [Repeat with local lease reporting fixed](verification.md#repeat-with-local-lease-reporting-fixed)

Failback can now replace a surviving degraded search bootstrap. The
controller qualifies one same-AZ regular surge with fresh fit evidence,
durably binds it to the bootstrap UID and revalidates before gate release.
It retires the bootstrap before the emergency Pod once qualified regular
endpoints can serve both zone and regional targets. Go tests, the plan
check and both real API-server suites passed. A five-node kind drill
verified the no-fit hold, both retirements and the final search result.
Two traffic samples recorded 41 attempts with no HTTP response across
the outage and retirement windows. Lease-probe failures were present in
the cluster events. Steady-state follow-up also reproduced connection
failures when both Java processes reported `valid_now: true` alongside
negative remaining lease times. Inspect the pinned `StatsHandler` and
`BlobLeaseMembership` together before qualifying traffic continuity.
[Local failback with surviving degraded bootstrap](verification.md#local-failback-with-surviving-degraded-bootstrap)

Strict search now keeps the completion gate when the bootstrap Pod is
lost and another serving search Pod is Ready. The Go suite, plan check and
`TestStrictBootstrapIntentOnAPIServer` passed. A four-node Kubernetes 1.32.2
drill used the real Java shell and a supplied S3-compatible repository.
Both the temporary and regular replacement search Pods reported completed
warm-up without timeout and each had a local reader. After explicit
outage clearance, the temporary Pod was evicted and the search result
still contained the indexed document. All 105 sampled requests after the
simulated loss returned HTTP 200, including during retirement. Continued
degraded bootstrap, abrupt cloud loss, ingress and larger-cache behavior
remain unqualified. [Local strict search failback](verification.md#local-strict-search-failback)

A local Java/S3 drill uncovered two controller bugs. Comparing timestamps
with Kubernetes semantic equality lets an unchanged emergency proposal
settle after API read-back. Endpoint verification accepts the Pod reference
format generated by Kubernetes and rejects a conflicting reported zone.
The ConfigMap watch now queues the same-namespace instance when its outage
evidence changes. The Go suite, Kubernetes API-server rollout suite, and
plan check passed. The API-server fixture now covers restart, competing
intent and real eviction admission. The three-node kind drill also
recovered regular indexing capacity, retired one temporary Pod and read
the acknowledged document through the original Service. The repeat run
recorded HTTP 200 on all 95 sampled reads spanning the transition. [Local indexing failback](verification.md#local-indexing-failback)

`--development-emergency-failback` enables one retirement at a time.
The controller persists the temporary Pod UID in `status.emergencyRetirement`,
blocks a competing operation claim, and rechecks regular serving capacity,
the live Service, fresh explicit outage clearance, and the owned zonal PDB.
It uses the namespaced eviction subresource with Pod UID and version
preconditions and observes the original UID before retry or cleanup.
The Go suite covers renewed outage, PDB denial and a lost eviction response.
WSL API server tests passed for CRD installation and the shared eviction
executor's PDB and ambiguous-response behavior. `python tools/verify_plan.py`
passed. The full failback controller still needs a real cluster drill with
Java shutdown, ingress traffic, and cloud node loss.

Development serving protection now observes regular Pods separately when
checking the capacity available for failback. It matches qualified Pod
versions to Ready Service endpoints and requires all regular zone targets
and the regional floor without relying on emergency Pods. The controller
reports the result in `regularCapacityRecovered`. The Go suite covered a
Ready temporary writer, a replacement missing its endpoint, and a restored
regular endpoint. The regenerated development CRD loaded in the WSL
Kubernetes API server test (`TestRolloutStatusAPIServerConflict`), and
`python tools/verify_plan.py` passed. The result remains diagnostic until
an outage-aware eviction flow persists and revalidates a retirement intent.

Ready emergency Pods now contribute to serving capacity without
satisfying the regular per-zone replica target. The API server test
removed a regular Pod while its zone still had a Ready emergency Pod
and verified that the controller planned a regular replacement. The
controller also waits for emergency capacity to clear before staging or
promoting a rollout. Automatic retirement still needs a guarded
failback path.

The development controller can now create a temporary Pod from a fresh
emergency capacity proposal once normal provisioning has no eligible
creation. It records the selected surviving zone and pins the evidence
UID and version through restart. The API server test changed evidence
while the Pod was gated, verified withdrawal, and then observed a fresh
marked indexing Pod released with zone B pinned. Kubernetes scheduling,
cloud capacity and failback retirement remain separate work.

Development instance reconciliation can now produce a bounded emergency
capacity proposal from an explicitly configured namespaced platform
ConfigMap. It binds outage and fit evidence to the instance generation
and live capacity profile, checks owned Pod observations and pending
limits, and clears stale proposals. Tests cover a qualifying survivor and
expiry. The proposal remains diagnostic until emergency creation,
release revalidation, and failback are implemented.

The emergency placement planner now proposes one temporary Pod for a
role with a confirmed unavailable zone and a regional shortfall. It
selects a surviving zone with qualified fit, counts already committed
temporary Pods even in a failed zone, and respects per-zone, role and
shared pending limits. Existing pending recovery can satisfy the
shortfall without surge. Controller integration still needs fresh outage
and fit inputs, durable intent, and retirement during failback.

The provisioning queue now gives the next eligible creation slot to a
serving role below its configured regional Ready floor before work that
addresses only a zone floor or fills a reserve. Preview Pods do not count
toward the regional floor. Existing AZ holds, desired counts and release
limits still apply. This prioritizes recovery within existing capacity
targets and does not authorize extra replicas or Pod retirement.

When a role and zone are already held, provisioning now records withdrawal
for an existing gated Pod or a stale creation intent. The controller still
requires a matching Pod identity and gate before deletion. A release that
won the race is tracked under its pinned UID while the retired name stays
fenced. The hold remains scoped to its original role and zone. This
change frees the shared slot for eligible work in another zone. It does
not diagnose an outage or create emergency replicas.

The EndpointSlice diagnostic now preserves verified survivor counts and
reports zero for an active zone with no ready endpoints. It distinguishes
loss below a zone floor from loss below the role's regional floor and
keeps unverified observations separate. Blue/green promotion holds new
selector changes when source Pods are below either floor. The target's
qualified Pods and route endpoints must also meet the regional floor.
Status uses the configured Pod zones. Regional edge behavior, actual AZ
failure load testing, and emergency provisioning remain open.

The S3 rollback fixture now kills the green ingest JVM after restoring the
blue routes. Blue ingest acknowledged a new write 30.016 seconds after
process loss, and the shard head advanced from green ownership at term 1
to blue ownership at term 2. The restored search route returned both the
green-owned write and the new blue-owned write. This is one local recovery
measurement with request retries, not a cloud or zero-error guarantee.

The S3 rollout test verified a write on a shard owned by green ingest.
The original ingest process reported the same green owner and term before
and after the HTTPS write. After rollback, the restored search route
returned the acknowledged document. The fixture now allocates distinct
ports together for the S3 gateway and detects early gateway exits.

The five-process Java rollout test now also passed against SeaweedFS 4.53
as the S3 remote store. The fixture provisions its own disposable bucket
outside the operator and uses a keystore per Java process. The promoted
HTTPS indexing route acknowledged a write, and both the promoted and
restored search routes returned the documents written before and after
promotion. The rollback state machine used injected qualification and
route evidence followed by an actual read through the restored proxy.
The run does not qualify AWS S3, cloud ingress, or shard ownership.

The opt-in API server rollout test passed with five real Java shell
processes. Direct warm-up and lease checks and the local HTTPS proxy used
those processes. Search on the promoted route returned documents written
before and after the switch, and the stale search responder held indexing
on the old Service selector. The test used a shared filesystem store,
fixture-supplied Pod status and EndpointSlices, and injected platform
security and compatibility conclusions. Kubelet and real ingress remain
separate qualification work.

The Kubernetes API server rollout fixture now runs selector switches and
commit with the signed publisher, the configured HTTPS route reader and
EndpointSlice observations. A local TLS proxy follows the actual stable
Service selectors. A stale search responder holds the indexing switch
until the search route reaches the target fleet. The fixture uses
simulated Java and EndpointSlice controller observations. Development
route probes can load a private CA bundle at manager startup.

The Kubernetes API server integration now drives the configured development
reconciler with ConfigMaps written by the signed publisher. It verifies
that one role's evidence leaves the claim free and that valid evidence
for both roles allows preparation. The fixture uses real Kubernetes
Node, Pod, Service, operation and ConfigMap objects with injected Java
observations. Optional publisher RBAC separates namespaced writes and
cluster-scoped Node reads from the operator. Live Java and ingress still
need qualification in this combined path.

The platform now has a one-shot signed-evidence publisher. It verifies the
exact live rollout and Pod cohort, reads each Node's zone under a separate
platform identity, then publishes the signed ConfigMap. The operator
retains namespaced access. A fixture verified that the publisher signs
matching placement and declines to refresh a statement after the Node
zone changes. The platform still needs a verifier for peer, security and
rollback compatibility and a deployment that supplies fresh statements.

The shipped manager now has opt-in development rollout flags for one
named instance and distinct production HTTPS routes. It reads signed
namespaced evidence and verifies the operation, instance generation, role,
revision, and exact Pod, Node, actual zone and Java process identities.
Optional RBAC adds Service patch and EndpointSlice list without Node
permissions. Tests cover valid, expired, tampered and mismatched evidence.
The platform signer and cloud/ingress qualification remain deployment work.
See [Development blue/green rollout](development-rollout.md).

The live rollout evidence adapter combines per-Pod Java probes, the
configured serving floor in each active zone, independent platform
qualification, and HTTPS plus EndpointSlice checks for applied routes.
It rechecks all Pod resource versions after the external observations.
A TLS fixture and fake Kubernetes client verify that an unavailable platform
check or a changed Pod omits that role's evidence. The manager still needs
deployment-specific route configuration and a trusted provider for actual
placement, peer readiness, security, and rollback compatibility.

The operation reconcile entry point now drives the rollout state machine
when supplied an uncached reader, a qualification provider, and the Pod
template's DNS configuration. Promotion qualifies both roles and checks
target protection before taking the claim. The reconciler stages intents,
advances traffic, finalizes commits, and uses an operation finalizer to keep
claimed records available for recovery. Deleting a partially switched
promotion holds it for authorized rollback. Deleting a prepared rollback
with an unresolved claim also holds for recovery. The shipped manager
still needs a live qualification provider before enabling this path.

The internal rollout finalizer verifies that the current Pod template renders
the committed serving revision and that both stable Services still select
the committed fleet. It records operation success on one pass and releases
the claim on a later pass. Rollback after release uses the instance's
retained target to recover both original Service UIDs and revisions. That
path works independently of the old promotion object. Manager wiring and
qualification with real Java, ingress, and retained Pods remain open.

The promotion commit now records both original fleet identities alongside
the serving mapping. The record pins each role's revision and Service UID.
If configured, the rollback window starts at commit and expires new claim
transfers at its deadline. An omitted window leaves the record without a
deadline. An explicitly authorized rollback can use this record after both
Services switched while the promotion still holds the claim. The executor
rechecks target capacity and routing and clears the record on successful
rollback. This record does not provision or retire retained Pods, and
manager wiring still needs qualification.

Before either promotion selector write, the executor now checks that the
original search and ingest revisions still match the live source Pod
inventory. A mismatch holds further switches. If a switch has already
taken effect, the executor can still record it so recovery starts from the
actual route. This check identifies source revisions and does not establish
cache warmth, peer health, or rollback capacity.

Promotion preparation now records the source Pod revision for each role
alongside the Service UID and target revision. It requires one consistent
revision among the owned source Pods and a Ready Pod for each role. Rollback
uses the persisted source revisions from the superseded promotion. It holds
if an older promotion record lacks either revision. This identifies the
target of recovery. Application health and retained fleet lifecycle still
require their own checks.

The mixed rollback handoff and executor now have a passing Kubernetes API
server integration test. It starts with a search selector switch that the
promotion has not recorded. Rollback takes the claim, prevents the old
promotion from advancing, restores search in place, verifies and adopts
ingest without changing its Service version, and commits both roles to the
original fleet. The fixture uses real Kubernetes objects with simulated Java,
route and disruption controller observations. Manager wiring and live client
traffic remain to be qualified.

The internal rollback handoff can supersede an uncommitted promotion after
an observed Service switch. It first persists rollback intents for both
original Service UIDs. On a later pass it rechecks the live selectors
against the approved mapping and moves the claim and mixed serving mapping
together in one optimistic instance status update. A role already on the
rollback target is marked for adoption and requires route verification
without a selector patch. Tests with fake Kubernetes clients cover a lost
promotion response, stale route mapping, changed revision and restart.
Manager wiring, retained fleet lifecycle, and real traffic recovery still
need qualification.

Rollout preparation and traffic mutations now check the target fleet's live
PodDisruptionBudgets for every active search and ingest zone. The check
requires exact ownership, selectors and serving floors, plus current PDB
controller status with enough healthy Pods for that floor. A missing,
weaker or unobserved budget holds the transition. The check can still record
a selector that already switched, which supports recovery after a lost patch
response. The API server fixture supplies PDB status directly. Real eviction
behavior and actual AZ placement require separate qualification.

The promotion executor now requires current qualification for both roles
before the first search switch. Before either selector mutation or the
serving mapping commit, it checks that the live candidate still matches the
prepared revision. The Kubernetes API server regression fixture drops ingest
qualification and separately replaces the recorded revision. Both cases hold
with the search Service still on the source fleet. Recovery can still record
a selector change observed after an ambiguous patch response.

The internal promotion preparation path verifies the current candidate
revision and both pinned preview Services, reads both production Services
with their expected source selectors, and requires fresh qualification for
search and ingest. It persists both production Service UIDs and the candidate
revision in one optimistic operation status update. It rechecks the instance
version before persisting. No Service changes in this step. Qualification
still comes from the caller, and the manager does not invoke preparation yet.

The rollout route observer now requires the Pod that answered the HTTPS probe
to appear as a ready endpoint with the same Kubernetes UID and resource
version. One deadline covers the HTTPS probe and subsequent Kubernetes reads.
A TLS fixture with a fake Kubernetes client changes the Pod version between
the two observations and verifies that the combined evidence is rejected.
The observer still needs live ingress qualification and controller wiring.

The prepared blue/green executor now commits both serving role mappings and
the exact operation identity in one optimistic instance status update. It
requires durable convergence and fresh candidate and route evidence for both
roles. A new executor can recover that commit from API state without replaying
the Service mutations. The operation claim stays in place. Controller
registration, live evidence, operation finalization, and retained fleet
management are still required before promotion can be exposed to users.

An internal blue/green executor can now advance one prepared rollout step.
It reads the operation and instance directly, asks the caller for qualified
evidence, and reads both role Services. It checks the operation and instance
UIDs and resource versions again before recording progress or applying a
conditional selector patch. The Kubernetes API server test changed the
instance during evidence collection and observed a hold. A second pass
switched the search Service in place, preserved the ingest selector, and
recorded the observed search switch on the following pass.

The test supplies candidate evidence. Production evidence, controller
registration, rollback claim transfer and live route
qualification remain open. Instance, operation and Service updates are
separate API transactions. The executor requires serialized rollout writers.

The installed real shell auth plugin and experimental HTTP TLS provider now
have a passing combined local Java integration run. The operator's HTTPS
account client created, read, rotated and deleted an account; the old and
deleted credentials were rejected, and an untrusted server certificate was
rejected. This used one JVM, a self-signed loopback certificate, a filesystem
store and a local OCI protocol fixture. The client remains unwired and this
does not qualify Kubernetes, peer TLS, multi-node revocation, authorization
policy or production TLS rotation.

An opt-in Java integration test now packages the pinned shell's actual
`ServerlessAuthPlugin` JAR as an OCI artifact, installs it from a local
registry protocol fixture, creates a disposable keystore and starts the shell.
It passed anonymous rejection and live account creation, password rotation and
deletion on one JVM. This is a real shell plugin but not the upstream Security
plugin. The run used plaintext loopback and a filesystem store; it does not
qualify TLS, Kubernetes, multi-node revocation or authorization policy reload.

The real Distribution registry integration test now builds and runs the
actual plugin-installer executable with its explicit registry CA option,
rather than using only a test-supplied Go client. It rejects an untrusted
certificate and bad credentials, installs the approved artifact, retries the
completed installation after the registry stops, and starts Java with the
installed fixture plugin. The local Distribution 3.1.2 run passed. It does
not exercise a kubelet, an installer image, or a production plugin.

The development OCI installer now accepts an optional `ca.crt` in the same
immutable registry credentials Secret. The controller validates and records
its presence with the Secret UID, and mounts it only in the installer; the
installer adds it to system trust for registry HTTPS requests. Linux CLI tests
passed with a local TLS registry fixture, including rejection without trust,
and a Kubernetes 1.32 API-server test preserved the intent and Pod arguments.
The Go suite and plan verifier passed. This bundle applies to all registries
used by that installer, and Secret-by-name mount races and actual kubelet
mounts remain unverified; rotation requires a new immutable Secret and Pod.

The opt-in `registry_integration` test passed against the upstream standalone
Distribution 3.1.2 registry with a self-signed TLS certificate and Basic
authentication. It pushed and installed an OCI plugin, rejected wrong
credentials and an untrusted CA, retried the completed set after the registry
stopped, rejected a fresh install during the outage, and started the pinned
Java shell with the installed fixture plugin. The Linux release archive's
SHA-256 was verified against the upstream release. This is a disposable
local registry and synthetic plugin, not a kubelet, production registry or
production plugin compatibility test.

The opt-in OCI plugin/Java integration test now installs a compiled fixture
plugin that registers a REST handler, shuts down the local registry fixture,
retries the completed installation and starts the pinned Java shell. The test
passed after calling the installed plugin's endpoint. The registry is a local
protocol fixture, the plugin is test code, and the test reuses a completed
local volume; it does not establish a fresh Pod install during a registry
outage, real registry interoperability or production plugin compatibility.

The isolated development cluster-settings backend now re-reads its admin
Service after each HTTP request and requires the same UID and resource
version, with the expected routing shape. A changed Service invalidates a
read; after a successful write response it makes the outcome unknown so the
operation must read back. Unit tests cover both cases, and a Kubernetes 1.32
API-server test changes the Service during a simulated HTTP read. The Go
suite and plan verifier passed. This is not an atomic HTTP/Service transaction,
does not provide authenticated administration, and does not establish
fleet-wide effective settings or resolve concurrent external writers.

Immediately before a development scheduling-gate release, the controller
now re-resolves the primary store and compares it with the persisted identity.
A missing, deleting or changed repository ConfigMap holds release; restoring
the original configuration permits it. A Kubernetes 1.32 API-server test
changed the referenced bucket after the release intent was persisted and
verified that the Pod stayed gated until restoration. Rendering continues to
use the persisted store identity. This narrows, but cannot eliminate, the race
between a ConfigMap read and a separate Pod update.

The same-image mixed-process candidate-owner test also passed against a local
SeaweedFS 4.53 S3-compatible gateway using the patched Java shell bundle.
The candidate retained the same observed shard owner and term across its
write, the original ingest read the document before and after candidate stop,
and the later ingest-replacement and search checks passed. The disposable
fixture and runner are `tools/java-s3-mixed-smoke.ps1` and
`tools/java-role-smoke.ps1`. This is not AWS S3 conformance, production IAM,
multi-version compatibility, Kubernetes routing or operator promotion.

The opt-in mixed-process smoke test can now require a candidate-owned shard
with `OPERATOR_TEST_CANDIDATE_OWNS=1`. A local patched-shell run observed the
candidate as owner at the same shard term before and after a new write, read
that document through the original ingest process, stopped the candidate and
read the document again through the original process. The full smoke run also
passed its ingest replacement and search checks. This uses the same Java image
and a filesystem store; it does not prove S3, mixed-version compatibility,
Kubernetes routing or operator promotion and rollback.

An opt-in `OPERATOR_TEST_MIXED_FLEET=1` local shell smoke scenario now starts
a second same-image ingest process against the same filesystem store while
the original ingest and search processes remain up. A write submitted to the
candidate endpoint was readable via the original ingest endpoint before and
after the candidate process stopped; the subsequent ingest-replacement and
search checks also passed. The write may have been forwarded to the original
shard owner. This is not evidence of candidate shard ownership, different-image
compatibility, S3, Kubernetes routing, rollback execution or promotion.

The development candidate-search observer passed against a real warmed
search JVM in the patched pinned shell bundle, via the opt-in
`TestLiveCandidateSearchObservation` in `tools/java-role-smoke.ps1`. The
run also passed strict warm-up, same-name search restart, separate-role
index/search and ingest replacement checks. The Kubernetes Pod and candidate
metadata were synthetic: this does not establish actual AZ, preview Service
traffic, cache durability or blue/green promotion behavior.

The opt-in development controller now records a historical candidate-search
sample for strict warm-on-join pools. It requires the complete allocated Pod
set and reports either warmth alone or warmth plus the pinned preview
Service's EndpointSlices; a later failure replaces the previous result.
Unit fixtures cover two zones, preview endpoints and failed follow-up. The Go
suite, plan verifier and Kubernetes 1.32 API-server status round-trip passed.
This diagnostic does not prove actual AZ, durable cache residency, live edge
traffic or promotion eligibility; its sample and endpoint reads are not atomic.

When endpoint observation is enabled, the development candidate-ingest
diagnostic now checks the pinned preview Service and EndpointSlices against
all sampled candidate Pod UIDs and allocated per-zone counts. Its historical
result distinguishes lease-only from lease plus preview endpoints. A unit
fixture covers a missing slice after success. A Kubernetes 1.32 API-server
fixture exercised preview Service and EndpointSlice identity, using a
synthetic backend. The Go suite and plan verifier passed. These separate
reads do not prove a live client route, actual AZ or atomic process/endpoint
state, and do not authorize promotion.

The gated-Pod admission check now preserves the full relative ordering of
requested and operator-managed init containers. A native sidecar moved after
the DNS preflight is rejected; an injected init container remains allowed when
the original order is preserved. The Go suite and plan verifier passed. This
uses a unit fixture and does not exercise a mutating webhook or kubelet.

The development pool API now accepts `nativeSidecars`, rendered in declared
order as init containers with restart policy `Always` before the operator's
DNS preflight and plugin installer. The existing effective-resource fit logic
accounts for restartable init containers. Admission checks preserve their
launch, configuration, order and requests and allow them to mount declared
sidecar volumes. Names must be unique across regular/native containers and
operator-reserved containers. The Go suite, plan verifier and Kubernetes 1.32
API-server CRD/Pod admission test passed, including rejection of a changed or
missing native sidecar. No kubelet was run; startup, shutdown, mount behavior
and older Kubernetes compatibility require separate qualification.

The candidate-ingest observer passed against a real local ingest JVM in the
patched pinned shell bundle using `TestLiveCandidateIngestObservation` from
`tools/java-role-smoke.ps1`. The same smoke run started separate ingest and
search JVMs, indexed and searched, then verified a replacement ingest process
could write and the original document remained searchable. Kubernetes Pod
metadata and candidate identity were synthetic; this did not test an actual
candidate Pod, promotion, AZ placement, secure transport or multi-fleet routing.

The opt-in development controller now samples candidate ingest Pods using the
same process-incarnation, readiness and ingest-lease checks as serving ingest.
It requires the complete allocated candidate role/AZ Pod set, its pinned
revision and a stable re-list, then records a timestamped historical success
or unavailable status at most once per minute. A unit fixture exercises a
successful sample followed by a failed one; the Kubernetes 1.32 API-server
test preserves the new status field. The Go suite and plan verifier passed.
The sample uses plaintext only with an explicitly qualified development image;
it does not establish actual AZ, write success, takeover, peer compatibility,
preview routing or permission to promote. No real Pod or multi-JVM candidate
was tested in this change.

Endpoint observation now re-lists the selected EndpointSlices and requires
the same names, UIDs and resource versions before reporting a sample. A
fault-injected unit test rejects a changed slice; the existing Kubernetes 1.32
API-server bootstrap flow passed with the additional reads. The full Go suite
and plan verifier passed. Slices and Pods can still change after their final
reads; this does not establish atomic routing state or enable promotion.

Endpoint observation now re-reads each distinct Ready backend Pod and rejects
an empty or changed resource version or UID, including a readiness change
during collection. Repeated references to the same Pod must agree on version.
A fault-injected unit test covers a readiness update between reads. The Go
suite and plan verifier passed. Kubernetes reads are still not atomic: the
EndpointSlices or a Pod may change after their reads, and the observation is
not proof of future routing, actual AZ or edge convergence. Promotion remains
unwired.

Sidecar `emptyDir` volumes can now specify a positive `sizeLimit`, which is
passed to the Pod. Controller validation rejects invalid quantities and the
CRD rejects attaching a limit to Secret or ConfigMap sources. A size limit is
not a scheduler resource request and does not replace sidecar ephemeral
storage requests. Kubelet enforcement and disk-pressure behavior remain
unverified. The Go suite, plan verifier and Kubernetes 1.32 API-server test
passed; the latter preserved a 2 GiB limit through the CRD and Pod API and
rejected a limit on a Secret volume. The development revision is still fleet-wide; role-only rollout
and safe migration to per-role revision identity remain unimplemented.

The Kubernetes 1.32 scheduler fixture now tests a regular sidecar together
with a restartable init sidecar, an installer, RuntimeClass overhead and a
resident agent. The development fit check and scheduler both rejected a Pod
10 millicores over the remaining CPU and admitted the exact-fit Pod. The
fixture uses synthetic Nodes and a pre-bound agent; it does not run a kubelet,
DaemonSet controller, Karpenter or GKE autoscaler.

The development CRD now rejects sidecar volumes with no source, multiple
sources, a reserved or invalid name, or duplicate names. The volume list is a
map keyed by name; existing objects with duplicates must be fixed before an
upgrade to this schema. Controller validation independently checks the same
cases and additionally validates referenced object names. Kubernetes 1.32
API-server tests exercised the positive and negative cases. The Go suite and
plan verifier passed.

The development pool API now declares role-scoped sidecar Secret, ConfigMap
and emptyDir volumes. Rendering adds them to the Pod, and admission checks the
declared source and excludes mounts by Java, init containers and injected
containers. Referenced objects remain platform-owned and can change in place.
Controller validation rejects reserved names and ambiguous or invalid sources;
the generated development CRD includes the new fields. These checks do not
isolate a privileged sidecar or prove actual volume mounting by kubelet.
The Go suite and plan verifier passed. A Kubernetes 1.32 API-server test
round-tripped a ConfigMap volume through the CRD, admitted the rendered Pod,
and checked rejection of a changed source and a mount added to Java in copies
of the response; no mutating webhook or kubelet was involved.

The gated-Pod check also preserves explicitly configured sidecar environment,
probes, lifecycle, mounts, security fields and limits across admission. It
allows object fields to be defaulted, while preserving ordered lists exactly.
The Kubernetes 1.32 API-server test accepts actual probe defaults and rejects
a changed environment in a copied admission result. This is an admission
check, not a test of sidecar runtime behavior or a mutating webhook.

Before recovering or releasing a development gated Pod, admission validation
now requires each pool-requested sidecar to retain its name, image, command,
arguments and at least its requested resources. Additional admission-injected
containers remain allowed and are included in fit checks. Unit tests reject a
missing or changed sidecar and reduced CPU request while accepting an injected
container. This does not yet validate every sidecar field, run a kubelet, or
qualify sidecar startup, shutdown and readiness behavior.

The development account client now rejects reads and mutations for its
configured bootstrap username. Java checks the configured bootstrap password
even if an indexed account with that name is changed or removed, and indexed
existence does not describe the configured account. Bootstrap credential
rotation still needs a controlled startup replacement. A local HTTPS fixture
tests the rejection; the client remains unwired.

An unwired development client supports the shell's built-in Basic Auth account
endpoints over verified HTTPS. It rejects unsafe transport configurations and
reports unconfirmed writes as unknown outcomes without making an explicit
retry. A local TLS fixture tests account operations and a lost response after a
write. Four selected tests passed against the pinned staged Java shell for
account creation, cache expiry, change-marker invalidation and an ambiguous
password change. These in-process tests do not establish multi-process
revocation, Kubernetes integration or HTTP TLS for the Java shell. The client
is not connected to reconciliation; policy activation remains unimplemented.

The development diagnostics now sample serving ingest Pods across configured
zones, binding each local readiness and ingest-role lease observation to a
stable Pod, container and JVM incarnation and checking the stable Service's
EndpointSlices. A failed sample replaces the previous historical success. The
sample does not prove write success, shard ownership or takeover and is not
used for eviction. Unit tests cover wrong roles and same-Pod JVM restart; a
local patched-shell smoke run qualified a real ingest JVM and completed write,
search and replacement checks. A Kubernetes 1.32 API-server test preserved the
new status field, but did not probe a real Pod. The full Go suite and plan
verifier passed. HTTP authentication, actual Node AZ and Kubernetes/Java
end-to-end ingest retirement remain unqualified.

The opt-in development observer now samples serving search protection across
all configured role/AZ capacity. It requires each observed serving search Pod
to pass the process-local warm-up and lease check, verifies stable Service and
EndpointSlice identities against that Pod set, and re-lists Pods afterward. It
records a historical result, count and Service identity at most once a minute;
a failed sample replaces the previous success. Unit tests use fake Kubernetes
objects and a local HTTP fixture to reject a warm-up timeout, foreign endpoint
and changed Service selector. Kubernetes 1.32 API-server tests preserve the
status schema and existing bootstrap behavior. The full Go suite and plan
verifier passed. The sample does not establish actual Node AZ, cache residency,
peer reachability, ingress health or permission to evict, and running candidate
retirement remains unimplemented.

Retired candidate PDBs are now discovered after the candidate fleet enters
retained history. The reconciler verifies the controlling owner, deterministic
name and exact candidate role/AZ selector, persists the PDB UID, then deletes
one at a time with UID and resource-version preconditions. It holds a changed
selector or replacement UID and leaves serving PDBs intact. Kubernetes 1.32
API-server tests cover a concurrent edit, lost successful deletion response and
same-name replacement. The development namespaced Role now includes PDB delete.
To find older unlabelled budgets the reconciler pages through up to 8,192 PDBs
in the namespace per pass; exceeding that bound holds cleanup. Each retired
fleet retains up to 64 PDB identities, with no garbage collection yet. The
full Go suite and plan verifier passed. Running candidate Pod retirement and
real disruption-controller behavior remain unqualified.

Once candidate-labelled Pods and preview Services are absent and lifecycle work is
settled, the reconciler now moves the candidate identity into retained history
and allows a new candidate with a different fleet name. A retired fleet cannot
be reused. Reconciliation checks for late candidate Pods and preview Services;
an owned, still-gated late Pod is moved into the existing retired-Pod path,
while a released Pod or replacement Service UID is held. Kubernetes 1.32
API-server tests exercised finalization, a delayed gated Pod, a replacement
Service and staging a different candidate. The Go suite and plan verifier
passed. Retired fleet history is capped at 64 and is not garbage-collected;
each reconciliation lists up to 512 instance Pods and reads up to two preview
Services per retired fleet. Like existing Pod recovery, this relies on the
namespace trust boundary for instance and fleet labels. Bound-Pod retirement and
real scheduler, Java and ingress behavior remain unfinished.

Candidate removal now records preview Service retirement before deleting at
most one Service per reconciliation with UID and resource-version preconditions.
Unpinned matching Service UIDs are persisted before deletion; a same-name new
UID or changed routing is held. The retirement marker remains after both
Services disappear, including if the candidate request is restored. The
development namespaced Role now includes Service delete permission. Kubernetes
1.32 API-server tests covered removal, same-name replacement, a concurrent
Service edit and a lost successful delete response. The full Go suite and plan
verifier passed. Service deletion does not prove ingress or existing connections
have drained. Running candidate Pods and retained tombstone garbage
collection remain unfinished.

The Kubernetes 1.32 API-server candidate cancellation test now injects a Pod
binding between the withdrawal read and DELETE. The API rejects the stale
resource-version precondition, and the reconciler retains the bound Pod and
returns it to startup monitoring. A separate injected lost response after a
successful DELETE recovers by observing the missing Pod and recording its
tombstone. The binding is issued by the test, not a running scheduler; kubelet,
Java handoff and running-Pod retirement remain untested. The Go suite and plan
verifier passed.

Candidate cancellation also handles the active provisioning attempt when its
Pod has been released but is still unbound. It first persists the exact
candidate creation identity, uses the existing UID and resource-version
guarded unbound withdrawal, then records a retired-creation tombstone and
clears the matching warm-up slot and attempt after the Pod is gone. If the
Pod binds before withdrawal, normal bound-Pod monitoring takes over; the
candidate is not deleted by this path. A Kubernetes 1.32 API-server test
exercised intent, withdrawal and tombstone recovery; a fake-client test
simulated binding after intent. The Go suite and plan verifier passed. Running
scheduler behavior, running-Pod retirement and ingress traffic drain
remain unqualified or unimplemented.

When a candidate request is removed, the opt-in development reconciler now
records a retirement tombstone for one still-gated, unbound candidate Pod at a
time. The existing retired-creation recovery path pins the Pod UID and performs
the resource-version-guarded withdrawal; released or bound Pods are left for a
future running-Pod retirement protocol. A Kubernetes 1.32 API-server test
removed a request, observed the candidate Pod disappear, retained its tombstone
and confirmed the serving Pod remained. A unit test verified a gate released
after retirement intent is recorded is not deleted. The Go suite and plan
verifier passed. Candidate status and retained tombstones are
not garbage-collected; running candidate Pods, application handoff and full
cancellation remain unimplemented.

Candidate generation recovery now permits an unchanged recorded candidate to
resume after a spec change is reverted. It recomputes the candidate template
revision, holds while Pod work or retired candidate creation records remain,
checks existing candidate Pod ownership and revision, and rechecks pinned
preview Services on the next reconciliation. Unit tests cover changed
templates and in-flight or retired work. A Kubernetes 1.32 API-server test
changed a candidate image, restored it and observed recovery without changing
the pinned Service UID. The full Go suite and plan verifier passed. Retirement
of running candidate Pods remains unimplemented.

Lease-aware serving checks and role Service/HTTPS route probes now require the
requested role in the shell's local stats as well as readiness and a valid
lease. Missing roles fail closed; existing health-probe binaries must be rebuilt
to pick up the change. Unit tests reject search-only nodes for ingest and a
wrong-role route response. The pinned local Java shell smoke test confirmed that
an ingest JVM reports `ingest` and not `search`, and completed indexing,
search and replacement-writer checks. The Go suite and plan verifier passed.
Local roles do not establish peer-advertised membership, shard ownership or
writer takeover; Kubernetes, cloud and end-to-end rollout remain unverified.

An unwired development candidate search observer now qualifies every search
Pod in the allocated candidate groups using the existing process-local warm-up
and lease adapter. It checks the exact revision and strict warm-up mode,
requires the complete per-zone Pod set, re-lists after probing, and returns
short-lived evidence scoped to the oldest observation. Tests use fake
Kubernetes objects and local HTTP fixtures: a green readiness response with a
warm-up timeout, a changed previously probed Pod and a missing group are held.
The full Go suite and plan verifier passed. The result does not establish
actual Node AZ, cache residency, peer compatibility, full production capacity
or route health. It is not wired into promotion and has not been exercised
against real candidate Java Pods in Kubernetes.

Development capacity sampling now records candidate role/AZ desired, observed,
unbound, Kubernetes Ready, terminating and terminal counts separately in
candidate status. It reports existing candidate Pods even when the candidate
policy is held, marks an unavailable desired count unknown, and leaves warmth
unknown. The timestamp and observed generation are shared with the serving
capacity sample. The development search qualification sampler also rotates
through eligible candidate search Pods; its single historical result does not
authorize a rollout or establish present cache warmth. Unit tests cover the
counts, held policy, copy isolation and candidate sampling. A Kubernetes 1.32
API-server test observed the candidate unbound count and unknown warmth during
normal reconciliation. The full Go suite and plan verifier passed. Real Pod
placement, current warmth and end-to-end promotion remain unqualified.

The Kubernetes 1.32 API-server test now also drives the normal instance
reconciler for a same-image candidate. It checks that serving and candidate
role/AZ PDBs are created with the configured floor before Pod creation,
records a gate-release intent, holds the gate while serving readiness is lost,
and releases it after readiness returns. The test itself writes PDB status;
envtest does not run the disruption controller, scheduler, kubelet or Java.
It therefore does not qualify real eviction, scheduling or serving behavior.
The Go suite and plan verifier passed.

The opt-in development gated-Pod path now admits a same-image candidate after
the preview Service UIDs are pinned and serving role/AZ groups reach configured
Ready capacity. It allocates `maxPreviewReplicas` across groups, counts terminal
and terminating candidate Pods against that limit, and creates candidate PDB
intent with zonal floors. Before candidate creation and gate release it
rechecks preview Service identities, serving Ready capacity and the preview
limit. Candidate search Pods use strict warm-up when configured and never claim
the serving fleet's cold bootstrap exception. Different images remain blocked.
A Kubernetes 1.32 API-server test called the gated reconciler directly, observed
a candidate creation intent, held creation when the serving Pod became unready,
and created the candidate with its scheduling gate after readiness returned.
The direct test bypassed the PDB reconciler and did not run a scheduler, kubelet,
Java, eviction or actual traffic. The full Go suite and plan verifier passed. The
candidate PDB and release path still need a real cluster test; promotion,
candidate retirement and different-image qualification remain unimplemented.

Instance reconciliation now accepts a BlueGreen candidate fleet and pinned
image, records its candidate revision and generation before creating preview
Services, and pins their observed UIDs. A changed candidate template or removed
request holds the recorded candidate; retirement and replacement are not yet
implemented. An unchanged template can be rebased after safe rechecks. Candidate configuration is excluded from
the serving Pod revision hash, although a generation change can still retire an
in-flight development Pod intent. Kubernetes 1.32 API-server tests exercised
the full instance reconciliation path, a restart between recording intent and
Services, and a changed candidate. The full Go suite and plan verifier passed.
Only same-image development candidate staging is wired; different-image
compatibility, preview authorization and promotion execution are not provided.

Candidate preview Service rendering and its creation helper provide
separate search and ingest selectors for an explicitly supplied candidate
fleet. Service names are derived from the instance UID, role and fleet. The
helper refuses a fleet currently recorded as serving, verifies owner, Service
shape and selector, and does not overwrite a conflicting Service. A Kubernetes
1.32 API-server test created and re-observed both Services, rejected an external
selector edit without overwriting it, and rejected the serving fleet. The full
Go suite and plan verifier passed. These are ClusterIP Services, not an access
control boundary. Preview authorization and production candidate qualification
remain missing.

An unwired HTTPS route probe now reads the shell's local lease through a
caller-supplied route and matches the responding node name to a qualified,
owned and Ready Pod. It checks the Service UID, selector and resource version
before and after the request, rejects redirects, proxy transport and disabled
TLS verification, and bounds the request and remaining lease. It can combine
the result with EndpointSlice observation. A local HTTPS-server test with fake
Kubernetes objects exercised this combination, a redirect, an unqualified Pod,
an insecure client and a Service change during the request. The full Go suite
and plan verifier passed. The probe is not enabled by the reconciler: callers
must supply the correct production path, a trusted certificate-verifying client,
any client certificate, and qualified Pod identities. HTTP authorization
headers and route configuration are not implemented. A single response is not
proof of all edge targets, peer execution, warmth or future availability; no
real ingress, ALB or production authentication was qualified.

An unwired rollout route observer now combines the existing Kubernetes
EndpointSlice observer with an independently supplied production-route probe.
Both observations must match the applied role, fleet and Service UID/version,
be fresh and follow the applied time. The responding Pod must be in the
qualified set and the observed ready endpoints. The combined timestamp is the
earlier observation. Unit tests use fake Kubernetes objects and injected edge
evidence to reject mismatched versions, identities, absent endpoints and stale
or future probes. The full Go suite and plan verifier passed. The HTTPS edge
probe is not enabled or production-qualified, and this helper trusts its caller to establish actual
Pod placement, process qualification and probe authenticity. No ingress, ALB,
network traffic or end-to-end promotion was tested.

The unwired rollout coordinator now reads both role Services from the API
before planning or recording a transition. Before proposing a selector patch,
recording convergence or reporting completion, it holds for a missing, stale,
replaced, invalid or conflicting role Service. It still records an observed
applied selector first after an ambiguous response, even if the other role is
conflicting. A Kubernetes 1.32 API-server test used actual Service objects and
showed a valid search patch plan followed by a hold when the ingest selector
was externally changed. The test supplied synthetic candidate evidence and
instance claim; it did not run the manager or change a selector through the
rollout executor. The two Service reads and a later patch are not atomic;
the executor must revalidate and conditionally patch, and cross-object races
remain an integration concern. The full Go suite and plan verifier passed.

Rollout role status and unwired persistence helpers now pin both prepared
intents in one optimistic status update and record observed application or
convergence one role at a time. The helpers check operation identity, claim,
expected role mapping and serving intent before planning. They never patch a
Service. The recorded application time is rounded forward to microsecond
precision so serialization cannot make earlier route evidence appear newer.
The Kubernetes 1.32 API-server test preserved fields, rejected a stale writer
and checked timestamp precision. A fake-client test re-read progress after an
ambiguous switch response and required route evidence before proceeding. The
full Go suite and plan verifier passed. The instance in the API-server test is
synthetic; claim changes and Service writes are not transactional across
objects. The operation reconciler, candidate provisioning, evidence producers,
claim transfer and actual switch execution remain unwired.

The unwired switch planner can now adopt an unchanged role in mixed-mapping
rollback. It observes the expected target Service and requests a durable
applied record without patching the Service. Candidate qualification and
route evidence observed after the recorded adoption time are still required.
The controller adapter rejects adoption for promotion or a changed selector.
Unit tests cover either role already on the rollback target and stale route
evidence. This is a planning protocol only: integrating persistent progress,
claim transfer from the superseded operation, route evidence producers and a
rollback executor are not implemented or qualified.

The unwired fleet switch planner sequences search before ingest and rechecks
previously converged roles against fresh candidate and route evidence. Its
unit tests cover an ambiguous successful patch, loss of candidate qualification,
changed Service version and inconsistent progress. It validates both role
intents before proposing a first switch. The planner itself does not persist state;
candidate provisioning, evidence producers and the operation executor remain
unimplemented. The full Go suite and plan verifier passed.

The reusable `EffectiveRequests` function now returns `(resources, error)` and
includes Pod-level requests conservatively, using the greater of each
Pod-level and aggregated container request before adding overhead. A missing
Pod-level request with a limit is also treated conservatively. Pod or
container dynamic resource claims and negative inputs return an error, which
both fit callers propagate. This changes the package API. Unit tests cover the
larger Pod request, smaller Pod request, limit fallback and claims. With
`PodLevelResources` enabled, the Kubernetes 1.32 scheduler fixture rejected a
Pod-level CPU request above the fit boundary and accepted an exact-fit one,
matching the reusable calculation. The 1.32 alpha API rejected combinations
of Pod-level resources and container ephemeral-storage requests, so these
Pod-level cases omit those requests and the development dedicated profile
correctly rejects them under its separate positive-ephemeral-request rule.
The full profile's earlier sidecar/agent/overhead case remains tested without
Pod-level resources. Newer Kubernetes, resource claims and real autoscalers
remain unqualified.

The Kubernetes 1.32 scheduler fixture now compares admitted-Pod fit with a
synthetic node's actual scheduler decision. A pre-bound agent consumes one CPU;
the test Pod has a restartable init sidecar, a later installer and RuntimeClass
overhead applied by admission. At 3.01 CPU of effective Pod demand, the
operator rejected fit and the scheduler reported insufficient CPU. At exactly
3 CPU, both accepted the Pod and it bound to the intended 4-CPU node. The
fixture passed in WSL. The agent is a synthetic Pod, not a DaemonSet controller
or kubelet, and no Karpenter, GKE, live agent rollout or cloud node is involved.

Retired creation cleanup now reads each recorded Pod name directly instead of
filtering a Pod list by the expected instance label. An occupied name with a
missing label or different owner is detected and held; it is not adopted or
deleted. The API-server test removed the late Pod's instance label, observed
the hold, restored it and then observed UID pinning and withdrawal. Unit tests
cover unlabelled and foreign Pods. The Go suite and API-server test passed.
The tradeoff is up to 64 namespaced Pod GETs per reconciliation while the
retired records remain, in addition to the existing 64-record cap. The manager
also watches Pods by an indexed retired name in the same namespace, including
unowned Pods. Unit tests exercise the name-to-instance mapping, and a WSL
envtest manager test observed the indexed watch for a newly created unowned
Pod. Watch delivery remains eventually consistent.

Stale creation and gate-release recovery now runs before current security,
TLS, storage and image qualification when generation, fleet, or a computable
template revision proves the persisted intent is stale. It only records or
finishes withdrawal; new creation and gate release still require their normal
checks. The Go suite and WSL API-server tests passed. The API-server test
removed the current web-identity image qualification during both stale
creation and stale gate-release recovery. A changed peer DNS domain now
retires the missing old intent and allows a fresh sequence after the domain
is restored. If the domain cannot be computed and generation and fleet still
match, a revision-only mismatch remains unknown and normal validation holds.
An active operation prevents initiating this preflight recovery.

Missing stale Pod-creation names are now retained as durable retired records
before the controller allocates another sequence. A late matching, still-gated
Pod is discovered by a direct read of the retired name, its UID is persisted,
and it is withdrawn with the guarded delete. The record remains after removal
because a delayed create has no proven upper time bound. Records are capped at
64 per instance; reaching the cap holds further retirement. Changed UIDs,
removed gates, binding and conflicts with active work hold cleanup. Unit tests
cover conflicts and the cap. An API-server test retired a missing name,
allocated a new intent, injected the delayed Pod, restarted after UID pinning
and observed deletion while retaining the record. The Go suite and API-server
test passed. This is development behavior, not an admission fence: matching
still requires the expected instance label, initial UID adoption depends on
the namespace trust boundary, and there is no safe record garbage collection or
automatic migration of names retired before this mechanism existed.

For a stale persisted creation intent, the development controller now recovers
an already-created Pod if its owner, intent annotation, labels, name, UID,
unbound state and scheduling gate match. It atomically records that observed
UID in a withdrawal decision and clears the creation intent, then resumes the
same guarded withdrawal state machine after restart. The API-server test
simulated a delayed create, restarted after recording withdrawal and observed
a fresh creation intent. Unit tests reject conflicting owner, labels, binding and gate. The Go
suite and API-server test passed. A missing target now follows the retired
name path above. The UID was not previously persisted, so the initial
adoption depends on namespace trust and the recorded owner and intent
metadata. Source resolution can still hold recovery when staleness cannot be
established independently.

The development controller now recovers a stale persisted gate-release intent
by first recording `withdrawalRequested`. After restart it rechecks the exact
Pod and withdraws it only while still gated and unbound, using UID and
resource-version preconditions. Once absence is observed it clears matching
warm-up and cold-bootstrap slots and the release intent, preserving the
release-rate reservation. A gate already removed is instead tracked as a
provisioning attempt. The API-server test persisted withdrawal, restarted the
reconciler, observed deletion and a new Pod release, and checked both release
reservations. Unit tests cover recovery when the gate was already removed; the
executor's API-server race test covers concurrent gate release. The Go suite
and plan verifier passed. Stale pending-creation intents with no observed Pod
now follow the bounded retired-name path above because Kubernetes does not
atomically fence Pod creation against instance status.
When staleness cannot be established from generation, fleet or a computable
revision, unavailable current sources can still hold this path.

The reusable Pod lifecycle package now offers `WithdrawGatedAtVersion`. It
re-reads the target, checks the controlling owner, UID, exact resource version,
unbound state and named scheduling gate, then deletes with UID and version
preconditions. Unit tests cover changed identities, gate, binding and version.
A WSL API-server race test removed the gate between the read and delete; the
delete conflicted and the released Pod remained. The controller now uses the
primitive for stale gate-release intents as described above; simply seeing an
absent Pod is not an atomic fence against a stale creator.

Development Pod revisions now include an explicit operator-owned template
version, covering the cross-namespace host affinity and token-mount changes
that are not represented by the instance spec. Stale creation intents with a
matching gated Pod and old gate-release intents follow the withdrawal paths
above. This does not roll existing Pods.

The Kubernetes 1.32 scheduler fixture now includes search and ingest nodes
in both zones. After the two zone-A search nodes were made unschedulable, a
zone-A search replacement stayed pending even with an available zone-B search
node and a zone-A ingest node. A zone-B ingest Pod still bound to its intended
node. The test passed with synthetic nodes; cordoning represents unavailable
scheduling capacity, not an AZ failure or actual node recovery.

The opt-in `TestDedicatedHostsAcrossNamespacesWithScheduler` passed in WSL
against Kubernetes 1.32.0 kube-scheduler and an envtest API server with
synthetic Ready nodes. The scheduler placed two dedicated Pods from different
namespaces on different hosts, reported anti-affinity for a later shared Pod
and a third dedicated Pod, and bound the third Pod to the freed host after
deletion. The scheduler binary was downloaded from the Kubernetes release
site and its SHA-256 matched the published checksum. Set
`KUBE_SCHEDULER_BINARY` and `KUBEBUILDER_ASSETS`, then run `go test -tags
envtest ./internal/workload -run TestDedicatedHostsAcrossNamespacesWithScheduler`.
Synthetic Nodes do not exercise kubelets, an autoscaler, actual zone binding,
provider topology or disruption. The first attempt exposed the initial
not-ready taint because envtest lacks the node lifecycle controller; the test
now removes that synthetic taint after reporting each node Ready.

Dedicated development Pods now use required hostname anti-affinity against
this operator's instance-UID label in all namespaces, rather than only the
same instance and namespace. This includes shared-node operator Pods and both
roles and fleets when the scheduler evaluates placement. The profile renderer
also requires the Pod's instance label to match its supplied owner identity.
The platform must still enforce node-pool taints and trust boundaries: an
unlabelled or directly bound workload is outside the selector. Existing bound
Pods are not migrated, and an in-flight gated Pod with the old affinity may
need recovery. The local scheduler test above does not qualify Karpenter or GKE.
The Go suite and plan verifier passed, and a WSL API-server test confirmed the
cross-namespace selector survived admission.

`tools/prepare_java_runtime.py` now archives the pinned Java commit into a new
directory, applies the three operator overlays and can build the runtime with
the peer-TLS source. Its staged output contains a manifest of commit, overlay,
source and JAR hashes. The development Dockerfile now consumes that staged
bundle, and a Dockerfile-specific ignore file excludes the full source tree.
A clean archive and offline Gradle bundle build passed on Windows with JDK 25;
the Java web-identity fixture passed against the staged bundle, whose 141 JAR
hashes matched its manifest. Docker
Desktop's Linux engine was unavailable, so no image build or container runtime
test was performed. The manifest is not signed build provenance and does not
prove the image's base, platform or runtime qualification.

The local web-identity store integration now also tests the operator's
lease-aware serving check. After a writer became eligible, the S3 proxy denied
its signed requests. Following expiry of a 12-second test lease, the check
rejected the writer; it became eligible again when store access returned. A
fresh process whose token the STS fixture rejected never passed the check in
five one-second samples. The complete local test passed on Windows outside the
sandbox. It does not test Kubernetes endpoint removal, real AWS denial, a
search node, or behavior beyond the observation window.

`TestWebIdentityS3WriteAndReplacement` passed outside the Windows sandbox with
the patched local Java bundle and SeaweedFS. It created a disposable bucket,
required the STS fixture's synthetic credentials at a local S3 proxy, wrote a
document, killed the writer, changed the token file, and recovered the
document with a fresh Java process that used the new token. The first test
attempt encountered an ambiguous index-create response followed by
`index_already_exists`; the test now recognizes that observed outcome.
This fixture does not validate signatures cryptographically, cloud IAM,
Kubernetes, AWS S3 conditional-write semantics or multi-node recovery. Run it
with `OPERATOR_JAVA_RUNTIME_LIB` and `OPERATOR_SEAWEED_EXE` set and
`go test -tags java_integration ./internal/java -run
TestWebIdentityS3WriteAndReplacement`.

The local Java S3 identity fixture now rotates its synthetic token file and
returns different short-lived credentials from STS. The real patched shell
requested credentials using the new token and signed a subsequent local S3
fixture request with those new credentials in three consecutive runs. This
does not exercise Kubernetes atomic-writer token projection or actual AWS.
The rendered identity Pod now requests that the default EKS identity webhook
skip all intended Java, sidecar and init containers, and gate validation
requires the annotation to survive admission. This avoids conflicting default
webhook injection when the webhook honors the annotation. Admission order,
other webhook configurations, injected later containers and real EKS remain
unverified; additional projections are not ruled out by this annotation.

The development controller now connects the S3 web-identity building blocks
to `credentials.type: WorkloadIdentity` and `credentials.awsRoleArn`, behind
`--development-s3-web-identity-image` matching the instance image. The role
and credential mode are part of the spec-derived Pod revision. Rendered Pods
receive the projected token and Java property, and admitted Pods are checked
before scheduling gate release. The Go suite, plan verifier and WSL API-server
creation and gate-release test passed. This is an experimental path: provider
webhook compatibility, real kubelet token projection, AWS IAM trust and real S3
remain unqualified. A webhook that changes Java's environment will cause
admission validation to reject the Pod.

The development building block renders an AWS web-identity token
projection with the STS audience, one-hour requested lifetime, an IAM role
environment variable and a read-only Java-only mount. It rejects conflicting
mounts or existing AWS environment entries; its admission checker detects
changed projection, Java environment, mount shadowing and token mounts in
sidecars or init containers. The managed JVM renderer reserves and can emit
the same token path. Unit tests cover these contracts; a WSL envtest run also
passed after creating the projected Pod and checking the API-server read-back.
The reconciler now connects this helper to the development CRD, image
qualification, persisted Pod creation path, and gate release. A platform
webhook may inject additional credentials or token projections and needs an
explicit provider contract. Cloud trust, kubelet permissions and refresh are
not established by these tests.

The experimental `java/patches/s3-web-identity-setting.patch` forwards exactly
`s3.client.default.identity_token_file` from JVM properties to the S3 client.
The patched shell bundle compiled with Gradle and JDK 25. The opt-in Go
`java_integration` test launched that shell, supplied a synthetic token and
role, observed an AssumeRoleWithWebIdentity request at a local STS fixture, and
observed the returned temporary credentials signing a request to a local S3
fixture. This is not a cloud IAM, Kubernetes projection, credential refresh,
or successful S3 data-operation test. The patch is not yet part of a release
image and the development path does not establish those boundaries.

New development Pods explicitly disable automatic Kubernetes ServiceAccount
token mounting for all containers. Admission verification rejects a missing or
enabled setting before gate release. Unit tests exercise both mutations. The
Windows Go suite, plan verifier and WSL envtest API-server read-back passed.
This does not stop another admission component from adding its own projected
token; provider-specific projections and their consumers must be checked by
the future identity integration. Existing in-flight gated Pods created before
this change can fail the stricter check and need explicit recovery. Sidecars
that need a Kubernetes API token need a separately designed projection; the
current development template does not provide one.

The unmodified pinned Java runtime does not qualify for the development
`WorkloadIdentity` path. At the pinned Java
revision, the S3 client enters its IRSA path only if at least one IRSA setting
is supplied; environment variables alone do not create that configuration.
The shell's JVM-property allowlist also does not forward `s3.client.*`.
Its fallback can select the AWS SDK container credentials provider when the
container-credentials URI is present, otherwise instance profile credentials.
The operator's exact admitted Java environment check can reject provider
injection before startup. These are source observations, not cloud tests.
Qualify provider admission handling, cloud trust, refresh and actual S3 access
before treating the development path as supported.
The omitted credentials field still permits ambient provider fallback and is not
evidence of a particular identity. Kubernetes API-server tests do not exercise
provider webhooks or cloud credentials.

For M09, the opt-in development direct-Pod controller persists plugin intent,
stages immutable role-specific request and policy snapshots, pins their UIDs and
hashes, and mounts a digest-pinned installer before releasing a gated Pod. Go
tests cover role resolution, input identity and admission mutation; an API-server
test covers the staging and gate path and same-name snapshot replacement.
A Linux subprocess test runs the installer CLI against a local TLS registry
protocol fixture with Basic authentication. It verifies rejection of an
untrusted certificate, bad credentials and corrupt blob, successful retry,
offline recovery of a completed set, and rejection of a changed request hash.
It also kills the installer during a blob download and verifies that retry
cleans the abandoned private staging directory before publishing. The
installer assumes one trusted writer for its private staging parent.
`go test ./cmd/plugin-installer -run TestInstallerCLIPrivateHTTPSRegistry`
passed in WSL; the Windows Go suite and `python tools/verify_plan.py` also
passed. The fixture is not a real registry deployment or a kubelet. An opt-in
`java_integration` test compiled a minimal plugin JAR, installed it through
`plugins.InstallSet` from a local HTTP registry protocol fixture, and observed
its constructor and Java readiness using the pinned local Java bundle, JDK 25
and OpenSearch 3.10.0 on Windows. Run it with `OPERATOR_JAVA_RUNTIME_LIB` set
to that bundle and `go test -tags java_integration ./internal/plugins -run
TestInstalledPluginLoadsInJavaShell`. This is not a real plugin compatibility
test or a Pod test. The development controller also retains its provisioning slot after binding
until the pinned plugin installer succeeds or its timeout is isolated by role/AZ.
The bound Pod remains in place; the isolated hold clears only when initialization
and Kubernetes readiness both succeed. Unit tests
and an API-server test cover the hold and recovery with synthetic init status.
A real kubelet and registry outage test remains necessary.
Secret-by-name mount races, registry deployment and
token-auth qualification, installer image build, kubelet volume isolation,
production plugin compatibility, revisions and rollout remain open.

The development provisioning slot now remains held through Pod startup for all
roles, until the same bound Pod is Running and Ready. Startup timeout and a
terminal Pod block the affected attempts; a Ready observation clears the former,
and the latter becomes an explicit role/AZ retry hold.
This serializes startup and uses Kubernetes readiness rather than proof of
warmth. A persisted release window now also limits repeated replacements after readiness.
Unit tests and API-server tests exercise synthetic status and recovery; real
kubelet and provider behavior remains unverified.

The development gate-release budget reserves each Pod UID and role/AZ in the
instance status in the same optimistic patch as release intent. Defaults are
32 per role/AZ per ten minutes, configurable through development manager flags.
History is retained for one hour with a 256-entry instance cap. Unit tests
cover group isolation, window changes, clock reversal and bounded history; the
API-server test checks persistence, a rate hold and a stale concurrent status
update. This also throttles initial scale-out. The planner skips rate-limited groups
before creating a Pod; an already-held or unbound Pod can still block other AZs. Provider node launches per pending Pod remain unbounded.

The release-aware planner checks each deficient group before creation and
selects an eligible role/AZ in the lifecycle planner’s deterministic order. It
checks the release budget again before persisting the actual release intent.
Unit tests and the API-server test cover skipping a throttled zone and waiting
when all deficits are throttled. Real scheduler and provider recovery remain
unverified.

The development controller now moves a `PodGone`, `PodTerminating` or `PodTerminal`
provisioning hold with recorded role/AZ into bounded instance status, then
allows other groups to replenish. Creation and gate release refuse a blocked
group. `RetryProvisioning` still requires the old Pod name to be absent and
clears only the acknowledged Pod UID, retaining other group holds. Legacy holds
without role/AZ remain instance-wide. Unit tests and API-server tests cover
status persistence, a surviving AZ, name reuse and operation restart; bound startup timeouts move into observed role/AZ holds; an active unbound
attempt still occupies the global slot until withdrawal. A terminal search Pod can also retain its warm-up slot until the
existing slot recovery rules clear it. A real autoscaler can continue launches already in flight, and
provider/AZ outage tests remain unrun.

A terminal startup Pod now moves to the same per-role/AZ hold without deleting
it. The API-server test verifies that another AZ can be selected while the
terminal Pod exists and that explicit retry remains blocked until its name is
absent. It uses synthetic Pod status, not a real kubelet failure.

Bound `StartupTimeout`, `PluginInstallTimeout` and `PeerDNSPreflightTimeout`
now move to role/AZ holds, while the controller continues observing the pinned
Pod. It clears only that hold after required init statuses and Running/Ready
match; missing, replaced, terminating or terminal Pods remain held with a
corresponding reason. The API-server test covers startup and DNS recovery, and
unit tests cover plugin completion, Pod replacement and disappearance. A live
kubelet, cloud recovery and Java cache warmth remain unverified. Legacy
attempts lacking role/AZ remain instance-wide, and a search warm-up slot can
still hold other search warm-ups.

The development zone-capacity status records per-role/AZ Ready shortfall,
minimum comparison, unbound, terminating and terminal Pod counts, and the
last successful observation time and generation. Unchanged samples refresh at
most once per minute during reconciliation; failed list or owner checks do not
refresh the timestamp. Kubernetes Ready counts require a bound Running Pod and
one Ready condition. Unit tests cover impaired groups and sample freshness; an
API-server schema test checks the new fields. The zone is a Pod label, not
verified node placement, and these counts do not establish an AZ failure,
traffic convergence or warm cache.

The development controller now prioritizes a deficient role/AZ group below
its Kubernetes Ready `minServing` before deficits in groups at their minimum.
This only orders existing creation proposals; it does not prove Java serving
or warmth and does not create surge above desired count. Blocked and
rate-limited groups remain excluded. Unit tests cover the priority and fallback;
an API-server test verifies the selected creation intent. Scheduler, client
traffic and provider behavior remain unverified.

M07 has an opt-in development-only role-Service sample wired into normal reconciliation. It makes one
Java stats request through the stable Service, matches the returned node name
to an independently supplied Pod UID set and an owned Running/Ready Pod, and
checks the Pod and Service resource versions again. It rejects redirects,
expired leases and configured HTTP TLS or security. Unit tests cover identity, rotation, rate limiting, failed-sample replacement,
lease, redirects and intervening mutations; an API-server test exercises normal
reconciliation with synthetic HTTP and real Service/Pod objects. Plain HTTP cannot authenticate the responder,
and a single Service sample does not prove ingress, ALB, all backends, cache
warmth or promotion convergence. No rollout operation uses this diagnostic sample.

M07 now also has an unwired one-role transition planner in `internal/rollout`.
It requires a persisted switch intent and matching claim, fresh Service and
candidate identities, qualified capacity, peers, security and rollback
compatibility before proposing a selector patch. A target selector observed
after an ambiguous patch is recorded before any further action. Convergence
requires fresh candidate evidence plus EndpointSlice and client-probe evidence
bound to the same Service UID and resource version after the applied time.
Unit tests cover stale, conflicting and missing evidence. These are supplied
assertions, not live qualification. A controller helper now reads the actual role Service, matches the operation
claim and recorded serving intent, and feeds the observation to the planner. A
second helper binds a proposed patch to the exact observed Service resource
version; a change requires a new plan. The API-server test uses synthetic
candidate qualification and verifies a stale plan cannot patch, then replans
and observes the switched selector. The caller must still revalidate the claim,
intent and application evidence immediately before the effect. No operation
reconciler invokes these helpers, creates preview fleets or orchestrates
promotion and rollback; the user-facing operation remains unsupported.

The S3 resolver validates endpoint scheme, host and port before pinning public
repository configuration. It rejects URL user information, queries and
fragments, and does not echo invalid endpoint or `pathStyleAccess` values in
errors. Unit tests cover inline and ConfigMap sources, while an API-server test
checks that a rejected endpoint does not change pinned store identity or leak
its test secret into status. Endpoint paths remain public values and must not
contain secrets. This is input validation, not S3 connectivity, identity refresh
or provider conformance.

M07 has an unwired `switchRoleService` primitive guarded by Service UID,
resource version, full expected selector, owner and routing shape. An API-server
test covers a competing selector update, lost successful response and
same-name/new-UID replacement. It does not qualify fleets or orchestrate
promotion, rollback, route convergence or client traffic. Role Service checks
also reject publishing unready endpoints and node-local internal traffic.

An opt-in development EndpointSlice observer records a timestamped Kubernetes
sample through normal reconciliation. It checks exact Service and slice
ownership, ready Pod UIDs, addresses, role/fleet labels and zonal minimums. Go
unit tests and an API-server test with synthetic EndpointSlices pass. The
optional separate Role grants namespaced list access. It does not qualify Java
or actual placement or authorize promotion; real controller/proxy and traffic
evidence remains necessary and the API reads are not atomic.

Use this record when transferring a task between agents or reviewers. Keep it with the change description or review rather than creating a new file for each task.

```text
Task ID and goal:
Current source revision and changed files:
Contracts and callers inspected:
Behavior implemented and important decisions:
Failure, retry and concurrency cases covered:
Commands run and observed results:
Acceptance criteria still unverified and why:
Known limitations and follow-up task IDs:
```

Reviewers trace the stated acceptance criteria to implementation and test evidence. For stateful actions inspect both the successful path and the outcome when the controller restarts before and after an external effect. Report actionable defects with the affected file and a reproducible trigger. Resolve findings or explicitly identify the remaining risk before claiming the task is complete.

## Current lifecycle handoff

- **Task:** M02 reusable Pod lifecycle; retirement-claim recheck for future replacement reconciliation.
- **Changed files:** `pkg/podlifecycle/replacement.go`, `pkg/podlifecycle/replacement_test.go`, and `docs/implementation/verification.md`.
- **Contract:** the caller proves ownership of a durable claim and supplies a complete, fresh snapshot; the planner does not persist claims or perform Kubernetes effects. Recheck clears only that claim in a copy and requires the planner to select the same victim again. Other retirement holds and current qualification continue to constrain the decision.
- **Verification:** `go test ./api/... ./pkg/... ./internal/... ./cmd/...` passed; `python tools/verify_plan.py` validated 63 tasks. The tests cover loss of qualification, a competing hold, a terminating victim, stale observation, identity mismatch, and preservation of the input snapshot.
- **Remaining:** this path is not integrated with a controller or tested with real eviction. Durable rollout state, application warmness qualification, authoritative re-observation, PDB admission and recovery from ambiguous Kubernetes effects remain required for M02/M06/M07 and TLS rotation. No cluster or cloud behavior is claimed by these unit tests.

M02.03 additionally has a real API-server binding-race test in
`pkg/podlifecycle/kubernetes/withdraw_envtest_test.go`: binding between the
read and delete produces a conflict and preserves the bound Pod. Run with
`go test -tags envtest ./pkg/podlifecycle/kubernetes -run TestWithdrawUnboundBindingRaceOnAPIServer` and a configured `KUBEBUILDER_ASSETS`. This is API-server evidence, not a scheduler or cloud test.

M02.04 has partial API-server evidence in `eviction_envtest_test.go`: PDB denial
preserves the observed Ready Pod, and a successful eviction with an injected
lost response is resolved by rereading the terminating UID. The old UID cannot
act on a same-name replacement. Envtest uses manually supplied PDB status and
force-completed deletion; real PDB-controller behavior, Java drain, durable
rollout recovery, and live traffic evidence remain outstanding.

M06.01 has an `internal/java.QualifySearch` adapter. It requires a
completed warm-up without timeout, a serving role and valid lease, and the same
patched Java process incarnation before and after the check. It records only
bounded observational evidence, not strict cache residency. Unit tests exercise
failure states and restart; Pod/container identity binding, fresh repeated
observation, actual Java restart and cache qualification remain required before
using the result to authorize a retirement.

The controller has a `qualifySearchPod` helper that brackets a direct
probe with Pod API reads and returns evidence tied to owner, Pod, container and
process. A development-only plaintext caller requires the explicitly selected
image and rejects HTTP TLS or security configuration. The Go suite and plan
verifier pass. The gated development controller records one timestamped,
historical sample per minute, rotating Pod UIDs, and the API-server test verifies
the status schema round trip. A production authenticated transport, aggregate
capacity reporting and rollout use remain unimplemented; test with real API
mutations and Java before using the evidence for retirement.

The experimental Java warm-up overlay includes a deadline recheck when a
queued peer retry starts, so executor delay cannot start a new copy after the
retry window. `SearchWarmupTests` now directly invoke the queued callback with
an expired deadline and with a draining node, verify degraded status and that
no readiness transition starts, and passed with `-Dtests.iters=3`. A real local
short-budget restart still ended degraded. Actual executor saturation and
Kubernetes/cloud behavior have not been exercised.

The health probe has an opt-in `-require-warmup-completion` search mode. The
development Pod template selects it for strict successors. Go tests and a local
Java smoke cover ready-but-degraded rejection and peer-warmed acceptance.
TLS-enabled Pod templates, Kubernetes traffic and the distinction between
warm-up completion and actual residency remain open.

The experimental warm-up overlay now offers an opt-in Java process-local gate
for local reader readiness and peer role advertisement. It is disabled by
default and selected by the development controller only with an explicit
degraded-bootstrap policy. Java tests and a local smoke cover successful
warm-up and degraded restart; live Kubernetes routing and strict cache
residency remain unqualified.

`coldStartPolicy: AllowOneDegraded` reserves one bootstrap Pod in instance
status before creation and pins its UID afterward. API-server tests cover this
sequence, a strict successor and an edited bootstrap mode. The API-server test
also exercises normal reconciliation through PDB observation, bootstrap gate
release, warm-up slot completion and strict successor creation and gate release
with simulated PDB, binding and Java observations. The bootstrap can
serve cold for its entire lifetime and counts toward PDBs. This path has no
automatic scale-down or complete recovery from a strict Pod stuck after the
bootstrap disappears; it requires real-cluster qualification before production.

Development Pod admission now validates the complete readiness probe rather
than just its handler, preventing admission from changing its timing or failure
threshold. Go tests and the Kubernetes 1.32 API-server reconciliation test pass;
the latter covers API defaulting, not real kubelet or EndpointSlice behavior.
