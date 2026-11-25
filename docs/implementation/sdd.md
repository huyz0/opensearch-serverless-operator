# Software design description

The operator reconciles Java OpenSearch Serverless fleets in Kubernetes. The current Java source baseline is `e71d42331a3ec82ec01109a5bb42d6a9a9162a5c`. The detailed design chapters own the behavior. This document records the architecture and acceptance boundaries agents must preserve when translating that design into Go types, controller code and tests. [Architecture and design index](../design/README.md) · [Implementation plan](README.md)

## Responsibilities and dependency boundaries

| Area | Owns | Uses | First tasks |
| --- | --- | --- | --- |
| `api/v1alpha1` | Versioned instance and operation input and status. Generated OpenAPI, structural field validation and conversion when versioning requires it. | Kubernetes API types. [Draft API contracts](../../config/contracts/README.md) | M01.02, M01.03 |
| `pkg/podlifecycle` | Deterministic plan from snapshot, group identity and budgets, plus guarded Kubernetes actions. | Workload-neutral value types and Kubernetes APIs at execution. | M02.01–M02.05 |
| Operator reconciliation | Resolve inputs, maintain instance-owner identity, persist intent, observe work, claim operations and choose a bounded effect. | API, lifecycle, Java, placement, serving and security adapters. | M04.01–M04.05 |
| Java runtime adapter | Pinned args and settings, process-local eligibility and application observations, supported admin operations. | Real shell endpoints, metrics and startup packaging. | M03.01–M03.05 |
| Placement adapter | Shared or dedicated placement, qualified role/AZ profiles, admitted Pod fit and pending budget. | Kubernetes scheduling, namespaced platform inputs and optional provider observations. | M05.01–M05.05 |
| Traffic and revision adapter | Stable role Services, per-role conditional switches, preview/rollback retention, endpoint and route observation. | Kubernetes, chosen edge integration and Java capabilities. | M07.01–M07.06 |
| Security and runtime inputs | Source validation, immutable inputs, checked TLS, credentials and OCI plugin delivery, separate hot security revisions. | Approved sources, qualified Java provider or gateway, OCI registry. | M08.01–M09.04 |
| Java and platform | Java owns leases, fencing, WAL, segments and shard recovery. The platform supplies nodes, IAM, primary store and edge policy. | The operator observes these boundaries and uses explicit adapter capabilities. | M03.04, M05.04, M05.05, M10.02, M10.03 |

An operation follows `observe → plan → persist intent → execute a bounded effect → read back → record observations`. Read authoritative state again immediately before a mutation whose target or eligibility may have changed. Reconcile from observations after restart or an ambiguous response. Pass deadlines and an injectable clock into time-dependent planning. Stable keys include Kubernetes owner and Pod UIDs, role, AZ, fleet, template revision and process incarnation where the observation is process-local. Maintain bounded history for diagnosis.

## Identity and placement invariants

- Creation intent and owner UID control adoption. Name reuse may be legitimate, so execution must verify UID and applicable resource-version or compare-and-set preconditions. A slot and physical node are different resources. Count each released pending request, terminating Pod and committed retirement once.
- Floor calculations draw from the role Service's included fleet and process-local eligibility, with separate counts for Java peers, warm search caches and indexing takeover. Deduct a selected victim and already committed retirements. PDBs enforce ordinary zonal eviction admission. A provider or an AZ fault can exceed a zonal PDB and a separate role-wide floor.
- Kubernetes placement comes from the admitted Pod and validated Kubernetes/provider observations. Compute fit after admission additions. Unknown AZ and profile evidence remain unknown. Optional observations and hints improve planning when present. The default ability to observe and replenish reserves uses namespaced permissions.
- Provisioning uses bounded scheduling slots and durable holds. A failed or stale request is withdrawn only while the selected Pod is still unbound. Worsening an existing shortfall requires a deliberate recorded policy exception for a committed operation. [Placement and recovery](../design/placement-and-recovery.md) · [Reconciliation budgets](../design/reconciliation.md#availability-and-resource-accounting)

## State transitions and failure cases

| Work | Transition and guard | On restart, rejection or ambiguity |
| --- | --- | --- |
| Create and bind | Record desired group, intent and name. Create owned Pod with gate. Observe its admitted resources and selected profile, then remove the gate if a slot is available. | Re-read exact owner and intent after an ambiguous create. Re-evaluate slot, UID and qualification before removing a gate. Conflicting creator or generation blocks progress. |
| Qualify and serve | Boot Java against the pinned store. Confirm lease and role, effective process readiness and any strict warm requirement. Use stable role Service selectors. | A stale custom condition cannot qualify a restarted JVM. A warm timeout, denied Secure Secret or failed OCI artifact yields a distinct failure. |
| Retire | Verify exact victim and post-retirement floor. Submit PDB-governed eviction. On accepted termination, perform the supported local Java handshake and withdrawal. | A denied eviction leaves the victim serving. An ambiguous response is resolved using UID and deletion state. No replay of an irreversible application effect based only on a timeout. |
| Change fleet | Stage qualified revisions, persist expected per-role mappings, compare and set one selector, then verify observed convergence before continuing. Retain approved rollback capacity. | Observe both mappings after a crash. A saved mixed mapping is recoverable. Resolve conflicting selector ownership without overwriting. Rollback checks current policy and compatibility. |
| Change fleet weights | Record ordered role schedules, both route identities and their non-weight spec digests, and backend Service UIDs before mutation. Patch one route and persist the read-back time before accepting proxy convergence. | A lost response can be recovered from the route weights. Progress waits for post-read-back evidence and holds on route drift or a changed claim. |
| Change shared cluster setting | Validate a scalar persistent patch, claim explicit operation, write with private identity and read back affected keys and effective behavior. | An unknown response uses read-back and the last known expected values. Shared configured state survives fleet rollback. |
| Rotate TLS or security | Validate inputs. Rotate leaf and trust by guarded waves, or stage a security revision and compare-and-set activation for a qualified hot provider. | For invalid precommit input, preserve the prior revision. After commit, reconcile forward or from a new approved recovery operation. Fence stale revisions at security admission when revocation requires it. |
| Decommission | Claim owner and phase, withdraw eligible unbound work and retire owned serving Pods under explicit policy. Observe old peers/leases before reattachment. | Deletion admission and garbage collection can interrupt orchestration. Keep external store, identity and source inputs outside ownership. |

The transition tests should force failures at boundaries between publishing intent, invoking the effect, and observing the effect. Fake clocks and API fault injection assist deterministic tests. Actual API state proves concurrency and ownership semantics. [Durable operations](../design/reconciliation.md#durable-operations)

## Inputs and revision identity

Resolve and validate startup configuration from the instance and role. Pass literal JVM argv, managed settings and qualified plugin format through separate renderers. Hash canonical non-secret inputs, input version identifiers, runtime and installer digests and effective role settings into the corresponding immutable revisions. Never use a secret-derived digest as public status. Keep Java and registry credentials scoped to the consumers that require them.

`spec.storage.repositoryRef` resolves the platform's S3 configuration into pinned `serverless.store.*` settings. The remote backend is part of identity. `spec.snapshots.repository` selects a logical repository inside the primary store. The runtime's S3 bundle is already on its classpath. Hot security configuration and live cluster settings are independent of Pod templates and require observed effective application. [API](../design/api-and-resources.md#primary-s3-remote-store) · [Runtime](../design/runtime-configuration.md) · [OCI plugins](../design/oci-plugins.md) · [Security reload](../design/security-reload.md)

## Decisions and evidence still required

| Decision | Owner task and proof |
| --- | --- |
| Kubernetes, Go, generator and Java matrix | M01.01 selects compatible tool versions. M03.01 makes the pinned Java image reproducible. M10.05 sets the release matrix. |
| API shape and defaulting of platform profile/hint sources, admission features and upgrades | M01.02 and M01.03 finalize versioned inputs and secure references. M05.01 and M06.04 exercise the consumers. |
| Provider-side hints and operating privileges | M05.04, M05.05 and M06.04 qualify namespaced and opt-in modes. |
| Warm proof and precisely bounded fallback | M06.01 qualifies a process-local signal or tracks a Java change. M06.05 checks it under loss. |
| Edge and coexistence policies | M07.02, M07.04 and M07.06 verify real ingress, ALB and revision overlap modes independently. |
| TLS and policy mechanisms | M08.01–M08.06 establish per-provider enforcement, revocation and overlap with measured evidence. |
| OCI inputs and retained image compatibility | M09.03 and M09.04 prove secure delivery, JDK and image compatibility and rollback. |
| Performance and operability thresholds | M10.02–M10.05 attach measured bounds and support declarations. |

Record a design decision when a task changes a published behavior, support boundary or important assumption. State the evidence, the chosen behavior, the consequences for consumers and which fixtures or acceptance criteria must change. Update this SDD only for changes that alter its dependency or state-transition contracts.
