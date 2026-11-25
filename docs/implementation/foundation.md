# Development foundation

The foundation finishes with a real local deployment that can replace a Pod and recover its data from the same store. Keep provider integration and rollout policy outside the reusable Pod lifecycle package. [Architecture](../design/README.md) · [Reconciliation](../design/reconciliation.md)

## M00 — Development workflow

**Goal.** An engineer or agent can take one task from the plan, identify its affected contracts and produce reviewable evidence.

| Task | Prerequisites | Goal and deliverable | Acceptance evidence |
| --- | --- | --- | --- |
| M00.01 | — | Baseline the plan against the design and source. Publish stable task IDs, dependencies, exit gates, coverage and an engineering decision path. | The verifier rejects an unknown dependency, a cycle and an unmapped task. The coverage review includes the user's latest S3 repository and OCI plugin requirements. |
| M00.02 | M00.01 | Ship repo-local `AGENTS.md`, the `opensearch-operator-dev` skill, and a task and review protocol that requires observed evidence and concrete negative cases. | Read and exercise the workflow against an API change and a reconcile fault case. It selects the correct proof layer and records unrun checks without treating them as passes. |
| M00.03 | M00.01 | Establish offline rendering of draft CRDs and examples. Define the complete positive and negative API-server fixture set for M01. Keep support claims separate from local examples. | Run the offline renderer and verify a malformed resource fails rendering. Review the API-server fixture inventory against storage, placement, autoscaling, rollout, security and OCI inputs. |
| M00.04 | M00.01, M00.02, M00.03 | Define the architecture and state transitions in the SDD. Assign unresolved decisions to tasks or release gates. | Review ownership, each irreversible side effect and its restart behavior, along with every capability still waiting on Java or cloud evidence. |

**Exit.** The plans, draft contracts and actual local checks are navigable from the repository root. Future milestones identify which environment proves their behavior.

## M01 — Go project and API contracts

**Goal.** Bootstrap the controller project, pin tools and make the namespaced APIs enforce the agreed contract.

| Task | Prerequisites | Goal and deliverable | Acceptance evidence |
| --- | --- | --- | --- |
| M01.01 | M00.04 | Establish repository/module identity, Go and controller-runtime versions, reproducible build scripts and CI. Initialize source control using the owner's repository when supplied. Add source lint, unit, race and generation-diff gates. | Clean-checkout Linux runner produces the controller binary and generated manifests. A gate fails on a stale schema. Pin the supported Kubernetes test versions and log tool versions. |
| M01.02 | M01.01, M00.03 | Translate CRD contracts into Go types and controller-gen output. Define action-specific payloads, status, defaults, enum behavior, exact units and patch ownership. Decide which draft constraints belong in schema, admission or reconciliation. | API server accepts both CRDs, valid fixtures and `/status` updates. Reject malformed fields, stale operation shape updates, incompatible modes and unknown fields under strict validation. Test upgrades and old valid input where relevant. |
| M01.03 | M01.02 | Build the validator and input authorizer for same-namespace approved refs, sidecars, Security/TLS, OCI and platform profiles. Keep external reference resolution and capabilities in controller conditions. | Check a trusted namespace happy path and reject disallowed credentials, registry publishers, illegal templating overrides and lookalike platform hints. A forbidden check fails explicitly. |
| M01.04 | M01.02 | Wire least-privilege namespace RBAC, webhook identities if needed, leader election and fetch boundaries. Define resource-name restrictions or validated allowlists for externally authored inputs. | Real API-server authorization tests show required actions succeed. Deny Nodes, NodeClaims, cross-namespace Secret access and unrelated cloud credentials by default. Exercise leader handover without duplicate effects. |
| M01.05 | M01.02, M01.04 | Establish the testing ladder and reproducible fixture namespaces, using `envtest` for API semantics and a disposable cluster for scheduler, garbage collector and node lifecycle. | One normal and one negative fixture run at each available layer. Record which behaviours need kubelet, PDB controller, registry, Java or cloud components. |

**Exit.** Typed APIs, strict validation and the supported namespace install path are proven. [API contract](../design/api-and-resources.md) · [CRD validation](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#validation) · [Envtest boundary](https://book.kubebuilder.io/reference/envtest)

## M02 — Reusable Pod lifecycle

**Goal.** Provide a reusable package with deterministic planning and guarded Pod mutations, independent of the operator's CRDs and Java.

| Task | Prerequisites | Goal and deliverable | Acceptance evidence |
| --- | --- | --- | --- |
| M02.01 | M01.01 | Define the workload-neutral `pkg/podlifecycle` snapshot, identities, placement groups, budgets, actions and reasons. Define ownership of clocks, retry state and application eligibility. | A minimal consumer using a different workload compiles and plans changes. Dependency tests prohibit OpenSearch CRD, controller and Java imports. |
| M02.02 | M02.01 | Implement deterministic creates, scheduling holds/releases and explicit retire selection with role/group counts and committed retirement deductions. Account for terminating, pending and quota consumption. | Table and property tests try scale, duplicates, unavailable state, simultaneous pending and terminating Pods and multiple groups. A voluntary action respects the floor unless a prior irreversible action must be completed. |
| M02.03 | M02.01, M01.05 | Implement application-independent Kubernetes adapters for exact owner and intent, deterministic create, gate removal, guarded eviction and owned unbound cancellation. | Real API tests cover same-name/new-UID objects, conflicting owners, resource-version conflict, an ambiguous create response and a Pod that binds during unbound cancellation. |
| M02.04 | M02.02, M02.03 | Integrate disruption policy with PDB-aware eviction and clear post-admission application handoff. Anchor retries to exact Pod identity. | Real PDB denies eviction and the victim remains eligible. Inject an admitted-but-lost response and prove the same retirement is not counted twice or directed at a replacement. |
| M02.05 | M02.02, M02.03 | Exercise crash and competing-worker traces using a deterministic clock, observation changes and bounded retry. Publish the package interface with one neutral example. | Replay produces the same plan given the same snapshot. Changed observation invalidates an unsafe action. A replacement Pod with stale action metadata is untouched. |

**Exit.** A workload-neutral client drives the package, and both planner invariants and real API behavior have evidence. [Lifecycle package research](../research/pod-lifecycle-package.md)

## M03 — Java runtime and primary store

**Goal.** Package and observe the real serverless shell against a supplied store before relying on it for capacity decisions.

| Task | Prerequisites | Goal and deliverable | Acceptance evidence |
| --- | --- | --- | --- |
| M03.01 | M01.01 | Build a Linux image from the pinned Java revision with the correct JDK, entrypoint, safe PID 1 behavior, paths, peer port, HTTP port and runtime UID. Capture provenance and build inputs. | Rebuild and identify the source revision and image digest. Start with a read-only root and explicit writable dirs. Verify termination signal reaches the JVM. |
| M03.02 | M03.01, M01.02 | Resolve `spec.storage.repositoryRef` or inline settings, pin the effective store identity and render typed `serverless.store.*`. Provision access outside the operator. Support storage access via verified workload identity or a qualified keystore. | Round-trip named config, reject mixed/invalid input, deny a redirection after the first accepted deployment, and verify denied credentials never mark Java eligible. Use the bundled `repository-s3`. |
| M03.03 | M03.01 | Build the Java adapter for membership, role, local serving, process incarnation, cache/ownership signals, search drain and shutdown. Version capabilities against image digest. | Contract tests capture the real response shapes and errors. Unavailable, stale or unsupported observations stay unknown. A new process invalidates the prior observations even inside the same Pod UID. |
| M03.04 | M03.02, M03.03 | Qualify ordinary data operations and recovery on the real qualified S3 backend. Use disposable inputs, separate search/index roles and fresh local cache paths. | Index identifiable docs, verify search, stop the writer and recover acknowledged data on a successor. Exercise forbidden access and any conditional-write behavior required for supported S3-compatible providers. |
| M03.05 | M03.04 | Wire the optional snapshot logical repository selector for snapshot feature consumers and an independent lookup status. | On a real shell, use the named repository on the primary store, then show that a missing repository blocks the snapshot operation while reads and writes continue. |

**Exit.** A pinned Java image and capability record demonstrate peer membership, read/write and recovery. [Runtime evidence](../research/java-serverless-runtime.md) · [Primary S3 remote store](../design/api-and-resources.md#primary-s3-remote-store)

## M04 — Namespaced controller vertical slice

**Goal.** Reconcile a functioning OpenSearch instance and expose two stable role endpoints from a namespaced operator.

| Task | Prerequisites | Goal and deliverable | Acceptance evidence |
| --- | --- | --- | --- |
| M04.01 | M01.03, M02.05, M03.03 | Render deterministic Pod templates and immutable identity by role, requested AZ, fleet and revision. Provide separate stable role Services and disjoint PDBs. | Real Pods are directly owned and role-targeted Services discover their Ready backends. Reconcile twice with unchanged desired state and show no extra Pods or selector churn. |
| M04.02 | M04.01 | Write bounded, versioned conditions, observed generation, per-role/AZ counts and operation progress through status. Keep Kubernetes readiness, current Java eligibility and warm/takeover observations distinct. | A restart and API update conflict converge without resurrecting stale conditions. Status records unknown binding/AZ proof and excludes credentials and private paths. |
| M04.03 | M04.01 | Implement the durable single-active operation claim and hold/recovery semantics for disruptive work. Persist identity and planned effect ahead of mutation. | Competing requests, stale instance UID, manager restart and an ambiguous side effect leave one recorded owner and a recoverable stage. Exercise pause/resume without undoing a committed operation. |
| M04.04 | M04.02, M04.03, M03.04 | Run the first split-role cluster with app-local termination hooks and ordered scaling. Apply process-local search drain on admitted termination, and JVM shutdown for writers. | Continuous smoke traffic observes replacement, denied eviction and actual writer failover. A blocked successor preserves predecessor capacity. Observe actual peer withdrawal. |
| M04.05 | M04.04 | Implement explicit `Decommission`, best-effort deletion and guarded reattachment. Preserve platform-owned S3, credentials and TLS objects. | Run orderly retirement, direct deletion and namespace deletion in a disposable environment. Reattach under a new owner only after old peers and leases are reconciled. Verify data and unauthorized adoption protections. |

**Exit.** The namespaced deployment demonstrates real normal reconciliation, two working traffic paths and the recovery semantics. [Operations and deletion](../design/reconciliation.md#durable-operations)
