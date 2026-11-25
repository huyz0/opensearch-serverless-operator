# Operator implementation plan

Build and qualify a Kubernetes operator for the pinned Java serverless shell. The implementation starts with a direct-Pod vertical slice and grows to cloud-qualified releases. Each task has an observable result. Implementers start with one task ID, read its owning design and required evidence, and keep the change reviewable on its own. The verification log records which development checks have passed. The milestone tables define the remaining release gates.

## Milestones

| Milestone | Goal | Exit evidence |
| --- | --- | --- |
| [M00](foundation.md#m00--development-workflow) | Establish repeatable agent tasks, review and verification. | The local harness passes, deliberately invalid cases fail, and an agent can select a task and identify its acceptance evidence. |
| [M01](foundation.md#m01--go-project-and-api-contracts) | Make the API and controller project buildable and installable. | Generated CRDs install in `envtest`. Strict admission, update rules, RBAC and informer scope are exercised. |
| [M02](foundation.md#m02--reusable-pod-lifecycle) | Implement direct Pod lifecycle control reusable by other workloads. | A non-OpenSearch consumer drives the planner and executors. Fault tests cover identity, ambiguous API effects, budgets and eviction. |
| [M03](foundation.md#m03--java-runtime-and-primary-store) | Boot a real Java search/index instance on the supplied store. | A reproducible image and runtime adapter pass indexing, search, and replacement using the same data. |
| [M04](foundation.md#m04--namespaced-controller-vertical-slice) | Reconcile search/index Pods and stable Services end to end. | A disposable cluster exercises normal reconciliation, concurrent controllers, drain, deletion and reattachment. |
| [M05](availability.md#m05--placement-and-provisioning) | Make shared or dedicated per-role placement predictable across AZs. | Scheduler and provisioner tests cover fit uncertainty, DaemonSet changes and bounded impossible requests. |
| [M06](availability.md#m06--warm-capacity-and-disruption) | Preserve warm search and writer capacity during disruption. | Each role/AZ warm reserve, automatic disruption, maintenance hints and loss of one AZ are tested. |
| [M07](availability.md#m07--traffic-and-fleet-revisions) | Shift production traffic safely through rolling, blue/green and weighted fleets. | Real probes and restart fault injection prove per-role route convergence, rollback and bounded retirement. |
| [M08](production.md#m08--transport-security-and-policy-reload) | Qualify TLS, credential rotation and dynamic security. | PKI and real-provider scenarios prove mixed-trust operation, policy activation, rollback protection and revocation bounds. |
| [M09](production.md#m09--runtime-configuration-and-oci-plugins) | Make startup and live settings and OCI plugin installation usable. | Role-specific launch and cluster setting tests pass. A private OCI registry test exercises automatic install and recovery. |
| [M10](production.md#m10--cloud-and-release-qualification) | Release supported EKS and GKE Standard combinations. | Per-platform matrix, recovery drills, supported features, release artifacts and runbooks reviewed against evidence. |
| [M11](production.md#m11--extensions-after-initial-qualification) | Deliver follow-ons through explicit qualification tasks. | Each extension has an independent compatibility and failure-mode gate. |

## How to use the plan

1. Use the prerequisite column to find a runnable task. Steps that depend on Java behavior include a pinned source contract or a task to implement and test it in Java. A dependency may be met by existing evidence if the task's entire acceptance criterion is demonstrated on the same relevant versions.
2. Start from the [software design description](sdd.md) and linked topic. Use the [development skill](../../skills/opensearch-operator-dev/SKILL.md) for the task handoff and proof. Record material decisions and changes to the source of truth with the code.
3. Run `python tools/verify_plan.py` for task graph and coverage consistency. As each implementation gate becomes available, also run the unit, API-server, cluster or cloud checks required by that task. [Verification and evidence](verification.md) · [Task handoff](task-handoff.md)

Use the [agent start prompt](agent-start.md) to assign an implementation task and choose evidence for review.

The [Java runtime build notes](java-runtime-build.md) record the first local shell bundle and startup check.

A local developer can start the Foundation tasks without a cloud account. Cloud tasks use disposable stacks and supplied test resources under separate authorization. Work in parallel where the prerequisites allow. For example, the lifecycle, Java and API work can proceed independently after their shared interfaces are recorded. Land API and behavior changes together when an existing consumer needs both.

## Scope and release rules

- **First support target.** Kubernetes with Go/controller-runtime and the pinned Java shell. Qualify AWS EKS with Karpenter and GKE Standard with managed node pools and Cluster Autoscaler independently. Pair GKE with a qualified primary store. The pinned shell provides `s3` and `fs` backends, and native GCS needs a new Java implementation and independent conformance.
- **Feature promotion.** A feature is supported only for the image, platform, store and integration versions that passed its gate. Dynamic security, native TLS hot reload, direct ALB and weighted routing have feature-specific gates. A release can advertise the verified platform subset and document the remaining work without closing the corresponding tasks.
- **Storage authority.** The platform supplies the exclusive S3 bucket, identity and repository input. Java owns durable data, leases and fencing. Kubernetes controllers own Pod and client Service lifecycle. Reattaching storage is a recovery procedure, with exclusive-use evidence.
- **Original evidence.** Pinned Java baseline `e71d42331a3ec82ec01109a5bb42d6a9a9162a5c`. Update the baseline and rerun affected qualification if a Java task requires a new build. The [design](../design/README.md) describes intended behavior. The [feature coverage](coverage.md) links every product area to its implementation and proof.
