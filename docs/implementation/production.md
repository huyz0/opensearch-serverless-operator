# Production features and release qualification

Production support is an evidence claim tied to specific dependencies. Record the Java image, Kubernetes and provider versions, exact OCI artifacts, store implementation and routing mode for every qualification run. [Delivery and validation](../design/delivery-and-validation.md)

## M08 — Transport security and policy reload

**Goal.** Provision secure connections and rotate configuration without weakening the admission policy or shortening the availability floor.

| Task | Prerequisites | Goal and deliverable | Acceptance evidence |
| --- | --- | --- | --- |
| M08.01 | M03.03, M01.03 | Choose and qualify Java providers for verified HTTP TLS and native peer mTLS. Separate the edge, peer and S3 trust domains. Validate SANs against lease advertise addresses, and restrict management and peer access. | Fresh HTTP and mutual peer handshakes pass both directions. Verify blocked wrong identity and trust root, secured management access, and distinction between ALB backend encryption and validated identity. |
| M08.02 | M08.01, M04.04 | Implement BYO Secret validation, immutable revisions, optional cert-manager issuance from approved issuers and bounded leaf rotation. | Test bad key pair, incompatible SAN, expired cert, issuer outage, role/AZ capacity shortage and recovery. A retained fleet can handshake and restart through the supported window. |
| M08.03 | M08.02 | Implement CA overlap and removal with evidence at each step. Plan rotation of startup credentials separately from leaf certificates. | Old and new fleets accept both trust roots in overlap, then serve new leaves, then reject old trust. Rollback checks current trust and expiry. |
| M08.04 | M08.01, M04.03 | Qualify built-in Basic Auth account updates and select a Java or gateway provider for hot-reloaded authorization. Trace REST, existing connections and native peers for bypass. Define minimum-revision fences and required Java changes if applicability is incomplete. | Live account add/rotate/delete works with known cache behavior. Authorized and denied examples prove every protected entry path, including preview, and show no accidental fallback. |
| M08.05 | M08.04, M07.05 | Build the `ApplySecurityConfiguration` operation with approved source snapshots, stage/live revisions, compare-and-set activation, per-process acknowledgements and restart/peer admission checks. | Invalid bundle keeps current policy. Delayed mount and mixed activation remain pending. Crash after commit converges. Rollback and same-Pod restart must meet the current revision. No Pod or JVM restart occurs during ordinary hot reload. |
| M08.06 | M08.05, M08.03 | Qualify emergency revocation and optional provider-specific in-process TLS reload separately on HTTP and transport. Set explicit convergence and isolation behavior. | A lagging or partitioned consumer cannot accept a revoked identity beyond the qualified bound. Test existing connections and fresh handshakes on both transports. Report unsupported modes before changing traffic. |

**Exit.** The selected TLS provider supports secured deployment and rotation. Hot reload and emergency revocation have their own support matrix and verified enforcement boundaries. [TLS design](../design/traffic-and-security.md#tls-and-credential-rotation) · [Security reload](../design/security-reload.md)

## M09 — Runtime configuration and OCI plugins

**Goal.** Let platform users configure OpenSearch without changing the base image, with behavior isolated by role where safe.

| Task | Prerequisites | Goal and deliverable | Acceptance evidence |
| --- | --- | --- | --- |
| M09.01 | M03.03, M01.03 | Implement common and role-specific startup settings, literal argv rendering, explicit heap and system properties, and stable config precedence. Add constrained bounded init containers and native/regular sidecars with mount and identity rules. | Exact arguments preserve spaces and metacharacters and reject managed identity, heap and credential duplicates. Exercise a real sidecar lifecycle, fit overhead and a role-only revision change. |
| M09.02 | M04.03, M09.01 | Implement `ApplyClusterSettings` over qualified scalar persistent writes. Record expected values, intent and ambiguity recovery. Verify observed effect independently from persistence. | Real Java tests demonstrate set, remove and unsupported keys. Suppress duplicate writes where read-back proves success, and record `Unknown` when the effect cannot be observed. Blue/green rollback leaves shared live settings at their committed values. |
| M09.03 | M09.01, M01.03 | Implement a pinned OCI installer, media type and digest checks, optional scoped pull credential and approved publisher enforcement. Verify descriptor and dependencies before atomic staging, include init requests and confine writable paths. | A real registry supplies an approved plugin. Initial and replacement Pods install automatically before Java starts. Reject truncated, wrong-digest, unsigned under policy, duplicate and unsafe-layout packages without passing the candidate. |
| M09.04 | M09.03, M07.04 | Qualify role scoping, the runtime/plugin/JDK/architecture tuple and removal, upgrade and rollback. Preserve approved filesystem bundles and reject duplicates with the bundled S3 implementation. | Use real plugin operations on relevant roles, test cross-fleet protocol/durable-state compatibility, unavailable retained digests and registry outages. Report exact failed input without credential values. |

**Exit.** Startup and live configuration use verifiably different rollout semantics. OCI artifacts are fetched and installed by declared reconciliation. [Runtime design](../design/runtime-configuration.md) · [OCI design](../design/oci-plugins.md)

## M10 — Cloud and release qualification

**Goal.** Ship supportable operator and Java artifacts for measured EKS and GKE Standard combinations, with recovery evidence and an upgrade path.

| Task | Prerequisites | Goal and deliverable | Acceptance evidence |
| --- | --- | --- | --- |
| M10.01 | M04.05, M07.05, M08.02 | Package versioned controller and Java images, Helm or Kustomize install assets, optional provider profiles, namespace RBAC and supported upgrade/rollback order. Publish signed digests and SBOMs as required by the release policy. | Install fresh, upgrade from the previous supported API and roll back supported components in disposable namespaces. Check CRD compatibility, default flags and exact artifact digests. |
| M10.02 | M05.04, M06.05, M07.02, M09.04, M08.03 | Run the EKS qualification matrix for fit, consolidated reserves, AZ failure, ALB/ingress, credential refresh, TLS and rollout. | Collect workload traces with node launch rates, pressure, cold cache, recovery loss and client latency/errors. Include node lifecycle, forced maintenance, and an operator replica failure under load. |
| M10.03 | M05.05, M06.05, M07.01, M09.04, M08.03 | Run the GKE Standard qualification matrix independently, including managed profiles, autoscaler, upgrade deadlines, same-region failure behavior and qualified object store connectivity. | Capture measured drain, provisioning, takeover and ingress behavior under load, including a Kubernetes upgrade and one AZ unavailable. Record any unsupported store and identity modes. |
| M10.04 | M04.05, M06.04, M07.05, M08.03 | Provide operator telemetry, condition reason runbooks and day-two procedures. Record ownership, useful failure observations and safe recovery for provisioner, S3, warm deficit, policy, route and expired trust. | An exercise operator can identify the failed boundary and recover without broad Kubernetes or cloud permission. Telemetry labels stay bounded, and sensitive data is absent. |
| M10.05 | M10.01, M10.02, M10.03, M10.04 | Define measurable support thresholds and produce release evidence. Declare optional feature combinations only after their respective tests. Publish known failure bounds and required configuration. | Independent release review checks each support row against reproducible evidence, fault runs and thresholds with actual values. Only verified features graduate. |

[Development day-two runbook](day-two.md)

**Exit.** Release claims are backed by real workload traces and test artifacts. Optional direct ALB, rainbow, hot reload and future store options have distinct qualification, so one successful run cannot promote them all. [Release evidence rules](verification.md#release-evidence)

## M11 — Extensions after initial qualification

**Goal.** Deliver the proposed follow-on features as separately qualified increments. Use product decisions to define support boundaries when the research documents alternatives.

| Task | Prerequisites | Goal and deliverable | Acceptance evidence |
| --- | --- | --- | --- |
| M11.01 | M06.05, M10.05 | Implement independent role autoscaling using bounded measurements and one replica owner. Stabilize decisions across AZs, distinguish store slowdown from compute saturation, and integrate with warm floors. | Multi-role load tests prove scaling uses the intended signals, remains within floors and caps during metric loss, and avoids conflicting replica controllers. |
| M11.02 | M03.04, M05.05 | Add a native GCS primary store only with an agreed Java backing-store contract and GCP credential refresh. Qualify any additional provisioner independently. | Required CAS/conditional-write, list and failure semantics pass on real GCS. Prove restart and recovery on GKE and accurately report the supported combinations. |
| M11.03 | M06.01, M10.05 | Define Java-backed pre-termination writer drain and, independently, hard per-index AZ policy with explicit failure and fallback semantics. | Verify exact writer handoff and acknowledged-write preservation under forced interruption. Exercise an index's declared zone policy in steady state, AZ loss and failback. |
| M11.04 | M03.05, M10.05 | Add explicit snapshot scheduling, retention and restore authorization. Design independent backup with separate recovery identity and validated copy/restore semantics. | Restore into a clean instance after losing access to the original store. Report measured RPO and RTO. A native same-store snapshot is classified as a different recovery mechanism. |
| M11.05 | M08.05, M09.04, M10.05 | Add approved application integrations such as dashboard access, scoped observability, ISM/templates and user policy ownership. Establish tenant isolation and bounded cost controls when exposing shared access. | Real tenant tests prove allowed and denied operations and request attribution across forwarding. Optional integrations can be disabled without altering the serving controller. |
| M11.06 | M04.05, M06.02, M10.05 | Qualify explicit suspension and resume from the same store, then evaluate an edge wake-up design if an always-reachable endpoint is required. | Capture declared zero-capacity response behavior and measured cold resume. Prevent competing ownership while suspended and prove data returns with fresh Pods. |

**Exit.** Each adopted extension has an agreed contract, negative tests and explicit support status. [Follow-on research](../research/operator-feature-inventory.md#follow-on-features)
