# Draft API contracts

These documents define the proposed resource fields and validation responsibilities for M01. They are design inputs. The implementation task generates structural CRDs from Go types and validates them with a Kubernetes API server. The examples in the design remain illustrative until those tests pass. [API design](../../docs/design/api-and-resources.md) · [M01](../../docs/implementation/foundation.md#m01--go-project-and-api-contracts)

The [operation CRD draft](operation.draft.yaml) and [promotion example](fixtures/operation-promote.yaml) give M01 a concrete starting point. Action-specific validation, maintenance payload and typed status still need design and API-server tests. The draft has not been installed or validated by a Kubernetes API server.

The [instance CRD draft](instance.draft.yaml) and [instance example](fixtures/instance.yaml) cover initial field shapes. Several nested objects intentionally preserve unknown fields while their schemas are designed. These drafts do not enforce all documented validation and must not be installed. M01 replaces them with generated, fully typed schemas and runs strict API-server fixtures.

## OpenSearchServerless

Namespaced `serverless.opensearch.org/v1alpha1` resource. The controller writes `status` separately from user-owned `spec`. Reject unknown fields using structural schema and strict API requests. Validate values at admission where possible and validate referenced resources and provider capabilities during reconciliation.

| Field | Contract | Validation owner |
| --- | --- | --- |
| `spec.image` | Image digest identifying the Java runtime. | Schema pattern, image compatibility during reconciliation. |
| `spec.serviceAccountName` | Existing same-namespace workload identity. | Schema and reference authorization. |
| `spec.storage` | Type and mutually exclusive inline S3 settings or `repositoryRef`, plus credentials. | Schema and admission for shape, controller pins resolved bucket and endpoint after first deployment. |
| `spec.pools.search`, `spec.pools.ingest` | Role-specific zones, replicas, floors, resource requests, heap, capacity profiles and warm policy. | Schema for types and bounds, admission for floor relationships, controller for placement and profile eligibility. |
| `spec.availability` | Role totals, surge, pending, warmup and retirement budgets, optional emergency scale-out. | Schema and admission validate nonnegative values and compatible limits. |
| `spec.rollout` | Rolling or blue/green strategy, preview and retention policy. | Schema and controller capability checks. |
| `spec.runtime` and role overrides | JVM and startup settings with a defined merge order. | Admission rejects reserved keys and duplicate typed settings, controller qualifies image/plugin settings. |
| `spec.plugins` | OCI manifest digests, names, optional roles and same-namespace registry credential references. | Schema validates shape, admission authorizes source, installer validates content and compatibility. |
| `spec.tls`, `spec.security` | HTTP and peer trust inputs, provider and security revisions. | Schema validates reference shape, controller verifies certificates and qualified provider capabilities. |
| `spec.snapshots.repository` | Optional logical snapshot repository name. | Schema validates name, operation verifies repository before use. |
| `status` | Observed generation, conditions, counts per fleet/role/AZ, routes, operation and dependency observations. | Controller records bounded, current evidence and avoids credential values. |

## OpenSearchServerlessOperation

Namespaced resource with immutable `spec`, an instance reference containing name and UID, an action and its action-specific payload. Support `Promote`, `Rollback`, `PrepareMaintenance`, `Decommission`, `ApplyClusterSettings` and `ApplySecurityConfiguration`. Validate action and payload combinations at admission. Persist operation claim and intent before side effects. Status records phase, targets and observations sufficient to resume after a restart. A stale instance UID or expected revision must block mutation. [Operation behavior](../../docs/design/api-and-resources.md#explicit-operations)

## Required API-server fixtures

M01 must add real positive and negative fixtures covering an instance with named S3 configuration, both roles and plugin digest, plus each action's required payload. Negative cases cover unknown fields, conflicting inline and referenced storage, negative budgets, invalid floor, unsupported enum, malformed digest, cross-namespace references and mutation of operation spec. Test defaults, status subresource, conversion and existing stored objects on upgrades. Resource existence, actual store identity, secret access and provider behavior require controller and integration tests as well as API validation.
