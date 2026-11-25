# Reusable direct Pod lifecycle package

Implement direct Pod control as an independently usable library. If the operator is written in Go, start with an importable package such as `pkg/podlifecycle` in the operator's Go module. Keep a small API and a dependency boundary that allows a second operator to import it. Use a separate Go module when there is a need to publish or version the library independently. [Go module layout](https://go.dev/doc/modules/layout)

| Owner | Responsibilities |
| --- | --- |
| Pod lifecycle package | Reconcile Pods under a supplied owner. Compute the next create, wait, drain-request, or retire step for an owner-provided revision and pool. Track Kubernetes availability and terminating capacity. Enforce supplied replica floors, surge limits, and disruption budgets. Retire a selected Pod by its UID. |
| OpenSearch operator | Translate the CR into Pod templates, role and AZ pools, and rollout policy. Supply Java eligibility and victim-selection observations. Invoke search drain, verify membership and handoff, manage Services and blue/green promotion, and publish CR status. |
| Java shell | Own peer leases, advertised roles, cache warm-up, search drain, shard ownership, writer shutdown and replay. |

Treat pool keys, revisions, serving eligibility, disruption allowance, and retirement intent as inputs. Use Kubernetes resource types at the package boundary where they fit. Export neither OpenSearch CRD types nor Java client types through that API. Keep the manager, watches, configuration, and status subresource with the caller.

## Reconciliation and recovery

1. Read owned Pods and verify ownership by controller reference and UID. Use deterministic creation identity for a given recorded action so retries converge without adopting another owner's Pod. Retire only the chosen UID and re-observe if it changed. [Owner references](https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/) · [Deletion preconditions](https://kubernetes.io/docs/reference/kubernetes-api/definitions/delete-options-v1-meta/)
2. Plan from desired counts, observed Pod state, and the workload's current eligibility checks. Recheck floors before disruption. An unready or unreachable successor cannot take a serving slot. Limit both in-flight replacements and terminating resource overlap according to the supplied policy.
3. Persist the operation stage and selected UIDs through caller-owned status before invoking a drain that changes traffic. Re-observe on restart and replay only steps whose outcome can be checked. If a drained process restarted under the same Pod UID, the workload adapter must check the new process incarnation before accepting the old drain result.
4. Use the Eviction API for planned retirement. Let Kubernetes admit the operation before the local termination path withdraws application serving. A blocked eviction leaves the old Pod serving. A direct delete has different disruption semantics and requires a separate explicit policy decision. [Replacement sequence and concurrency](../design/reconciliation.md#create-qualify-and-replace-one-pod) · [API-initiated eviction](https://kubernetes.io/docs/concepts/scheduling-eviction/api-eviction/)

Qualify the package with a plain workload and a fake eligibility adapter as well as OpenSearch. The decisive cases are retries after an ambiguous create, loss of eligibility during replacement, restart mid-drain, a stale victim UID, and an eviction blocked by a budget. Reuse should be possible without linking an OpenSearch controller, CRD, or Java client into the second workload.
