# Placement, autoscaling, and recovery

The operator treats each **fleet, role, and AZ** as a placement group. Targets and floors belong to the role and AZ. Fleet identity keeps rolling or blue/green work from borrowing a candidate ready count before that fleet can take the intended traffic. Kubernetes performs binding with the normal scheduler. Karpenter or GKE Cluster Autoscaler provides nodes when eligible Pods need them. [AZ scheduling research](../research/multi-az-workload-research.md) · [Dedicated node research](../research/dedicated-node-placement.md)

## Dedicated node profiles

For one OpenSearch Pod per host, require the group's `topology.kubernetes.io/zone`, match its platform capacity profile, and tolerate the profile's exact permanent taint. Require `kubernetes.io/hostname` to exist in the node affinity as well as using it for required Pod anti-affinity. Keep hostname values consistent and unique per host, and use a stable instance selector spanning both roles and all fleet revisions. This is independent of the node-pool role split and still applies to mixed blue/green placement. A policy spanning multiple instances needs an explicit shared fleet selector and namespace scope. Required system DaemonSets must tolerate the appropriate profile taints. Platform admission controls what other workloads can tolerate them. [Node affinity and anti-affinity](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/) · [Taints and tolerations](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/)

The platform names one qualified profile per role and capacity generation. Keep allowed shapes, zones, node images and startup assumptions as part of each qualification. A capacity profile labels nodes with the value that eligible Pods require, so a new profile can be selected without changing which Services or PDBs admit the Pod. A shortage in one zone does not relax its normal hard affinity. Any emergency Pod has a separate explicit placement target. Render the Java zone from the admitted, hard-pinned target. Report that target and Java's advertised zone separately from observed binding topology. Independently confirm binding topology through trusted binding-time Pod topology labels or a platform observation where supported. `spec.nodeName` alone carries no AZ, and a later node relabel can change topology without evicting a bound Pod. Report missing or mismatched topology evidence explicitly. [Kubernetes PodTopologyLabels](https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/#podtopologylabels) · [Java zone and lease behavior](../research/multi-az-operator-design.md#what-the-java-shell-already-does-with-zones)

| Platform | Pool and warm reserve policy |
| --- | --- |
| EKS with Karpenter | The platform creates role-specific NodePools carrying the capacity-profile labels and taints. Restrict instance types and subnets to qualified combinations. Declare actual temporary `startupTaints` separately. `WhenEmpty` preserves nodes running ordinary warm Pods while allowing cleanup after a Pod leaves. Use an absolute drift budget such as one node per pool and coordinate changes across pools. Size NodePool limits and provider quotas for steady, surge and recovery demand. Expiration and interruptions follow their own rules. [Karpenter NodePools](https://karpenter.sh/docs/concepts/nodepools/) · [Karpenter consolidation and disruption](https://karpenter.sh/docs/concepts/disruption/) |
| GKE Standard | The platform creates managed pools with matching profile labels and taints. Set per-zone minimums for the number of hosts that should remain warm. Use separate zonal pools where minima differ. A total regional minimum is an aggregate. The operator sets Pod `cluster-autoscaler.kubernetes.io/safe-to-evict: "false"` in the initial create request where warm placement needs protection from autoscaler scale-down. Keep it for the Pod's lifetime. GKE upgrades and repair remain separate. [GKE per-zone and total limits](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/cluster-autoscaler#autoscaling-limits) · [GKE scale-down reasons](https://docs.cloud.google.com/kubernetes-engine/docs/troubleshooting/cluster-autoscaler-scale-down) |
| GKE Standard with Karpenter GCP | Optional platform integration after qualifying the separate GCP provider, including node sizing and registration. Give GKE Cluster Autoscaler and Karpenter disjoint capacity responsibility. Java's qualified primary store remains a separate decision. [GCP provider study](../research/karpenter-scheduling-research.md#references-and-validation) |

A standing reserve is a running Java Pod in the serving fleet. Warm search replicas already hold cache state and can handle traffic. Standing indexing replicas add eligible writer capacity, with enough spare shard admission and replay capacity for an abrupt takeover. Empty nodes reduce launch latency and serve a different purpose. Make node pool minimums reflect the protected footprint. GKE minima do not necessarily scale a currently smaller pool up without demand, so use initial pool size or the desired Pod demand to provision it. [GKE scale-up behavior](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/cluster-autoscaler)

## Fit calculation and capacity evidence

Qualify each admitted workload profile against registered `Node.status.allocatable` from representative fresh nodes for each machine shape, AZ and node image. Include kubelet settings, CNI, local disk and Pod slots in qualification. The platform publishes a namespaced snapshot containing the profile revision, supported shapes and zones, minimum observed allocatable per dimension, expected matching DaemonSet footprint, required headroom and observation time. It refreshes that evidence when the platform changes the node or the applicable DaemonSets. Cluster-scoped Node and DaemonSet evidence can stay with the platform. The namespaced operator can use a published qualification together with the admitted Pod and its own scheduling observations.

Use a bounded versioned payload in the platform profile. Resolve it by name inside the instance namespace and verify the requested role and AZ against qualified entries. For a profile that permits multiple shapes in an AZ, use the conservative per-dimension minimum across the permitted combinations, or require eligibility for a restricted, explicitly qualified subset before releasing a Pod. Store resource quantities in native Kubernetes quantity syntax. Include the provider and platform revision, observation time and invalidation or expiry signal. The operator reports which profile revision it evaluated and keeps the last usable serving configuration during requalification. A new Pod release requires current evidence for the requested profile.

Evaluate CPU, memory, ephemeral storage, Pod slots and any requested extended resources separately.

```text
effective admitted Pod requests
+ expected matching DaemonSet and required node Pod requests
+ explicit headroom
<= minimum observed node allocatable
```

Kubelet reservations are already reflected in allocatable. Count expected pods that have yet to land on a fresh node. Include sidecars, effective init-container requests, RuntimeClass overhead, ephemeral storage and one slot for each Pod. For agents updated in place or through a DaemonSet rollout, measure the larger expected footprint and count both generations when they can overlap. Check hard scheduler constraints such as host ports, taints and storage topology as well as the resource vector. Preserve a separately measured operational headroom for JVM and native memory, replay and agent utilization. Restrict the profile to qualified shapes so the autoscaler cannot choose a smaller shape solely because its simulation accepts it. [Node Allocatable](https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources/) · [Karpenter estimate and DaemonSet gaps](../research/karpenter-scheduling-research.md)

For planned platform updates, qualify new images, agents and settings before broad rollout. If observed allocatable drops or agent requests grow past the budget, report the dimension and discrepancy. Keep eligible serving Pods and either qualify a larger shape or change the approved budget. Heap and request changes are explicit new workload revisions.

### Development profile format

The opt-in development controller reads an immutable same-namespace ConfigMap named by `capacityProfileRef`. Its `profile.json` has the following shape; values are illustrative and must come from the platform's actual qualification:

```json
{
  "revision": "search-v1",
  "role": "search",
  "zones": ["zone-a"],
  "nodeSelector": {"platform.example/profile": "search-v1"},
  "taint": {"key": "platform.example/dedicated", "value": "search", "effect": "NoSchedule"},
  "observedAt": "2026-01-01T00:00:00Z",
  "expiresAt": "2026-01-02T00:00:00Z",
  "allocatable": {"cpu": "8", "memory": "16Gi", "ephemeral-storage": "40Gi", "pods": "20"},
  "reserved": {"cpu": "1", "memory": "2Gi", "ephemeral-storage": "5Gi", "pods": "4"},
  "headroom": {"cpu": "1", "memory": "2Gi", "ephemeral-storage": "5Gi", "pods": "1"}
}
```

The controller checks role, zone, expiry and ConfigMap identity; renders the selector, exact toleration, required hostname label and required Pod anti-affinity; and compares admitted requests with the declared resource floor before release. The anti-affinity matches Pods carrying this operator's instance-UID label across all namespaces, roles and fleets, including other instances. An empty namespace selector deliberately selects all namespaces. Every application and init container must have positive CPU, memory and ephemeral-storage requests for this check. Omitting the profile selects the development shared-node path. The profile publisher and admission policy must control who can author profiles, labels and tolerations. The rule does not exclude unlabelled workloads, Pods bound outside the scheduler, or workloads that bypass the pool taint. Existing bound Pods are not moved when this rule changes, and an older gated Pod with the previous affinity can require recovery. Cross-namespace anti-affinity can increase scheduler cost. Pod host ports, volumes, architecture, actual binding zone, cloud node provisioning, changes after release and the platform evidence itself are not yet qualified. An immutable ConfigMap can still be deleted and recreated after a check; the controller cannot atomically bind a Pod to its UID. [Kubernetes affinity and namespace selectors](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/)

A local Kubernetes 1.32 scheduler test with synthetic nodes verified separate
hosts across namespaces, rejection of an additional dedicated or shared
operator Pod on occupied hosts, and placement after one host was freed. When
zone-A search hosts were cordoned, a zone-A search replacement remained
pending despite available wrong-role and wrong-zone nodes; a zone-B ingest Pod
scheduled to its matching node. It does not run kubelets, simulate an AZ
failure, or qualify provider provisioning and topology.

The same fixture compared the fit calculation with the Kubernetes 1.32
scheduler using a pre-bound synthetic agent, a restartable init sidecar, an
installer and RuntimeClass overhead. Both rejected a 10-millicore CPU excess
and accepted the exact-fit Pod. It does not establish real DaemonSet rollout
footprints or provider allocatable estimates. [Kubernetes effective Pod requests](https://kubernetes.io/docs/concepts/resource-management/effective-pod-requests-limits/)

The calculation also accounts for admitted Pod-level resources by taking the
greater of the Pod request and aggregate container request for each resource,
then adding overhead. This is conservative when Kubernetes uses a smaller
Pod-level request instead of the container aggregate. Dynamic resource claims
fail fit until separately qualified. With the alpha Pod-level feature enabled,
a Kubernetes 1.32 scheduler test matched the reusable CPU fit calculation at
and above the boundary. That alpha API rejected a Pod combining Pod-level
resources with the container ephemeral-storage requests required by this
development profile; the Pod-level test therefore checks the reusable
calculation separately and confirms the profile holds the incomplete Pod.
Newer versions require their own qualification. [Pod-level resource semantics](https://kubernetes.io/docs/tasks/configure-pod-container/assign-pod-level-resources/) · [Kubernetes validation issue](https://github.com/kubernetes/kubernetes/issues/132449)

## Provisioning guard

The operator bounds demand it sends to the scheduler. For strict admitted-Pod fit, create the Pod with a scheduling gate, confirm the gate survived admission, and check the admitted Pod including injected containers and runtime overhead before release. Deferred creation of an ungated Pod permits scheduling as soon as admission returns. Qualify that weaker mode only for templates whose relevant resource and placement inputs are fixed by admission policy. Release pending Pods within role/AZ and total limits. Treat an unbound provisionable Pod as demand even if no launch has been observed. [Kubernetes scheduling gates](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-scheduling-readiness/)

Before gate release, the operator also checks the controller reference,
Pod network mode, OpenSearch restart policy, and the runtime data volume
and its mounts. The admitted OpenSearch container must retain configured
resource limits. Any admitted memory limit must exceed the configured JVM
heap, including a limit added by namespace defaulting.

| Evidence | Operator action |
| --- | --- |
| Pod binds | Stop counting it as provisionable demand and keep tracking any search warm-up slot. Observe image, startup, lease and serving readiness independently. A bound but unhealthy Pod is a separate recovery path. |

| Node launches or registers repeatedly without a binding | Where the platform supplies those signals, correlate by group, time, and nomination if available. Stop new demand after the configured no-progress limit. A NodeClaim can represent multiple Pods and is platform-owned. |
| Pod stays unbound and platform telemetry is unavailable | Use a bounded pending timeout and scheduler reason. Mark evidence as incomplete and hold further triggers until the release policy is met. |
| Qualification changed or hard constraints conflict | Hold the affected group and report expected versus admitted requirements. Keep surviving capacity serving. |
| Persistent attempt failure | Persist the block, then conditionally delete only the instance's still-unbound Pod with UID and resource-version preconditions. Keep the desired deficit visible. A pause by itself leaves existing unschedulable demand visible to Karpenter. |

When selecting among deficient groups, the development controller prioritizes those below their zonal `minServing` count using Kubernetes Ready observations. It does not infer cache warmth or create surge beyond the desired group size. Blocked and rate-limited groups are excluded from this choice.

The development controller retains one provisioning slot through Pod readiness and reserves gate releases in a persistent per-role/AZ window. Before creating a Pod it skips rate-limited groups when another deficit can proceed. An expired gate-release intent is settled before reading new placement or startup inputs. If the Pod still has the gate, the controller records retirement before withdrawing it. If the gate was already removed, the controller tracks the same Pod as released. After a pending Pod is withdrawn and is gone or terminating, a startup Pod becomes terminal, or a bound Pod times out in initialization or readiness, the development controller persists a role/AZ hold and frees the active slot for other groups. It observes a timed-out bound Pod and clears its own hold only if the same Pod completes required initialization and becomes Ready. The terminal Pod is retained for inspection. An explicit retry clears that hold by Pod UID. Its global 256-record history and an active instance-wide pending or startup attempt can still delay healthy groups until it times out; configure the release window and limit for recovery needs. A gate reservation is counted even if its eventual API result is ambiguous. This protects only operator-created demand and does not bound provider node launches for an individual pending Pod. The production recovery and per-group scheduling design remains to be implemented and qualified.

Back off and retry on a material fit or capacity change, or a bounded policy interval for transient failures. If the Pod binds before withdrawal, handle it as a normal bound Pod. Provider caps and cloud quotas limit the effects of already-in-flight launches. A one-Pod concurrency limit does not cap claims to one per Pod. Karpenter can provision in parallel and its limits are eventually consistent. [Detailed provisioning guard](../research/dedicated-node-placement.md#limit-the-effect-of-provisioning-failures)

Reserve an independent search warm-up slot before releasing a Pod that will start warm-on-join. Release it after the actual background work completes or cancels, or the process is observed gone. A Pod can bind, become Ready, or time out while background copying continues. Recover slots from durable intent and current process observations after controller restart. Without evidence of completion, keep the slot held or cancel through a supported, bounded runtime action. The operator bounds warmups it initiates. Autonomous JVM restarts and Java writer replay can still create additional store load. [Java search warm-up implementation](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/shell/src/main/java/org/opensearch/serverless/warm/SearchWarmup.java#L75-L136)

The experimental Java overlay starts retries for failed peer copies before the
warm-on-join maximum wait and reports the worker as running between attempts.
An in-flight copy can finish later. A failed or
exhausted attempt cannot qualify a strict replacement. A ready endpoint alone
does not release the warm-up slot. Local restart tests exercise stale lease
recovery and exhaustion; cloud and cluster qualification remain necessary.

`status.lastSearchQualification.localSearchActivity` reports the local
shard reader count and cumulative block cache hits, misses and bytes
fetched. These counters come from the same JVM as the completed warm-up
observation. Zero readers is a valid sample, including on an empty
store or a Pod that can forward queries. A missing field means the
operator lacks a verified local activity sample. The operator uses these
counters for diagnostics. Readiness and retirement rely on live process
and endpoint checks. Proving cache residency requires evidence about the
assigned shards and the blocks needed for the expected queries.

## Maintenance and an AZ failure

The namespaced controller reads the Kubernetes Pod `DisruptionTarget`
condition. `status.zoneCapacity[].disruptionTargetPods` reports active
signals by serving role and zone alongside the Kubernetes Ready count.
The operator discounts these Pods from source fleet serving floors and
requires disruption-free target and endpoint observations before recording
convergence or retiring temporary capacity. A signal that clears releases
the hold on a fresh observation. This path uses the existing Pod access.

For regular serving provisioning, the controller excludes marked Ready Pods
when assessing regional and zonal floors for creation priority. If a role is
below its regional floor, an eligible zone below its own floor gets priority
over other zones for that role. The signal leaves the Pod in the replica
inventory, so a group already at `replicas + warmSpare` gets no extra Pod.
Provisioning and warm-up budgets still apply.

`status.zoneCapacity[].readyDisruptionTargetPods` counts the Ready subset of
the marked Pods. `belowMinServingAfterDisruption` reports whether discounting
that subset puts the zone below its floor. `kubernetesReady` and
`belowMinServing` describe current Kubernetes readiness.

For each role and zone with a requested reserve, the operator first counts
regular, disruption-free Ready Pods toward `replicas`. Any remaining Pods
are possible reserve capacity. `warmReserveReadyShortfall` is the number
of requested reserve Pods that cannot be accounted for by this Ready
capacity. `warmReserveStatus` is `InsufficientReadyPods` when that count
is positive and `Unknown` when there are enough Ready Pods and warmth
still needs verification. Groups without a reserve use `NotRequested`.

Preview admission discounts disruption-marked serving Pods. If the remaining
Ready serving capacity falls below its configured target, pending preview
creation yields to serving recovery. A late create remains fenced by its
recorded name. A created preview can be withdrawn while it is still unbound
and scheduling-gated. The operator uses UID and resource-version checks and
releases the shared warm-up slot after it observes the Pod gone. A confirmed
gated deletion returns its rate-limit reservation. If the gate has already
been released, the controller can still withdraw the unbound preview after
another serving-capacity check. A recovered serving fleet cancels that
withdrawal. A scheduler binding wins over deletion and startup remains
tracked. Released or ambiguous attempts keep their rate-limit charges.
If a released preview used the last charge in its role and AZ, serving
replacement can start when that charge expires or another eligible
target's budget permits it.
Preview admission leaves space in the bounded retired-creation history for
both preview cleanup and serving recovery.

Kubernetes can set the condition during eviction, preemption or shutdown,
and can clear it if a disruption is canceled. The condition alone leaves
Service readiness under Kubernetes control. Regular replacement uses the
existing lifecycle when the old Pod terminates. Cross-zone temporary
placement requires trusted outage and fit evidence. Keep warm reserves
in place for a drain that gives less time than the runtime needs to warm.
[Kubernetes Pod disruption conditions](https://kubernetes.io/docs/concepts/workloads/pods/disruptions/#pod-disruption-conditions)

The development controller can evaluate an emergency capacity proposal.
Configure `spec.availability.emergencyCapacity.evidenceRef.name` with a
platform-owned ConfigMap in the instance namespace. Use
`maxTemporaryByRole` and `maxTemporaryPerZone` to bound the proposal.
The zonal cap is shared by search and index. A committed temporary Pod
continues to use a slot while it is terminating, until the controller
observes it gone.
Availability policy edits leave the Pod template revision stable. After
upgrading from a version that included availability in the hash, wait
for the controller to record the existing label in
`status.templateRevisionAliases` before editing the policy. Failback
requires a single literal revision per role, including on instances
that already had mixed labels before the upgrade.
Each opted-in role needs a regional serving floor and a dedicated capacity
profile. A positive `maxPendingProvisionablePods` limits shared demand.
Restrict evidence ConfigMap writes to the trusted platform publisher.

The ConfigMap's `evidence.json` names the instance UID and generation,
the observed and expiry times, the unavailable zones, and positive
additional fit by role and surviving zone. The fit must identify the
current capacity profile by UID and resource version.

```json
{
  "instanceUID": "instance-uid",
  "generation": 4,
  "observedAt": "2026-11-24T01:00:00Z",
  "expiresAt": "2026-11-24T01:02:00Z",
  "unavailableZones": ["zone-a"],
  "fitByRole": {
    "search": {
      "zone-b": {
        "additionalPods": 1,
        "profileUID": "profile-uid",
        "profileVersion": "42"
      }
    }
  }
}
```

Evidence is valid for at most five minutes. The controller checks owned
Pod counts and writes the selected role, zone, fleet, and evidence identity
to `status.emergencyCapacityProposal`. Expired or unverified evidence
clears the proposal without stopping normal recovery. This development
status describes the most recent proposal. With development gate release
enabled, the controller can stage one temporary Pod when normal creation
has no eligible next action. It pins the evidence UID and resource
version, rechecks the outage and fit before creation and release, and
withdraws a gated Pod if its original evidence changes or expires. The
Pod stays pinned to the selected surviving zone. Existing temporary Pods
count against the surge limits. A Ready temporary Pod contributes to the
regional serving floor. The zone's regular replica shortfall still
reflects any missing regular Pods, so the controller can replenish them
while temporary Pods serve. Rollout staging and promotion wait until
temporary capacity clears. The serving protection samples expose
`regularCapacityRecovered` when a fresh observation finds enough regular
Pods with role-specific Java qualification and matching ready Service
endpoints in every configured zone and across the region. The sample
requires an enabled retirement budget and an observed temporary Pod.

A current outage report also holds ordinary placements in its unavailable
zones. The controller leaves bound Pods in place. If an already released
Pod is still unbound in a held zone, the controller records withdrawal and
rechecks the report and Pod identity before withdrawing it. This frees the
pending slot without adding a persistent provisioning hold. If outage
evidence clears or expires before the withdrawal, the attempt returns to
its ordinary scheduling policy.
With `--development-emergency-failback`, the controller requires fresh
platform evidence containing an explicit empty `unavailableZones` list.
It records one temporary Pod UID in status, then checks role capacity,
the Service, the outage report and the victim's zonal PDB again before
submitting an eviction with UID and resource-version preconditions.
Other operator operations wait for the intent to clear. A terminating
Pod keeps the retirement slot until it disappears while provisioning
can still replenish capacity. Missing evidence, an unhealthy Pod in the
observed role, or a renewed outage prevents the next eviction.

Removing `spec.availability.emergencyCapacity` stops new temporary Pod
admissions. The operator retains the evidence ConfigMap name and UID from
admission and uses that report to finish failback under the current
retirement budget. The publisher must keep updating the same ConfigMap
with evidence for the new instance generation until retirement finishes.
A missing or replaced report, or a zero retirement budget, pauses
cleanup. On upgrade, the operator can record the configured report for
existing temporary Pods when their admission annotations match its UID.

After an upgrade, different revision labels in the serving fleet can
qualify together when each label resolves to the same fleet template
through a verified alias or the current canonical revision. A recorded
serving pin remains the literal label for new Pods in that role.

A degraded search bootstrap Pod can serve while the operator restores
strict capacity.
If it survives the outage, the controller can create one additional regular
search Pod in the bootstrap Pod's AZ once regular allocations are Ready.
The explicitly cleared outage report must include positive additional fit
for that AZ and its live capacity profile. The surge, retirement, pending
and warm-up budgets must permit the attempt. The controller pins the
bootstrap UID and report version before creating the replacement and
rechecks them before gate release. It retires the bootstrap first only
when warm regular endpoints alone cover the configured zone and regional
targets, then rechecks before retiring temporary capacity. Without
qualified fit, the temporary capacity continues serving. If the bootstrap
disappears during the outage and a Ready search peer survives, the
regular replacement must complete strict warm-up. Local
Kubernetes drills exercised indexing and strict search failback after
controlled zone loss. Ingress continuity and cloud AZ loss require
separate qualification. [Local indexing failback](../implementation/verification.md#local-indexing-failback) · [Local strict search failback](../implementation/verification.md#local-strict-search-failback)

The development EndpointSlice sample retains counts for healthy zones when
a configured zone has no ready endpoints. It reports
`KubernetesEndpointsBelowZoneFloor` when regional capacity still meets the
role floor, `KubernetesEndpointsBelowRegionalFloor` when it falls below
that floor, and `Unavailable` when the endpoint observation cannot be
verified. These counts use Pod placement labels and reject a conflicting
zone reported by an EndpointSlice. They do not diagnose a physical zone
outage or prove application health.

Ordinary external drains use each role/AZ PDB. Coordinate platform batches and the operator's own retirement budget, because independent PDBs and NodePools can each allow disruption. For planned maintenance where the platform cooperates, prewarm a Pod in the same AZ on capacity outside the drain scope before starting the drain. Enforce that distinction with a separately qualified capacity generation or another verified scheduling exclusion before acknowledging maintenance readiness. A pre-created namespaced maintenance hint can describe that scope without Node permissions. For automatic maintenance without a hint, detect disruption from owned Pod conditions and Java observations and replenish an already-warm reserve. GKE can honor a typical surge-upgrade PDB for up to an hour, then proceeds. A Pod termination hook starts after the decision and cannot produce guaranteed warm overlap. [Maintenance and disruption limits](../research/node-disruption-recovery.md#how-long-a-drain-can-wait) · [Namespace-only recovery](../research/node-disruption-recovery.md#automatic-drains-without-a-platform-publisher)

For loss of one AZ, use the following policy.

1. **Detect from workload evidence.** Correlate losses or unreachable peers across several hosts in a zone while other zones can still serve. Separate API lag, loss of the controller's observations, and shared-store faults from an observed zonal capacity loss. A namespaced observer reports the workload impact and confidence.
2. **Hold planned disruption and maintain ingress reachability.** Freeze retirements, blue/green promotion, scale-down and normal rotation. Keep the stable role Services pointing at their recorded serving fleets. Route through surviving ingress and Ready endpoints. Hard failure still leaves a brief detection and convergence interval. [ALB failure path](traffic-and-security.md#ingress-and-alb)
3. **Recover at the right layer.** Java redistributes reader activity and fences and replays lost writers through the shared store. Keep the failed AZ's normal desired counts visible. When that AZ's attempt policy is held, promptly withdraw its owned unbound triggers using the guarded delete so they release pending slots. Within the overall limits, prioritize serving-floor recovery over preview and reserve work. If the emergency policy is enabled and there is saturation or recovery backlog, admit temporary Pods pinned to surviving AZs. Bound operator-initiated warmups and account for concurrent Java replay when evaluating store pressure.
4. **Fail back gradually.** After sustained recovery, restore original per-AZ targets while holding emergency capacity. Observe fresh leases, warmup and actual traffic. Retire emergency extras with the same serving-floor checks, then resume planned work.

With `3` search and `2` indexing Pods per AZ across three equal zones, one AZ loss leaves `6` search and `4` indexing Pods. Each surviving Pod takes `1.5` times its previous traffic under ideal redistribution. Qualify the two-AZ system at peak load with the real ingress, cache misses, inter-AZ traffic, store usage, and writer replay. Treat the control plane, ingress and object store as availability dependencies. Run the controller redundantly across zones and qualify leadership takeover. [One-AZ recovery research](../research/node-disruption-recovery.md#recover-from-the-loss-of-one-az)

## Role autoscaling after initial qualification

Start with fixed per-role/AZ targets. Add optional autoscaling through one delegated desired-count writer for each role. Indexing can use writer admission, queue latency and Java's `fleet.wanted_nodes` as inputs. The Java estimate is a local view of writer usage and refusals. Search needs its own query latency, saturation and cache/store signals. Apply stabilization, floor protection and explicit placement policy to convert role scale requests into per-AZ targets. Keep normal steady floors and a maintained warm reserve. A shared-store slowdown or cold-start fault must not trigger an unbounded scale or restart response. [Java operations signals](../research/java-serverless-operations.md)
