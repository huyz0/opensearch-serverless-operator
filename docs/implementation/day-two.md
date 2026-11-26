# Diagnose and recover a development instance

Use this runbook with the secure development install and the opt-in
features enabled on the exact manager and Java digests under test.
A namespace administrator can grant a reader the optional
[diagnostic Role](../../config/dev/diagnostic-reader-rbac.yaml) through
a RoleBinding in the instance namespace. The Role grants reads of
instance and operation status, Pods, Services, EndpointSlices, Leases,
events, disruption budgets, Deployments and namespace HTTPRoutes.

The namespace administrator applies the Role and binds the approved
operator identity. The example binds a Kubernetes username.

```sh
NS=operator-development
READER='replace-with-kubernetes-username'
kubectl -n "$NS" apply -f config/dev/diagnostic-reader-rbac.yaml
kubectl -n "$NS" create rolebinding opensearch-serverless-diagnostics \
  --role=opensearch-serverless-diagnostics --user="$READER"
```

## Establish current state

Use `kubectl -n <namespace> get opensearchserverless -o wide` for
availability, the recorded search and index intents, and the claimed
operation. Use `kubectl -n <namespace> get opensearchserverlessoperations -o wide`
to see its phase and reason. During an active rollout, read the Service
selectors for live routing. [Preview both role routes](first-instance.md#preview-promotion-and-rollback)

To explain a claimed operation or provisioning hold using the diagnostic
Role, run the read-only command from the repository root.

```sh
NS=operator-development
INSTANCE=catalog
python tools/diagnose_operator.py "$INSTANCE" -n "$NS"
```

It prints the exact operation or Pod UID, the current reason, and the
next safe check for common holds. The condition state age measures how
long the condition has had its current True, False, or Unknown value.
Kubernetes can change the reason within that state. Provisioning attempt
age starts when the attempt was recorded. [Provisioning and capacity](#provisioning-and-capacity)
Pass `--operation <name>` to inspect a named request that is still waiting
for the instance claim.

Set the namespace and instance name before running these checks.

```sh
NS=operator-development
INSTANCE=catalog

kubectl -n "$NS" get deployment opensearch-serverless-operator
kubectl -n "$NS" get pods -l app.kubernetes.io/name=opensearch-serverless-operator
kubectl -n "$NS" get lease opensearch-serverless-operator.serverless.opensearch.org \
  -o 'custom-columns=HOLDER:.spec.holderIdentity,RENEWED:.spec.renewTime,DURATION_S:.spec.leaseDurationSeconds'
kubectl -n "$NS" get opensearchserverless "$INSTANCE" \
  -o jsonpath='{.metadata.generation}{"\n"}{range .status.conditions[*]}{.type}{"\t"}{.status}{"\t"}{.reason}{"\t"}{.observedGeneration}{"\n"}{end}'
kubectl -n "$NS" get opensearchserverlessoperation \
  -o 'custom-columns=NAME:.metadata.name,INSTANCE:.spec.instanceRef.name,ACTION:.spec.action,PHASE:.status.phase'
```

If the manager repeatedly restarts and the Lease stops advancing,
check namespace Pod events for health probe failures and ask the
platform owner to confirm API reachability and node pressure. Compare
`renewTime` with the current UTC time and `leaseDurationSeconds`.
A recorded holder may refer to a previous process. Wait for renewal
to resume and confirm that the instance reconciles before changing
its desired fleet. Manager metrics are disabled by default.

Compare condition `observedGeneration` with `metadata.generation`.
For endpoint or AZ observations also check the timestamp and serving
fleet. `Available=True` describes the sampled Kubernetes endpoint
floors. Prove the client-facing TLS and Basic Auth path with an
authorized index write and search through the intended route.
[Secure service checks](../../config/operator-dev-secure/README.md#verify-the-roles)

### Endpoint observation telemetry

When metrics are enabled with `--metrics-bind-address`, the manager
exports `opensearch_serverless_endpoint_observation_duration_seconds`.
Use the histogram count to see completed samples by outcome, and use
the buckets to inspect time spent reading endpoints, retrying and
writing status.

```promql
sum by (outcome) (
  increase(opensearch_serverless_endpoint_observation_duration_seconds_count[5m])
)
```

`ReadyRoleEndpointsObserved` and `RoleEndpointFloorShortfall` are
verified Kubernetes observations. `RoleEndpointsUnverified` means the
sampler could not verify coverage. `StatusPatchFailed` means the sample
could not be recorded in the instance status. Time spent between
samples is outside the histogram. Use the namespace checks above to
identify the affected instance and inspect its last sample.

## Provisioning and capacity

Read the current attempt and per-role/AZ holds. Use the exact Pod name
and UID from the attempt when comparing it to Kubernetes state.

```sh
kubectl -n "$NS" get opensearchserverless "$INSTANCE" \
  -o jsonpath='{.status.provisioningAttempt}{"\n"}{.status.blockedProvisioningAttempts}{"\n"}{.status.zoneCapacityObservedAt}{"\n"}{.status.zoneCapacity}{"\n"}'
POD='replace-with-pod-name-from-status'
kubectl -n "$NS" get pod "$POD" \
  -o jsonpath='{.metadata.uid}{"\t"}{.spec.nodeName}{"\n"}{.status.conditions}{"\n"}'
kubectl -n "$NS" get events --field-selector "involvedObject.name=$POD"
```

- For an unbound Pod, send the scheduler reason, role and AZ, and
  admitted resource requests to the platform owner. Ask them to check
  profile fit, including DaemonSet overhead and taints. An expired
  pending attempt can leave a scoped hold after withdrawal.
- For `StartupTimeout`, inspect the bound Pod's init and readiness
  status. The same Pod becoming Ready can clear the startup timeout.
  A DNS preflight or plugin install timeout has its own reason.
- For a hold eligible for `RetryProvisioning`, fix the underlying
  issue first and use the exact eligible UID and original Pod absence
  checks. Repeated retries can create more node demand.
  [Provisioning holds and retry](../../config/dev/README.md#provisioning-holds-and-retry)

## Primary store and search warm-up

- `StorageUnavailable` or `StorageIdentityChanged` means the
  repository could not be resolved or its bucket, region, endpoint, or
  addressing mode differs from `status.primaryStore`. Ask the
  repository owner to restore the pinned inputs and check the existing
  bucket and workload identity. If the repository resolves and Java
  still cannot start or take a lease, have the store owner check
  credentials, reachability and store errors. Keep the primary store
  under exclusive ownership during recovery.
- For a search shortfall, compare current Ready endpoints with
  `status.lastSearchQualification` when strict warm-up is enabled.
  A historical `WarmupCompleted` applies to the recorded Pod and JVM
  at the observation time. If a Pod or process has since changed,
  wait for current qualification and verify a search request before
  counting that capacity toward a planned drain.
  [HTTPS warm-up verification](verification.md#strict-search-warm-up-over-https)

## Routing, policy and trust

```sh
kubectl -n "$NS" get service "${INSTANCE}-search" "${INSTANCE}-index" \
  -o 'custom-columns=NAME:.metadata.name,SELECTOR:.spec.selector'
kubectl -n "$NS" get endpointslice -l "kubernetes.io/service-name=${INSTANCE}-search"
kubectl -n "$NS" get endpointslice -l "kubernetes.io/service-name=${INSTANCE}-index"
```

- For `RoleEndpointsUnverified` or `RoleEndpointFloorShortfall`,
  compare each stable Service selector with
  `status.servingFleetIntentByRole`. List EndpointSlices for each
  Service and check their ready endpoints against the corresponding
  role Pods. If the Service path works and the public path fails,
  give the gateway owner the hostname, role and observation time.
  Compare accepted route status, proxy backend health and
  authenticated traffic for each role.
- During promotion or rollback, inspect `status.activeOperation` on
  the instance and `status.rolloutRoles` and conditions on the
  referenced operation. Use the same operation's evidence and
  rollback procedure. Editing a live selector or clearing its claim
  by hand can invalidate the controller's recorded state.
  [Blue/green promotion and rollback](../design/traffic-and-security.md#bluegreen-promotion-and-rollback)
- For a managed account operation with `InputsUnavailable`, have the
  security owner verify the approved runtime and original immutable
  credential references. Check the operation's phase before replacing
  any input. Verify the expected allow or deny result on both roles
  after the operation converges.
  [Basic Auth reload verification](verification.md#basic-auth-reload-across-search-and-ingest)
- For TLS failures, check the actual hostname and presented
  certificate expiry against the trusted CA bundle. Check client to
  gateway and gateway to Java separately. Have the certificate or
  gateway owner restore valid trust or use the managed HTTP leaf or CA
  rotation procedure. Keep hostname and certificate verification
  enabled for the recovery probe.
  [Managed HTTP leaf rotation](../design/traffic-and-security.md#rotate-the-managed-http-leaf) ·
  [HTTP CA rotation](../design/traffic-and-security.md#rotate-the-http-ca)

For an escalation, include the manager and Java digests, namespace,
instance and operation names, condition reason and generation, and the
affected role, AZ and Pod UID. Include the observation time and
client result. Keep credentials, private keys and raw authorization
headers out of the incident record.
