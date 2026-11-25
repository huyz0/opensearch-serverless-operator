# API and resource contracts

The development controller re-resolves the primary repository immediately
before releasing a Pod scheduling gate and holds if its values no longer
match the pinned store. The Pod is rendered from the pinned identity. The
repository read and gate update are separate Kubernetes operations, so this
check is not atomic with an external ConfigMap edit.

The proposed namespaced APIs are `OpenSearchServerless` for desired state and `OpenSearchServerlessOperation` for an explicit action against a particular instance and revision. Both use `serverless.opensearch.org/v1alpha1` in the examples. An instance spec describes what should exist. Its status and the operation status describe what the controller has observed and which stage can resume. [Kubernetes custom resources and subresources](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/)

## Instance example

This example requests three regular search Pods and two regular indexing Pods, plus one spare for each role, in every AZ. The platform provisions the S3 bucket, grants OpenSearch Pods access through their runtime identity and publishes the named repository ConfigMap. The instance selects it through `spec.storage.repositoryRef`. Use the actual zone identifiers visible to Kubernetes.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: catalog-remote-store
  namespace: search-system
immutable: true
data:
  bucket: example-catalog-exclusive
  region: ap-southeast-2
---
apiVersion: serverless.opensearch.org/v1alpha1
kind: OpenSearchServerless
metadata:
  name: catalog
  namespace: search-system
spec:
  image: registry.example.com/opensearch/serverless@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  serviceAccountName: catalog-runtime
  runtime:
    settings:
      cluster.name: "catalog-production"
    jvm:
      options: ["-XX:+ExitOnOutOfMemoryError"]
      systemProperties:
        user.timezone: "UTC"
  storage:
    type: s3
    repositoryRef:
      name: catalog-remote-store
    credentials:
      type: WorkloadIdentity
      awsRoleArn: arn:aws:iam::123456789012:role/serverless-store
  snapshots:
    repository: catalog-snapshots
  pools:
    search:
      capacityProfileRef: search-dedicated
      zones:
        ap-southeast-2a: { replicas: 3, warmSpare: 1, minServing: 2 }
        ap-southeast-2b: { replicas: 3, warmSpare: 1, minServing: 2 }
        ap-southeast-2c: { replicas: 3, warmSpare: 1, minServing: 2 }
      protectFromScaleDown: true
      resources:
        requests: { cpu: "6", memory: 24Gi, ephemeral-storage: 100Gi }
        limits: { memory: 24Gi, ephemeral-storage: 150Gi }
      jvmHeap: 12Gi
      warmOnJoin: true
      warmReadiness: Strict
      runtime:
        settings:
          serverless.search.warm_on_join.max_wait: "4m"
    ingest:
      capacityProfileRef: ingest-dedicated
      zones:
        ap-southeast-2a: { replicas: 2, warmSpare: 1, minServing: 1 }
        ap-southeast-2b: { replicas: 2, warmSpare: 1, minServing: 1 }
        ap-southeast-2c: { replicas: 2, warmSpare: 1, minServing: 1 }
      protectFromScaleDown: true
      resources:
        requests: { cpu: "8", memory: 32Gi, ephemeral-storage: 150Gi }
        limits: { memory: 32Gi, ephemeral-storage: 200Gi }
      jvmHeap: 16Gi
  availability:
    minServingTotal: { search: 6, ingest: 4 }
    maxConcurrentRetirements: 1
    maxRollingSurgePerGroup: 1
    maxPendingProvisionablePods: 2
    maxConcurrentSearchWarmups: 2
    emergencyScaleOut:
      enabled: false
  rollout:
    strategy: BlueGreen
    maxPreviewReplicas: 21
    rollbackWindow: 30m
  tls:
    transport:
      source: Secret
      certificateRef: { name: catalog-peer-leaf }
      trustBundleRef: { name: catalog-peer-trust, key: ca.crt }
    http:
      source: Secret
      certificateRef: { name: catalog-http-leaf }
      trustBundleRef: { name: catalog-http-trust, key: ca.crt }
```

### Spec semantics

| Input | Validation and behavior |
| --- | --- |
| `image`, `serviceAccountName` | Require a digest for recorded revisions and an existing workload service account. Java starts as a JVM process with the documented settings and private runtime paths. Service account permission belongs to the namespace's deployment policy. |
| `plugins` | Optional OpenSearch plugin bundles referenced by OCI manifest digest. The operator automatically downloads and installs them before Java starts in each affected Pod. Qualify the resolved set against the Java image. Changing a bundle creates a Pod template revision. [OCI plugin contract](oci-plugins.md) |
| `storage` | Configures the primary object store. For `s3`, select a named repository with `repositoryRef` or supply the settings inline. Use a pre-created exclusive bucket on a qualified provider. The platform retains ownership of the bucket and its access policy through `Decommission` and CR deletion. The `fs` mode is qualified separately for development or a filesystem that meets Java's locking and atomicity contract. [Primary S3 remote store](#primary-s3-remote-store) |
| `snapshots.repository` | Optional name of a snapshot repository registered with the Java shell. Snapshot operations use this logical name within the primary object store. [Snapshot repository parameter](#snapshot-repository-parameter) |
| `pools.<role>.zones` | Keys are exact Kubernetes zone label values. The desired count is `replicas + warmSpare`; both are nonnegative and `minServing` cannot exceed their sum. Spares are running, fungible members of the serving fleet, not idle Pods or separately labelled victims. A zero total means no desired Pods in that group. Actual warmth must be qualified separately; an additional desired Pod is not proof of a warm cache. Existing instances remove capacity through an explicit phased scale-down. |
| `pools.<role>.protectFromScaleDown` | When enabled, create every Pod in that role with `cluster-autoscaler.kubernetes.io/safe-to-evict: "false"` for its lifetime. This avoids an annotation-toggle race, but may retain more nodes than the spare count. It is an autoscaler scale-down hint, not a guarantee against upgrade, repair, interruption or forceful eviction. Changing the field for existing Pods requires a qualified replacement rollout. |
| `minServingTotal` | Separate floors for operator-controlled actions for each role. Coordinate provider drains when a hard limit across groups is required. Validate the floors against steady targets. When qualifying one-AZ tolerance, check surviving measured role capacity after loss of the largest zone. |
| `capacityProfileRef` | Refers to a platform-authored namespaced profile containing the selector, toleration and qualified shapes or observed resource floor. Profile absence, stale qualification, and unsupported zones are surfaced before releasing new scheduling demand. Shared node placement can omit the dedicated profile while keeping role/AZ constraints. [Profile contract](placement-and-recovery.md#dedicated-node-profiles) |
| `resources`, `jvmHeap`, sidecars | Validate that declared heap and runtime overhead are compatible with memory limits. Require explicit resource requests for workload containers and sidecars in dedicated mode. Resource fit uses the admitted Pod spec including injected containers and runtime overhead. |
| `runtime`, `pools.<role>.runtime` | Common and per-role startup settings and JVM options. The operator resolves them into the launch configuration and validates them against the selected image's supported settings and ownership rules. [Runtime configuration](runtime-configuration.md#override-rules-and-rendering) |
| `pools.search.warmReadiness` | `Strict` requires a container-local probe tied to the current Java process's qualified warm-up signal before the Pod can count toward the maintenance PDB. A persisted custom Pod condition alone can outlive a JVM restart. `ServingReady` uses shell serving readiness and membership while separately displaying the warm deficit. Strict admission requires a compatible image and an explicit all-cold fallback policy. [Readiness and shutdown](traffic-and-security.md#readiness-and-shutdown) |
| `pools.search.coldStartPolicy` | The experimental `AllowOneDegraded` policy permits one durable bootstrap Pod per serving fleet to use serving readiness even if cold; it can count toward a PDB for its lifetime. Remaining development Pods use the Java and probe completion gates. The option requires strict readiness and an explicitly qualified development image. It is not yet qualified for production recovery. |
| `availability` | Controls planned disruption, rolling surge, pending scheduling demand, and concurrent search warm-up starts through separate budgets. Emergency scale-out has explicit per-role limits, eligible zones, and a total cap when enabled. Authenticate and validate any platform hint before using it to activate this policy. |
| `rollout` | `Rolling` uses the active fleet. `BlueGreen` stages a second fleet within its preview capacity budget and promotes only on a matching approved operation. Preview/rollback budgets account for both fleets as well as terminating resources. `rollbackWindow` governs intended retention. Certificate expiry and storage compatibility can end rollback eligibility earlier. |
| `tls` | Independent settings for HTTP and peer transport. A leaf and its trust bundle reference same-namespace sources for the supplied Secret mode. An optional issuer mode requests a cert-manager `Certificate`. Identity and usages must match what the pinned Java image can validate. [TLS contract](traffic-and-security.md#tls-and-credential-rotation) |
| `security` | Selects an approved provider and references same-namespace credential and policy inputs. `HotReload` requires verified support for the inputs in scope. [Dynamic security configuration](security-reload.md#proposed-operator-api) |

### Primary S3 remote store

Pod arguments preserve literal dollar signs in the resolved repository
settings. Kubelet expands the operator's `POD_IP` reference for the Java
bind address. Runtime-qualified fleets also compare the rendered store
arguments with the pinned repository settings.

For keystore credentials, the controller rechecks the pinned Secret UID
immediately before Pod creation and gate release. It verifies the mount and
rejects aliases or shadows for both keystore-only and Basic Auth Pods.
Kubelet resolves the Secret by name, so platforms must still prevent
replacement of the Secret under that name between gate release and mounting.

Use `spec.storage.repositoryRef.name` to select a platform-authored, same-namespace ConfigMap for the primary repository. It provides required `bucket` and `region` values and accepts optional `endpoint` and `pathStyleAccess` values for qualified S3-compatible endpoints. Credentials stay under `spec.storage.credentials`. Verify the selected image can load and refresh those credentials as applicable. Deployments that supply the settings inline omit `repositoryRef`. Reject mixed inline and referenced repository settings and report missing or invalid input. The development resolver accepts an HTTP(S) endpoint or a host with optional port, including a public path where needed. It rejects URL credentials, query strings, fragments and invalid host or port values. The entire endpoint, including any path, is public configuration recorded in status and passed to Java; do not put secrets there. Use the configured credential mechanism instead.

The opt-in development direct-Pod path supports `WorkloadIdentity` with an explicit AWS role ARN, a projected ServiceAccount token for the STS audience, and an image matching `--development-s3-web-identity-image`. That image must contain the experimental Java overlay. This is an experimental contract, not cloud qualification. IAM must trust the cluster's token issuer and grant the role access to the supplied bucket. Admission that injects or changes Java credentials can cause gate validation to reject the Pod. The local Java fixture checks credential refresh after a token-file change; real kubelet rotation, IAM trust and cloud access remain unverified. An omitted credentials field does not establish workload identity: the Java client can fall back to container or instance-profile credentials. The operator does not create the bucket or configure cloud IAM.

The development Pod disables automatic ServiceAccount token mounting. Its gate checks that this setting remains disabled, but that setting alone does not prevent a separate admission component from adding a projected token. The web-identity path mounts its token only in Java and checks the admitted projection, Java environment and mounts before gate release. It asks the EKS identity webhook to skip all intended containers using its documented annotation; different webhook configurations and later admission mutations need independent qualification. This does not establish isolation against independently injected token volumes or a privileged container; admission policy and namespace access remain platform responsibilities. [EKS webhook annotation](https://github.com/aws/amazon-eks-pod-identity-webhook#usage)

The operator resolves and pins the effective repository settings and renders the shell's `serverless.store.*` startup settings for all roles and compatible blue/green fleets. The pinned Java shell bundles `repository-s3` and constructs its internal repository named `serverless` during startup. The configured bucket holds durable metadata, shard state, the write-ahead log and Lucene segments. The ConfigMap name identifies the operator's configuration source. The bucket and endpoint identity are immutable after the first accepted deployment. Record the resolved settings so source edits, deletion or recreation cannot redirect a running instance or its replacement Pods to another bucket. [Java storage settings and credentials](../research/java-serverless-runtime.md#object-storage-and-credentials) · [S3 repository construction](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/shell/src/main/java/org/opensearch/serverless/store/ObjectStores.java#L71-L89) · [Repository factory call](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/shell/src/main/java/org/opensearch/serverless/store/ObjectStores.java#L120-L137)

### Snapshot repository parameter

Set `spec.snapshots.repository` to select an existing logical repository for snapshot operations. Register the name separately through the authenticated Java API, for example `PUT /_snapshot/catalog-snapshots` with `{"type":"fs"}`. The shell uses `fs` to identify its native repository type even when `spec.storage.type` is `s3`. Repository metadata and snapshots live in the instance's configured object store. Blue and green see the same registration because they share that store. [Repository REST contract](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/shell/src/main/java/org/opensearch/serverless/snapshot/RepositoryHandler.java#L53-L69) · [Native repository type](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/shell/src/main/java/org/opensearch/serverless/repository/RepositoryDescriptor.java#L27-L49)

Resolve the exact name with `GET /_snapshot/{repository}` before running an operator-managed snapshot operation. If lookup fails, report the problem on that operation and allow the serving fleets to continue. Omitting the field leaves snapshot selection to the caller. Changing the field selects a different repository for future operations without replacing Pods. Ownership of repository registration and its contents stays with the caller. A native snapshot shares the primary store's failure domain. [Snapshot and restore behavior](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/STATUS.md#L111-L131)

### Identity and ownership

Pod roles use `search` and `ingest`. `ingest` is the Java runtime name for an indexing Pod. The operator renders explicit `serverless.roles` and the corresponding zone setting for each group. Each Pod carries controller-owned labels under `serverless.opensearch.org/` for instance UID, role, AZ, fleet and template revision. A **fleet** is an immutable traffic and availability cohort. A rolling replacement can change template revisions within a fleet. Blue/green stages a distinct fleet and then changes the stable Service selectors. Pod placement labels and selectors are immutable for that Pod. [Java runtime and settings](../research/java-serverless-runtime.md#launch-and-configuration)

| Resource | Ownership and identity |
| --- | --- |
| Pods | Owned directly by the instance CR via a same-namespace controlling owner reference. Each Pod has a recorded creation intent and deterministic name for retry, and an observed UID once created. Pod names are not Java node identities. |
| Production Services | `<instance>-search` and `<instance>-index`, each selecting instance UID, role and the serving fleet. DNS and port remain stable across promotions. |
| Preview Services | Candidate-scoped search and index Services are created during staging and retired with that preview. |
| Gateway backend Services | Opt-in ClusterIP Services for individual role and fleet pairs. The operator records deterministic names and pins UIDs in `status.gatewayBackends`. These Services survive changes to preview or serving roles. [Per-fleet Gateway Services](../implementation/development-rollout.md#per-fleet-gateway-services) |
| PDBs | One non-overlapping PDB per fleet, role and AZ group, with integer `minAvailable` because Pods are directly owned. During rolling, old and new template revisions share that group's PDB. The controller checks workload-level floors in addition to the limits Kubernetes evaluates. |
| ConfigMaps and Secret snapshots | Versioned names scoped to the instance and input revision, with owner references limited to material generated by the operator. A retained fleet keeps the startup inputs it needs for restart and rollback and must meet the current security revision before readmission. A qualified file-based provider receives live changes through a stable delivery Secret. User inputs remain externally owned. [Security activation](security-reload.md#apply-a-security-revision) |
| `Certificate` | Optional operator-owned cert-manager request using an approved issuer reference. cert-manager owns issuance. The controller validates and pins a produced Secret version before activating it. |
| Capacity profiles and maintenance hints | Platform-authored. The operator reads pre-created namespaced objects. The platform protects writes to these objects through namespace isolation or admission. A profile identifies qualified placement and a maintenance hint can describe an operation scope and deadline. A deployment without the hint integration still reconciles from Pod state and maintains warm reserves. |

Require controlling owner UID equality when observing, modifying or deleting generated objects. Conflicting names, forged labels or a newly created CR with the same name must not cause adoption. Rerender mutable fields only in resources the operator owns, and report Service selector ownership conflicts as blocked operations. Keep persisted status sufficient to recover the last admitted fleet and a partial Service switch. [Kubernetes ownership](https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/) · [Server-side apply field ownership](https://kubernetes.io/docs/reference/using-api/server-side-apply/)

## Explicit operations

Separate approval from spec editing. `Promote` and `Rollback` target a specific fleet and carry the expected traffic assignment **by role**. `PrepareMaintenance` targets a platform-supplied scope and can report when extra serving capacity is available. `Decommission` requests ordered shutdown while the instance still exists. `ApplyClusterSettings` makes an explicit shared persistent settings change through the Java API, with persistence and effective application tracked separately. Changing the desired image or startup configuration can stage a rollout. Approval applies to the named and qualified candidate. [Live cluster settings operation](runtime-configuration.md#live-cluster-settings)

The installed v1alpha1 operation CRD accepts `Promote`, `Rollback`,
`Decommission`, `ApplyClusterSettings`, `ApplySecurityConfiguration`,
`RetryProvisioning`, and `RetireWriter`. Admission checks the action's
payload on creation and keeps the operation spec immutable. The
controller still checks intent and authority before acting. Existing
operations that predate these checks can update status and clear
finalizers after a CRD upgrade.

`ApplySecurityConfiguration` activates a prepared security revision and carries the expected active revision. Its status tracks the committed revision and confirmed application by the required consumers. [Security activation](security-reload.md#apply-a-security-revision)

```yaml
apiVersion: serverless.opensearch.org/v1alpha1
kind: OpenSearchServerlessOperation
metadata:
  name: catalog-promote-20261123
  namespace: search-system
spec:
  instanceRef:
    name: catalog
    uid: 258b3be2-0f79-4e96-aeb8-637de33ad6d3
  action: Promote
  expectedServingFleetByRole: { search: fleet-a17, ingest: fleet-a17 }
  targetFleet: fleet-b28
```

Operation spec is immutable after creation. The controller claims at most one disruptive operation for an instance and records the operation UID before any external side effect. A duplicate with the same intent converges on the recorded result. A stale per-role mapping, instance UID, unsupported target, or competing operation is reported without changing traffic. Pause and resume apply to the active operation. To roll back a partially applied promotion, an authorized rollback request carries `supersedesOperationUID`, the observed mixed per-role mapping and the retained target fleet. The controller records it as a recovery transition under the same active operation lock. It stops further promotion steps and conditionally restores only roles that switched. [Operation recovery](reconciliation.md#durable-operations)

## Status contract

| Field or condition | What to report |
| --- | --- |
| `observedGeneration` | The spec generation that produced the status. Condition updates identify the generation evaluated. |
| `servingFleetByRole` | Actual fleet selected by each production Service. During an interrupted promotion the two values can differ. Status separates recorded intent from evidence read back from each Service. |
| `groups` | Desired, scheduled, ready, Java-eligible, warm or takeover-capable, draining and pending counts for each relevant fleet, role and requested AZ. Include the floor, observed spare capacity and shortfall. Distinguish requested AZ from independently verified binding topology, including provenance or unknown. Keep high-cardinality Pod details in operation observations or bounded recent events. |
| `Available`, `Progressing`, `Degraded` | Give a reason and bounded message. Distinguish insufficient fit, failed bootstrap, warm-up below threshold, lost Java lease, provider capacity shortfall, invalid TLS input, and unsupported or stale observations. |
| `operation` | UID, desired generation and operation stage, chosen Pod identities, targets applied, outstanding checks and deadline if applicable. Preserve progress even when the controller restarts. |
| `dependencies` | Qualification revision, validated public TLS fingerprint and expiry, known input capabilities, and observation timestamps. Include endpoint names. Keep private keys, credentials, object-store keys and unbounded log content out of status. |

The operator should not mark an operation successful merely because Pods exist or a Service patch succeeded. Success requires the operation's observed availability, traffic or shutdown postconditions. Provider health and operator observations can be stale. Use explicit unknown status where a check cannot run.

For example, this excerpt describes an interrupted search-first promotion. The candidate search floor is still served in the sampled AZ. Indexing is still routed to the original fleet. The operation must resume against `fleet-b28` or reconcile an authorized rollback to `fleet-a17`.

```yaml
status:
  observedGeneration: 12
  servingFleetByRole: { search: fleet-b28, ingest: fleet-a17 }
  groups:
    - fleet: fleet-b28
      role: search
      zone: ap-southeast-2a
      desired: 3
      scheduled: 3
      ready: 3
      eligible: 3
      warm: 3
      minServing: 2
  operation:
    uid: 49517f90-a122-43fe-82b7-c618e45f8f3a
    action: Promote
    phase: SwitchIndexing
    targetFleet: fleet-b28
    previousFleet: fleet-a17
```

## Sidecars and Pod isolation

Accept a pool-level list of named sidecars and bounded init containers. A native sidecar uses `initContainers[*].restartPolicy: Always` where the supported Kubernetes version has it. Select this for a proxy or helper that must start ahead of Java or remain available during Java shutdown. Use normal sidecars for helpers without that ordering requirement. Validate container names, ports, probe semantics, shared mount paths and resource requests. Include sidecar identity and effective admitted resources in revision and fit checks respectively. [Kubernetes sidecar containers](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/)

Keep reserved arguments, labels, lifecycle hooks, critical mounts, credentials and exposed ports under operator control. Admission must prevent the CR author from using the operator's service account to bypass namespace Pod Security, approved images, workload identity and Secret access boundaries. Prefer a namespace operated by trusted deployers. An untrusted tenant who can create instances requires explicit reference and template validation before enabling the API for that audience.

## Permission boundary

| Identity | Required capability |
| --- | --- |
| Controller in each watched namespace | Read its CRs and inputs, update its own status and finalizers, manage owned Pods, Services, PDBs and generated configs, read EndpointSlices for route convergence, and create `pods/eviction` for PDB-aware retirement. A supplemental readiness gate adds permission to patch `pods/status` in that namespace. Verify ownership in the controller and enforce reserved labels and mutation policy at admission where trust boundaries require it. Namespaced RBAC grants resource verbs. It does not enforce ownership by label. Network reachability to health and route probes is separate from these permissions. |
| Controller for optional TLS sources | Read only allowed Secret inputs and manage its generated snapshot Secrets. Apply resource-name scoping where feasible and use direct reads or appropriately filtered watches. Managed issuance adds access to the selected cert-manager API and approved issuer reference. Avoid granting a cluster-wide Secret informer by default. |
| Optional cluster settings action | Use a narrowly authorized OpenSearch admin identity through the private Java settings endpoint. Limit the action to qualified keys. That identity is separate from the controller's Kubernetes or cloud permissions. [Live cluster settings](runtime-configuration.md#live-cluster-settings) |
| Workload | Connect to peers and the exclusive object store using platform-issued identity. The local health endpoint and JVM shutdown require no Kubernetes API token. Set token projection deliberately if workload identity uses it. |
| Installer and platform | Install CRDs and namespace RBAC, run the edge and autoscaler, grant opt-in publisher permissions and publish capacity profiles. Optional node-level observation stays with a platform component or a separately authorized read-only controller integration. |

The standard deployment can recover without Node and NodeClaim permissions or cloud API credentials in the OpenSearch controller. In an install that needs cluster-wide watches, review each resource and restrict the controller to the namespaces or trust domains it is intended to serve. [Kubernetes RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)
