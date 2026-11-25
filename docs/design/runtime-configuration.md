# Runtime and cluster settings

The isolated development settings backend checks the admin Service UID,
resource version and routing shape before and after an HTTP request. It
rejects a read if the Service changed and treats a write as unconfirmed if
the Service changed before the post-write check. This does not make the HTTP
request atomic with Service changes or protect against external settings
writers.

The development gated-Pod path checks that each requested sidecar still has
its name, image, command, arguments, minimum resource requests, specified
limits and explicitly configured environment, probes, mounts, lifecycle and
security fields after admission. Kubernetes may add defaulted object fields;
specified lists remain ordered and exact. It permits additional injected
containers. This check does not establish full sidecar configuration or
lifecycle equivalence; those require further validation before qualification.

`pools.<role>.nativeSidecars` declares restartable init sidecars. The operator
sets their restart policy to `Always` and places them in declaration order
before its DNS preflight and plugin installer. Names must be unique across all
requested containers and cannot use the operator's reserved container names.
The development fit check includes their overlap with later init containers.
Admission checks preserve their order relative to the operator's init
containers while allowing additional injected init containers.
Kubelet lifecycle behavior and supported Kubernetes versions remain to be
qualified; the API-server admission fixture used Kubernetes 1.32.

The development API supports `pools.<role>.sidecarVolumes` with a name and
exactly one of `secretRef`, `configMapRef` or `emptyDir: true`. A sidecar can
mount one of these names through its standard `volumeMounts`; references are
in the instance namespace. The operator does not copy or own the source
objects. Secret and ConfigMap contents can change independently of the Pod
revision. Admission checks hold a Pod if a declared volume changes or a
non-requested container mounts it. This is not a security boundary against
privileged containers, shared process or network access, or platform
administrators; namespace admission policy must constrain those capabilities.
The CRD treats the volume list as a map keyed by name and rejects duplicate,
reserved and invalid volume names and missing or conflicting sources.
Controller validation also checks referenced object names. Existing duplicate
volume names must be corrected before updating to this CRD schema.
An `emptyDir` sidecar volume may specify `sizeLimit` as a positive Kubernetes
quantity. The controller validates the quantity; the CRD rejects attaching a
limit to a different volume source. The limit is not a scheduler reservation:
configure appropriate container ephemeral-storage requests and limits too,
and qualify node disk pressure and kubelet eviction behavior separately.

Expose three configuration paths. Use `spec.runtime` for common startup settings and JVM options, `spec.pools.<role>.runtime` for role-specific overrides, and an explicit `ApplyClusterSettings` operation for supported live, shared settings. Keep heap and Kubernetes resource sizing in the typed pool fields. This design targets the published Java `feature/serverlessshell` revision `e71d42331a3ec82ec01109a5bb42d6a9a9162a5c`. The API examples are proposed. [API and resources](api-and-resources.md) · [Java deployment contract](../research/java-serverless-runtime.md)

| Input | How it reaches Java | How a change takes effect |
| --- | --- | --- |
| `pools.<role>.jvmHeap` | The launcher derives explicit `-Xms` and `-Xmx` from the configured quantity. | A new Pod template revision for the role. |
| `runtime.jvm.options` | An ordered list of supported JVM argument tokens, such as GC or diagnostic flags. | A new Pod template revision for every role whose resolved options changed. |
| `runtime.jvm.systemProperties` | Flat map of supported JVM-only properties, each rendered as `-Dkey=value`. | A new Pod template revision for affected roles. |
| `runtime.settings` | Flat map of startup OpenSearch settings, each rendered as `-Dkey=value` before the Java main class. | A new Pod template revision for affected roles, subject to compatibility checks. |
| `ApplyClusterSettings` | An authenticated `PUT /_cluster/settings` containing a narrow `persistent` patch. | The shared object-store register changes. Each qualified consumer applies the setting using its own refresh path. |
| TLS keys, store credentials and other secure settings | Typed references to credentials, workload identity, keystore or provider inputs. | The supported credential or TLS rotation path. |
| Authentication and authorization policy | Provider-specific Secret and policy references under `spec.security`. | Explicit activation and per-process verification for qualified in-place changes. [Dynamic security configuration](security-reload.md) |

## Startup configuration example

The following fragment uses common JVM options and a time zone, with a
longer warm-up window and a distinct JVM option list on search Pods.
Indexing uses the common options. The surrounding instance spec defines
resources, zones, storage and TLS.

```yaml
spec:
  runtime:
    jvm:
      options:
        - "-XX:+ExitOnOutOfMemoryError"
      systemProperties:
        user.timezone: "UTC"
  pools:
    search:
      jvmHeap: 12Gi
      warmOnJoin: true
      runtime:
        settings:
          serverless.search.warm_on_join.max_wait: "4m"
        jvm:
          options:
            - "-XX:+ExitOnOutOfMemoryError"
            - "-XX:+UseG1GC"
    ingest:
      jvmHeap: 16Gi
```

Enable the development startup catalog on the operator with
`--development-runtime-settings-image=<image@sha256:digest>` and gated
Pods. The configured digest must match the instance image. The catalog
accepts `-XX:+ExitOnOutOfMemoryError`, `-XX:+UseG1GC`, and
`user.timezone` with a valid zoneinfo name. Search also accepts
`serverless.search.warm_on_join.max_wait` as a whole number of seconds
or minutes from `1s` through `10m`. The development operator keeps
`cluster.name` fixed to the instance name.

The shell entry point reads eligible **JVM system properties** and ignores application arguments. Its startup prefixes are `serverless.`, `node.`, `cluster.`, `path.`, `http.`, `transport.`, `network.`, `thread_pool.`, and `telemetry.`. The shell has no stock `opensearch.yml` input. A prefix controls forwarding, not whether a setting has a runtime consumer. For example, a classic allocation key can reach the settings object without gaining the classic cluster-manager behavior. The installed image's supported-setting catalog determines which startup keys the operator accepts. [Java prefix list](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/shell/src/main/java/org/opensearch/serverless/shell/ServerlessBootstrap.java#L99-L114) · [Entrypoint setting extraction](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/shell/src/main/java/org/opensearch/serverless/shell/ServerlessBootstrap.java#L452-L478)

## Override rules and rendering

Custom arguments are escaped before kubelet expands environment
references. A literal dollar sequence in a custom value reaches Java
as supplied. The operator's own Pod IP argument still expands from
the downward API. Fleet qualification checks the exact custom
arguments in order and verifies the launcher argument structure.
Pod admission checks the values of the managed settings against the
rendered template.


1. Start from the selected image's launcher defaults. Overlay instance settings, then explicitly present role entries for keys the catalog marks role-configurable. Merge `settings` and `jvm.systemProperties` by exact key. Omission in a role map inherits the instance value. Removing a key from the instance map returns to the image's default when the role does not provide it. Require quoted scalar strings. Any encoding of a list requires a setting-specific rule in the catalog.
2. Use the instance JVM option list when a role omits it. A provided role list replaces the common custom list, including an explicit empty list. Image-required launcher arguments remain separate. Preserve the user list order. Use typed fields for controls such as heap and memory. Reject duplicate heap flags and alternate inputs that would bypass that budget. Qualify on-heap and non-heap memory against the Pod limit, including native, direct-buffer and thread-stack usage.
3. Inject operator-owned identity and deployment values once. Reserve `node.name`, role and zone identity, managed awareness, publish addresses and Service ports, work and config paths, object-store identity, warm-on-join enablement, and TLS/secure settings managed by typed fields. Reject an attempt to set the same value through another path. Permit a custom `cluster.name` at instance scope. Require a common value across roles and compatible fleets. Treat changes to that value on an existing fleet as a separately qualified migration. It does not partition the storage bucket. [Java zone resolution](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/shell/src/main/java/org/opensearch/serverless/cluster/Zones.java#L53-L72) · [Store and membership inputs](../research/java-serverless-runtime.md#object-storage-and-credentials)
4. Render each effective configuration as an immutable, versioned input. Use separate argument tokens. A launcher can read a structured mounted argument file and directly replace itself with `java`, keeping values with literal spaces and `$()` intact and letting `SIGTERM` reach the JVM. Avoid shell interpolation and unsupported flag sources that change the effective config after validation, such as user-supplied Java `@` files or option environment variables. Keep `-D` in the two typed maps and route OpenSearch-prefixed keys through `runtime.settings`. Keep launcher and classpath selection and image-required options under the image contract. Java launcher flags that can execute arbitrary code or redirect bootstrap are a deployment policy concern. Admit only options approved for the image and workload trust boundary. [Java launcher argument and environment behavior](https://docs.oracle.com/en/java/javase/25/docs/specs/man/java.html) · [Kubernetes termination hooks](https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/)

Use distinct catalog entries for typed values, approved JVM options, startup keys, and confirmed live keys. Record startup scope, value format, whether a role override is allowed, and the default or fallback. For a custom plugin, qualify its key against the installed plugin and how the shell actually reads it. An unknown key, unsupported value, reserved setting or incompatible cross-fleet change blocks the new revision with a reason. The controller preserves existing serving capacity during validation.

Declare installed plugin bundles in `spec.plugins` and qualify settings against the resolved plugin digests for each role. [OCI plugins](oci-plugins.md)

Hash the resolved public settings and ordered arguments with the image digest and versioned references to secure inputs. Keep confidential values out of arguments, ConfigMaps, status and logs. Give the old fleet its pinned config so that container restart and blue/green rollback reproduce the intended launch inputs. For a rolling change, compare effective configuration by role and stage only the paths that changed. Reuse the existing role/AZ canary and retirement gates. [Reconciliation](reconciliation.md#normal-reconcile-loop)

### Stage startup settings in blue/green

Put the candidate's complete common and role startup configuration in
`spec.rollout.candidate.runtimeOverride`. The candidate uses the same
qualified image and a positive rollback window. Omitting the override
inherits the serving fleet's settings. An explicit `runtimeOverride: {}`
uses the launcher defaults. Managed settings such as heap, storage,
identity, TLS and warm-up readiness mode still come from their typed
instance fields.

```yaml
runtimeOverride:
  common:
    jvm:
      options: ["-XX:+UseG1GC", "-XX:+ExitOnOutOfMemoryError"]
      systemProperties: {user.timezone: "Asia/Tokyo"}
  byRole:
    search:
      settings:
        serverless.search.warm_on_join.max_wait: "5m"
```

For a newly created instance, the operator records the initial fleet's
startup settings with its first Pod creation intent. Before staging a
candidate, it checks the source roles' admitted Java arguments and
validates the candidate against the catalog. It records both fleets'
startup settings before creating preview Pods. Replacement Pods use
their fleet's pins after promotion or rollback. Change startup values
through the candidate override once a fleet is pinned.

The operator rejects imported environment variables on the Java
container. The runtime launcher clears the JVM option environment
variables before starting Java.

## Live cluster settings

The shell implements `GET /_cluster/settings` and `PUT /_cluster/settings` over one `cluster/config` register in the shared store. Its write path supports persistent scalar values and explicit `null` removal. Nonempty transient settings and list values are refused. The register can store keys with no live runtime consumer, so an HTTP `200` and a matching `GET` only prove persistence. Qualify supported shell settings and qualified dynamic settings of installed plugins for each image. The invoking node can apply some changes immediately and other nodes refresh later. [REST handler](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/shell/src/main/java/org/opensearch/serverless/rest/ClusterSettingsHandler.java#L180-L274) · [Register behavior](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/shell/src/main/java/org/opensearch/serverless/metadata/ClusterConfig.java#L38-L130) · [Plugin refresh](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/shell/src/main/java/org/opensearch/serverless/plugins/PluginClusterSettings.java#L28-L41)

This proposed operation illustrates changing bucket assignment for **new indices** after qualifying the policy. `set` accepts approved string values, `remove` is a disjoint list of approved keys, and the controller translates each removed key to `null` in the REST patch. Unspecified keys are left alone. [Java bucket assignment test](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/testkit/src/test/java/org/opensearch/serverless/testkit/BucketAssignmentTests.java#L171-L189)

```yaml
apiVersion: serverless.opensearch.org/v1alpha1
kind: OpenSearchServerlessOperation
metadata:
  name: catalog-update-cluster-settings
  namespace: search-system
spec:
  instanceRef:
    name: catalog
    uid: 258b3be2-0f79-4e96-aeb8-637de33ad6d3
  action: ApplyClusterSettings
  clusterSettings:
    set:
      serverless.bucket: "off"
    remove: []
```

Serialize the change with other configuration and rollout operations. A persistent setting reaches both active and preview fleets when they share the store. Initially apply shared changes when rollout and rollback retention are settled and the instance has one running fleet. Use an authenticated private admin path with an identity authorized to read and write the qualified settings. Choose a single authorized writer for operator-managed keys. Java makes register merges atomic, but this REST API has no expected-version input for the operator to enforce one writer's read-before-write against another external writer. Read relevant keys before applying the patch, check readback after a success or ambiguous response, and block an automatic retry if the observed values conflict with both the recorded base and the intended result. Require setting-specific observation of effective behavior before reporting it as applied across the fleet. Report `Persisted` separately from `VerifiedEffective` when there is no per-node proof.

The opt-in development controller requires leader election. A finalizer
keeps a claimed operation available for recovery. The controller writes
the terminal outcome before releasing the instance claim. `Persisted`
means the requested values were read back. `VerifiedEffective` remains
`Unknown`, and subsequent workload reconciliation can proceed.
`WritePrepared` records the original values and the admin Service UID.
Before the controller authorizes a write, an unexpected value or
Service identity ends the operation as `Conflict`, and deletion ends
it as `Cancelled`.

The controller records `Writing` before submitting a single PUT. A
timeout or crash can leave Java processing that request. Later
reconciles read back through the recorded admin Service. They release
the claim when they observe the target value. An uncertain write leaves
the operation in `Writing` with the claim and finalizer in place, even
after a deletion request. A crash before sending the PUT can also
leave this hold. The Java endpoint needs a conditional write or a
completion receipt to resolve that case automatically. Keep the
qualified admin path available until in-flight settings writes settle.
If the original instance or claim is lost, the operation retains its
finalizer and unresolved outcome. A same-name instance cannot supply
read-back evidence for the original write.
`Prepared` records from earlier development builds also use read-back
because those builds could send a write from that phase.

A cluster settings change does not create a Pod template revision by itself. Traffic rollback changes the serving fleet and cannot undo a persistent config write. To restore configuration, submit a separate qualified operation with the prior values or removals. Index-scoped settings, mappings and templates use their Java API and ownership contracts rather than the startup or cluster-settings routes. [Java reads and API behavior](../research/java-serverless-architecture.md#reads-search-placement-and-api-behavior)
