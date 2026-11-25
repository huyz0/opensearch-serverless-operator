# OpenSearch plugins from OCI registries

Publish plugin bundles independently of the Java image and select them by OCI digest. The operator adds an installer init container to affected Pods. The development blue/green path can stage a replacement plugin set on the same qualified Java image.

## Instance configuration

```yaml
spec:
  plugins:
    - name: catalog-analysis
      artifact: registry.example.com/opensearch-plugins/catalog-analysis@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
      registryCredentialsRef:
        name: plugin-registry-pull
```

The example uses a private registry and an illustrative digest. `name` must be unique and match the installed plugin descriptor. The digest identifies the OCI manifest. `registryCredentialsRef` is optional and refers to an approved pull-only Secret in the instance namespace. Both node roles get the same plugin set by default. An optional `roles` list accepts `search` and `ingest` for a plugin qualified to run on only those roles. Require analyzers, codecs, mappings, peer actions and security enforcement wherever the application depends on them, including retained fleets that still participate in the peer plane. Pass plugin startup settings through `spec.runtime` and provide credentials or reloadable security policy through their typed references. [Runtime configuration](runtime-configuration.md) · [Security policy reload](security-reload.md)

## Artifact and installation contract

1. **Publish.** Use an OCI image manifest with the proposed `artifactType` of `application/vnd.opensearch-serverless.plugin.v1` and a single `application/zip` layer. The ZIP contains `plugin-descriptor.properties`, the plugin JAR and its dependencies in the expected OpenSearch layout. A qualification record ties the full plugin set to the Java image digest, JDK and any required architecture. The initial format covers ZIPs that can be installed into the plugin directory alone. Bundles needing native libraries, generated configuration or launcher changes require a separately qualified package contract. [OCI artifact manifest](https://github.com/opencontainers/image-spec/blob/main/manifest.md#guidelines-for-artifact-usage)
2. **Stage.** A digest-pinned, operator-supplied init container fetches the OCI manifest and layer using a registry client such as ORAS. Check the manifest and blob digests, the allowed publisher or signature policy, media types, size, descriptor name and declared dependencies before installing. Reject extraction outside the destination, symlinks and ambiguous duplicate files. Assemble the complete plugin set in staging and publish only after it validates, so a retry can rebuild a clean directory. Mount registry credentials only into the installer. Kubernetes `imagePullSecrets` can pull the installer image. The installer uses its separately configured registry credentials for artifact requests it makes itself. [ORAS OCI artifacts](https://oras.land/docs/concepts/artifact/) · [Kubernetes init containers](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/)
3. **Load.** Mount the assembled Pod-local volume read-only at `<path.home>/plugins` in the Java container. The mount covers filesystem plugins in the Java image. If that image bundles any required filesystem plugins, a preparation init container using the same image digest copies them into staging before OCI installation. Validate name collisions across both inputs. The pinned shell discovers the mounted path with `Environment` and calls OpenSearch's `PluginsService` to check compatibility, build plugin classloaders and order dependencies. `serverless.plugins` remains the mechanism for classes already on the Java classpath. [Plugin path](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/server/src/main/java/org/opensearch/env/Environment.java#L136-L146) · [Shell plugin loader](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/shell/src/main/java/org/opensearch/serverless/node/NodeServiceBuilders.java#L91-L128)

Plugins execute inside the OpenSearch JVM with the Pod's application permissions. Restrict installation to approved registries, publishers and digests, and control who can change `spec.plugins`. Limit installer CPU, memory and ephemeral storage and include its effective requests in dedicated-node fit. Keep approved digests available for replacement and rollback through the registry's retention policy. [Dedicated-node fit](placement-and-recovery.md#dedicated-node-profiles) · [Kubernetes init container resource sharing](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/#resource-sharing-within-containers)

The pinned Java shell already bundles `repository-s3` as a runtime dependency and uses it to open the primary S3 store. Select that repository through `spec.storage.repositoryRef`. An extra OCI install of `repository-s3` can collide with the bundled classes, so reject the duplicate during plugin qualification. A future provider for the primary store needs a qualified Java integration as well as a packaging contract. [Primary S3 remote store](api-and-resources.md#primary-s3-remote-store) · [Shell Gradle dependencies](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/shell/build.gradle#L33-L43)

### Development installer

`cmd/plugin-installer` builds a new directory from a JSON array of `{ "name": "...", "artifact": "registry/repository@sha256:..." }` entries passed with `-request`. An independently controlled `-policy` JSON file supplies `approvedArtifacts` (the exact references), `openSearchVersion`, `javaVersion`, `maxBytes` (per artifact and extracted archive) and `maxFiles` (per archive). An optional `-credentials` file contains a JSON object keyed by exact registry host, with `username` and `password` fields. Mount that file only into the installer. `-output` names an absolute directory under a private Pod-local parent; the parent must have a single trusted writer. An already published set is accepted on retry only when its completion marker matches the requested set and compatibility policy. On retry, the installer removes abandoned directories in its reserved `.plugin-set-<number>` staging namespace. The parent must be private to a single installer, including across restarts. The runtime image must include trusted CA certificates. The current implementation uses HTTPS, stages the complete set, checks exact descriptor version strings, and rejects extended-plugin dependencies and `repository-s3`. It publishes no directory when an installation fails. The digest allowlist is a policy input, not a signature check.

The immutable credentials Secret may also contain a PEM bundle under `ca.crt`.
When present, the controller passes it only to the installer, which adds it to
system roots for all registry HTTPS connections in that installation. Treat the
bundle as a trusted platform input and rotate it through a new immutable Secret
and Pod. The Secret UID and presence of the bundle are pinned, but Kubernetes
mounts a Secret by name, so the name-reuse race still requires qualification.

The development direct-Pod controller can stage plugins when gated Pods are enabled and the operator is configured with all three flags: `--development-plugin-installer-image` (a digest-pinned installer image), `--development-plugin-runtime-image` (the exact qualified Java image digest), and `--development-plugin-policy` (the name of an immutable policy ConfigMap in the watched namespace). Grant the separate `config/dev/plugin-staging-rbac.yaml` Role for snapshot creation. The policy ConfigMap contains the installer policy as `policy.json`; it must independently approve each artifact digest. An optional immutable credentials Secret contains `credentials.json` and is mounted only in the installer. All selected plugins for a role must use the same credentials Secret, if any.

For example, an administrator can create a policy ConfigMap like the following in the watched namespace. Replace the illustrative artifact digest and version strings with the exact qualified values; these do not establish compatibility by themselves.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: plugin-approval
immutable: true
data:
  policy.json: |
    {
      "approvedArtifacts": ["registry.example.com/opensearch-plugins/catalog-analysis@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"],
      "openSearchVersion": "3.0.0",
      "javaVersion": "21",
      "maxBytes": 268435456,
      "maxFiles": 1000
    }
```

A private registry can use an immutable Secret with `credentials.json` as a JSON object keyed by the exact registry host (including a port when present), with `username` and `password` fields. Store and provision those credentials through the environment's secret management system rather than committing them to a manifest. The installer checks descriptor versions against the policy strings exactly.

Reference the approved artifact and credentials Secret from the instance. This example installs the plugin only on search Pods.

```yaml
spec:
  plugins:
    - name: catalog-analysis
      artifact: registry.example.com/opensearch-plugins/catalog-analysis@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
      registryCredentialsRef: {name: registry-pull}
      roles: [search]
```

Before creating a Pod, the controller persists the selected source identities,
then creates or reuses immutable request and policy ConfigMaps and records
their UIDs. Each ConfigMap name derives from the instance UID and the
corresponding content digest. Pods of the same instance can share identical
inputs across roles, scale-out and replacement. A recorded per-Pod snapshot
pair from an older controller remains valid for that Pod. Kubernetes
garbage-collects the owned ConfigMaps when the instance is deleted.
Before reverting to an older controller that expects per-Pod names, let
outstanding Pod creation and scheduling-gate release intents finish.

The controller rechecks the source and snapshot identities before gate
release. The installer verifies the exact request and policy SHA-256 values
before downloading. Each Pod uses its own 2 GiB `emptyDir` and an installer
with CPU, memory and ephemeral-storage requests. Java runs as UID 10001
and mounts the installed directory read-only. Use a qualified Java image
whose required plugins are on its classpath. The volume hides filesystem
plugins in the image.

After a plugin-enabled Pod binds, the development controller keeps its single provisioning slot until the matching installer init container reports a successful termination. If installation does not complete within `--development-pending-timeout`, it records `PluginInstallTimeout`, retains the bound Pod and isolates its role/AZ hold. The controller continues to observe that Pod and clears its group hold only after the installer succeeds and the Pod reports Running and Ready. An absent or changed Pod or installer identity instead requires recovery. This bounds the operator’s own sequential demand; it cannot prevent provider launches already in progress or other workloads from scaling.

Kubernetes mounts a Secret by name, so deletion and recreation after the UID check can still change the credentials mounted by the kubelet. ConfigMap create access and Secret get access are namespace-wide. A recorded fleet holds Pod creation on source identity changes. Rolling replacement, credential rotation and recovery from changed identities remain open. The installer uses the digest approval list. Signature verification and credential helpers remain open. Its image recipe is `runtime/plugin-installer.Dockerfile`. Pin and qualify both supplied base images. A kind drill verified private-registry Basic authentication, kubelet mount isolation, Java loading and outage recovery with a synthetic plugin. Application compatibility is specific to the selected plugin and runtime. [Live OCI verification](../implementation/verification.md#live-oci-installation-and-registry-outage)

### Stage a blue/green plugin change

In the immutable installer policy, approve all artifact references required
by both fleets. For a changed plugin set, add an entry to
`approvedFleetTransitions` with `from` and `to` fleet digests.
This entry records the administrator's qualification that the sets can
coexist on the configured Java image and that the source can serve
again after the destination writes. Keep the source artifacts and
credentials available through the rollback window. To add an approval,
configure a new immutable policy ConfigMap and keep the earlier
ConfigMaps available to their pinned fleets. Each fleet replaces Pods
using its recorded install policy. New staging uses the currently
configured policy to approve coexistence.

For each role, sort the selected plugins by name and encode a compact
JSON array of objects with `name` and `artifact` fields in that order.
The role hash is the lowercase hexadecimal SHA-256 of those JSON bytes.
A role with no plugin selection uses an empty string. To compute a fleet
digest, concatenate the ingest role hash, a newline, the search role hash,
and a final newline. Take SHA-256 and prefix the lowercase hex with
`sha256:`.

```yaml
spec:
  rollout:
    strategy: BlueGreen
    rollbackWindow: 5m
    maxPreviewReplicas: 3
    candidate:
      fleet: green
      image: registry.example.com/opensearch/runtime@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
      pluginOverride:
        plugins:
          - name: catalog-analysis
            artifact: registry.example.com/opensearch-plugins/catalog-analysis@sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789
            registryCredentialsRef: {name: registry-pull}
            roles: [search]
```

The override replaces the whole candidate set. Use `pluginOverride: {}`
to remove all filesystem plugins. Omitting it inherits the serving set.
The controller checks the Ready source cohorts before recording their
first plugin pins. It verifies the installer image, input digests,
mounts and successful init status against the recorded inputs. It pins
both fleets before creating preview Pods.
A simultaneous edit to `spec.plugins` cannot replace the set observed
on the running source Pods. Promotion and rollback still require the
existing traffic and process checks. Use the updated installer binary,
which recognizes the fleet approval policy field.

## Rollout and verification

Include the resolved plugin names, artifact digests, installer image and installation contract version in the affected role revisions. A missing blob, failed signature check or incompatible plugin blocks the candidate before it can receive traffic. Report the affected plugin and a bounded error without exposing registry credentials. Use the existing bounded surge and retry controls during registry outages. A functioning candidate must pass Java readiness and a check that the expected plugins loaded, followed by representative requests for their features. Keep the previous usable revision while validating a candidate.

OpenSearch version matching is necessary for loading, and full compatibility depends on the shell's classpath and services. The pinned source reports a configuration where OpenSearch Security runs with companion bundles. Qualify that exact combination for authentication, authorization and TLS before using it, with separate evidence for in-place policy reload. For stateful plugins, check index and wire compatibility through mixed-fleet operation and rollback. Hold promotion if a plugin could write state that the retained fleet cannot read. [Pinned plugin compatibility notes](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/STATUS.md#L169-L199) · [Delivery tests](delivery-and-validation.md#tests-and-acceptance-criteria)
