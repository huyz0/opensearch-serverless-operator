# Install the development operator

For HTTPS with startup Basic Auth, start with the
[secure development overlay](../operator-dev-secure/README.md).

Build the manager image and make it available on the Kubernetes nodes that
can run the controller. The default Dockerfile pins the Go builder and
includes its CA roots in the non-root manager image. The local build below
uses the tested Linux AMD64 platform.

```sh
docker build --platform linux/amd64 --provenance=false -t opensearch-serverless-operator:dev .
python tools/import_kind_images.py --node kind-control-plane --platform linux/amd64 opensearch-serverless-operator:dev
kubectl apply -f config/generated-dev
kubectl apply -k config/operator-dev
kubectl -n operator-development rollout status deployment/opensearch-serverless-operator
```

The Kind command assumes the default cluster name. Import the image on
each eligible node, substituting its container name. For a registry
deployment, use Kustomize's image override to set the Deployment image to
your published manager digest. The overlay creates the development
namespace, reuses the namespaced Role, and enables the development Pod
creation and scheduling gates. The Pod's namespace supplies its watch
scope and leader election namespace. Install the cluster-scoped CRDs
with cluster administration credentials before starting the manager.
[Development RBAC and feature flags](../dev/README.md)

For an image published to a registry, place the override in the overlay
that imports this package:

```yaml
images:
  - name: opensearch-serverless-operator
    newName: registry.example.com/platform/opensearch-serverless-operator
    digest: sha256:replace-with-manager-digest
```

## Start one search Pod and one index Pod

Prepare the existing S3 bucket and the Java runtime image first. Ensure
the runtime image is available by digest on the workload nodes. Create an
immutable Secret named `runtime-s3` in `operator-development` with your
OpenSearch keystore under the key `opensearch.keystore`. The bucket and
credentials remain platform-owned. [Java runtime build](../../runtime/README.md)
· [S3 repository configuration](../../docs/design/api-and-resources.md#primary-s3-remote-store)

Copy `instance.example.yaml` and fill in the Java image digest, bucket and
region. Set both zone names to match the nodes' `topology.kubernetes.io/zone`
label. For an S3-compatible gateway, add the repository's `endpoint` and
`pathStyleAccess` values. Apply the filled-in manifest. The operator creates
the two role Pods and the stable `catalog-search` and `catalog-index`
Services in the development namespace. The example uses internal plaintext
HTTP. Use an isolated development namespace and network.

## Roll the manager

Keep the CRDs and namespaced Role installed while upgrading or rolling back
the manager image. Change the Deployment image via the same overlay and
wait for its rollout and leader lease handoff. Reapply the previous overlay
to restore the original manager image. Manager rollbacks across different
API or Java versions require separate compatibility evidence.

For the tested development builds, apply the updated CRD before the new
manager image and retain that CRD on manager rollback. The test covers
the addition of `status.lastEndpointObservation.observedServingFleets`
within `v1alpha1` and the same Java runtime digest.
[Cross-build verification](../../docs/implementation/verification.md#cross-build-manager-and-additive-schema-upgrade)

```sh
kubectl -n operator-development get lease opensearch-serverless-operator.serverless.opensearch.org
```

The lease holder starts with the name of the active manager Pod.
