# Install the development operator with HTTPS and Basic Auth

This overlay runs one namespaced manager and enables verified HTTP TLS,
startup Basic Auth and role Service endpoint observation for the secure
instance example. It reuses the development manager, replica lifecycle
and Service definitions. Run it in a
fresh namespace with a precreated S3 bucket and an approved digest-pinned
Java runtime containing the Basic Auth plugin.
[Java runtime build](../../runtime/README.md)

## Package a versioned development install

Use Python 3 and `kubectl` with Kustomize to render the namespace,
manager Deployment, RBAC and runtime ConfigMap. Supply the exact
manager and Java image references available to the cluster. Both must
use SHA-256 digests.

```sh
mkdir -p dist
python tools/package_development_release.py \
  --version v0.1.0-dev.1 \
  --namespace operator-development \
  --cluster-domain "$CLUSTER_DNS_DOMAIN" \
  --manager-image "$MANAGER_IMAGE" \
  --java-image "$JAVA_RUNTIME_IMAGE" \
  --output dist/v0.1.0-dev.1
```

The output contains `crds.yaml`, `manager.yaml` and `release.json`.
Retain the package and both images for a manager rollback.

To sign the package, add `--signing-key "$RELEASE_SIGNING_KEY"` to the
packaging command. Set it to an Ed25519 private PEM key outside the
output directory and have OpenSSL 3 available on `PATH`. The command
adds `release.json.sig`. The signature covers both image references,
the namespace and DNS domain, and the manifest checksums.
[OpenSSL signing and verification](https://docs.openssl.org/3.0/man1/openssl-pkeyutl/)

Obtain the release public key through your trusted distribution channel.
Before applying a signed package, verify the signature and both manifest
hashes, then check the printed version, namespace and image digests
against the release you intend to install.

```sh
python tools/verify_development_release.py dist/v0.1.0-dev.1 \
  --public-key "$RELEASE_PUBLIC_KEY"
```

To include image SBOMs, put Syft on `PATH` and pass both
`--manager-oci-archive "$MANAGER_OCI_ARCHIVE"` and
`--java-oci-archive "$JAVA_OCI_ARCHIVE"` to the packaging command.
Supply OCI archives containing the exact platform manifests referenced
by the package. The default platform is `linux/amd64`. Set
`--oci-platform` for a different platform.

The packager verifies each pinned manifest and its config and layer
blobs. It checks that Syft scanned the expected manifest digest.
The package includes `manager.spdx.json` and `java.spdx.json`. When
signing is enabled, the signed inventory covers both file checksums.
Publication and registry image signatures use the release publisher's
artifact workflow.
[Syft OCI archive scans](https://oss.anchore.com/docs/guides/sbom/scan-targets/)

```sh
kubectl apply -f dist/v0.1.0-dev.1/crds.yaml
kubectl wait --for=condition=Established --timeout=60s \
  crd/opensearchserverlesses.serverless.opensearch.org \
  crd/opensearchserverlessoperations.serverless.opensearch.org
kubectl apply -f dist/v0.1.0-dev.1/manager.yaml
kubectl -n operator-development rollout status deployment/opensearch-serverless-operator
```

Continue at [repository, credentials and certificate](#supply-the-repository-credentials-and-certificate).
For another namespace, set the same namespace in the instance example,
the Secret commands and the certificate DNS names.

For a manager image-only upgrade or rollback, verify the desired
package against the package currently deployed. Both packages must
verify with the supplied public key. The preflight requires the same
CRDs, Java digest, namespace and DNS domain. It also compares the
rendered manager manifests after substituting the manager image,
so differences in RBAC, arguments, or other install settings fail.

```sh
python tools/verify_development_release.py "$DESIRED_PACKAGE" \
  --public-key "$RELEASE_PUBLIC_KEY" --manager-image-only-from "$CURRENT_PACKAGE"
```

Capture a healthy baseline before applying the desired package. The
observer reads the signed release's namespace and requires a Ready
manager with a current leader Lease and a fresh instance endpoint
observation. It checks that Ready endpoints in each stable Service's
EndpointSlices match the eligible Java Pods by UID and IP. It
records the instance and Ready Java Pod UIDs and the role Service
UIDs and ClusterIPs.

```sh
python tools/observe_development_release.py "$CURRENT_PACKAGE" \
  --public-key "$RELEASE_PUBLIC_KEY" --context "$KUBE_CONTEXT" \
  --instance "$INSTANCE" > before.json
```

After the baseline succeeds, apply the desired package's
`manager.yaml` and wait for the Deployment rollout. Compare the live
state with the baseline. The observer requires a different manager
Pod and image, with the recorded workload identities preserved.

```sh
python tools/observe_development_release.py "$DESIRED_PACKAGE" \
  --public-key "$RELEASE_PUBLIC_KEY" --context "$KUBE_CONTEXT" \
  --instance "$INSTANCE" --baseline before.json > after.json
```

Verify an authenticated write and search through the role Services.
Use the post-upgrade observation as the baseline for rollback. Keep
prior packages, images and any referenced generated ConfigMaps.
Changes beyond the manager image require their own compatibility
check and rollout procedure.

## Install the manager

Make the manager and Java runtime images available on the nodes that run
them. Set `JAVA_RUNTIME_IMAGE` to the full repository and digest of the
approved Java runtime and set `CLUSTER_DNS_DOMAIN` to the cluster's DNS
domain. To change the manager image, consume this overlay from another
Kustomization with an `images` override.
[Manager image and Kind import](../operator-dev/README.md)

```sh
kubectl apply -f config/generated-dev
kubectl apply -f config/operator-dev/namespace.yaml
kubectl -n operator-development create configmap opensearch-secure-runtime \
  --from-literal=javaImage="$JAVA_RUNTIME_IMAGE" \
  --from-literal=clusterDomain="$CLUSTER_DNS_DOMAIN" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -k config/operator-dev-secure
kubectl -n operator-development rollout status deployment/opensearch-serverless-operator
```

The manager uses the same approved Java digest for Pod DNS advertisement,
HTTP TLS, startup Basic Auth and role Service probes. The overlay grants
EndpointSlice listing through its existing namespace Role. Install the
cluster-scoped CRDs separately with your cluster administration credentials.

## Supply the repository, credentials and certificate

1. Copy [instance.example.yaml](instance.example.yaml) and set the same Java
   image digest, existing bucket, region and node zone. For an S3-compatible
   endpoint, add the repository's `endpoint` and `pathStyleAccess` values.
2. Prepare the OpenSearch keystore with the repository credentials and
   `serverless.auth.bootstrap.password`. Supply the same password bytes in
   a separate file. Create one immutable `runtime-s3` Secret with keys
   `opensearch.keystore` and `bootstrap-password`. The manager uses the
   latter key for authenticated runtime probes.
3. Apply the instance manifest to obtain the instance UID. The manager
   waits for the TLS inputs before creating Java Pods.
4. Issue an HTTPS server certificate with a wildcard DNS SAN for the
   instance's Pod DNS suffix. Compute the suffix from the UID as shown
   below. To let clients connect directly to both stable Services, also
   include `catalog-search.operator-development.svc.<cluster-domain>`
   and `catalog-index.operator-development.svc.<cluster-domain>` in the SANs.
5. Create immutable `http-leaf` and `http-roots` Secrets. The leaf uses
   the Kubernetes TLS keys `tls.crt` and `tls.key`. Store the PEM CA bundle
   under `ca.crt` in the trust Secret.

```sh
uid=$(kubectl -n operator-development get opensearchserverless catalog \
  -o jsonpath='{.metadata.uid}')
identity=$(printf '%s' "$uid" | openssl dgst -sha256 | awk '{print substr($NF,1,20)}')
printf 'Pod certificate SAN is *.peer-%s.operator-development.svc.%s\n' \
  "$identity" "$CLUSTER_DNS_DOMAIN"
```

On a new install, you can use `kubectl create secret ... --dry-run=client -o
json`, set `immutable: true` in the output and submit it with `kubectl create
-f -`. Keep private keys and password files outside the manifests and source
control.

## Verify the roles

Wait for the instance to qualify its two stable role Services.

```sh
kubectl -n operator-development wait --for=condition=Available \
  opensearchserverless/catalog --timeout=5m
```

From a client inside the cluster, connect over HTTPS to the two stable
Services on port 9200 with the configured CA bundle and the `admin`
bootstrap username. Check both ready endpoints. Create an index and write
a document via `catalog-index`, then search for it via `catalog-search`.
An anonymous request should receive
HTTP 401. Use a scoped application account for application traffic.

This profile covers BYO HTTP TLS and startup Basic Auth. Peer mTLS,
cert-manager issuance, credential rotation, external Gateway routing and
blue/green rollout use their dedicated development procedure and enablement.
[TLS and routing](../../docs/design/traffic-and-security.md) · [Blue/green rollout](../../docs/implementation/development-rollout.md)
