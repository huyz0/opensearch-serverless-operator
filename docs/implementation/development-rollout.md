# Development blue/green rollout

Enable the rollout for one named instance in the operator's watch namespace.
The development path requires leader election, gated Pod creation and release,
and the same qualified digest for the development warm-up and Service probes.
Apply [rollout-rbac.yaml](../../config/dev/rollout-rbac.yaml) in the watch
namespace alongside the standard development role. It grants Service patch
and EndpointSlice list access. ConfigMap reads use the existing role.

Set these operator arguments:

```text
--development-rollout-instance=catalog
--development-rollout-search-url=https://search.example.internal
--development-rollout-ingest-url=https://index.example.internal
--development-rollout-platform-key=<padded base64 Ed25519 public key>
```

The two HTTPS URLs are the bases of the actual production client routes
through the stable proxy. The probe appends `/_serverless/stats`. The
operator verifies client TLS against its system trust store. Direct Pod
probes use the transport configured for the approved runtime image.
Build both the candidate and rollback fleets with the
[route process observation overlay](java-runtime-build.md#local-lease-observation).
The controller requires matching JVM identities in direct readiness and
stats checks before switching to a fleet. After the switch, it verifies
that the role route serves the same qualified process. Keep stats
responses uncached at the proxy. Promotion also checks the retained
source Pods for an incarnation and valid lease so an older bundle cannot
become an unusable rollback target. A prepared rollout reports
`SourceProcessUnqualified` when that check fails.

Indexing advancement and the final commit require fresh evidence for both
roles. If a previously qualified Pod or EndpointSlice changes while the
controller checks another role, promotion waits for the next successful
observation. Watch `CandidateNotQualified` for Pod changes and
`RouteNotConverged` for lost route evidence.
For a proxy with a private CA, set
`--development-rollout-route-ca-file=/path/to/ca-bundle.pem`. The operator
adds that bundle to system trust at startup. Restart the manager to load a
changed bundle.

## Use managed HTTPS and Basic Auth

For a managed `ServerlessBasicAuth` instance, enable the same digest-pinned
runtime for peer DNS, HTTP TLS, Basic Auth, warm-up and Service probes. Point
each Envoy `HTTPRoute` at its stable role Service, `catalog-search` or
`catalog-index`. Configure the Gateway HTTPS listener with the client-facing
certificate. Apply a `BackendTLSPolicy` to each Service with the JVM CA and a
hostname present in the JVM certificate. Both `HTTPRoute` bindings and their
backend TLS policies must be accepted before testing.

Give both operator role probes an ordinary account that works on the serving
and candidate fleets. Set `--development-rollout-search-auth-secret` and
`--development-rollout-ingest-auth-secret` to immutable Secrets in the instance
namespace. Each Secret has type `serverless.opensearch.org/rollout-probe` and
keys `username` and `password`. Both arguments can reference the same Secret.
Anonymous probes must return HTTP 401 on both routes. Use
`--development-rollout-route-ca-file` for the Gateway CA when the operator's
system trust lacks it.

For a local Docker-backed kind cluster, import locally built images before
creating Pods that use them. With Docker API 1.48 or later, the import helper
streams a single platform directly to the node's `k8s.io` containerd
namespace. Run it for each node that can host the workload. [Docker image save](https://docs.docker.com/reference/cli/docker/image/save/#platform)

```powershell
python tools/import_kind_images.py --node oss-https-control-plane --platform linux/amd64 oss-https-runtime:auth-standard oss-https-operator:secure-rollout-verified
```

Containerd creates the `repository@sha256:...` reference from the archive
during import. The helper checks the alias's manifest digest and verifies
that kubelet can resolve the imported references through CRI.
Use the reported digest-pinned reference in the instance and the operator's
runtime approval arguments.

You can also supply a locally available manifest digest directly. The
selected platform export must preserve that digest. For a build whose local
tag points to an OCI index, use the tag to find the platform manifest. This
form requires Docker API 1.49 or later. [Docker image inspect](https://docs.docker.com/reference/cli/docker/image/inspect/)

```powershell
$tag = 'oss-https-operator:secure-rollout-verified'
$manifest = docker image inspect --platform linux/amd64 --format '{{.Descriptor.Digest}}' $tag
python tools/import_kind_images.py --node oss-https-control-plane --platform linux/amd64 "$tag@$manifest"
```

The helper requires the selected platform manifest to match the requested
digest before export and checks the imported manifest again before publishing
its digest reference. Use the repository-qualified digest printed by the
helper in Kubernetes.

See the [authenticated Envoy drill](verification.md#authenticated-envoy-bluegreen-rollout)
for the tested topology and the search and index checks.

## Check a missing local lease expiry

Inspect the Pod's readiness events and the same JVM's
`/_serverless/stats` response. A true `lease.valid_now` can appear
before the first lease renewal. Readiness also requires the local
lease's `expires_in_millis` and sufficient time before expiry. The
probe reports a missing expiry from the membership snapshot separately
from an incomplete stats response.

If the expiry is absent, check the configured remote-store endpoint,
credentials, and the gateway's data-write errors. A successful bucket
creation or listing proves that the bucket is reachable. Data writes
can still fail because of policy or capacity. For a multi-bucket
SeaweedFS development fixture, allow enough writable volume slots for
each collection and confirm it can store objects. In the capacity drill,
adding a volume server let the existing JVM recover on its next lease
renewal. Verify Pod readiness and the role EndpointSlice, then run an
index write and read it through search. [Capacity drill](verification.md#plaintext-readiness-after-remote-store-capacity-exhaustion)

## Revision labels on existing fleets

Enabling or editing `spec.rollout` keeps the same Pod template revision.
If an older controller labeled a fleet using the rollout policy, the new
controller can verify that label against the current configuration and
record its equivalent template in `status.templateRevisionAliases`. Existing
Pods retain their labels and replacements use the same label for the same
role and fleet. Newly created fleets use the current template revision
unless they already have a verified pin.

When upgrading an existing deployment, leave its current rollout policy
in place until the controller has reconciled the fleet. A legacy label
that was generated with a policy the controller can no longer verify
will block new Pods for that fleet. Restore the original policy if the
runtime configuration still matches, then allow the controller to record
the alias before editing the policy again. An alias stops qualifying
when its recorded runtime template changes.

## Per-fleet Gateway Services

Set `spec.rollout.gatewayBackends: true` to provision ClusterIP backends
for the serving fleets and any recorded candidate or rollback fleet.
The operator records each name and UID in `status.gatewayBackends`,
keyed by role and fleet, for example `search/green` or `ingest/initial`.
Wait for `GatewayBackendsConfigured=True` for the current generation
before using a name from that map in a platform-managed HTTPRoute.
That condition describes the Services. Application and proxy health
require separate checks.

The stable role Services and candidate preview Services remain
available. The backend selectors stay fixed to their role and fleet.
Disabling provisioning or clearing a candidate retains its Gateway
Services because a platform-managed route may still reference them.
The operator reports a missing or replaced pinned Service as
`BackendUnavailable`. Route-aware retirement of these backend Services
remains open.

## Enable weighted blue/green execution

Enable the standard development rollout arguments and point the operator
at two platform-managed HTTPRoutes in the instance namespace.

```text
--development-rollout-search-httproute=catalog-search
--development-rollout-ingest-httproute=catalog-index
--development-rollout-gateway-service=envoy-gateway-system/<frontend-service>
--development-rollout-gateway-service-uid=<frontend-service-uid>
```

Use the exact Service and UID for the Envoy frontend that serves both
routes. Apply the [frontend read Role](../../config/dev/weighted-rollout-frontend-rbac.example.yaml)
in the Gateway namespace and bind it to the operator's ServiceAccount.
The manager compares signed replica UIDs with live EndpointSlices and
the ready Pods behind that Service, then rechecks their versions before
applying a step. If an existing weighted deployment has neither frontend
flag, weighted operations hold at `WeightedGatewayUnbound` until the
administrator adds the binding.

Set `resourceNames` in
[weighted-rollout-rbac.example.yaml](../../config/dev/weighted-rollout-rbac.example.yaml)
to the same two names and apply it in the instance namespace. Each
HTTPRoute must have one rule referencing the two pinned backend Services
for its role on port 9200. Start with explicit 100 and 0 weights for
the current and candidate fleets.

Create the usual `Promote` operation with
`spec.routingMode: WeightedGateway`. The operator admits the candidate
at 1% before full cutover, search first and then indexing. To use extra
steps, set the candidate percentages for each role under
`spec.weightedTraffic`. The lists must increase strictly from 2 to 99
and may contain up to 30 entries per role. The operator adds the 1%
admission and 100% cutover automatically.

```yaml
spec:
  routingMode: WeightedGateway
  weightedTraffic:
    search: [10, 45]
    ingest: [20]
```

This sequence runs search at 1%, 10%, 45% and 100% before indexing
moves through 1%, 20% and 100%. Keep
publishing signed fleet and proxy observations through claim release.
The operator updates the stable Services after both weighted routes
converge and waits for their EndpointSlices before committing.

A `Rollback` naming the weighted promotion inherits weighted routing.
For an interrupted promotion, set `expectedServingFleetByRole` to
the fleets selected by the stable Services. The operator observes the
live weighted routes separately and recovers from their actual weights.
Keep the approved route names configured until the operation releases
the instance claim.

## Platform publisher contract

A trusted platform process must independently verify the actual Node AZ,
peer and security readiness, and safe rollback for the target role and
revision. The provided `rollout-evidence-publisher` command takes the
platform verifier's JSON from standard input, checks the live rollout,
Pod set, and each Node's `topology.kubernetes.io/zone` label, then signs
and publishes the statement. It does not infer peer, security or
compatibility approval from Pod labels. Supply those findings from the
platform verifier and invoke the command again for each fresh observation.

```sh
go build ./cmd/rollout-evidence-publisher
./rollout-evidence-publisher --namespace search-test --operation promote-green \
  --signing-key-file /secure/platform-ed25519.pem < role-observation.json
```

Run the publisher under a platform identity separate from the operator.
It needs namespaced reads of the instance and operation, Pod list access,
ConfigMap get/create/update, and cluster-scoped Node get access. The
operator uses namespaced permissions only. Mount the PKCS#8 PEM private
key into the publisher's environment and provide its public key to the
operator. A failed publication leaves the last signed evidence in place
until its normal freshness deadline.

For an in-cluster publisher, apply the optional
[publisher RBAC](../../config/dev/rollout-publisher-rbac.yaml) in the
instance namespace. Bind the defined Node reader ClusterRole to the
publisher ServiceAccount in that namespace. The manifest binds only the
namespaced role. The platform administrator chooses which publisher
receives cluster-scoped Node reads.

The publisher writes one ConfigMap per operation and role in the instance
namespace. Name it `rollout-evidence-<hash>-<role>`, where `<hash>` is
the first 24 lowercase hex characters of SHA-256 over the operation UID
string. Use role `search` or `ingest`.

Put the JSON payload in `data["evidence.json"]` and the padded base64
Ed25519 signature in `data["signature"]`. Sign the UTF-8 bytes of
`opensearch-serverless-rollout-evidence/v1`, a single newline, and the
exact payload. Keep the private key in the platform process and supply only
the public key to the operator. The signed observations expire after 30
seconds. Set `observedAt` to the oldest observation used for the statement.
Refresh it through new observations and a new signature.

```json
{
  "namespace": "search-test",
  "instanceUID": "instance-uid",
  "instanceGeneration": 3,
  "operationUID": "operation-uid",
  "role": "search",
  "fleet": "green",
  "revision": "target-revision",
  "observedAt": "2026-11-24T01:00:00Z",
  "peerQualified": true,
  "securityQualified": true,
  "rollbackCompatible": true,
  "pods": [
    {
      "uid": "pod-uid",
      "resourceVersion": "42",
      "nodeName": "platform-node",
      "zone": "actual-node-zone",
      "processIncarnation": "process-incarnation"
    }
  ]
}
```

Include every current Pod in that target fleet and role. The operator compares
the signed Pod versions, Nodes, zones, and process incarnations to its direct
observations and requires the role's serving floor in each active zone.
Refresh the statement when any Pod identity, version, or process changes.

The reader and publisher are tested with Kubernetes client fixtures.
Deployment of the publisher and verifier, verification against cloud
AZ labels and peer/security policy, and live ingress acceptance remain
qualification work.

## Signed proxy evidence for weighted routes

Run the same publisher with `--weighted-route` to submit a trusted
platform verifier's observations for a claimed promotion or rollback.
The publisher needs namespaced HTTPRoute read access in the instance
namespace. The operator reads the resulting namespaced ConfigMap
using its existing permissions. The platform verifier needs its own
access to inventory the Gateway replicas and probe the data plane.

```sh
./rollout-evidence-publisher --weighted-route --namespace search-test \
  --operation promote-green --signing-key-file /secure/platform-ed25519.pem \
  < proxy-observation.json
```

Include the instance generation, operation UID, role, route name,
route UID and resource version, and current fleet `weights`.
Supply `inventoryObservedAt` and `servingProxyUIDs` from an independent
inventory of all replicas that can serve this route. Add one entry
in `proxies` per serving replica. Each report contains `proxyUID`,
`routeVersion`, `observedAt`, `healthy`, `requests` and `failures`.
Use the oldest observation in that report as `observedAt`.
The verifier must associate the live proxy configuration with the
reported Kubernetes route version.

For each fleet, `healthy` maps admitted Pod UIDs to their JVM
incarnations. `requests` counts successful client-path probes by the
fleet that actually responded. Count failed or unidentified responses
in `failures`. The controller checks these observations against the
live route, direct application and fleet qualification, and its sampling
requirements. The publisher checks Kubernetes identity and scope. It
relies on the platform verifier for the actual inventory and observations.
When a weight change keeps the same positive-weight fleets, each proxy
report must include `configuredWeights` read from its active configuration.
The operator checks these exact percentages alongside the traffic
sample. This prevents an unchanged endpoint set or sampling tolerance
from masking a stale configuration.

The publisher writes `weight-evidence-<hash>-<role>` and signs the
payload under the `opensearch-serverless-weighted-route-evidence/v1`
domain. The same UID hash and ConfigMap data keys apply. The statement
expires 30 seconds after its oldest inventory or proxy observation.
Route replacement and route-version changes require a new statement.
The signed reader and publisher have API-server coverage.

For managed Basic Auth, the signed scope also includes `authProbeURL`,
`authSecretName`, `authSecretUID`, and `authSecretVersion`. Use the same
HTTPS base URL and rollout-probe Secret configured for that operator role.
List the literal hostname in the weighted HTTPRoute and make its single
rule match the probe's `GET /_serverless/stats` path, including any base
path. The publisher and operator recheck the binding against the live
route. Changing the Secret requires fresh proxy and Pod observations.

### Collect Envoy proxy evidence

Build `./cmd/rollout-proxy-observer` and
`./cmd/rollout-evidence-publisher`. The observer reads the serving
Gateway Service's EndpointSlices and the active operation, route and
fleet approvals. It supports the single-rule EDS layout exercised with
Envoy Gateway 1.8.6. Keep the Envoy admin endpoints private. From the
trusted platform environment, open loopback tunnels to each serving
Pod's admin port and its HTTPS listener.

Record both local addresses for each corresponding Gateway Pod UID.
The observer compares these UIDs with the independently listed
EndpointSlice targets and rejects a missing serving replica.

```json
{
  "first-proxy-pod-uid": {
    "adminAddress": "127.0.0.1:19630",
    "clientAddress": "127.0.0.1:19640"
  },
  "second-proxy-pod-uid": {
    "adminAddress": "127.0.0.1:19631",
    "clientAddress": "127.0.0.1:19641"
  }
}
```

Collect one role and publish its report promptly. Repeat with fresh
observations for each role as the operation advances.

```sh
KUBECONFIG=/secure/observer.kubeconfig ./rollout-proxy-observer \
  --namespace search-test --operation promote-green \
  --role search --gateway-namespace envoy-gateway-system \
  --gateway-service catalog-gateway --gateway-service-uid "$GATEWAY_SERVICE_UID" \
  --route-url https://search.example.internal --route-ca-file /secure/edge-ca.pem \
  --platform-key "$ROLLOUT_PLATFORM_PUBLIC_KEY" --tunnels-file /secure/envoy-tunnels.json \
  | KUBECONFIG=/secure/publisher.kubeconfig ./rollout-evidence-publisher \
      --weighted-route --namespace search-test \
      --operation promote-green --signing-key-file /secure/platform-ed25519.pem
```

Bind a read-only identity to a Role in each namespace. In the instance
namespace, grant `get` on `opensearchserverlesses`,
`opensearchserverlessoperations`, `httproutes` and fleet approval
`configmaps`, and `list` on `pods`. In the Gateway namespace, grant
`get` on the named Service and `list` on `endpointslices`. Use the
platform identity to establish the tunnels and the publisher identity to
sign and write reports. The collector emits JSON to standard output.
It requires current approvals for every positive-weight fleet. If a
fleet cannot be qualified during recovery, use a platform verifier that
can attest the surviving fleet independently.

For a protected route, pass `--route-auth-secret` to the observer. Grant
the observer and publisher `get` on that named Secret in the instance
namespace. The observer requires each serving Envoy replica to reject an
anonymous request before sending the ordinary account to the bound HTTPS
URL. The admin tunnel receives no client credential. Before changing
weights, the operator requires authenticated responses from every fleet
that will retain traffic on every serving proxy. A step that removes a
fleet can proceed despite failed requests if the retained fleets still
pass. Convergence requires the sampled distribution to meet tolerance
with zero failed requests.

The weighted executor also needs a separate fleet approval for each
fleet that will receive traffic after the step. Use the publisher's
normal fleet-approval mode for these reports. After the operator
records a weighted journal, the publisher accepts the exact fleet and
revision pinned for that role. Target approvals retain the existing
ConfigMap name. Other fleets append a hyphen and the first 12 lowercase
hex characters of SHA-256 over the fleet name.

The development evidence adapter checks Pod readiness, JVM identity
and leases, the role's serving floors, and disruption budgets before
qualifying a fleet. It pairs those observations with the signed route
reports. A fleet losing all weight need not qualify for the step
that removes it.

## Java promotion and rollback regression

With Kubernetes envtest binaries available through `KUBEBUILDER_ASSETS`,
set `OPERATOR_JAVA_RUNTIME_LIB` to the staged shell's runtime library
directory and put the matching Java version on `PATH`. Run
`go test -tags envtest ./internal/controller -run '^TestRolloutStatusAPIServerConflict$' -count=1 -v`.
The opt-in run starts real source and target Java processes on loopback.
By default it uses a shared filesystem store. Set
`OPERATOR_ROLLOUT_S3_BINARY` to the absolute path of a
[SeaweedFS 4.53](https://github.com/seaweedfs/seaweedfs/releases/tag/4.53)
binary to use the shell's S3 remote store instead. The fixture starts an
isolated gateway, provisions a fresh bucket outside the operator, and
configures the Java processes with disposable keystores.

A local TLS proxy follows the stable Service selectors.
Kubernetes Pod status and EndpointSlices are
set by the fixture. The test checks an indexing write through the promoted
route and confirms search returns documents written before and after the
switch. It also reads the same documents through the restored search route
after rollback. The lease identifies the process that accepted the request.
In S3 mode, the test separately creates single-shard indexes to find one
owned by green ingest. It checks the shard head from blue ingest before
and after an acknowledged write through green, requiring the same owner
and ownership term. The restored search route must return that document.
After rollback, the S3 test stops green ingest and retries a write through
the restored indexing route. The test requires blue ingest to own the
shard in a newer term and the restored search route to return both the
green write and the new blue write. The elapsed time includes retries and
is a local recovery observation.

## Confirm fleet retirement

After promotion, leave `spec.rollout` in place and remove
`spec.rollout.candidate`. Wait for `PromotedPreviewDetached` and the end of the
configured rollback window. A promotion without a configured window retains
its rollback target indefinitely. After rollback, use the completed rollback
operation to retire the detached fleet that lost traffic.

Publish a signed `Reserve` request before draining. Wait until
`status.fleetRetirement` contains its UID and the `FleetRetirement` condition
has reason `DrainReserved`. The reservation and normal instance operation
claims use the same resource-version check. An active or racing operation
must settle before the reservation can commit. While reserved, new rollout,
rollback, account and settings claims wait. Serving capacity can still
reconcile, including retries of withdrawn provisioning attempts. The
reservation records the displaced Pod UIDs before the platform drains them.

The platform must drain traffic and establish that every displaced JVM has
stopped or is fenced, including any JVM whose Pod object was force-deleted.
For weighted Gateway routing, verify that no serving route sends traffic to
the displaced fleet. Keep its backend Services while routes reference them.
Wait for the old fleet's Pod objects to disappear.

Publish the request and completion receipt with the namespace, completed
operation name and the platform Ed25519 signing key file.

```sh
rollout-evidence-publisher --fleet-retirement --namespace "$namespace" \
  --operation "$operation" --signing-key-file "$signing_key" < retirement.json
```

Both JSON objects include `namespace`, `instanceUID`, `instanceGeneration`,
`commitUID`, `fleet`, `phase` and `observedAt`. Use the current instance
generation, the UID of its latest committed rollout operation, the exact
displaced fleet, and an RFC 3339 observation time within the last 30 seconds.
Use `revision` when both displaced roles share a literal revision. Otherwise
use `revisionsByRole` with `search` and `ingest`. After promotion, use the
revisions in `status.rollbackTarget.roles`. After rollback, use the displaced
fleet's revisions in `status.retiredCandidateFleets`. Supply one revision
representation in each signed message.

For `phase: "Reserve"`, omit the attestations and `reservationUID`.
For `phase: "Complete"`, set `reservationUID` to
`status.fleetRetirement.uid` and set both `trafficDrained` and
`processesFenced` to `true`. Take the completion observation after the
reservation was recorded.

The operator validates the receipt with the configured rollout public key and
rechecks that the displaced Pod UIDs are absent, even if their labels changed.
`FleetRetirement=True` with reason `FleetFenced`
means it released the reservation and the next candidate can stage. If the
request expires before it reserves, publish a new request. Once reserved,
resume interrupted drain and submit a fresh completion receipt for the same
reservation. The platform carries out drain and JVM fencing.

## Roll over the HTTP CA

Use immutable Secrets for the replacement leaf and CA bundle. For the first
promotion, use a bundle that validates both the active and new leaves. Keep
the active fleet's Secret references in `spec.tls.http` and set the candidate
references. The bundle key may differ from the original Secret key.

```yaml
spec:
  rollout:
    candidate:
      fleet: green
      image: registry.example.test/runtime@sha256:<current-digest>
      httpCertificateRef: {name: http-leaf-next}
      httpTrustBundleRef: {name: http-ca-overlap, key: ca.pem}
```

Before signing each role's rollout approval, the platform verifies the
effective backend trust at production proxies and other validating clients.
It excludes competing backend trust writes until the transition converges.
Add `httpTrustSHA256` with the SHA-256 of the exact approved bundle bytes
and set `httpTrustFenced` to `true` in the signed approval. For promotion,
use the target fleet's bundle. For rollback, keep the outbound fleet's
bundle while traffic moves back. After a CA change has staged, subsequent
rollouts require this approval even when the two Pod trust pins match.
The controller validates the signature and digest. The platform manages
and fences the Gateway policy.

With Envoy Gateway, capture `/config_dump?include_eds=true` from each
serving proxy. Check the exact CA certificate set on every active search
and index endpoint before signing approval.

```sh
python tools/verify_envoy_backend_trust.py envoy-config.json http-ca.pem \
  --namespace "$namespace" --route catalog-search --route catalog-index
```

The check follows each endpoint's TLS socket match to its active validation
context. It verifies the CA certificate set. Sign the hash of the original
PEM bundle bytes in the rollout approval.

After the rollback window and signed retirement of the original fleet,
stage a candidate with the new-only trust bundle. Omit `httpCertificateRef`
to keep the serving leaf. The new bundle must still validate the retained
fleet's leaf through its rollback window. Once the new-only fleet is
committed and that window closes, retire the overlap fleet before removing
its trust Secret. Keep any Secret referenced by a serving or retained fleet.

If the initial CA change was rolled back, the platform can remove the unused
root from the Gateway after retirement, under the same backend trust write
exclusion.

## Stage a peer TLS rotation

Keep the initial transport references in `spec.tls.transport`. Supply a
leaf and trust pair on each new fleet and set a finite rollback window.
The candidate uses the same qualified image as the serving fleet.

```yaml
spec:
  rollout:
    strategy: BlueGreen
    maxPreviewReplicas: 3
    rollbackWindow: 30m
    candidate:
      fleet: peer-overlap
      image: registry.example.test/runtime@sha256:<current-digest>
      peerTLS:
        source: Secret
        certificateRef: {name: peer-leaf-original}
        trustBundleRef: {name: peer-ca-overlap, key: ca.crt}
```

For a leaf renewal under the same CA, supply the renewed leaf and the
current trust bundle. For a CA change, promote three distinct fleets in
order. First use the original leaf with both CAs. Then use a new leaf
with both CAs. Finally use the same new leaf with only the new CA.
Complete signed retirement of the displaced fleet after each rollback
window before staging the next one. A candidate that omits `peerTLS`
inherits the serving fleet's peer snapshot.

The controller pins the candidate snapshot before allocating preview Pods
and checks that both leaf chains validate against both trust bundles
through the rollback window and one hour of startup margin. The same
compatibility check applies at preview Pod admission, gate release and
before production traffic changes. The platform must test fresh native
peer mTLS connections in both directions before signing the existing
`peerQualified` approval. Recheck Pod and Java process identities
after the probes and sign only the tested incarnations. A successful
client HTTPS probe covers the client transport only. A missing pinned
snapshot holds the operation while the two fleets are part of the
transition. The API-server fixture
exercises staging and trust checks. The live selector drill covered
the three stages on one-AZ kind with Envoy.
[Live Java peer TLS rotation](verification.md#live-java-peer-tls-rotation)

Build `cmd/peer-tls-probe` for the runtime's platform and run it from
each source Pod with that Pod's mounted peer credentials. Supply a
destination Pod IP, its advertised DNS identity and the expected
leaf hashes from both fleet pins. The probe verifies the server chain
and identity, checks the mounted client leaf and presents it on request
during a fresh TLS 1.2 handshake. Use `--without-client-certificate`
for a negative check and require a remote TLS alert. Verify a
forwarded write separately to exercise the Java request protocol.

```sh
peer-tls-probe --address "$target_ip:9300" --server-name "$target_dns" \
  --server-leaf-sha256 "$target_peer_leaf_sha256" \
  --client-leaf-sha256 "$source_peer_leaf_sha256"
```

## Issue peer TLS certificates with cert-manager

Configure a dedicated namespaced cert-manager `Issuer` and supply its
peer trust CA in a separate Secret. The tested profile uses cert-manager
1.20 with `StableCertificateRequestName=true`, the default for that
release. [cert-manager controller flags](https://cert-manager.io/v1.20-docs/cli/controller/#flags)

Apply
`config/dev/cert-manager-rbac.yaml` in the operator's watched namespace
and set `--development-peer-cert-issuers=peer-ca` on the operator. The
optional Role grants get and create on Certificates and get on
CertificateRequests in that namespace.

```yaml
spec:
  tls:
    transport:
      source: CertManager
      certificateRef: {name: peer-primary}
      issuerRef: {name: peer-ca}
      trustBundleRef: {name: peer-roots, key: ca.crt}
```

`certificateRef.name` identifies the issuance slot. The operator creates
one owned Certificate per instance, with a generated `peer-issuer-`
name for both the Certificate and its output Secret. Give cert-manager
exclusive write access to generated `peer-issuer-` names. Operator
Pods cannot reference these mutable output Secrets directly. The
request derives its wildcard DNS SAN from the instance's headless
Service. The Certificate
requests client and server usages and a new private key on each
issuance. The operator checks the Certificate's current readiness and
matches the output certificate to the approved request owned by that
Certificate UID before staging the immutable snapshot.
[Certificate issuance and renewal](https://cert-manager.io/docs/usage/certificate/)

After cert-manager renews the source, stage a new fleet with the same
channel under `spec.rollout.candidate.peerTLS`. The serving fleet keeps
its snapshot until promotion and retirement. The existing rollback
checks still apply. For a peer CA change, supply the leaf and overlap
trust bundle through the Secret workflow.

## Issue HTTP TLS certificates with cert-manager

Apply the optional cert-manager Role described above. Configure a
separate namespaced Issuer for HTTP and allow it with
`--development-http-cert-issuers=http-ca`. Supply its CA bundle in an
immutable Secret. HTTP issuance uses the same CertificateRequest
approval and provenance checks as peer issuance.

```yaml
spec:
  tls:
    http:
      source: CertManager
      certificateRef: {name: http-primary}
      issuerRef: {name: http-ca}
      trustBundleRef: {name: http-roots, key: ca.crt}
```

The Certificate requests server authentication for the Pod DNS
wildcard and the two stable role Service DNS names. For instance
`catalog` in namespace `operator-development`, configure the search
and index BackendTLSPolicies to validate
`catalog-search.operator-development.svc.cluster.local` and
`catalog-index.operator-development.svc.cluster.local` respectively.
Both policies use the Issuer's CA. Edge TLS remains a separate
configuration on the Gateway.

Cert-manager renews the mutable `http-issuer-` Secret independently.
The operator records the exact source Secret revision before creating
an immutable `http-issued-` snapshot. Existing Pods and their
replacements use the snapshot pinned to their fleet. To deploy a
renewed leaf, set `spec.rollout.candidate.httpRenewFromIssuer: true`
with a finite `spec.rollout.rollbackWindow`. Promotion uses the
normal blue/green qualification and traffic checks. Rollback returns
to the prior pinned leaf while it remains valid. For a CA change,
stage an explicit immutable leaf and trust bundle using the Secret
workflow.

For each instance, the operator retains up to 64 distinct managed
snapshot intents, including abandoned ones. After the cap, it holds
new issuance staging. Kubernetes garbage-collects the owned snapshots
on instance deletion.
