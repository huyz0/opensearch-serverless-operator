# Dynamic security configuration

Support in-place changes to credentials and authorization policy where the configured provider enforces them. Keep security revisions independent of Pod template revisions and cluster settings. A fleet rollback must continue to enforce the current security revision, including revocations made after the fleet was deployed.

## What the Java shell supports today

| Input | Behavior at `e71d42331a3ec82ec01109a5bb42d6a9a9162a5c` |
| --- | --- |
| Built-in Basic Auth accounts | The bootstrap account can create or replace a password with `PUT /_serverless/security/users/{user}` and delete an account with `DELETE` on the same path. This works without a restart. Nodes cache successful authentication and recheck a shared change marker. If the marker cannot be read, a cached credential can still be accepted until its configured TTL expires. A successful write alone gives no fleet-wide revocation guarantee. [Account routes](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/auth/src/main/java/org/opensearch/serverless/auth/UsersHandler.java#L60-L111) · [Cache and marker](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/auth/src/main/java/org/opensearch/serverless/auth/CredentialStore.java#L575-L599) |
| Bootstrap credentials and auth settings | The built-in provider reads its configured account and secure password when constructing the credential store. Plan a controlled replacement for changes to this startup configuration. For an application password rotation, prefer creating a second account, moving the client and then deleting the old account. [Credential store initialization](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/auth/src/main/java/org/opensearch/serverless/auth/CredentialStore.java#L191-L225) |
| Roles, permissions and external identity provider configuration | The built-in plugin authenticates REST requests. Its account-management check is reserved for the bootstrap account, and ordinary authenticated accounts share broad privileges. Native forwarding does not propagate the end user's identity. Fine-grained authorization requires a gateway that owns the full client boundary or a Java provider and enforcement changes on the relevant paths. [Authentication and forwarding boundary](https://github.com/huyz0/OpenSearch/blob/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c/serverless/auth/src/main/java/org/opensearch/serverless/auth/ServerlessAuthPlugin.java#L61-L81) |
| HTTP and peer TLS | Qualify reload for each transport and provider independently. The upstream OpenSearch Security plugin has certificate reload features. The pinned shell's peer mTLS fixture establishes handshake support, so plan its certificate rotation through the validated rolling path until the intended provider passes in-process reload tests. [Upstream TLS configuration](https://docs.opensearch.org/latest/security/configuration/tls/#hot-reloading-tls-certificates) · [Shell TLS evidence](../research/tls-certificate-rotation.md#reference-behavior-and-the-java-boundary) |

The built-in provider has passed a local rotation and deletion test
with one ingest JVM and one search JVM on a healthy shared store. Both
enforced the credential change on existing and fresh HTTPS connections
without restarting. The test covers a filesystem store and a SeaweedFS
S3 bucket. The revocation bound during a store partition still requires
qualification. [Basic Auth cohort test](../implementation/verification.md#basic-auth-reload-across-search-and-ingest)

## Development Basic Auth at startup

Set `spec.security.provider` to `ServerlessBasicAuth` before the first Pod
is created. Set `authenticationSecretRef` to a password key in the same
immutable Secret named by `spec.storage.credentials.keystoreSecretRef`.
The Secret must also contain `opensearch.keystore` with the same password
in secure setting `serverless.auth.bootstrap.password`. The Java
bootstrap username is `admin`. The Secret is namespaced. Managed Pods
mount the keystore and password as files in the OpenSearch container.
Prepare the runtime with `tools/prepare_java_runtime.py --basic-auth`
to include the pinned auth plugin on the classpath, then build an image and
qualify that same image through `--development-basic-auth-image` and
`--development-http-tls-image`. The auth plugin is loaded from the image
for this provider, so it does not need a separate OCI install request.

```yaml
spec:
  security:
    provider: ServerlessBasicAuth
    authenticationSecretRef:
      name: catalog-runtime
      key: bootstrap-password
```

The controller pins the Secret identity and a checksum of both files
before creating Pods. The checksum incorporates the randomized keystore
container and never publishes a verifier of the password alone. Startup
validates and copies the same bytes into the private Java configuration
directory. This catches replacement data even if Kubernetes mounts it
between gate release and first startup. Both the kubelet exec probe
and direct controller health checks verify the Pod certificate and
require anonymous health requests to return HTTP 401 before using the
credential. The operator holds new releases if the Secret UID changes.
Keep the bootstrap credential off public routes and restrict Secret
access to trusted administrators. Reserve the bootstrap username
`admin` for the configured account. The Java provider can accept a
distinct indexed account of the same name if one exists. This mode leaves
`reload` and `authorizationConfigMapRef` unset. It supports an S3 keystore credential
source. Workload identity and bootstrap rotation are still open.
For blue/green routing with this provider, see the
[managed HTTPS and Basic Auth Gateway configuration](traffic-and-security.md#managed-https-and-basic-auth).

## Development application account operations

Enable `--development-basic-auth-operations` with leader election on the
operator for a managed `ServerlessBasicAuth` instance. Route all account
writes for that remote store through this operator. The Java account API
has no compare-and-set or operation token. A second instance sharing the
credential index, or a client calling the bootstrap account API directly,
can bypass the Kubernetes instance claim. Use an independently fenced
store before enabling concurrent administrators or reusing a store after
an interrupted instance deletion.

Submit one `ApplySecurityConfiguration` operation per account change.
Use `desiredPasswordRef` alone to create an account, both references to
rotate it, and `previousPasswordRef` alone to remove it. Each references
an immutable `serverless.opensearch.org/account-password` Secret in the
instance namespace. Supply the exact password bytes in the selected key
and retain both Secrets until verification finishes. The operation refuses
the configured bootstrap account.

```yaml
apiVersion: serverless.opensearch.org/v1alpha1
kind: OpenSearchServerlessOperation
metadata:
  name: rotate-catalog-client
spec:
  action: ApplySecurityConfiguration
  instanceRef:
    name: catalog
    uid: <instance-uid>
  securityConfiguration:
    expectedActiveRevision: <observed-revision>
    candidateRevision: catalog-client-v2
    account:
      username: catalog-client
      previousPasswordRef:
        name: catalog-client-v1
        key: password
      desiredPasswordRef:
        name: catalog-client-v2
        key: password
```

Use `initial` when `status.activeSecurityRevision` is empty. After each
verified change, pass the complete value read from that status field. It
includes the operation UID, so reusing a candidate name cannot revive a
stale expected revision.

The operator checks record existence on an approved ingest Pod and checks
the previous and desired credentials on every observable JVM over verified
HTTPS. It pins source Secret UIDs and the pre-dispatch Pod and process
identities, then records `AccountWriting` before sending one request.
The operation advances the revision after the expected record and password
behavior are observed across the current cohort, including every process
recorded before dispatch. This also covers visible preview and retained Pods.
The Pod and instance observations bracket both account-record reads and the
credential probes. A Pod joining or restarting during the final record read
invalidates that result.

An unacknowledged request is verified by observation. If the previous state
remains or an original JVM disappears, the operator keeps the claim and
finalizer. It never replays that request or clears an uncertain dispatch
solely because the operation was deleted or a timeout elapsed. A held claim
also pauses ordinary Pod creation. Recovery for an unobservable cohort or
an instance deleted during dispatch requires independently establishing
that the original writer and request are fenced. This mode does not bound
revocation during a store partition or implement authorization policy reload.

## Proposed operator API

The development account client submits the built-in provider's account
operations over verified HTTPS. It treats an unconfirmed mutation as
an unknown outcome and does not itself retry it. Account presence cannot prove
which password is active, and the Java API has no compare-and-set operation;
recovering an ambiguous password change therefore needs an explicit workflow.
This account client does not implement authorization policy activation or
revocation under partition.
It rejects operations on the configured bootstrap username: the shell checks
the configured password separately from an indexed account of the same name,
so editing or deleting that record cannot rotate or revoke the configured
password. Bootstrap rotation requires changing the startup credential and a
controlled replacement.

`UserAdmin.VerifyAccount` can observe credential acceptance and rejection
on one JVM. Use the direct, verified HTTPS endpoint for each expected
process. The observer checks the pinned node name, role and incarnation
before and after credential probes on the same TLS connection. It checks
anonymous denial and recognizes the shell provider's specific response
for a rejected password. A replacement connection, different process,
throttled response or unreachable endpoint leaves the result unverified.
Use the account lookup separately to check whether the indexed record
exists. The Kubernetes observer lists the namespace's Pods and checks
both owner references and instance labels. It includes observable
production, preview, retained and terminating JVMs, even when Pods are
unready. An owned Pod whose runtime or identity cannot be verified holds
the observation. The observer connects directly to each Pod IP and
checks the certificate against the Pod IP or its individual managed DNS
identity. It rereads the Pod set and instance after the probes. A failed
Pod phase alone supplies no termination proof when the node is lost.
Coverage applies to Pods still registered with Kubernetes. A future
activation operation must also fence force-deleted consumers and
new or restarted processes before reporting application, including on
rollback. [Kubernetes Pod lifecycle](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase)
[Basic Auth observation tests](../implementation/verification.md#basic-auth-reload-across-search-and-ingest)

Select the security provider explicitly. `HotReload` requires a qualified provider for the selected image and each input being changed. `RollingReplacement` handles supported startup-only inputs through the usual role and AZ capacity gates. Reject unsupported configuration and report the missing capability. An eligible provider can be a Java plugin or a gateway under the deployment's control. With gateway enforcement, restrict Java HTTP access to the authenticated gateway and approved administration paths, and keep native transport on its own trusted network path. The names below illustrate the proposed API for a qualified provider.

```yaml
spec:
  security:
    provider: catalog-security
    authenticationSecretRef:
      name: catalog-authn-r42
      key: credentials.yaml
    authorizationConfigMapRef:
      name: catalog-authz-r42
      key: policy.yaml
    reload:
      mode: HotReload
```

Use same-namespace references to approved sources. Put credentials, private keys and sensitive configuration in Secrets. Use a ConfigMap for non-secret policy. Grant the controller access to approved inputs and its generated delivery Secret. Limit who can create or change the references and approve operations so they cannot use the controller to read unrelated Secrets or widen policy. A volume consumer needs no Kubernetes API credentials. Report opaque revision IDs, phase, applied counts and bounded errors. Keep credential values, verifier digests and private policy content out of status and logs. [Permission boundary](api-and-resources.md#permission-boundary) · [Kubernetes Secret access](https://kubernetes.io/docs/concepts/configuration/secret/#configure-least-privilege-access-to-secrets)

## Apply a security revision

1. **Prepare.** Resolve the approved names into an immutable snapshot of the complete input, and validate its schema, references and provider compatibility. Record an opaque candidate revision. `ApplySecurityConfiguration` names that candidate and the expected active revision. Serialize activation per instance and hold traffic promotion while eligible peers are on different security revisions. Do not turn edits to source objects into unapproved activation.
2. **Stage.** For a file-based provider, mount a stable, operator-owned Secret directory in the existing Pods. Publish the active and candidate revisions as one complete payload in a single Secret update. Each consumer parses and validates the entire candidate before acknowledging it. Pin the payload size to fit the Secret size limit or qualify a different delivery method. Projected Secret changes arrive eventually, so the provider must retry reads and report what it actually loaded. Mount the directory without `subPath`. [Kubernetes Secret volume updates](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod)
3. **Activate.** After staging is confirmed for the required cohort, compare and set the persisted activation record against the expected active revision and publish the committed revision. Each process atomically swaps its validated configuration and invalidates or versions any affected credential, authorization and session caches. Authentication and authorization for one newly admitted request use the same local snapshot. Nodes can switch at different times. Report mixed application as pending and reconcile toward the committed revision after controller restarts.
4. **Verify.** Observe the active revision from each enforcing process over a protected management path. Tie each acknowledgement to its Pod UID and process incarnation, and discard it on restart. Verify representative allowed and denied requests through the paths the policy protects. Keep a new or restarted process out of serving and protected peer admission until it confirms and loads the committed revision. Verification must use the current activation record or a qualified admission fence, since even a new process can see a stale volume. Count every reachable production, preview and retained rollback consumer, plus native peers where the policy applies. A fleet that missed the update must catch up before readmission or promotion.

Applying an invalid candidate leaves the active revision in place and reports the error. If activation has already committed and a consumer lags, report partial application and hold promotion. Recovery resumes convergence to that revision or applies a newly approved recovery revision. Review any restored policy against revocations already in force. Restrict the stage and status endpoints to a private management path. A vanilla kubelet file update, Kubernetes readiness, or the Java warm-up timeout cannot establish that a policy is effective.

## Revocation and acceptance

Treat emergency revocation as a distinct operation from a routine update. A qualified gateway can deny the credential or permission before distribution if every corresponding client path goes through it, including HTTP access to preview and rollback fleets. For protection enforced in Java, require a provider that checks the minimum security revision at request admission or can fence stale consumers on every affected path. A partitioned process must reject affected requests when it can no longer prove it meets that requirement. A provider using expiring admission leases must include the remaining lease time in its revocation bound. Removing a Pod from a Service alone leaves other paths, including established connections and native peers. If the deployment cannot enforce the fence, escalate to the platform owner and report revocation as unconfirmed. Already-admitted work can finish unless the provider supports cancellation. Close or reauthenticate existing sessions where required by the revocation contract.

Qualify in-place reload on the real image or gateway. Test invalid policy and mismatched references, a delayed mount, a restart between staging and activation, a controller retry after commit, a partitioned consumer, and attempts to use revoked access over both new and existing connections. Include a live blue/green pair with preview access and eligible rollback, exercise native forwarding where applicable, and confirm ordinary reload preserves Pod UIDs and process incarnations. For TLS, use fresh HTTP and peer handshakes to verify the served certificate and trust, and keep the CA overlap procedure. [TLS rotation](traffic-and-security.md#tls-and-credential-rotation) · [Delivery and validation](delivery-and-validation.md#tests-and-acceptance-criteria)
