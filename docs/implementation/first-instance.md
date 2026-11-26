# Deploy and change your first instance

Use a development namespace with an existing S3 bucket dedicated to this
instance, a digest-pinned Java runtime, and Nodes in at least one known
zone. Install the CRDs and manager, then follow the secure profile for the
keystore and certificate setup. The [secure install](../../config/operator-dev-secure/README.md)
lists the required image approvals, Secret keys, and certificate names.

## Start search and indexing

Copy the [HTTPS instance example](../../config/operator-dev-secure/instance.example.yaml)
to `catalog.yaml`. Set `spec.image`, the bucket and region in the
repository ConfigMap, and the zone under both pools. Keep one replica
and a serving floor of one per role to start. Use the zone label values
provided by your platform. If you can read Nodes, list the values with
`kubectl get nodes -L topology.kubernetes.io/zone`.

The API role `ingest` handles indexing. `catalog-index` selects ingest
Pods. `catalog-search` selects search Pods. Run the admission check
before creating the objects, then follow the secure install's step for
issuing the leaf after the instance UID exists.

```sh
kubectl apply --dry-run=server -f catalog.yaml
kubectl apply -f catalog.yaml
kubectl -n operator-development get opensearchserverless catalog -o wide
kubectl -n operator-development wait --for=condition=Available \
  opensearchserverless/catalog --timeout=5m
```

`Available` reports the last observed Kubernetes endpoint condition. The
intent columns show the fleets the operator has recorded. During a partial
cutover, use the stable Service selectors to see where Kubernetes routes
each role. Verify client traffic from an authorized client that can reach
both HTTPS routes. Use URLs covered by their certificates, the matching
CA bundle, and a scoped account that can create, write, and search a test index.
Set `INDEX_URL` and `SEARCH_URL` to the base URLs of the two role routes,
and `SMOKE_INDEX` to an unused test index name. `curl --user "$OS_USER"`
prompts for its password. For an in-cluster client, the direct index URL
is `https://catalog-index.operator-development.svc.<cluster-domain>:9200`
when the JVM certificate includes that name. The search URL uses
`catalog-search` with the same namespace and cluster domain.

```sh
curl --fail-with-body --cacert "$OS_CA" --user "$OS_USER" \
  -H 'Content-Type: application/json' -X PUT \
  "$INDEX_URL/$SMOKE_INDEX" --data-binary '{"settings":{"number_of_shards":1}}'
curl --fail-with-body --cacert "$OS_CA" --user "$OS_USER" \
  -H 'Content-Type: application/json' -X PUT \
  "$INDEX_URL/$SMOKE_INDEX/_doc/probe?refresh=true" \
  --data-binary '{"message":"operator routing check"}'
curl --fail-with-body --cacert "$OS_CA" --user "$OS_USER" \
  "$SEARCH_URL/$SMOKE_INDEX/_search?q=_id:probe"
```

Check that the write was acknowledged and the search response includes
`probe`.

## Preview different warm-spare counts

This example adds two search spares and one index spare in `zone-a`.
Replace `zone-a` with a configured zone from the instance. Keep its
existing replica and minimum counts if they differ from the example.

```yaml
# capacity-patch.yaml
spec:
  pools:
    search:
      zones:
        zone-a: {replicas: 1, minServing: 1, warmSpare: 2}
    ingest:
      zones:
        zone-a: {replicas: 1, minServing: 1, warmSpare: 1}
```

```sh
kubectl -n operator-development patch opensearchserverless catalog \
  --type=merge --patch-file capacity-patch.yaml --dry-run=server -o json > proposed.json
python tools/preview_operator_change.py capacity catalog -n operator-development \
  --proposed proposed.json
```

The preview shows the desired count before and after, the proposed serving
floor, and the recorded Kubernetes Ready count with its sample timestamp.
The new targets here are three search Pods and two index Pods. Search cache
warmth needs its own qualification. Apply the same patch without `--dry-run`
after confirming there is room on eligible Nodes. Re-run the preview without
`--proposed` to see the latest sampled counts. Use the
[provisioning and capacity runbook](day-two.md#provisioning-and-capacity) before reducing serving capacity.

Inspect API field descriptions for placement, runtime, security, or rollout.
The [configuration reference](../design/api-and-resources.md#spec-semantics)
describes the role maps and examples in more detail.

```sh
kubectl explain opensearchserverless.spec.pools --recursive
kubectl explain opensearchserverless.spec.runtime
kubectl explain opensearchserverless.spec.tls
kubectl explain opensearchserverlessoperation.spec
```

## Preview promotion and rollback

Use an instance with the [selector blue/green profile](development-rollout.md)
enabled. For a first test with one search and one ingest replica, merge
the following into `spec` and replace the image with the approved runtime
digest. Increase `maxPreviewReplicas` to cover the desired candidate
Pods across both roles and all zones if you added capacity earlier.

```yaml
rollout:
  strategy: BlueGreen
  maxPreviewReplicas: 2
  rollbackWindow: 30m
  candidate:
    fleet: green
    image: <approved-java-image@sha256:digest>
```

Apply the instance and wait for its candidate revision to appear. Before
creating a `Promote` operation, check both stable Service selectors and
the staged candidate's per-zone capacity.

```sh
python tools/preview_operator_change.py promote catalog -n operator-development
kubectl -n operator-development get opensearchserverless catalog \
  -o jsonpath='{.metadata.uid}'
```

Copy the [promotion manifest](../../config/contracts/fixtures/operation-promote.yaml).
Use your namespace, the instance UID, the staged candidate for `targetFleet`,
and the two current selector values for `expectedServingFleetByRole`. The
keys are `search` and `ingest`. Validate the manifest before applying it.
Publish the required signed evidence using the rollout runbook. Then watch
the operation and check the report's reason if progress holds.

```sh
kubectl apply --dry-run=server -f promote.yaml
kubectl apply -f promote.yaml
kubectl -n operator-development get opensearchserverlessoperations -o wide -w
```

After promotion, preview the retained target with `rollback` in place of
`promote`. When you want to restore traffic, create a new operation with a
new name and `action: Rollback`, the retained `targetFleet`, and the current
two selector values. Check the retention deadline and use the runbook for
superseding an in-progress operation. Verify index and search after each
change. Weighted Gateway moves use the [weighted procedure](development-rollout.md#enable-weighted-bluegreen-execution).

The preview only reads Kubernetes objects. The samples may age between reads.
The controller still checks current route convergence, JVM and store evidence,
disruption budgets, and signed approval during execution.
