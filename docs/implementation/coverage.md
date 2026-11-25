# Feature coverage and proof owners

Use this map to find the implementation tasks that make a design claim true. A task reference indicates where evidence belongs. The plan and these mappings are checked by `tools/verify_plan.py`.

| Requirement | Authoritative design | Tasks |
| --- | --- | --- |
| Development plan, skill, schema, SDD and proof workflow | [Verification](verification.md) | M00.01, M00.02, M00.03, M00.04 |
| Typed APIs, admission and namespace-only defaults | [API contracts](../design/api-and-resources.md) | M01.01, M01.02, M01.03, M01.04, M01.05 |
| Reusable direct Pod planner, exact identity and execution | [Lifecycle package](../design/reconciliation.md#package-boundaries) | M02.01, M02.02, M02.03, M02.04, M02.05 |
| Java image, named primary remote store, auth and recovery | [Primary S3 store](../design/api-and-resources.md#primary-s3-remote-store) | M03.01, M03.02, M03.03, M03.04 |
| Logical snapshot repository and independent backup | [Snapshot repository](../design/api-and-resources.md#snapshot-repository-parameter) | M03.05, M11.04 |
| Role/AZ lifecycle, status, approved operations and deletion | [Reconciliation](../design/reconciliation.md) | M04.01, M04.02, M04.03, M04.04, M04.05 |
| Dedicated/shared host sizing and Karpenter/GKE fit | [Dedicated node profiles](../design/placement-and-recovery.md#dedicated-node-profiles) | M05.01, M05.02, M05.03, M05.04, M05.05 |
| Confirmed warmth, search/index reserves and drain policy | [Availability](../design/placement-and-recovery.md#maintenance-and-an-az-failure) | M06.01, M06.02, M06.03, M06.04 |
| AZ failure, emergency scale-out and failback | [AZ resilience](../design/placement-and-recovery.md#maintenance-and-an-az-failure) | M06.05 |
| Stable ingress destinations, ALB target failure, retries and same-AZ preference | [Ingress and ALB](../design/traffic-and-security.md#ingress-and-alb) | M07.01, M07.02, M07.08 |
| Same-AZ rolling, canary, shared-store blue/green, rollback and weighted routing | [Promotion](../design/traffic-and-security.md#bluegreen-promotion-and-rollback) | M07.03, M07.04, M07.05, M07.06, M07.07 |
| Peer/HTTP TLS, issuer, credential and CA rotation | [TLS rotation](../design/traffic-and-security.md#tls-and-credential-rotation) | M08.01, M08.02, M08.03, M08.06 |
| Built-in users, Java/gateway policy reload and revocation | [Dynamic security](../design/security-reload.md) | M08.04, M08.05, M08.06 |
| JVM, role launch settings, sidecars and live cluster settings | [Runtime settings](../design/runtime-configuration.md) | M09.01, M09.02 |
| Automatic OCI fetch, install and plugin upgrade | [OCI plugins](../design/oci-plugins.md) | M09.03, M09.04 |
| Release build, EKS/GKE production gates and on-call | [Delivery](../design/delivery-and-validation.md) | M10.01, M10.02, M10.03, M10.04, M10.05 |
| Role autoscaling, GCS, writer drain and per-index AZ policy | [Follow-ons](../research/operator-feature-inventory.md#follow-on-features) | M11.01, M11.02, M11.03 |
| App resources, tenant protection and scale-to-zero | [Follow-ons](../research/operator-feature-inventory.md#follow-on-features) | M11.05, M11.06 |
