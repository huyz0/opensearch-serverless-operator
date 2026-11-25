# OpenSearch Serverless Operator research

Start with the [operator architecture and detailed design](../design/README.md) for the proposed system. This index records the evidence and design studies behind it.

The upstream operator is a useful reference for Kubernetes reconciliation, resource ownership, rollout status, and test structure. Its workload lifecycle assumes classic OpenSearch, including cluster-manager quorum, voting exclusions, shard allocation, and per-node data volumes. The Java serverless shell publishes membership leases and owns shards through shared object storage. Those are different contracts for readiness, replacement, and recovery.

| Study | What it covers |
| --- | --- |
| [Upstream OpenSearch Kubernetes Operator](upstream-operator.md) | Source walkthrough from the manager and CRD through bootstrap, workload generation, scaling, restarts, upgrades, and deletion |
| [Java serverless deployment contract](java-serverless-runtime.md) | JVM entry point, object storage and credentials, node identity, peer transport, probes, and shutdown |
| [Java serverless shell architecture](java-serverless-architecture.md) | Request routing, object-store ownership, WAL fencing and replay, and search visibility |
| [Operating the Java shell on Kubernetes](java-serverless-operations.md) | Role pools, rollout and scale behavior, telemetry, and deployment tests |
| [Implications for a serverless operator](operator-design-implications.md) | Reuse boundaries, implementation options, open decisions, and runtime checks |
| [StatefulSet AZ skew and PostgreSQL operator references](multi-az-workload-research.md) | Scheduler and rollout skew, CloudNativePG direct Pod control, and Crunchy per-instance StatefulSets |
| [Multi-AZ workloads for the Java serverless operator](multi-az-operator-design.md) | Role and zone replica targets, rollout availability, service routing, and the shell's shard locality behavior |
| [Operator feature inventory](operator-feature-inventory.md) | Proposed features, upstream comparison, Java dependencies, and release checks |
| [Reusable direct Pod lifecycle package](pod-lifecycle-package.md) | Package boundary, retirement sequencing, and recovery across retries |
| [Blue/green deployment](blue-green-deployment.md) | Candidate fleets, promotion and rollback, and the shared Java data-plane boundary |
| [TLS certificate setup and rotation](tls-certificate-rotation.md) | Provided Secrets, optional managed issuance, leaf renewal, and CA rollover |
| [Karpenter scheduling research](karpenter-scheduling-research.md) | AWS capacity estimates, DaemonSet request accounting, and repeat launches when Pods cannot bind |
| [Dedicated nodes for indexing and search](dedicated-node-placement.md) | One OpenSearch Pod per node, role-specific profiles, DaemonSet changes, and bounded provisioning on EKS and GKE |
| [Node maintenance and loss](node-disruption-recovery.md) | Provider maintenance, namespaced disruption signals, shutdown hooks, and recovery of serving capacity and warm search |

## Source snapshots

- **Upstream operator.** The full Git clone is in [`upstream/opensearch-k8s-operator`](../../upstream/opensearch-k8s-operator). The study pins `main` at `b0be10474334e676c6044e7943322685af2e2d34` (11 November 2026). This is a source snapshot beyond the `opensearch-operator-3.0.14` tag. [Upstream commit](https://github.com/opensearch-project/opensearch-k8s-operator/commit/b0be10474334e676c6044e7943322685af2e2d34)
- **Java target.** The published `huyz0/OpenSearch` branch `feature/serverlessshell` was verified at `e71d42331a3ec82ec01109a5bb42d6a9a9162a5c` on 23 November 2026 Sydney time and inspected directly from Git. The earlier study used the divergent local checkout at `d3c40c491176a97746408e2b4c78e64099681a21`. The current Java pages pin the published revision. [Java branch snapshot](https://github.com/huyz0/OpenSearch/commit/e71d42331a3ec82ec01109a5bb42d6a9a9162a5c)
- **PostgreSQL references.** The shallow clones are in [`upstream/cloudnative-pg`](../../upstream/cloudnative-pg) at `e9eec0d3b49adb088f6fbcfa024d4ea288105c6e` and [`upstream/crunchy-postgres-operator`](../../upstream/crunchy-postgres-operator) at `0478c1dea748e6bc4cc382aba1210aaefdedd75a`. [CloudNativePG revision](https://github.com/cloudnative-pg/cloudnative-pg/commit/e9eec0d3b49adb088f6fbcfa024d4ea288105c6e) · [Crunchy revision](https://github.com/CrunchyData/postgres-operator/commit/0478c1dea748e6bc4cc382aba1210aaefdedd75a)
- **Karpenter references.** The shallow clones are in [`upstream/karpenter-provider-aws`](../../upstream/karpenter-provider-aws), [`upstream/karpenter`](../../upstream/karpenter), and [`upstream/karpenter-provider-gcp`](../../upstream/karpenter-provider-gcp). The [Karpenter study](karpenter-scheduling-research.md#references-and-validation) gives the provider revisions and the exact core revisions they use.

The findings come from static source inspection. The AZ study also checked its count examples in a small model. No build, Java test, or Kubernetes deployment was run for this research.
