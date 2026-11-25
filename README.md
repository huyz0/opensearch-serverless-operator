# OpenSearch Serverless Operator

Build and run the current implementation with the
[HTTPS and Basic Auth development install](config/operator-dev-secure/README.md).
The [plain HTTP development install](config/operator-dev/README.md) is
available for isolated tests.
Use the [day-two runbook](docs/implementation/day-two.md) to triage a running development instance.

Start with the [high-level architecture](docs/design/README.md), then follow the detailed design for Kubernetes APIs, Pod lifecycle, placement and recovery, traffic and security, and delivery. The [research index](docs/research/README.md) records the Java source baseline and comparison studies behind the design.

The [implementation plan](docs/implementation/README.md) defines milestones, task dependencies and acceptance evidence. The [software design description](docs/implementation/sdd.md) records the implementation boundaries.

The `pkg/podlifecycle` package provides workload-neutral Pod lifecycle types and operations.

Licensed under the [Apache License, Version 2.0](LICENSE).
