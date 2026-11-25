---
name: opensearch-operator-dev
description: Implement or review OpenSearch Serverless Operator tasks using the repository architecture and evidence requirements.
---

# Operator development

Read `docs/implementation/README.md`, the task's milestone and its linked design, and `docs/implementation/sdd.md`. Inspect current code and the pinned Java source before relying on planned behavior. Identify user-facing behavior, state transitions, concurrency and failure cases. Keep the workload-neutral lifecycle package independent of operator and Java types.

For state changes, persist intent before an external effect, re-read authoritative state after ambiguous outcomes, and guard owner and Pod identity. Choose tests that can disprove acceptance criteria. Use a real API server for admission and concurrency semantics and a real cluster for scheduling, kubelet, eviction and traffic behavior. Run `python tools/verify_plan.py` for plan consistency. Run task-specific checks from `docs/implementation/verification.md` where the environment exists.

Use `docs/implementation/task-handoff.md` to report changed behavior, commands and observed results, missing evidence, and compatibility decisions. Update the owning design when implementation evidence changes a contract. Never mark an unrun cloud or Java test as passed.
