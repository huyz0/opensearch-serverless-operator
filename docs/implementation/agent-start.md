# Starting an implementation task

Use this prompt with an agent working in this repository. Replace the task ID and include any constraints that apply to the particular change.

```text
Implement task M02.01 from docs/implementation/foundation.md.
Read AGENTS.md, the task's prerequisites and linked design, and the
operator development skill. Inspect the current repository before editing.
Implement the task and run the checks that establish its acceptance criteria.
Report the task handoff, including evidence and any criteria not verified.
```

For a review, give a fresh reviewer the task ID, the change and the relevant design links. Ask for defects that could violate the task's acceptance criteria, especially behavior under stale state, retries and process restart. The reviewer should cite concrete code and a triggering sequence. Record unresolved findings in the handoff. Independent review can use a separate agent when the user has authorized delegation.

## Examples of choosing evidence

- A change to an instance field needs schema generation and API-server tests for acceptance, rejection, status isolation and compatibility. A YAML parser establishes syntax only.
- A change to Pod retirement needs deterministic planner tests and API-server tests for UID and eviction behavior. A cluster test supplies PDB controller and kubelet evidence.
- A change to the warm gate needs the actual Java process and restart cases. A custom Pod status condition alone cannot establish the current process is warm.
- A change to S3, TLS, OCI delivery or ingress needs the actual integration in addition to fakes. EKS and GKE claims require their respective platform runs.

If a prerequisite or test environment is unavailable, complete independent work and record the missing evidence and exact blocker. The task remains open until its own acceptance conditions are met.
