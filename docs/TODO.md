# marspi-graph TODO

## Foundation sprint

| Status | Item | Notes |
|--------|------|-------|
| done | Graph-only Resume docs (ADR 0004) | Amended for P1 durable boundary |
| done | LoopCtx error + agentspec unify | Error propagation |
| done | Orchestrator → agentspec; ThreadID; changedFiles | No dual agent builders |
| done | Interrupt/Resume guards + tests | Engine edge cases |
| done | CLI cancel ctx + unique ThreadID | Esc stops /loopg /supervise |
| done | CLI HITL on Supervisor coder handoff | `RequireApprovalFor` + `OnInterrupt` + Confirm |
| done | Supervisor reliable routing (handoff tool-call) | `handoff(to,reason,task)` enum; no prose JSON |
| done | SQLite checkpointer (latest-per-thread) | **Legacy only** — not P1 production backend |

## P1 Durable Runtime (MySQL)

Design: [`docs/design/p1-durable-runtime.md`](./design/p1-durable-runtime.md)  
ADRs: 0005 AgentStore, 0006 Checkpoint history, 0007 Graph events

| Status | Item | Notes |
|--------|------|-------|
| done | Design docs + ADR 0005–0007 | Phase 0 |
| done | MySQL history + Memory DurableCheckpointer | CommitStep, CAS, prune |
| done | Graph lifecycle events | ADR 0007; sync handler + panic recover |
| done | Checkpoint-scoped AgentStore + step artifacts | PersistSession |
| done | Supervisor / CodingLoop durable alignment | Durable + ResumeFromCheckpoint |

**SQLite:** remains latest-only for CLI/single-process. No history / AgentStore extension. No SQLite→MySQL auto-migrate in P1.

**MySQL integration tests:** set `MARSPI_MYSQL_DSN` to enable.

## P1.5 Execution Lease

Design: [`docs/design/p15-execution-lease.md`](./design/p15-execution-lease.md)
ADR: [0008](./adr/0008-execution-lease.md)

| Status | Item | Notes |
|--------|------|-------|
| done | Execution lease contract | Background heartbeat; monotonic epoch; pause releases |
| done | `LeaseGrant` + `graph.ExecutionLease` + Memory impl | Whole-run heartbeat and cancellation |
| done | MySQL lease migration + fenced commit | `lease_run_id` / `lease_epoch` / `lease_expires_at` |
| done | Supervisor / CodingLoop lease wiring | `Lease` + `LeaseTTL` on configs |
| done | Concurrency / HITL / fence tests | Memory default; MySQL opt-in via `MARSPI_MYSQL_DSN` |

## Deferred product work (later)

| Priority | Item | Notes |
|----------|------|-------|
| — | HTTP idempotent run API / SSE | Service surface |
| — | Async event transport | Sync handler only in P1 |
| — | Time-travel Resume API | History queryable in P1 |
| — | Parallel / Send fan-out | Needs reducer-safe executor (ADR 0002) |
| — | Per-worker `transfer_to_*` tools | Equivalent to single `handoff(to=…)` already shipped |
| — | Redis ExecutionLease backend | Interface reserved in ADR 0008 |
