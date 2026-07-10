# marspi-graph TODO

## Foundation sprint

| Status | Item | Notes |
|--------|------|-------|
| done | Graph-only Resume docs (ADR 0004) | Honest checkpoint semantics |
| done | LoopCtx error + agentspec unify | Error propagation |
| done | Orchestrator → agentspec; ThreadID; changedFiles | No dual agent builders |
| done | Interrupt/Resume guards + tests | Engine edge cases |
| done | CLI cancel ctx + unique ThreadID | Esc stops /loopg /supervise |
| done | CLI HITL on Supervisor coder handoff | `RequireApprovalFor` + `OnInterrupt` + Confirm |
| done | Supervisor reliable routing (handoff tool-call) | `handoff(to,reason,task)` enum; no prose JSON |
| done | SQLite checkpointer (latest-per-thread) | `checkpoint.OpenSQLite`; inject via SupervisorConfig |
| later | AgentStore (persist agentctx per node) | Needed for true HITL chat resume |

## Deferred product work

| Priority | Item | Notes |
|----------|------|-------|
| — | Parallel / Send fan-out | Needs reducer-safe executor (ADR 0002) |
| — | Checkpoint history / time-travel | SQLite currently latest-only (Memory-compatible) |
| — | Per-worker `transfer_to_*` tools | Equivalent to single `handoff(to=…)` already shipped |
| — | Tool-based `transfer_to_*` as alternate handoff UX | Related to reliable routing above |

When picking up reliable routing: start with a single `handoff(to, reason, task)` tool on the supervisor node; do not stack business tools + structured output in one call.
