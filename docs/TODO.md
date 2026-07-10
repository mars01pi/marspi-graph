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
| later | AgentStore (persist agentctx per node) | Needed for true HITL chat resume |

## Deferred product work

| Priority | Item | Notes |
|----------|------|-------|
| P2 (low) | **Supervisor reliable routing** — prefer handoff **tool-call** (or provider structured output) over free-text JSON | Prompt-only JSON + one retry is MVP-fragile. Industry default: route via `tool_calls` / strict schema; validate `next ∈ workers∪{END}`; optional repair pass. Keep writing the same State keys (`next`, `handoff`, `messages`). See ADR 0003. |
| — | Parallel / Send fan-out | Needs reducer-safe executor (ADR 0002) |
| — | SQLite checkpointer | Memory is fine for CLI demos |
| — | Tool-based `transfer_to_*` as alternate handoff UX | Related to reliable routing above |

When picking up reliable routing: start with a single `handoff(to, reason, task)` tool on the supervisor node; do not stack business tools + structured output in one call.
