# ADR 0004: Resume scope

## Status

Accepted (amended for P1 Durable Runtime)

## Context

P0.5 added `Compiled.Resume` and interrupt/Command. Callers may assume
resume restores full multi-agent conversations. ADR 0001 already keeps
`agentctx.Manager` private to each agent instance and out of Snapshot.

P1 adds an optional MySQL DurableCheckpointer with checkpoint-scoped
AgentStore (ADR 0005 / 0006). Resume semantics must stay precise.

## Decision

### Legacy / default (no DurableCheckpointer AgentStore)

**Graph-only Resume:**

1. A checkpoint guarantees recovery of graph `State`, cursor `Node`,
   step counter, and interrupt metadata.
2. Checkpoints do **not** serialize per-agent `agentctx` message lists.
3. With Memory or SQLite (legacy latest-only), cross-process Resume
   re-enters the node with a **fresh** agent conversation.

### With MySQL DurableCheckpointer + PersistSession

**Checkpoint-boundary Resume:**

1. Resume still loads the **latest** graph Snapshot (P1; no time-travel API).
2. Agentspec agents with `PersistSession` restore messages bound to that
   checkpoint (via parent-checkpoint session refs).
3. Recovery is at the **successful super-step** boundary only — not mid
   ReAct / tool iteration.
4. Ephemeral agents (`PersistNone`, e.g. CodingLoop verifier/updater)
   remain fresh on each visit.

## Consequences

- HITL that only needs graph State works with any checkpointer.
- HITL that needs "continue the same implementer/worker chat" requires
  DurableCheckpointer + `PersistSession` (ADR 0005).
- Docs must not claim iteration-level crash recovery.
- See also ADR 0001, ADR 0005, ADR 0006, and
  `docs/design/p1-durable-runtime.md`.
