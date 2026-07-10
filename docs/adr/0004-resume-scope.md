# ADR 0004: Resume scope (graph-only)

## Status

Accepted

## Context

P0.5 added `Compiled.Resume` and interrupt/Command. Callers may assume
resume restores full multi-agent conversations. ADR 0001 already keeps
`agentctx.Manager` private to each agent instance and out of Snapshot.
This ADR makes the product meaning explicit for CodingLoop / Supervisor.

## Decision

**Graph-only Resume:**

1. A checkpoint guarantees recovery of graph `State`, cursor `Node`,
   step counter, and interrupt metadata.
2. Checkpoints do **not** serialize per-agent `agentctx` message lists.
3. CodingLoop and Supervisor support durable checkpointers (SQLite).
   Cross-process or post-crash Resume re-enters the node with a **fresh**
   agent conversation; prior implementer/worker turns are not restored.
4. AgentStore / session time-travel is explicitly out of scope for this
   sprint; a future ADR may add optional per-node session persistence.

## Consequences

- HITL that only needs graph State + interrupt payload works across
  process restarts when using `checkpoint.OpenSQLite` + the same
  `thread_id` and graph topology.
- HITL that needs "continue the same implementer chat" requires AgentStore.
- Docs and CLI must not claim crash recovery restores agent memory.
- See also ADR 0001 (state vs agentctx boundary).
