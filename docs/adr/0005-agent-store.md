# ADR 0005: Checkpoint-scoped AgentStore

## Status

Accepted

## Context

ADR 0004 made graph Resume honest: checkpoints restore graph cursor/State,
not per-agent chat. HITL and crash recovery that need "continue the same
worker/implementer conversation" require a second persistence surface.

Storing agent messages as latest-only `(thread_id, agent_id)` would desync
from checkpoint history: Resume could pair graph State at step N with chat
from step M.

## Decision

1. **Keep Snapshot free of agentctx** (ADR 0001). Agent sessions are separate
   rows bound by `checkpoint_id`.
2. **Physical model:** immutable `graph_agent_sessions` blobs +
   `graph_checkpoint_agents` refs. Each checkpoint has a full agent→session
   map; only changed agents insert a new blob. Unchanged agents copy refs.
3. **Write path:** `agentspec` with `PersistSession` loads from the parent
   checkpoint, runs, then `RegisterStepArtifact`. `Compiled.run` passes
   Snapshot + artifacts to `CommitStep` in one transaction.
4. **Policy:** `PersistNone` (default) vs `PersistSession`. CodingLoop
   persist only implementer; verifier/updater stay ephemeral.
5. **Coverage:** agentspec agents only. Supervisor routing uses graph State.
   Custom `WorkerSpec.Run` is not auto-persisted.
6. **System prompt:** empty manager → load → append system only if unloaded.
7. **Recovery boundary:** successful super-step only; not mid-ReAct turns.

## Consequences

- Cross-process Resume with DurableCheckpointer + PersistSession restores
  worker/implementer chat at the latest checkpoint.
- Without AgentStore / PersistSession, ADR 0004 graph-only semantics remain.
- Prune must delete orphan session blobs after removing old checkpoint refs.
- See design: `docs/design/p1-durable-runtime.md`.
