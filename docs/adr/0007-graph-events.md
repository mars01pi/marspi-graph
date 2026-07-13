# ADR 0007: Graph lifecycle events

## Status

Accepted

## Context

Orchestrators currently only forward `agent.Emitter` from marspi-core.
Graph-level lifecycle (node enter/exit, checkpoint, interrupt, route) is
invisible, which blocks durable-runtime debugging and future SSE surfaces.

## Decision

1. **Separate buses, shared correlation:** Graph emits lifecycle events;
   core remains LLM/tool events. Both carry `RunID`, `ThreadID`, `NodeID`,
   `AgentID`, and graph also carries `CheckpointID`.
2. **Injection:** Run-scoped only — `WithEventHandler` on `RunOption`.
   One Compiled graph may serve many requests with different handlers.
3. **Delivery:** Synchronous callback. Handlers must be fast. Graph recovers
   handler panics and continues. Async transport is P1.5+.
4. **Payload:** Metadata-only by default (no full State / messages /
   interrupt body). Stream modes `updates`/`values` are deferred.
5. **Event set:** `run_start`, `run_resume`, `node_start`, `node_end`,
   `node_error`, `route`, `checkpoint`, `checkpoint_error`, `interrupt`,
   `run_end`, `custom`. Handoff is an orchestrator `custom` event, not a
   core graph enum member.
6. **Ordering (required):**
   - Success node: `node_start → node_end → route → checkpoint`
   - Interrupt: `node_start → checkpoint → interrupt → run_end(paused)`
   - Node error: `node_start → node_error → run_end(error)`
   - Checkpoint error: `… → checkpoint_error → run_end(error)`
   - Cancel / max steps: always terminate with `run_end`
   - Resume: `run_resume` then same per-node sequence; new `RunID`, same
     `ThreadID` and parent checkpoint
7. **agentspec** fills core `agent.Event` RunID/NodeID/AgentID from graph
   context so LLM/tool events join the same trace.

## Consequences

- Phase 1.5 ships the minimal skeleton for durable debugging; Phase 3
  completes route/custom and ordering tests.
- Consumers must not rely on event payloads for durable state — use
  checkpointers.
- See design: `docs/design/p1-durable-runtime.md`.
