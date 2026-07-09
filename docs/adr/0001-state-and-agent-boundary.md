# ADR 0001: State vs agentctx boundary

## Status

Accepted

## Context

marspi-core already has `agentctx.Manager` for per-conversation messages,
compaction, and session persistence. marspi-graph needs a shared workflow
state that multiple agent nodes can read/write across super-steps.

## Decision

1. **Graph `State`** is a `map[string]any` (plus typed helpers) owned by the
   graph engine. It holds workflow fields: goal, status, handoffs, shared
   scratchpad, and optionally a message channel for supervisor routing.
2. **`agentctx.Manager`** remains **private to each AgentSpec instance**.
   An agent node may copy task text from State into its Manager as a user
   message, run `Runner.LoopCtx`, then write results back into State.
3. Reducers apply only to graph State updates (e.g. append-only message
   lists). They do not mutate another agent's Manager.
4. Checkpoints serialize graph State (+ node cursor), not every agent
   session file. Agent sessions may still be saved under a run-scoped path
   for debugging.

## Consequences

- Clear ownership: conversation quality stays in core; orchestration in graph.
- Agents can use different models/tools without sharing one message list.
- Time-travel resumes the **graph** cursor and State; replaying an agent
  turn re-runs the node with a fresh `agentctx` unless a future AgentStore
  exists. See ADR 0004 (Resume scope).
