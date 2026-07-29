# ADR 0011: Spawn tool vs Supervisor handoff

## Status

Accepted

## Context

Coding agents (Claude Code, Cursor, Pi extensions) isolate exploration in
**subagents**: the parent keeps the main conversation and calls a tool that
runs a child agent with a fresh context, returning only a summary.

`marspi-graph` already has **Supervisor + handoff** (ADR 0003): a star graph
that routes among peer workers via shared State. Contributors conflate that
with subagent spawn.

## Decision

1. **Handoff** remains graph-edge transfer of control (shared State, durable
   resume, HITL). Owned by `orchestrator`.
2. **Spawn** is an **agent-as-tool** registered on a parent `tool.Registry`.
   It creates a fresh `agentspec.Instance`, runs `RunOnce`, and returns the
   final text to the parent tool result. Owned by `agentspec`.
3. Spawn does **not** live in `marspi-core` builtins (would reverse-depend on
   agentspec) or in the graph engine (no Subgraph / Send in v1).
4. Default `MaxDepth=1`: children do not receive the `spawn` tool. Depth is
   also enforced via context so nested calls fail closed.
5. Child sessions use `PersistNone` so they do not write Durable AgentStore
   keys under the parent thread.

```text
Handoff:  Supervisor ──edge──► Worker ──edge──► Supervisor
Spawn:    Parent Agent ──tool──► Child RunOnce ──summary──► Parent
```

## Consequences

- Supervisor workers may optionally `RegisterSpawn` on their registries for
  nested explore/review without changing graph topology.
- Subgraph / parallel Send remain deferred (ADR 0002); spawn is not a
  substitute for durable nested workflows.
- See [`docs/design/p3-spawn-tool.md`](../design/p3-spawn-tool.md).
