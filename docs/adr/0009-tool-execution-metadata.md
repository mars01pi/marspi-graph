# ADR 0009: Tool execution metadata and contextual tools

## Status

Accepted

## Context

P1.5 execution lease (ADR 0008) fences graph checkpoint commits and cancels
the run context on lease loss. External tool side effects still ran through
`marspi-core` with a context-blind `Tool.Run(args)` path, so:

- bash/MCP used `context.Background()` and could outlive lease cancellation;
- tools had no standard way to receive `LeaseEpoch` or an idempotency key;
- custom `NodeFunc` authors had no shared contract for downstream dedup.

Exactly-once for arbitrary effects still requires downstream cooperation
(idempotent APIs, unique constraints, or an effect journal). This ADR defines
the metadata contract and a backward-compatible core bridge — not a journal.

## Decision

1. Introduce `graph.ToolExecution` with:
   - stable `IdempotencyKey()` from thread + parent checkpoint + node + tool
     name + caller `operationID`;
   - `FenceToken()` = lease epoch (`0` if no lease).
2. Exclude `runID` and `leaseEpoch` from the idempotency key so retry and
   lease takeover of the same logical operation reuse the same downstream key.
3. Keep core independent of graph: define `tool.ExecutionMeta` and optional
   `ContextualTool.RunCtx(ctx, meta, args)` in `marspi-core`.
4. Add `Registry.ExecuteQuietCtx`; legacy `ExecuteQuiet` remains.
5. `agent.Runner.ToolMeta` optionally fills metadata; `agentspec` wires it from
   graph context, defaulting `operationID` to the LLM `tool_call_id`.
6. Upgrade bash and MCP to honor parent context cancellation.

## Consequences

- Agentspec tool calls cancel sooner after lease loss when tools implement
  `RunCtx` (or stop at the registry gate on already-canceled ctx).
- Tools that ignore metadata are unchanged; they are not automatically
  exactly-once.
- Future durable tool-effect stores and Redis leases can reuse the same
  key/epoch contract without changing ToolExecution field semantics.
- See [`docs/design/p15-execution-lease.md`](../design/p15-execution-lease.md)
  Phase 4 section.
