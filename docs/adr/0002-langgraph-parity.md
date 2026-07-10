# ADR 0002: LangGraph semantic parity (subset)

## Status

Accepted

## Context

LangGraph's real core is a Pregel/BSP **state-update runtime**:
super-steps, channels/reducers, checkpoint boundaries, interrupt/Command,
and later Send/subgraphs — not merely a node/edge DSL.

`marspi-graph` today is a **serial state machine MVP** (single cursor,
last-write-wins merge, checkpoint Put without Resume). That is enough to
validate CodingLoop, but not enough for enterprise HITL, crash recovery,
or parallel fan-out.

Go community ports (`langgraphgo`, `langgraph-go`, CloudWeGo Eino) already
cover resume/interrupt/parallel. We will **not** replace our engine with
those libraries (they fight `marspi-core`'s Agent/MCP runtime). We align
on **semantics**, keep ownership of the runtime, and integrate AgentSpec.

## Decision

### Parity target (P0.5 / near-term)

1. **Per-key reducers** — default last-write-wins; optional `Append` (and
   custom) for list channels. Required before safe parallel writes.
2. **Resume** — `Compiled.Resume(threadID)` loads latest Snapshot and
   continues from `Snapshot.Node`.
3. **Interrupt + Command** — a node may call `graph.Interrupt(payload)`;
   Invoke returns `ErrInterrupted` after checkpointing. Resume with
   `Command{Resume: value}` re-enters the interrupted node; resume value
   is available via `graph.ResumeValue(ctx)`.
4. **Stay serial for now** — one active node per step. Parallel
   super-steps / Send are explicitly **out of P0.5**.

### Explicit non-goals (later)

- Pregel multi-node parallel execute within one super-step
- Dynamic `Send` fan-out
- Subgraphs as first-class compiled nodes
- Cross-thread Store
- Full stream modes (values/updates/events) — emit graph events later

### Gap summary vs LangGraph

| Capability | LangGraph | marspi after P0.5 |
|------------|-----------|-------------------|
| Node/edge/conditional | yes | yes |
| Checkpoint write | yes | yes (Memory + SQLite latest-per-thread) |
| Resume / time-travel | yes | Resume yes; time-travel later |
| Interrupt + Command | yes | yes (minimal) |
| Reducers | yes | yes (per-key) |
| Parallel / Send | yes | no |
| Subgraph / Store / rich stream | yes | no |

## Consequences

- CodingLoop and Supervisor/Handoff can keep using the same Builder API.
- HITL and crash recovery become possible without rewriting AgentSpec.
- Parallel work must wait until reducers + a fan-out executor exist.
- Document for contributors: prefer extending `graph` semantics over
  importing an external LangGraph Go port as the orchestrator core.
