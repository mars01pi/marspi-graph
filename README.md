# marspi-graph

LangGraph-style multi-agent orchestration for Marspi.

Depends on [`marspi-core`](../marspi-core) for the single-agent ReAct runtime.
Consumed by `marspi-cli` (and later platform services).

## Packages

| Package | Role |
|---------|------|
| `graph` | StateGraph engine: nodes, edges, reducers, invoke/resume, interrupt |
| `checkpoint` | Checkpointer interface + in-memory (SQLite later) |
| `agentspec` | Named agent factory wrapping core.Runner + tool views |
| `orchestrator` | Preset patterns: Pipeline, CodingLoop, Supervisor |

## P0.5 semantics (LangGraph subset)

- **Reducers**: `AddReducer(key, fn)` — default last-write-wins; `AppendSlice` for lists
- **Resume**: `Compiled.Resume(threadID)` continues from latest checkpoint
- **Interrupt**: node returns `graph.Interrupt(v)` / `InterruptOrResume`; resume with `WithCommand(Command{Resume: ...})`

See `docs/adr/0002-langgraph-parity.md`.

## Dependency rule

`cli → graph → core` (never reverse).
