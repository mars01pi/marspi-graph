# marspi-graph

LangGraph-style multi-agent orchestration for Marspi.

Depends on [`marspi-core`](../marspi-core) for the single-agent ReAct runtime.
Consumed by `marspi-cli` (and later platform services).

## Packages

| Package | Role |
|---------|------|
| `graph` | StateGraph engine: nodes, edges, compile, invoke/stream/resume |
| `checkpoint` | Checkpointer interface + in-memory / SQLite backends |
| `agentspec` | Named agent factory wrapping core.Runner + tool views |
| `orchestrator` | Preset patterns: Pipeline, Supervisor, Handoff |

## Dependency rule

`cli → graph → core` (never reverse).
