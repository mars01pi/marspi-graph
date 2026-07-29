# P3 Spawn Tool

## Goal

Give any `agentspec` ReAct agent a Claude Code / Cursor-style **spawn** tool:

- Parent keeps its conversation and control
- Child runs in a **fresh** `agentctx` with its own tools / prompt / model
- Parent receives only the child's final summary (truncated)

This is **orthogonal** to Supervisor handoff (ADR 0003 / ADR 0011).

## Non-goals (v1)

- Background spawn / completion notifications
- Multi-task parallel spawn in one tool call
- Fork parent conversation / git worktree isolation
- Markdown agent definitions (`.pi/agents`)
- Subgraph checkpoints or Send fan-out (ADR 0002)
- CLI wiring (`marspi-cli` registers spawn later)

## Module layout

```text
agentspec/
  spawn.go       # ChildSpec, SpawnConfig, RegisterSpawn, spawn tool
  spawn_test.go
```

Dependency direction unchanged:

```text
cli/service → agentspec → graph + marspi-core
```

`marspi-core` does **not** gain a builtin `spawn` tool.

## API

```go
type ChildSpec struct {
    ID           string
    Description  string
    SystemPrompt string
    Provider     llm.Provider // nil → inherit parentProvider
    Registry     *tool.Registry
    AllowTools   []string
    MaxIter      int
}

type SpawnConfig struct {
    Children  []ChildSpec
    MaxDepth  int // default 1
    MaxOutput int // default 8KiB
}

func RegisterSpawn(parent *tool.Registry, parentProvider llm.Provider, cfg SpawnConfig) error
```

Tool name: `spawn`

| Arg | Required | Meaning |
|-----|----------|---------|
| `agent_type` | yes | ChildSpec.ID |
| `task` | yes | Self-contained instructions for the child |
| `description` | no | Short label for logs / UI |

## Runtime semantics

1. Reject if `spawnDepth(ctx) >= MaxDepth`.
2. Resolve `ChildSpec` by `agent_type`.
3. Build `agentspec.Spec` with `PersistNone`, inherit provider when nil.
4. `New` + `RunOnce(withSpawnDepth(ctx, depth+1), task)`.
5. Return success content = final text, truncated to `MaxOutput`.
6. Propagate cancellation via `ContextualTool.RunCtx` (ADR 0009).

Children are **not** registered with `spawn` when `MaxDepth == 1` (default).

## Tests

| # | Case |
|---|------|
| 1 | Register success + schema params |
| 2 | Empty / duplicate children → error |
| 3 | Sync success path |
| 4 | Unknown agent_type |
| 5 | Context isolation (child request has no parent history) |
| 6 | AllowTools view |
| 7 | Depth guard |
| 8 | ctx cancel |
| 9 | MaxOutput truncate |
| 10 | Provider inherit |
| 11 | AsNode light integration |

## Phasing

| Status | Item |
|--------|------|
| done | Design + ADR 0011 |
| done | `RegisterSpawn` + tool |
| done | Unit tests |
| deferred | CLI register on main agent |
| deferred | Background / parallel / Markdown agents |
