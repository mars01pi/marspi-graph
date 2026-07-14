# marspi-graph

[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue)](LICENSE)
[![LangGraph-Style](https://img.shields.io/badge/Pattern-LangGraph%20Style-7B68EE)](https://github.com/langchain-ai/langgraph)

> LangGraph 风格的多智能体编排引擎 — StateGraph、Checkpointer、Pipeline、CodingLoop、Supervisor

Depends on [`marspi-core`](../marspi-core) for the single-agent ReAct runtime.
Consumed by `marspi-cli` (and later platform services).

---

## 📦 Packages

| 包 | 角色 |
|------|------|
| [`graph`](./graph/) | **StateGraph 引擎**：节点、边、Reducer、Invoke/Resume、Interrupt、Events |
| [`checkpoint`](./checkpoint/) | **检查点**：Memory / SQLite（legacy latest-only）+ MySQL / DurableMemory（P1 history） |
| [`agentspec`](./agentspec/) | **Agent 工厂**：包装 core.Runner；可选 `PersistSession` |
| [`orchestrator`](./orchestrator/) | **预设模式**：Pipeline、CodingLoop、Supervisor |

---

## 🚀 快速开始

```bash
# 构建
go build ./...

# 运行测试
go test ./...
```

> `marspi-graph` 是库项目，不提供独立二进制。通过 `marspi-cli` 的 `/loopg` 和 `/supervise` 命令使用。

---

## 🧠 StateGraph 引擎

### 核心概念

| 概念 | 说明 |
|------|------|
| **State** | `map[string]any`，节点间共享的轻量状态 |
| **Node** | `func(ctx, State) → (Update, error)`，单步执行单元 |
| **Edge** | 从一个节点到另一节点的有向连接 |
| **Conditional Edge** | `func(ctx, State) → string`，根据状态动态选择下一节点 |
| **Reducer** | `AddReducer(key, fn)` — 默认 last-write-wins；`AppendSlice` 用于列表合并 |

### 典型用法

```go
b := graph.NewBuilder()
b.AddReducer("messages", graph.AppendSlice)

b.AddNode("step1", func(ctx context.Context, s graph.State) (graph.Update, error) {
    return graph.Update{"result": "done", "messages": "step1 ok"}, nil
})
b.AddNode("step2", func(ctx context.Context, s graph.State) (graph.Update, error) {
    return graph.Update{"result": "done", "messages": "step2 ok"}, nil
})

b.SetEntry("step1")
b.AddEdge("step1", "step2")

g, _ := b.Compile(graph.WithCheckpointer(checkpoint.NewMemory()))
out, _ := g.Invoke(ctx, graph.State{"goal": "hello"})
```

### Interrupt / Resume

| 操作 | 说明 |
|------|------|
| `graph.Interrupt(v)` | 节点返回中断信号，暂停执行等待外部输入 |
| `Compiled.Resume(threadID)` | 从最新检查点继续；默认 **graph-only**（ADR 0004） |
| `WithCommand(Command{Resume: …})` | 恢复时注入外部数据 |
| `checkpoint.OpenSQLite(path)` | **Legacy** latest-only 文件持久化（CLI） |
| `checkpoint.OpenMySQL(dsn)` / `NewDurableMemory()` | **P1** append history + AgentStore（ADR 0005/0006） |
| `WithEventHandler(h)` | Graph lifecycle events（ADR 0007） |
| `WithExecutionLease(l)` | P1.5 执行租约（ADR 0008） |
| `graph.ToolExecutionFrom` | 工具幂等键 + lease fence token（ADR 0009） |

```go
// Legacy CLI
cp, _ := checkpoint.OpenSQLite("checkpoints.db")
g, _ := b.Compile(graph.WithCheckpointer(cp))

// P1 durable (MySQL or in-memory twin)
d, _ := checkpoint.OpenMySQL(dsn) // or checkpoint.NewDurableMemory()
g, _ := b.Compile(graph.WithDurableCheckpointer(d))
out, _ = g.Invoke(ctx, state, graph.WithThreadID(id), graph.WithEventHandler(handler))
out, _ = g.Resume(ctx, id, graph.WithCommand(graph.Command{Resume: true}))
```

With `Durable` + `agentspec.PersistSession`，跨进程 Resume 会恢复 agent 对话（super-step 边界）。详见 [`docs/design/p1-durable-runtime.md`](docs/design/p1-durable-runtime.md)。

Supervisor / CodingLoop 可通过 `Checkpointer` 或 `Durable` 注入；`ResumeFromCheckpoint` 跳过 Invoke 直接续跑。

Custom nodes / ContextualTools can pass stable downstream metadata:

```go
exec, _ := graph.ToolExecutionFrom(ctx, "charge_card", orderID)
req.Header.Set("Idempotency-Key", exec.IdempotencyKey())
req.Header.Set("X-Marspi-Lease-Epoch", strconv.FormatInt(exec.FenceToken(), 10))
```

agentspec 已把 graph context 注入 `Runner.ToolMeta`；bash/MCP 会遵守父 `ctx` 取消。详见 ADR 0009。

---

## 🔁 预设编排模式

### Pipeline — 线性流水线

节点按顺序串行执行：`node1 → node2 → … → END`。

```go
order := []string{"read", "analyze", "report"}
g, _ := orchestrator.Pipeline(nodes, order)
```

### CodingLoop — 三智能体工程循环

模拟 **Implementer → Verifier → Updater** 三阶段编码循环，直到全部验证通过或达到最大轮次。

```go
g, _ := orchestrator.CodingLoop(orchestrator.LoopConfig{
    Goal:     "Implement feature X",
    Provider: provider,
    Registry: registry,
    Reporter: console,
})
out, _ := g.Invoke(ctx, graph.State{"goal": "..."})
```

> 对应 `marspi-cli` 中 `/loopg` 命令。

### Supervisor — 星型多 Agent 编排

**Supervisor** 节点作为路由中心，根据 LLM 决策将任务分发给 **Worker** 节点，worker 执行完毕后返回 supervisor 再次决策，直到满足目标。

```go
res, _ := orchestrator.RunSupervisor(ctx, orchestrator.SupervisorConfig{
    Goal:     "Fix bug in parser",
    Workers:  []orchestrator.WorkerSpec{
        {ID: "researcher", Description: "Read/search code", AllowTools: []string{"read", "grep"}},
        {ID: "coder",      Description: "Edit files"},
    },
    Provider:             provider,
    RequireApprovalFor:   []string{"coder"}, // HITL gate before RunOnce
    OnInterrupt: func(ctx context.Context, info orchestrator.InterruptInfo) (bool, error) {
        return confirm(info), nil // approve → Resume; deny → ErrApprovalDenied
    },
})
```

路由：Supervisor 默认通过 **handoff tool-call**（`to`/`reason`/`task`，`to` 为 worker∪{END} 枚举）决策，不解析正文 JSON。State 仍写 `next` / `handoff` / `messages`。

HITL：`RequireApprovalFor` 中的 worker 入口会 `Interrupt`；`OnInterrupt` 返回 true 时 `Resume(Command{Resume: true})` 再执行。审批的是 handoff，不恢复 agent 私聊（ADR 0004）。

> 对应 `marspi-cli` 中 `/supervise`（派 `coder` 前会 Confirm）。

#### 流程图

```
                        ┌─────────────┐
                        │  Supervisor  │  ← LLM 决定 next worker
                        └──────┬──────┘
                   ┌───────────┼───────────┐
                   ▼           ▼           ▼
             ┌──────────┐ ┌──────────┐ ┌──────────┐
             │researcher│ │   coder  │ │  writer  │
             └──────────┘ └──────────┘ └──────────┘
                   │           │           │
                   └───────────┼───────────┘
                               ▼
                        next=END → 完成
```

---

## 📐 架构设计规则

```
cli → graph → core    （单向依赖，禁止反向引用）
```

- `graph` 包仅依赖标准库 + `marspi-core` 的 `llm` / `tool` 类型
- `orchestrator` 依赖 `graph` + `agentspec` + `marspi-core`
- `agentspec` 包装 `core.Runner` 提供标准化 Agent 节点

---

## 📁 项目结构

```
marspi-graph/
├── graph/                    # StateGraph 引擎 + events + durable APIs
├── checkpoint/               # Memory / SQLite legacy / MySQL + DurableMemory
├── agentspec/                # Agent 工厂（PersistSession）
├── orchestrator/             # Pipeline / CodingLoop / Supervisor
└── docs/
    ├── design/p1-durable-runtime.md
    └── adr/                  # 0001–0007
```

---

## 📚 架构决策记录 (ADR)

| ADR | 主题 |
|-----|------|
| [0001](./docs/adr/0001-state-and-agent-boundary.md) | State 与 Agent 的边界划分 |
| [0002](./docs/adr/0002-langgraph-parity.md) | LangGraph 功能对等清单 |
| [0003](./docs/adr/0003-supervisor-handoff.md) | Supervisor Handoff 协议设计 |
| [0004](./docs/adr/0004-resume-scope.md) | Resume 作用域（graph-only / checkpoint-boundary） |
| [0005](./docs/adr/0005-agent-store.md) | Checkpoint-scoped AgentStore |
| [0006](./docs/adr/0006-checkpoint-history.md) | MySQL checkpoint history |
| [0007](./docs/adr/0007-graph-events.md) | Graph lifecycle events |
| [0008](./docs/adr/0008-execution-lease.md) | Execution lease with monotonic fencing |
| [0009](./docs/adr/0009-tool-execution-metadata.md) | ToolExecution + ContextualTool bridge |

---

## 🛠 开发

```bash
go test ./...          # 运行全部测试
go test -race ./...    # 竞态检测
go vet ./...           # 静态分析
# MySQL 集成测试（可选）
MARSPI_MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/marspi?parseTime=true' go test ./checkpoint -run MySQL
```

---

## 📄 License

[Apache License 2.0](LICENSE)
