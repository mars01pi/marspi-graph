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
| [`graph`](./graph/) | **StateGraph 引擎**：节点、边、Reducer、Invoke/Resume、Interrupt |
| [`checkpoint`](./checkpoint/) | **检查点接口**：In-Memory 实现（后续 SQLite） |
| [`agentspec`](./agentspec/) | **Agent 工厂**：包装 core.Runner + 工具视图，用于编排节点 |
| [`orchestrator`](./orchestrator/) | **预设模式**：Pipeline、CodingLoop（三阶段循环）、Supervisor（星型编排） |

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
| `Compiled.Resume(threadID)` | 从最新 **graph** 检查点继续执行（不会恢复 agent chat 记忆 — 见 ADR 0004） |
| `WithCommand(Command{Resume: …})` | 恢复时注入外部数据 |

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
├── graph/                    # StateGraph 引擎
│   ├── builder.go            # 图构建器（AddNode/AddEdge/Compile）
│   ├── state.go              # State 类型
│   ├── command.go            # Interrupt/Resume 命令
│   └── interrupt.go          # 中断原语
├── checkpoint/               # 检查点接口
│   └── memory.go             # In-Memory 实现
├── agentspec/                # Agent 工厂
│   └── spec.go               # AgentSpec：包装 Runner 为可编排节点
├── orchestrator/             # 预设编排模式
│   ├── pipeline.go           # Pipeline — 线性流水线
│   ├── coding_loop.go        # CodingLoop — 三阶段编码循环
│   ├── supervisor.go         # Supervisor — 星型多 Agent 编排
│   └── handoff.go            # Handoff 协议（Decide/ParseDecision）
└── docs/adr/                 # 架构决策记录
    ├── 0001-state-and-agent-boundary.md
    ├── 0002-langgraph-parity.md
    ├── 0003-supervisor-handoff.md
    └── 0004-resume-scope.md
```

---

## 📚 架构决策记录 (ADR)

| ADR | 主题 |
|-----|------|
| [0001](./docs/adr/0001-state-and-agent-boundary.md) | State 与 Agent 的边界划分 |
| [0002](./docs/adr/0002-langgraph-parity.md) | LangGraph 功能对等清单 |
| [0003](./docs/adr/0003-supervisor-handoff.md) | Supervisor Handoff 协议设计 |
| [0004](./docs/adr/0004-resume-scope.md) | Resume 作用域：仅 graph 状态，不恢复 agent chat |

---

## 🛠 开发

```bash
go test ./...          # 运行全部测试
go test -race ./...    # 竞态检测
go vet ./...           # 静态分析
```

---

## 📄 License

[Apache License 2.0](LICENSE)
