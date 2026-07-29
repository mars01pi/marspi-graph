# P2 Eval Runtime

## Goal

Give marspi a **DeepEval-class evaluation surface** without embedding Python in the Go runtime:

1. **Unified Trace** — one exportable artifact per graph run (or multi-run thread), assembled from graph lifecycle + agent LLM/tool events (+ optional session/checkpoint enrichment)
2. **Collector** — non-intrusive adapters on existing buses (`graph.EventHandler`, `agent.Emitter`)
3. **Eval contract** — Golden / Suite / MetricResult schemas that DeepEval (or a Go judge) can consume
4. **Hybrid scoring** — deterministic metrics in Go; LLM-as-judge via optional Python DeepEval sidecar

marspi **owns traces**. DeepEval (or Confident AI) **consumes traces**. Evaluation never drives the StateGraph engine.

## Non-goals (P2)

- Rewriting DeepEval metrics in Go
- Shipping Confident AI as a hard dependency
- Persisting full State / message bodies on every graph event (ADR 0007 stays metadata-only on the live bus)
- Changing SSE semantics to replay historical traces
- Online production auto-scoring as a required path (optional sampling later)
- Parallel / Send fan-out eval (depends on ADR 0002)

## Why now

Durable runtime (P1) already provides:

| Primitive | Eval use |
|-----------|----------|
| `RunID` / `ThreadID` / `NodeID` / `AgentID` / `CheckpointID` | Trace correlation (ADR 0007) |
| `agent.Emitter` (llm/tool events) | Span payloads for tools, usage, previews |
| Durable `AgentSession` | Authoritative conversation for PersistSession agents |
| Checkpoint history | Offline enrichment when live events lack bodies |
| Service approvals | Human labels / feedback → future goldens |

Gaps today: no Trace schema, no durable event log, service SSE forwards **graph** events only, no dataset/runner/metrics package.

## Architecture

```text
                    ┌──────────────────────────────────────┐
                    │  Eval Control (later)                │
                    │  datasets · suites · CI gates · HITL │
                    └──────────────────┬───────────────────┘
                                       │
                    ┌──────────────────▼───────────────────┐
                    │  Runner                              │
                    │  in-process (graph/orchestrator)     │
                    │  HTTP (marspi-service Thread/Run)    │
                    │  Python DeepEval sidecar (JSONL)     │
                    └──────────────────┬───────────────────┘
                                       │ score / assert
                    ┌──────────────────▼───────────────────┐
                    │  marspi-eval                         │
                    │  schema · collect · export · metrics │
                    └──────────────────┬───────────────────┘
                                       │ observe (opt-in)
          ┌────────────────────────────┼────────────────────────────┐
          ▼                            ▼                            ▼
   graph.EventHandler           agent.Emitter              DurableCheckpointer
   (lifecycle skeleton)         (llm/tool flesh)           (session enrich)
          │                            │                            │
          └────────────────────────────┴────────────────────────────┘
                         marspi-graph / marspi-core / service worker
```

### Module layout

New sibling module **`marspi-eval`** (Apache-2.0), dependency direction:

```text
marspi-cli / marspi-service ──┐
                              ├──► marspi-eval ──► marspi-graph ──► marspi-core
                              └──► marspi-graph ──► marspi-core
```

`marspi-eval` may import graph + core. Graph/core must **not** import eval (same rule as service).

```text
marspi-eval/
  schema/     # Trace, Span, Golden, Suite, MetricResult (JSON tags stable)
  collect/    # Collector implementing graph.EventHandler + agent subscribe
  enrich/     # Optional: load AgentSession / interrupt value from durable store
  export/     # JSONL writer; OTLP reserved
  metrics/    # Deterministic scorers (tool correctness, status, step caps)
  runner/     # In-process + HTTP drivers (phase 2+)
  deepeval/   # Optional: docs + example Python adapter (not a Go import)

marspi-eval-py/   # optional later; or `marspi-eval/python/`
  adapters/       # JSONL → deepeval.LLMTestCase
  tests/          # deepeval test run suites
```

Service integration is a thin worker hook (opt-in config), not a new control plane.

## Trace data model

### Design principles

1. **Trace ≈ DeepEval end-to-end `LLMTestCase`** — `input`, `actual_output`, tools, retrieval context.
2. **Span ≈ component `LLMTestCase`** — node / agent / llm / tool / retriever / handoff / interrupt.
3. **Events assemble Spans; Spans never replace checkpoints** — durable truth for State/sessions remains checkpointer (ADR 0007 consequence).
4. **Redaction by default for export** — full prompt/completion optional via `CaptureMode`.
5. **Stable JSON schema version** — `schema_version: 1` on every export file.

### Capture modes

| Mode | What is stored on spans | Default |
|------|-------------------------|---------|
| `metadata` | IDs, kinds, names, status, usage, tool name/id, previews truncated | yes (library default) |
| `eval` | + truncated inputs/outputs suitable for scoring (tool args, LLM content caps) | eval runner default |
| `full` | + optional raw messages / provider payloads (debug only; never default in prod) | opt-in |

Caps (configurable): content 8 KiB, tool result preview 2 KiB, max spans per run 10_000.

### Go / JSON sketch

```go
// schema/trace.go (illustrative)

type SchemaVersion int // = 1

type CaptureMode string // metadata | eval | full

type TraceStatus string // completed | failed | paused | cancelled | lease_lost | running

type Trace struct {
    SchemaVersion int         `json:"schema_version"`
    TraceID       string      `json:"trace_id"` // usually = graph RunID
    PublicRunID   string      `json:"public_run_id,omitempty"`
    PublicThreadID string     `json:"public_thread_id,omitempty"`
    GraphThreadID string      `json:"graph_thread_id"`
    GraphID       string      `json:"graph_id,omitempty"`
    TenantID      string      `json:"tenant_id,omitempty"`
    CaptureMode   CaptureMode `json:"capture_mode"`

    Input        string         `json:"input,omitempty"`
    ActualOutput string         `json:"actual_output,omitempty"`
    Status       TraceStatus    `json:"status"`
    ErrorCode    string         `json:"error_code,omitempty"`
    ErrorMessage string         `json:"error_message,omitempty"`

    StartedAt time.Time  `json:"started_at"`
    EndedAt   *time.Time `json:"ended_at,omitempty"`
    Usage     Usage      `json:"usage,omitempty"`

    Spans []Span `json:"spans"`

    // Optional enrichment refs (not embedded blobs by default)
    CheckpointIDs []string `json:"checkpoint_ids,omitempty"`
    AgentSessions []AgentSessionRef `json:"agent_sessions,omitempty"`
}

type SpanKind string

const (
    SpanRun       SpanKind = "run"
    SpanNode      SpanKind = "node"
    SpanAgent     SpanKind = "agent"
    SpanLLM       SpanKind = "llm"
    SpanTool      SpanKind = "tool"
    SpanRetriever SpanKind = "retriever"
    SpanHandoff   SpanKind = "handoff"
    SpanInterrupt SpanKind = "interrupt"
    SpanRoute     SpanKind = "route"
    SpanCustom    SpanKind = "custom"
)

type SpanStatus string // ok | error | denied | interrupted

type Span struct {
    SpanID     string     `json:"span_id"`
    ParentID   string     `json:"parent_id,omitempty"`
    Kind       SpanKind   `json:"kind"`
    Name       string     `json:"name"`
    Status     SpanStatus `json:"status"`
    StartedAt  time.Time  `json:"started_at"`
    EndedAt    *time.Time `json:"ended_at,omitempty"`

    RunID        string `json:"run_id,omitempty"`
    ThreadID     string `json:"thread_id,omitempty"`
    NodeID       string `json:"node_id,omitempty"`
    AgentID      string `json:"agent_id,omitempty"`
    CheckpointID string `json:"checkpoint_id,omitempty"`
    Step         int    `json:"step,omitempty"`
    Iteration    int    `json:"iteration,omitempty"`

    Input  string `json:"input,omitempty"`
    Output string `json:"output,omitempty"`
    Error  string `json:"error,omitempty"`

    ToolsCalled       []ToolCall `json:"tools_called,omitempty"`
    RetrievalContext  []string   `json:"retrieval_context,omitempty"`
    Usage             Usage      `json:"usage,omitempty"`
    Metadata          map[string]string `json:"metadata,omitempty"`
}

type ToolCall struct {
    ID        string         `json:"id,omitempty"`
    Name      string         `json:"name"`
    Arguments map[string]any `json:"arguments,omitempty"`
    OK        *bool          `json:"ok,omitempty"`
    Denied    bool           `json:"denied,omitempty"`
    Preview   string         `json:"preview,omitempty"`
}

type Usage struct {
    PromptTokens     int `json:"prompt_tokens,omitempty"`
    CompletionTokens int `json:"completion_tokens,omitempty"`
    TotalTokens      int `json:"total_tokens,omitempty"`
}

type AgentSessionRef struct {
    AgentID      string `json:"agent_id"`
    CheckpointID string `json:"checkpoint_id"`
    // Messages only when CaptureMode=full or enrich explicitly requested
    MessageCount int             `json:"message_count,omitempty"`
    Messages     json.RawMessage `json:"messages,omitempty"`
}
```

### Span tree (typical Supervisor / CodingLoop)

```text
run (SpanRun)
├─ node:supervisor
│  ├─ agent:supervisor
│  │  ├─ llm (iter 0)
│  │  └─ tool:handoff → SpanHandoff (also emitted as graph custom)
│  └─ route
├─ node:coder
│  ├─ agent:coder
│  │  ├─ llm …
│  │  ├─ tool:read_file …
│  │  └─ tool:attempt_completion …
│  └─ checkpoint
└─ interrupt? (HITL) → SpanInterrupt
```

`span_id` generation: collector-local monotonic / UUIDv7; parent linkage from open-span stack keyed by `(run_id, node_id, agent_id, iteration)`.

## Event → Span mapping

### Graph lifecycle (`graph.Event`)

| EventType | Collector action |
|-----------|------------------|
| `run_start` / `run_resume` | Open root `SpanRun`; set `StartedAt`; resume creates **new** Trace keyed by new RunID (same ThreadID) |
| `node_start` | Open `SpanNode` under run |
| `node_end` | Close node span `ok` |
| `node_error` | Close node span `error` |
| `route` | Emit zero-duration `SpanRoute` (metadata: next node) |
| `checkpoint` | Record `CheckpointID` on open node/run; append to `CheckpointIDs` |
| `checkpoint_error` | Annotate run error |
| `interrupt` | Emit `SpanInterrupt`; Trace status → `paused` |
| `run_end` | Close open spans; set Trace `Status` / `EndedAt` |
| `custom` + `metadata.kind=handoff` | Emit `SpanHandoff` (to/reason/task from metadata) |
| lease_* | Metadata on run span only (not scored) |

Graph events remain **metadata-only** on the bus. The collector **does not** require State in events; it may later call enrichers.

### Agent events (`agent.Event`)

Requires the graph factory / orchestrator to share one `*agent.Emitter` with the collector (already supported by CodingLoop/Supervisor `Events` field and agentspec `Spec.Events`).

| EventType | Collector action |
|-----------|------------------|
| `run_start` (agent) | Open `SpanAgent` under current node |
| `run_end` (agent) | Close agent span |
| `llm_start` | Open `SpanLLM` |
| `llm_end` | Close LLM span; attach `Usage`, `FinishReason`; in `eval` mode store truncated `Content` as Output |
| `message_delta` | Ignore for traces (too noisy); final text comes from `llm_end` / `message_end` |
| `message_end` | Optional Output fill if llm_end content empty |
| `tool_start` | Open `SpanTool`; record name/id/args (eval mode) |
| `tool_end` | Close tool span; OK/Denied/Preview; append to parent `ToolsCalled` |
| `tool_update` | Ignore or merge preview |
| `error` / `warn` | Annotate open span / Metadata |

**Retriever:** when `ToolName == "search_knowledge"` (or configured names), also emit/annotate `SpanRetriever` and append result lines into `RetrievalContext` on the agent/run span (eval mode).

### Correlation rules

Align with ADR 0007:

- Graph fills `RunID`, `ThreadID`, `NodeID`, `AgentID`, `CheckpointID`
- agentspec already copies `RunID`/`NodeID`/`AgentID` onto core events
- Collector must tolerate missing ThreadID on agent events (inherit from open run span)

### End-to-end fields

| Trace field | Source |
|-------------|--------|
| `Input` | Invoke JSON `input` / State keys configured per graph (`goal`, `task`, …) via `CollectorOption.InputKeys` |
| `ActualOutput` | State keys (`last_output`, `status`, …) via `OutputKeys` **after** run, or final agent `llm_end` content if keys empty |
| `Status` | Graph `run_end` + service Finish status when present |
| `Usage` | Sum of LLM span usages |

For HTTP runner: pass invoke body as Input; after completion GET checkpoint / run result to fill ActualOutput.

## Collector API

```go
type Collector struct { /* unexported */ }

func NewCollector(opts ...Option) *Collector

func (c *Collector) GraphHandler() graph.EventHandler
func (c *Collector) SubscribeAgent(em *agent.Emitter) (unsubscribe func())
func (c *Collector) ObserveRun(ctx context.Context, meta RunMeta) // bind public IDs / graph_id

func (c *Collector) Snapshot(runID string) (Trace, bool)
func (c *Collector) Finalize(runID string) (Trace, error) // close dangling spans
func (c *Collector) Reset(runID string)

// Enrichment (optional, post-run)
func (c *Collector) EnrichFromDurable(ctx context.Context, d graph.DurableCheckpointer, runID string) error
```

**Performance contract (same as ADR 0007):** handlers are synchronous and must be fast. Collector:

- Allocates on hot path only within caps
- Never blocks on disk/network inside `GraphHandler` / agent subscribe
- Buffers in memory per RunID; `Finalize` returns Trace; export is caller's job (worker Finish hook, test teardown)

**Panic:** graph already recovers handler panics. Agent subscribe should recover locally too.

## Export

### JSONL (P2 required)

One file or stream of records:

```text
{"type":"trace", ...Trace...}
{"type":"golden", ...}      # optional companion in suite files
{"type":"metric_result", ...}
```

Eval suites typically write:

```text
artifacts/evals/<suite>/<timestamp>/<run_id>.json
```

or NDJSON for batch.

### OTLP (reserved)

Map Span → OTel span attributes (`marspi.*`). Defer exporter implementation; schema fields should not block OTLP later.

### DeepEval adapter (Python, sidecar)

```text
Trace.input / actual_output / tools_called / retrieval_context
  → deepeval.test_case.LLMTestCase

Conversational turns from AgentSession messages (role/content)
  → ConversationalTestCase

Per-span metrics (optional)
  → update_current_span equivalents via component cases
```

Recommended first metrics:

| Metric | Source |
|--------|--------|
| TaskCompletionMetric | whole Trace |
| ToolCorrectnessMetric | `tools_called` vs golden expected tools |
| GEval (custom criteria) | Trace input/output |
| FaithfulnessMetric | when RetrievalContext present |

## Golden / Suite / MetricResult

```go
type Golden struct {
    ID               string         `json:"id"`
    Input            string         `json:"input"`
    ExpectedOutput   string         `json:"expected_output,omitempty"`
    ExpectedTools    []string       `json:"expected_tools,omitempty"`
    ExpectedHandoffs []string       `json:"expected_handoffs,omitempty"`
    Metadata         map[string]string `json:"metadata,omitempty"`
}

type Suite struct {
    Name      string   `json:"name"`
    GraphID   string   `json:"graph_id"`
    Goldens   []Golden `json:"goldens"`
    Metrics   []string `json:"metrics"` // e.g. "tool_correctness", "status_success", "geval:correctness"
    Threshold float64  `json:"threshold,omitempty"` // default 0.5 for judge metrics
}

type MetricResult struct {
    Name      string  `json:"name"`
    Score     float64 `json:"score"`
    Threshold float64 `json:"threshold"`
    Passed    bool    `json:"passed"`
    Reason    string  `json:"reason,omitempty"`
    TraceID   string  `json:"trace_id"`
    SpanID    string  `json:"span_id,omitempty"`
    GoldenID  string  `json:"golden_id,omitempty"`
}
```

### Go deterministic metrics (ship first)

| Name | Pass condition |
|------|----------------|
| `status_success` | Trace/State status in configured success set |
| `tool_correctness` | Expected tool names ⊆ observed (order-insensitive; args optional exact match) |
| `handoff_target` | Expected worker IDs appeared in SpanHandoff |
| `max_steps` | Graph steps ≤ N |
| `no_error` | Trace status not failed |

LLM-as-judge stays in DeepEval sidecar for P2 unless a thin Go `Judge` interface wrapping `llm.Provider` is needed for pure-Go CI.

## Runner modes

### A. In-process (library)

```text
collector := eval.NewCollector(eval.WithCaptureMode(eval.CaptureEval))
orch Events + EventHandler wired to collector
result := orchestrator.Run… / graph.Invoke
trace := collector.Finalize(runID)
export + metrics.Score(trace, golden)
```

Primary for unit/regression tests beside existing `orchestrator/*_test.go`.

### B. HTTP (service)

```text
POST /v1/graphs/{id}/threads
poll run / SSE until terminal
Finalize from worker-side collector OR reconstruct from exported artifact
```

Requires service to enable collector on worker (config flag `eval.capture=true`, write path / object store later).

### C. Python DeepEval

```text
go test/runner → write JSONL traces
deepeval test run tests/test_marspi_suite.py
```

CI: PR runs A + deterministic metrics; nightly runs C with real judge model.

## Service wiring (opt-in)

```text
worker.execute:
  col := eval.NewCollector(...)
  opts += WithEventHandler(compose(col.GraphHandler, existingSSE))
  // factories must expose/share agent.Emitter — registry contract extension
  defer: trace := col.Finalize(runID); export if enabled
```

**Registry gap:** today's builtin `echo` has no agent Emitter. Eval-ready graphs must document:

```go
type EvalAwareFactory interface {
    registry.GraphFactory
    AgentEvents() *agent.Emitter // shared bus for all agents in the compiled graph
}
```

Or pass Emitter into `factory.New` via context value — prefer explicit interface on factories used in eval suites.

HITL: on `FinishWithApproval`, Trace status `paused`; `DecisionNote` can be attached post-hoc as `MetricResult` name `human_approval` or written into golden feedback store (phase 3).

## Privacy & safety

- Default capture = `metadata` in production workers
- Eval mode truncates; strip env-like secrets via simple redaction hooks (`API_KEY=`, `Bearer `)
- `full` mode forbidden unless `MARSPI_EVAL_FULL=1` or explicit config
- Exported artifacts gitignored by default in examples
- Do not put prompt bodies into SSE

## Phased delivery

| Phase | Deliverable | Exit criteria |
|-------|-------------|---------------|
| **0** | This design + ADR 0010 | Accepted |
| **1** | `marspi-eval` module: schema + Collector + JSON export + unit tests with mock graph/agent events | Golden fixture round-trip |
| **2** | Wire CodingLoop + Supervisor tests with collector; deterministic metrics | 1 suite green in `go test` |
| **3** | HTTP runner against service + opt-in worker export | e2e with `echo` or eval graph |
| **4** | Python DeepEval adapter + nightly workflow | TaskCompletion + GEval on fixture traces |
| **5** | Approvals → feedback/golden promotion | Documented manual path |

## Consistency with existing ADRs

| ADR | Relationship |
|-----|----------------|
| 0001 State vs agentctx | Trace may embed session only via enrich; State keys for I/O are graph-owned |
| 0003 Supervisor handoff | `custom` handoff → `SpanHandoff` |
| 0005 AgentStore | Enrichment source for PersistSession agents |
| 0006 Checkpoint history | Offline reconstruction / checkpoint_id list |
| 0007 Graph events | Collector is a consumer; does not expand live event payloads |
| 0008 Lease | lease events annotated, not scored |
| 0009 ToolExecution | Tool spans align with tool_call_id for future idempotent journals |

## Open questions

1. **Trace identity across Resume:** one Trace per RunID (recommended) vs stitch Thread-level super-trace. Decision: **per RunID**; suite tooling may group by ThreadID.
2. **Where does `marspi-eval` live?** Sibling repo vs monorepo folder. Decision: **sibling module** like service/graph (go.mod replace in local dev).
3. **Should agent.Event gain ThreadID?** Nice-to-have; collector can inherit. Defer core change unless needed.
4. **Prompt capture:** wrap `llm.Provider` vs rely on events. P2 uses events; Provider wrapper is P2.5 if Faithfulness needs raw prompts reliably.

## References

- DeepEval: https://deepeval.com/
- Tracing model: https://deepeval.com/docs/evaluation-llm-tracing
- Metrics: https://deepeval.com/docs/metrics-introduction
- ADR 0007, design `p1-durable-runtime.md`
