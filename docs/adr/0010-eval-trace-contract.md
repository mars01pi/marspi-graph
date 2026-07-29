# ADR 0010: Eval trace contract

## Status

Proposed

## Context

marspi can orchestrate multi-agent graphs with correlated lifecycle and
agent events (ADR 0007), durable sessions (ADR 0005/0006), and hosted
Thread/Run + HITL (marspi-service). Product-quality evaluation still
lacks a stable artifact: datasets, scorers, and tools like DeepEval
expect **traces** (end-to-end + component spans), not live SSE metadata.

DeepEval is Python-native; marspi is Go-native. Embedding DeepEval in
the runtime or rewriting its metric library are both poor fits.

## Decision

1. **Own traces in marspi; consume externally.** Introduce sibling module
   `marspi-eval` that defines a versioned Trace/Span/Golden schema and a
   Collector. DeepEval (or any judge) is an optional sidecar that reads
   exported JSON/JSONL — never a runtime dependency of graph/core.

2. **Collector is opt-in and non-intrusive.** It adapts existing buses:
   - `graph.WithEventHandler` → run/node/route/checkpoint/interrupt skeleton
   - `agent.Emitter` Subscribe → llm/tool flesh
   - optional post-run `DurableCheckpointer` enrichment for AgentSession
   Live graph events remain metadata-only (ADR 0007). Capture of bodies
   happens inside the collector buffer under an explicit `CaptureMode`
   (`metadata` | `eval` | `full`), not by expanding the graph event bus.

3. **Trace identity = graph `RunID`.** Resume produces a new Trace sharing
   `ThreadID`. Suites may aggregate by thread; exporters do not merge runs
   by default.

4. **Span kinds are eval-oriented, not a second event enum.** Map from
   graph/agent events to `run|node|agent|llm|tool|retriever|handoff|
   interrupt|route|custom`. Scoring targets Trace (e2e) or Span
   (component), mirroring DeepEval's Trace/Span → LLMTestCase model.

5. **Dependency rule:** `marspi-eval → graph → core`. Graph and core must
   not import eval. Service/CLI may depend on eval for worker export and
   runners.

6. **Hybrid metrics:** deterministic pass/fail metrics ship in Go first;
   LLM-as-judge (G-Eval, Task Completion, Faithfulness, …) preferred via
   DeepEval Python adapter in CI/nightly. A Go `llm.Provider` judge is
   optional later, not required for P2.

7. **Production default is `metadata` capture.** Eval runners use `eval`.
   `full` is debug-only and gated. Export must not dump prompts onto SSE.

## Consequences

- Design doc: `docs/design/p2-eval-runtime.md`.
- Phase 1 implements schema + Collector + JSON export + tests without
  changing graph event payloads.
- Eval-ready graph factories must expose a shared `agent.Emitter` (or
  equivalent) so the collector can subscribe; `echo`-style graphs remain
  lifecycle-only traces.
- Future OTLP / Confident AI upload plugs into the same schema without
  new orchestration APIs.
- Open follow-ups: Provider wrapper for raw prompt capture; approvals →
  golden promotion; durable trace store (beyond local export).
