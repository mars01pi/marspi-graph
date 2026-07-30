# ADR 0012: Fault tolerance (Retry + on_error)

## Status

Accepted

## Context

Hard node errors currently abort the whole run (`node_error` → `run_end(error)`).
Soft FAIL verdicts already rework via `pass_fail_rework`. Contributors need
transient LLM/network recovery and optional compensation branches without
polluting verdict routing. LangGraph offers `RetryPolicy` then `error_handler`
(ADR 0002 parity).

## Decision

1. **Engine-first:** `graph.RetryPolicy` and `Builder.SetOnError(from, to)` live
   in the StateGraph runtime. Workflow Spec maps `retry` / `on_error` onto them.
2. **Compose order:** retry (same super-step, fresh artifact collector per
   attempt) → if exhausted and `on_error` set, apply fixed keys
   (`error_node`, `last_error`, `status=failed`), route, checkpoint, continue;
   else bubble.
3. **Soft complete:** divert path that reaches END returns `(state, nil)`.
4. **Never retry** Interrupt, lease loss, cancel, caller deadline, and
   `NonRetryable()` markers.
5. **Session safety:** `agentspec.RunOnce` rolls back in-memory messages on
   error so reused Instances stay clean across retries.
6. **Out of scope:** TimeoutPolicy, per-attempt checkpoints, HTTP status parsing.

## Consequences

- CodingLoop / Supervisor / Workflow can opt into retry without changing soft
  FAIL semantics.
- Event consumers see per-attempt `node_start` / `node_error` with
  `attempt` / `will_retry` metadata; divert emits `route` with `reason=on_error`.
- See design: [`docs/design/p16-fault-tolerance.md`](../design/p16-fault-tolerance.md).
