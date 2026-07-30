# P1.6 Fault Tolerance (Retry + on_error)

## Goal

Make hard node failures recoverable without conflating them with soft PASS/FAIL
routing (`pass_fail_rework`). Align with LangGraph’s compose order:

**RetryPolicy → on_error divert → bubble**.

## Soft vs hard failure

| Kind | Signal | Handling |
|------|--------|----------|
| Soft | Verdict text contains FAIL | Conditional routes (`pass_fail` / `pass_fail_rework`) |
| Hard | Node returns `error` (or panic→error) | Retry, then `on_error`, else bubble |

Never convert hard errors into verdict FAIL.

## Compose order

```text
node attempt
  → success → Apply update → normal route → checkpoint
  → Interrupt → interrupt checkpoint (never retry)
  → error + retryable + attempts left → backoff → retry (same super-step)
  → error exhausted + on_error set → write fixed keys → route to on_error → checkpoint
  → error exhausted + no on_error → bubble (run_end error)
```

Retries share one logical leave: no success checkpoint per failed attempt.
`MaxSteps` increments only when leaving a node (success or on_error divert).

## Fixed State keys (on_error divert)

| Key | Value |
|-----|-------|
| `error_node` | Failed node id |
| `last_error` | Error string |
| `status` | `"failed"` |

## Soft complete

When `on_error` is configured and the graph later reaches END without a bubbled
error, `Invoke` / `workflow.Run` returns `(state, nil)`. Callers read
`status=failed` (and `last_error`) from State.

Without `on_error`, behavior is unchanged: return `error`.

## Never retry

- `Interrupt` / `ErrInterrupted`
- `ErrLeaseLost`
- `context.Canceled`
- Caller `context.DeadlineExceeded`
- Types implementing `NonRetryable() bool` returning true

`DefaultRetryOn` retries everything else (transient LLM/network). No HTTP
status parsing in v1.

## Agent session rule

Failed attempts must not leave unpersisted ReAct messages on a reused
`agentspec.Instance`. On `RunOnce` error, restore messages to the pre-attempt
snapshot. `PersistSession` artifacts are registered only after success;
failed attempts discard the step artifact collector.

## Spec surface

```yaml
nodes:
  - id: develop
    component: agent
    retry:
      max_attempts: 3
      initial_interval: 500ms
    on_error: error_sink
```

`on_error` is a side edge: still at most one normal outgoing edge per node.

## Non-goals (this round)

- `TimeoutPolicy`
- Per-attempt Durable checkpoints
- HTTP 5xx classification
- Changing builtin `devflow` defaults (opt-in only)
