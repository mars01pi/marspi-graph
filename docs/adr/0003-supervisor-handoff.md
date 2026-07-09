# ADR 0003: Supervisor vs Flow, and Handoff

## Status

Accepted

## Context

`marspi-graph` already has static presets (`Pipeline`, `CodingLoop`) and
engine primitives (conditional edges, reducers, resume/interrupt). We need
a clear vocabulary for the next orchestration pattern: **Supervisor**.

Contributors often conflate three ideas:

1. Conditional edges (routing *rules*)
2. Supervisor (routing *agent*)
3. Handoff (the *payload* of a transfer)

## Decision

### Flow vs Supervisor

| | **Flow** (Pipeline / CodingLoop) | **Supervisor** |
|---|----------------------------------|----------------|
| Who chooses next | Graph author (fixed edges or fixed predicates) | A Supervisor agent at runtime |
| Topology | Linear or fixed cycle | Star: supervisor ↔ workers → END |
| Where “planning” lives | Compile-time topology | Each supervisor turn (LLM) |

- **Flow ≈ static orchestration.** Conditional edges alone are still Flow
  when the predicate is a rule (`status == success`), not an LLM manager.
- **Supervisor ≈ dynamic orchestration.** The supervisor node emits
  `next` (worker id or `END`); the graph’s conditional edge only *reads*
  that field.

```text
Flow:        START → A → B → C → END
Supervisor:  START → Supervisor ⇄ {A,B,C} → END
```

### Handoff

**Handoff is not a third topology.** It is the transfer record / work order:

```json
{ "from": "supervisor", "to": "coder", "reason": "...", "task": "..." }
```

- Stored in graph State (`handoff` + append-only `messages`).
- Workers read `handoff.task` (fallback: `goal`), write `last_output`,
  and append a short summary to `messages`.
- v1 uses **structured JSON from the supervisor LLM**, not
  `transfer_to_*` tools (tool-based handoff is a later enhancement).

### State keys (v1)

| Key | Role |
|-----|------|
| `goal` | User objective |
| `messages` | Append-only handoff/result summaries (`AppendSlice`) |
| `handoff` | Latest `{from,to,reason,task}` |
| `next` | Conditional edge target (worker id or `__end__`) |
| `last_agent` / `last_output` | Last worker identity and result |
| `step` / `max_steps` | Loop guard |

## Consequences

- CodingLoop stays as the static multi-role demo; Supervisor is additive.
- CLI exposes `/supervise` without replacing `/loop` or `/loopg`.
- Future tool-based handoff can write the same State keys without changing
  worker node contracts.

## Future work (deferred)

v1 supervisor routing uses free-text JSON + parse/retry. That is intentionally
MVP-only. Prefer later: handoff via **tool calling** (or provider structured
output), plus enum validation of `next`. Tracked in [`docs/TODO.md`](../TODO.md)
as **P2 — Supervisor reliable routing**; not scheduled.
