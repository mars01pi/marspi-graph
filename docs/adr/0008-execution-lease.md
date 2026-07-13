# ADR 0008: Execution lease with monotonic fencing

## Status

Accepted

## Context

P1 Durable Runtime added append-only checkpoints and parent CAS at
`CommitStep` (ADR 0006). CAS prevents stale writers appending from an outdated
graph state, but it does not prevent two processes from executing the same
thread before either commits.

A TTL lease based only on a random `runID` is also insufficient:

- a blocking LLM/tool node can outlive TTL unless renewal runs concurrently;
- after expiry and takeover, the old runner can still pass parent CAS if the
  new owner has not committed;
- a random owner ID establishes identity but not temporal ordering.

Cross-process HITL (Interrupt in process A, Resume in process B) must remain a
first-class flow.

## Decision

1. Introduce backend-agnostic `graph.ExecutionLease` with `Acquire`, `Renew`
   and idempotent `Release`.
2. `Acquire` returns a `LeaseGrant` containing:
   - `RunID` as owner identity;
   - a backend-allocated, monotonically increasing `Epoch` as the fencing
     token;
   - `ThreadID` and informational `ExpiresAt`.
3. Hold the lease for the whole Invoke/Resume attempt. Acquire before any node
   work and, for Resume, before `loadLatest`.
4. Run a background heartbeat for the entire attempt, including inside a
   blocking node call. Renewal failure cancels the run context with
   `ErrLeaseLost`; returned node updates/artifacts are discarded.
5. When a lease and durable checkpointer are used together, require atomic
   fenced persistence (`CommitStepFenced`). In the same `graph_threads` row
   lock and transaction, validate owner + epoch + unexpired lease, then perform
   the existing parent CAS and checkpoint append.
6. Parent CAS remains mandatory and unchanged in meaning. Lease fencing does
   not replace it.
7. On Interrupt, first persist the interrupt checkpoint through fenced commit,
   then stop heartbeat and release the lease. Another process may immediately
   Acquire and Resume.
8. Release uses runID + epoch, is idempotent, and cannot clear a newer owner.
   Cleanup uses a bounded context independent of caller cancellation.
9. Implementations live outside `graph`: MySQL/Memory in `checkpoint`, future
   Redis behind the same contract. `nil` lease preserves P1 behavior.
10. MySQL is the clock authority for expiry and adds
    `lease_run_id`, persistent `lease_epoch`, and `lease_expires_at` to
    `graph_threads`.

See [`docs/design/p15-execution-lease.md`](../design/p15-execution-lease.md).

## Guarantees and limitations

The lease prevents concurrent execution among healthy, cooperating runners and
guarantees that a stale grant cannot commit graph state.

It does **not** guarantee exactly-once external side effects under process
pauses, network partitions, or nodes that ignore context. External systems need
idempotency keys or support for the lease epoch to provide that stronger
guarantee.

## Consequences

- Long nodes require a heartbeat goroutine, not inter-step renewal.
- MySQL CommitStep gains an atomic lease fence in addition to parent CAS.
- The engine validates that a production lease + durable-store pair supports
  fenced commit; unsupported combinations fail closed.
- HITL remains cross-process because paused runs release only after the
  interrupt checkpoint is durable.
- Redis requires an atomic epoch allocator/script; `SET NX PX` alone is not
  equivalent.
- See `checkpoint.MemoryLease` / `checkpoint.Fenced` and MySQL lease columns (schema v2).
