# P1.5 Execution Lease

## Goal and guarantee

Reduce concurrent execution of the same `thread_id` across healthy,
cooperating processes, and guarantee that a runner which has lost its lease
cannot commit a checkpoint.

The guarantee has two parts:

1. A background heartbeat keeps the lease alive while a node is running.
2. A monotonic lease epoch is checked in the same MySQL transaction as parent
   CAS and checkpoint append.

This is not an exactly-once guarantee for arbitrary external side effects.
A process paused beyond TTL or a node ignoring context may continue calling an
external system. Exactly-once effects additionally require idempotency keys or
downstream support for the lease epoch.

## Problem

P1 `CommitStep` parent CAS (and MySQL `SELECT ... FOR UPDATE`) only serializes
the short commit transaction. Two instances can still:

1. Load the same latest checkpoint.
2. Run the same LLM/tool node.
3. Produce duplicate side effects.
4. Race at `CommitStep`.

CAS protects history consistency, but it acts too late to protect execution.
Conversely, a TTL lease alone is insufficient: after expiry, an old runner can
finish before the new owner commits and still pass parent CAS.

## Layering

| Layer | Scope | Prevents |
|-------|-------|----------|
| `ExecutionLease` | Whole Invoke/Resume attempt | Healthy runners starting the same thread concurrently |
| Background heartbeat | Including a blocking node body | Lease expiry during long LLM/tool execution |
| Lease epoch fence | Inside checkpoint transaction | A stale owner committing after lease loss |
| Parent checkpoint CAS | Inside checkpoint transaction | Append from an outdated graph state |
| Tool idempotency/fencing | External system | Duplicate irreversible side effects |

Lease does not replace parent CAS. Fenced commit and parent CAS are both
validated while holding the same `graph_threads` row lock.

## Lease identity

`runID` is an owner identity, not a fencing token. A true fencing token must be
monotonically increasing.

```go
type LeaseGrant struct {
    ThreadID  string
    RunID     string
    Epoch     int64
    ExpiresAt time.Time
}
```

- `RunID`: random ID generated once per Invoke/Resume attempt.
- `Epoch`: monotonically incremented by the backend whenever a new owner takes
  over an available or expired lease.
- `ExpiresAt`: informational server-authoritative expiry.

The epoch is never reset on Release.

## Backend-agnostic interface

The contract lives in `graph`; implementations live in `checkpoint` or a
future Redis package.

```go
type ExecutionLease interface {
    Acquire(
        ctx context.Context,
        threadID, runID string,
        ttl time.Duration,
    ) (LeaseGrant, error)

    Renew(
        ctx context.Context,
        grant LeaseGrant,
        ttl time.Duration,
    ) (LeaseGrant, error)

    // Release is idempotent. A free lease or mismatched stale grant is nil.
    Release(ctx context.Context, grant LeaseGrant) error
}

var (
    ErrLeaseHeld = errors.New("graph: thread execution lease held")
    ErrLeaseLost = errors.New("graph: execution lease lost or expired")
)
```

### Semantics

| Method | Required behavior |
|--------|-------------------|
| `Acquire` | Free/expired lease: increment epoch and return a new grant. Same active `(runID, epoch)`: idempotent success. Other active owner: `ErrLeaseHeld`. |
| `Renew` | Extend only when runID and epoch match and lease has not expired. Otherwise `ErrLeaseLost`; an expired grant cannot be revived. |
| `Release` | Clear owner/expiry only when runID and epoch match. Mismatch/free is an idempotent success and must not clear a newer owner. |

`ErrLeaseNotHeld` is intentionally omitted from the core contract because
Release is defer-safe and idempotent. Admin APIs may expose stricter diagnostics.

## Fenced persistence

When a durable store and lease are used together, checkpoint persistence must
support an atomic lease fence:

```go
type LeaseFencedCommitter interface {
    DurableCheckpointer
    CommitStepFenced(
        ctx context.Context,
        snap Snapshot,
        artifacts []StepArtifact,
        grant LeaseGrant,
    ) error
}
```

`CommitStepFenced` locks `graph_threads` and rejects with `ErrLeaseLost` when:

- `lease_run_id != grant.RunID`
- `lease_epoch != grant.Epoch`
- `lease_expires_at <= database current time`

It then performs the existing parent checkpoint CAS and append in the same
transaction. “Keep CAS unchanged” means retain its semantics, not skip the
lease check.

Production configuration rules:

- Lease + durable checkpointer requires `LeaseFencedCommitter`; Compile fails
  closed if the pair cannot provide atomic fencing.
- `lease == nil` keeps current P1 behavior.
- Legacy latest-only checkpointers are not production multi-instance lease
  backends.

## Engine integration

### Acquisition order

Acquire immediately after `prepareRun`, once `threadID` and `runID` are known:

- Invoke: acquire before emitting `run_start` and before executing entry.
- Resume: acquire **before `loadLatest`** to avoid a load/acquire TOCTOU window.

The grant is added to node context so future tools can use `LeaseEpoch(ctx)` as
a downstream fencing/idempotency input.

### Background heartbeat

Renew must run independently of the super-step loop because `fn(nodeCtx, state)`
is a blocking call and can exceed TTL.

```text
Acquire
  -> derive runCtx with context.WithCancelCause
  -> start heartbeat ticker
  -> run graph (heartbeat continues inside long node)
  -> stop and join heartbeat
  -> emit run_end
  -> bounded Release
```

Heartbeat policy:

1. Tick every `TTL/3`.
2. Renew using the current grant.
3. Any `ErrLeaseLost` cancels `runCtx` with that cause immediately.
4. Other renewal errors fail closed: cancel the run rather than risk overlap.
5. After node return, check cancellation cause before applying update,
   registering artifacts, routing, or persisting.
6. Never commit after heartbeat loss.

Nodes and tools must observe context cancellation. If a node ignores context,
the engine discards its returned update/artifacts, but cannot undo external
effects already performed.

### Persistence

Before every normal or interrupt checkpoint:

1. Check heartbeat/lost-lease cause.
2. Call `CommitStepFenced(..., grant)`.
3. The store atomically validates lease owner + epoch + expiry, then parent CAS.

A separate “Renew immediately before persist” may reduce expiry risk but is not
a substitute for transactional fencing.

### Exit and Interrupt

Interrupt behavior remains:

1. Persist the interrupt checkpoint with `CommitStepFenced`.
2. Return from the run loop.
3. Stop and join heartbeat.
4. Release lease.
5. Emit/return paused result.

Release happens only after the interrupt snapshot is durable. This allows a
different process to immediately Acquire and Resume from the correct cursor.

For success/error/cancel, stop heartbeat first and release via a bounded cleanup
context:

```go
releaseBase := context.WithoutCancel(ctx)
releaseCtx, cancel := context.WithTimeout(releaseBase, 3*time.Second)
defer cancel()
_ = lease.Release(releaseCtx, grant)
```

Do not use unbounded `context.WithoutCancel`, which can hang shutdown.

## Defaults and options

| Setting | Default | Configuration |
|---------|---------|---------------|
| TTL | 30s | Compile default; per-run override |
| Heartbeat | TTL/3 | Derived, must be positive and less than TTL/2 |
| Release timeout | 3s | Compile default |

Keep durations out of `ExecutionLease`; expose them through `CompileOption` /
`RunOption` such as `WithExecutionLease` and `WithLeaseTTL`.

## MySQL schema (migration v2)

```sql
ALTER TABLE graph_threads
  ADD COLUMN lease_run_id CHAR(32) NULL
    COMMENT '当前租约 owner runID',
  ADD COLUMN lease_epoch BIGINT NOT NULL DEFAULT 0
    COMMENT '单调递增 fencing epoch，释放时不重置',
  ADD COLUMN lease_expires_at DATETIME(6) NULL
    COMMENT '租约过期时间（MySQL UTC）';
```

The migration is tracked as schema version 2. Existing rows start with epoch 0
and no active owner. DDL migration must be idempotent according to the project’s
schema migration mechanism.

### MySQL clock authority

All expiry assignment and comparison use MySQL time:

```sql
UTC_TIMESTAMP(6)
UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND
```

Do not calculate expiry from application-host wall clocks.

### Acquire transaction

Thread upsert, row lock, owner decision, epoch increment and update occur in one
transaction:

```text
BEGIN
  INSERT graph_threads ... ON DUPLICATE KEY UPDATE thread_id=thread_id
  SELECT lease_run_id, lease_epoch, lease_expires_at
    FROM graph_threads WHERE thread_id=? FOR UPDATE

  if same active runID:
      return existing epoch and extend expiry
  else if free or expired:
      epoch = lease_epoch + 1
      set owner=runID, epoch, expiry=DB_NOW+TTL
  else:
      ErrLeaseHeld
COMMIT
```

### Renew

```sql
UPDATE graph_threads
SET lease_expires_at = UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND
WHERE thread_id = ?
  AND lease_run_id = ?
  AND lease_epoch = ?
  AND lease_expires_at > UTC_TIMESTAMP(6);
```

`RowsAffected == 0` means `ErrLeaseLost`.

### Release

```sql
UPDATE graph_threads
SET lease_run_id = NULL, lease_expires_at = NULL
WHERE thread_id = ?
  AND lease_run_id = ?
  AND lease_epoch = ?;
```

`RowsAffected == 0` is idempotent success. Never reset `lease_epoch`.

### Fenced commit

Inside the existing `CommitStep` transaction, after locking `graph_threads` and
before inserting checkpoint/session rows:

```text
validate owner runID
validate epoch
validate lease_expires_at > UTC_TIMESTAMP(6)
validate latest_checkpoint_id == snap.ParentCheckpointID
append checkpoint + artifacts
advance latest pointer
```

## Memory semantic twin

Use a separate mutex-protected lease implementation:

```text
threadID -> {runID, epoch, expiresAt}
```

It mirrors MySQL Acquire/Renew/Release and uses an injectable clock for
deterministic TTL tests. Do not fold execution lease semantics into the
`DurableMemory` commit mutex.

## Redis extension

Redis must preserve the same monotonic epoch semantics. `SET NX PX` alone is
insufficient. Use Lua to atomically allocate an epoch (`INCR`), compare owner /
epoch, and set TTL; Renew/Release also use compare-and-act scripts.

## Observability

Emit metadata-only graph events for:

- `lease_acquired`: runID, epoch, TTL
- `lease_renew_failed`: runID, epoch, error class
- `lease_released`: runID, epoch, terminal status

Do not emit secret connection or full state data.

## Explicit limitations

- A TTL lease cannot guarantee exactly-once arbitrary external effects.
- Sequential reuse of an existing thread with a new Invoke is a separate
  idempotency/thread-lifecycle concern.
- Force release/steal is an operations API, not part of the core interface.
- Redis ExecutionLease backend remains later work.

## Phase 4: Tool idempotency / fencing

Phase 4 closes the gap between checkpoint fencing and external tool effects.

### Delivered

1. **`graph.ToolExecution`** — builds a stable `IdempotencyKey` and `FenceToken`
   (lease epoch) from graph context. RunID and epoch are **excluded** from the
   key so retry/takeover of the same logical operation reuses downstream dedup.
2. **`marspi-core` ContextualTool bridge** — `ExecuteQuietCtx` + optional
   `RunCtx(ctx, meta, args)`. Legacy `Run(args)` tools still work.
3. **agentspec wiring** — `Runner.ToolMeta` fills `ExecutionMeta` from graph
   context; default operation ID is the LLM `tool_call_id`.
4. **bash / MCP** honor parent context cancellation instead of
   `context.Background()`.

### Tool author categories

| Category | Guidance |
|----------|----------|
| Read-only | Rely on ctx cancellation + lease; no key required |
| Idempotent write | Pass `IdempotencyKey` to downstream (HTTP header, unique index) |
| Fence-sensitive | Also pass `FenceToken` / lease epoch; refuse stale epochs |

### Example (custom NodeFunc or ContextualTool)

```go
exec, err := graph.ToolExecutionFrom(ctx, "charge_card", orderID)
if err != nil { return nil, err }
req.Header.Set("Idempotency-Key", exec.IdempotencyKey())
req.Header.Set("X-Marspi-Lease-Epoch", strconv.FormatInt(exec.FenceToken(), 10))
```

### Still out of scope

- Durable tool-effect journal / result replay
- Automatic exactly-once for tools that ignore meta
- Redis lease backend

See ADR 0009.

## Implementation phases

| Phase | Deliverable |
|-------|-------------|
| 0 | Corrected design + ADR 0008 |
| 1 | `LeaseGrant`, `ExecutionLease`, heartbeat wrapper, Memory lease |
| 2 | MySQL migration v2, lease ops, `CommitStepFenced` |
| 3 | Supervisor/CodingLoop wiring and lease events |
| 4 | ToolExecution helpers + core ContextualTool bridge (Redis optional later) |

## Success criteria for implementation

- Two concurrent starts on one thread: one runs, one gets `ErrLeaseHeld`.
- A node longer than TTL remains exclusively held through heartbeat renewals.
- After expiry and takeover, the old grant cannot Renew, Release the new owner,
  or CommitStep.
- Lease fence and parent CAS are verified in one MySQL transaction.
- Interrupt checkpoint is committed before Release; another process can Resume.
- Heartbeat loss cancels node context and discards node update/artifacts.
- Release remains bounded after caller context cancellation.
- `lease == nil` preserves existing behavior.
- Default tests do not require MySQL; MySQL concurrency tests are opt-in.
- ToolExecution keys are stable across runID/epoch; Contextual tools receive
  cancellation and ExecutionMeta via agentspec.
