# P1 Durable Runtime (MySQL)

## Goal

Ship a production-oriented durable runtime for serial StateGraph execution:

1. **MySQL checkpoint history** with atomic step commit and parent CAS
2. **Checkpoint-scoped AgentStore** for agentspec session recovery
3. **Graph lifecycle events** with correlation IDs

Memory provides a semantic twin for unit tests. SQLite remains a legacy latest-only backend and is **not** extended for P1 durable capabilities.

## Recovery boundary

Durability is defined at the **graph super-step** boundary:

- After a successful node (or interrupt checkpoint), graph `Snapshot` and any registered step artifacts are committed together.
- Mid-`RunOnce` / mid-ReAct iteration crashes are **not** recoverable as partial agent turns.
- Resume re-enters from the latest successful checkpoint; agent sessions are restored only when `PersistSession` was used and a DurableCheckpointer is attached.

## Consistency model

| Concern | Rule |
|---------|------|
| Logical separation | Snapshot does not embed `agentctx` (ADR 0001) |
| Physical binding | Snapshot + agent session refs share one `checkpoint_id` |
| Atomicity | One MySQL transaction via `CommitStep` |
| Concurrency | Parent checkpoint CAS on `graph_threads.latest_checkpoint_id` |
| Idempotency | Caller-supplied `checkpoint_id` is the primary key / retry key |
| Time-travel | History is queryable; Resume-from-ID is **out of P1** |

CAS prevents stale writers from silently overwriting a thread. It is **not** a long-running execution lease (P1.5).

## Interfaces

```go
type DurableCheckpointer interface {
    CommitStep(ctx context.Context, snap Snapshot, artifacts []StepArtifact) error
    GetLatest(ctx context.Context, threadID string) (Snapshot, bool, error)
    GetByID(ctx context.Context, threadID, checkpointID string) (Snapshot, bool, error)
    List(ctx context.Context, threadID string, beforeRevision, limit int64) ([]SnapshotMeta, error)
    DeleteThread(ctx context.Context, threadID string) error
    Prune(ctx context.Context, threadID string, keepLast int) error
    GetAgentSession(ctx context.Context, threadID, checkpointID, agentID string) ([]llm.Message, bool, error)
}
```

Legacy `Checkpointer` (`Put`/`Get`) continues to work for Memory/SQLite latest-only callers. When a `DurableCheckpointer` is attached via `WithDurableCheckpointer`, the engine uses `CommitStep` instead of `Put`.

## MySQL schema

InnoDB, `utf8mb4`, UTC `DATETIME(6)`, versioned via `schema_migrations`.

- `graph_threads` — latest pointer + revision
- `graph_checkpoints` — append-only history
- `graph_agent_sessions` — immutable message blobs
- `graph_checkpoint_agents` — per-checkpoint agent→session refs

`CommitStep` flow:

1. Upsert/lock thread row
2. If `checkpoint_id` already exists with matching thread/parent → success (idempotent retry)
3. Else CAS: require `parent_checkpoint_id == latest_checkpoint_id` (or both empty for first step)
4. Assign `revision = latest_revision + 1`
5. Insert checkpoint
6. Copy parent agent refs; overlay changed agent_session artifacts
7. Update latest pointer
8. Commit

`Prune(keepLast)` deletes older checkpoints/refs, then orphan session blobs no longer referenced by remaining refs.

## Step artifacts

Nodes remain agent-agnostic. During a node call, context carries a collector:

```go
RegisterStepArtifact(ctx, StepArtifact{Kind: "agent_session", Key: agentID, Payload: messagesJSON})
```

- Collector lifetime = one node execution
- Duplicate `(Kind, Key)` in one step → error
- Node error / cancel → discard artifacts
- Interrupt → commit only artifacts already registered before interrupt

## AgentStore policy

`agentspec.Spec.Persist` defaults to `PersistNone`.

| Policy | Behavior |
|--------|----------|
| `PersistNone` | No load/save (verifier, updater, ephemeral workers) |
| `PersistSession` | Load by parent checkpoint; register session artifact after success |

System prompt rule: create empty manager → load session if present → append system prompt only when nothing was loaded.

Coverage: agentspec only. Supervisor routing memory stays in graph `State["messages"]`. Custom `WorkerSpec.Run` is caller-owned.

## Graph events

Run-scoped synchronous handler (`WithEventHandler`). Metadata-only by default.

Events: `run_start`, `run_resume`, `node_start`, `node_end`, `node_error`, `route`, `checkpoint`, `checkpoint_error`, `interrupt`, `run_end`, `custom`.

Interrupt order: `checkpoint → interrupt → run_end(paused)`.

Correlation: `RunID`, `ThreadID`, `CheckpointID`, `NodeID`, `AgentID`. Core `agent.Event` fields are populated by agentspec.

## Phased delivery

| Phase | Deliverable |
|-------|-------------|
| 0 | This design + ADR 0005–0007 + ADR 0004/TODO updates |
| 1 | MySQL + Memory DurableCheckpointer (history, CAS, prune) |
| 1.5 | Minimal lifecycle events + correlation context |
| 2 | AgentStore + step artifacts + PersistPolicy |
| 3 | Full event paths + core correlation tests |
| 4a | Supervisor durable resume alignment |
| 4b | CodingLoop Resume + implementer-only persistence |

## Out of scope (later)

- SQLite history / SQLite→MySQL migration / Postgres
- HTTP idempotency, async event transport, SSE
- Parallel/Send, subgraphs, cross-thread store
- Embedding agentctx into Snapshot
- LLM/tool iteration-level recovery
- Time-travel Resume API

Distributed execution leases are specified in
[`p15-execution-lease.md`](./p15-execution-lease.md) (ADR 0008); implementation
is tracked under P1.5 in `docs/TODO.md`.
