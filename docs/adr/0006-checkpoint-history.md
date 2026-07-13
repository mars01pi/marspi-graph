# ADR 0006: MySQL checkpoint history

## Status

Accepted

## Context

P0.5 checkpointers (Memory, SQLite) store one latest Snapshot per thread.
Production needs append-only history, conflict detection, and an atomic
commit that can include agent session artifacts without changing ADR 0001.

SQLite remains useful for single-process CLI latest-only use. Extending it
for history would fork migration and concurrency semantics we do not want
to maintain twice. MySQL 8 / InnoDB is the P1 durable backend.

## Decision

1. **Backend:** MySQL 8 InnoDB is the only P1 durable store. Memory mirrors
   the same semantics for tests. SQLite stays legacy latest-only (`Put`/`Get`);
   it is not extended with history or AgentStore.
2. **API:** Introduce `DurableCheckpointer` with `CommitStep`, `GetLatest`,
   `GetByID`, `List`, `DeleteThread`, `Prune`, and `GetAgentSession`. Do not
   silently change legacy `Checkpointer.Put` overwrite semantics.
3. **Snapshot fields:** `CheckpointID`, `ParentCheckpointID`, `Revision`,
   `CreatedAt` (plus existing fields). Graph generates `CheckpointID`
   (128-bit hex) before commit; it is the idempotency key.
4. **CAS:** `CommitStep` requires `parent_checkpoint_id == threads.latest`
   (or both empty). Mismatch → `ErrCheckpointConflict`. Same ID with matching
   thread/parent → idempotent success.
5. **Resume:** `Resume(threadID)` uses latest only. History is queryable;
   time-travel Resume is deferred.
6. **Schema:** `graph_threads`, `graph_checkpoints`, plus agent tables from
   ADR 0005. Versioned via `schema_migrations`. No SQLite→MySQL auto-migrate.

## Consequences

- Engine attaches durable store via `WithDurableCheckpointer`.
- Concurrent stale writers fail loudly instead of silent overwrite.
- Callers needing CLI local latest-only continue using SQLite unchanged.
- See design: `docs/design/p1-durable-runtime.md`.
