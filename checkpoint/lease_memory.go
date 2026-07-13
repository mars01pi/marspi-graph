package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mars/marspi-graph/graph"
)

// MemoryLease is an in-process ExecutionLease with injectable clock for tests.
type MemoryLease struct {
	mu     sync.Mutex
	now    func() time.Time
	leases map[string]*memLeaseState
}

type memLeaseState struct {
	runID     string
	epoch     int64
	expiresAt time.Time
}

// NewMemoryLease creates an empty memory lease store.
func NewMemoryLease() *MemoryLease {
	return &MemoryLease{
		now:    func() time.Time { return time.Now().UTC() },
		leases: map[string]*memLeaseState{},
	}
}

// SetClock injects a clock for deterministic TTL tests. nil resets to wall clock.
func (l *MemoryLease) SetClock(now func() time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now == nil {
		l.now = func() time.Time { return time.Now().UTC() }
		return
	}
	l.now = now
}

func (l *MemoryLease) clock() time.Time {
	return l.now().UTC()
}

// Acquire implements graph.ExecutionLease.
func (l *MemoryLease) Acquire(ctx context.Context, threadID, runID string, ttl time.Duration) (graph.LeaseGrant, error) {
	if err := ctx.Err(); err != nil {
		return graph.LeaseGrant{}, err
	}
	if threadID == "" || runID == "" {
		return graph.LeaseGrant{}, graph.ErrLeaseLost
	}
	if ttl <= 0 {
		ttl = graph.DefaultLeaseTTL
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock()
	st, ok := l.leases[threadID]
	if !ok {
		st = &memLeaseState{}
		l.leases[threadID] = st
	}

	active := st.runID != "" && st.expiresAt.After(now)
	if active && st.runID == runID {
		// Idempotent same owner: keep epoch, extend expiry.
		st.expiresAt = now.Add(ttl)
		return graph.LeaseGrant{ThreadID: threadID, RunID: runID, Epoch: st.epoch, ExpiresAt: st.expiresAt}, nil
	}
	if active {
		return graph.LeaseGrant{}, graph.ErrLeaseHeld
	}

	st.epoch++
	st.runID = runID
	st.expiresAt = now.Add(ttl)
	return graph.LeaseGrant{ThreadID: threadID, RunID: runID, Epoch: st.epoch, ExpiresAt: st.expiresAt}, nil
}

// Renew implements graph.ExecutionLease.
func (l *MemoryLease) Renew(ctx context.Context, grant graph.LeaseGrant, ttl time.Duration) (graph.LeaseGrant, error) {
	if err := ctx.Err(); err != nil {
		return graph.LeaseGrant{}, err
	}
	if ttl <= 0 {
		ttl = graph.DefaultLeaseTTL
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock()
	st, ok := l.leases[grant.ThreadID]
	if !ok || st.runID != grant.RunID || st.epoch != grant.Epoch || !st.expiresAt.After(now) {
		return graph.LeaseGrant{}, graph.ErrLeaseLost
	}
	st.expiresAt = now.Add(ttl)
	return graph.LeaseGrant{ThreadID: grant.ThreadID, RunID: grant.RunID, Epoch: st.epoch, ExpiresAt: st.expiresAt}, nil
}

// Release implements graph.ExecutionLease (idempotent).
func (l *MemoryLease) Release(ctx context.Context, grant graph.LeaseGrant) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	st, ok := l.leases[grant.ThreadID]
	if !ok {
		return nil
	}
	if st.runID == grant.RunID && st.epoch == grant.Epoch {
		st.runID = ""
		st.expiresAt = time.Time{}
		// epoch intentionally retained
	}
	return nil
}

// validateActiveLocked checks grant under l.mu.
func (l *MemoryLease) validateActiveLocked(grant graph.LeaseGrant) error {
	now := l.clock()
	st, ok := l.leases[grant.ThreadID]
	if !ok || st.runID != grant.RunID || st.epoch != grant.Epoch || !st.expiresAt.After(now) {
		return graph.ErrLeaseLost
	}
	return nil
}

// Fenced wraps a DurableMemory with a MemoryLease for atomic CommitStepFenced.
// Use Store with WithDurableCheckpointer and Lease with WithExecutionLease.
type Fenced struct {
	Store *DurableMemory
	Lease *MemoryLease
}

// NewFencedMemory returns a durable memory store and matching lease.
func NewFencedMemory() (*Fenced, *MemoryLease) {
	store := NewDurableMemory()
	lease := NewMemoryLease()
	f := &Fenced{Store: store, Lease: lease}
	return f, lease
}

// CommitStep implements DurableCheckpointer.
func (f *Fenced) CommitStep(ctx context.Context, snap graph.Snapshot, artifacts []graph.StepArtifact) error {
	return f.Store.CommitStep(ctx, snap, artifacts)
}

// GetLatest implements DurableCheckpointer.
func (f *Fenced) GetLatest(ctx context.Context, threadID string) (graph.Snapshot, bool, error) {
	return f.Store.GetLatest(ctx, threadID)
}

// GetByID implements DurableCheckpointer.
func (f *Fenced) GetByID(ctx context.Context, threadID, checkpointID string) (graph.Snapshot, bool, error) {
	return f.Store.GetByID(ctx, threadID, checkpointID)
}

// List implements DurableCheckpointer.
func (f *Fenced) List(ctx context.Context, threadID string, beforeRevision, limit int64) ([]graph.SnapshotMeta, error) {
	return f.Store.List(ctx, threadID, beforeRevision, limit)
}

// DeleteThread implements DurableCheckpointer.
func (f *Fenced) DeleteThread(ctx context.Context, threadID string) error {
	return f.Store.DeleteThread(ctx, threadID)
}

// Prune implements DurableCheckpointer.
func (f *Fenced) Prune(ctx context.Context, threadID string, keepLast int) error {
	return f.Store.Prune(ctx, threadID, keepLast)
}

// GetAgentSession implements DurableCheckpointer.
func (f *Fenced) GetAgentSession(ctx context.Context, threadID, checkpointID, agentID string) (json.RawMessage, bool, error) {
	return f.Store.GetAgentSession(ctx, threadID, checkpointID, agentID)
}

// CommitStepFenced validates the lease then commits under both locks (lease first).
func (f *Fenced) CommitStepFenced(ctx context.Context, snap graph.Snapshot, artifacts []graph.StepArtifact, grant graph.LeaseGrant) error {
	_ = ctx
	if f == nil || f.Store == nil || f.Lease == nil {
		return graph.ErrLeaseLost
	}
	if snap.ThreadID != "" && grant.ThreadID != "" && snap.ThreadID != grant.ThreadID {
		return graph.ErrLeaseLost
	}
	f.Lease.mu.Lock()
	defer f.Lease.mu.Unlock()
	if err := f.Lease.validateActiveLocked(grant); err != nil {
		return err
	}
	f.Store.mu.Lock()
	defer f.Store.mu.Unlock()
	if snap.ThreadID == "" {
		return fmt.Errorf("checkpoint: CommitStep requires ThreadID")
	}
	if snap.CheckpointID == "" {
		return fmt.Errorf("checkpoint: CommitStep requires CheckpointID")
	}
	return f.Store.commitStepLocked(snap, artifacts)
}
