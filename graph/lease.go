package graph

import (
	"context"
	"errors"
	"time"
)

// ErrLeaseHeld is returned when another active runner owns the thread lease.
var ErrLeaseHeld = errors.New("graph: thread execution lease held")

// ErrLeaseLost is returned when the current grant is expired, stolen, or mismatched.
var ErrLeaseLost = errors.New("graph: execution lease lost or expired")

// LeaseGrant is an acquired execution lease. Epoch is the fencing token.
type LeaseGrant struct {
	ThreadID  string
	RunID     string
	Epoch     int64
	ExpiresAt time.Time
}

// ExecutionLease grants exclusive execution rights for a thread.
type ExecutionLease interface {
	Acquire(ctx context.Context, threadID, runID string, ttl time.Duration) (LeaseGrant, error)
	Renew(ctx context.Context, grant LeaseGrant, ttl time.Duration) (LeaseGrant, error)
	// Release is idempotent: free or mismatched/stale grant → nil.
	Release(ctx context.Context, grant LeaseGrant) error
}

// LeaseFencedCommitter persists checkpoints only while grant is still valid.
type LeaseFencedCommitter interface {
	DurableCheckpointer
	CommitStepFenced(ctx context.Context, snap Snapshot, artifacts []StepArtifact, grant LeaseGrant) error
}

type leaseGrantKey struct{}

// WithLeaseGrant stores the active grant on context for tools/nodes.
func WithLeaseGrant(ctx context.Context, grant LeaseGrant) context.Context {
	return context.WithValue(ctx, leaseGrantKey{}, grant)
}

// LeaseGrantFrom returns the active grant, if any.
func LeaseGrantFrom(ctx context.Context) (LeaseGrant, bool) {
	g, ok := ctx.Value(leaseGrantKey{}).(LeaseGrant)
	return g, ok
}

// LeaseEpoch returns the fencing epoch from context, or 0.
//
// Use as a downstream fencing token for external systems (see ToolExecution).
// It prevents a stale lease owner from being trusted after takeover; it does
// not by itself dedupe tool side effects — pair with IdempotencyKey for that.
func LeaseEpoch(ctx context.Context) int64 {
	g, ok := LeaseGrantFrom(ctx)
	if !ok {
		return 0
	}
	return g.Epoch
}

// DefaultLeaseTTL is the default execution lease lifetime.
const DefaultLeaseTTL = 30 * time.Second

// DefaultLeaseReleaseTimeout bounds Release after caller cancellation.
const DefaultLeaseReleaseTimeout = 3 * time.Second
