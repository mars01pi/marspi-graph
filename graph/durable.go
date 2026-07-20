package graph

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

// ErrCheckpointConflict is returned when CommitStep's parent CAS fails
// or an idempotent retry collides with a different existing checkpoint.
var ErrCheckpointConflict = errors.New("graph: checkpoint conflict")

// ErrExpectedCheckpointMismatch is returned when Resume is asked to continue
// from a specific checkpoint ID that is no longer the thread's latest.
var ErrExpectedCheckpointMismatch = errors.New("graph: expected checkpoint mismatch")

// ArtifactAgentSession is the StepArtifact.Kind for agentspec message blobs.
const ArtifactAgentSession = "agent_session"

// StepArtifact is a versioned side product registered during one node execution
// and committed atomically with the graph Snapshot.
type StepArtifact struct {
	Kind    string
	Key     string
	Payload json.RawMessage
}

// SnapshotMeta is a lightweight history row for List / admin APIs.
type SnapshotMeta struct {
	CheckpointID string
	ThreadID     string
	Node         string
	Step         int
	Revision     int64
	Interrupt    bool
	CreatedAt    time.Time
}

// DurableCheckpointer is the P1 durable persistence API (MySQL / Memory).
// Legacy Checkpointer (Put/Get overwrite) remains for SQLite CLI use.
type DurableCheckpointer interface {
	CommitStep(ctx context.Context, snap Snapshot, artifacts []StepArtifact) error
	GetLatest(ctx context.Context, threadID string) (Snapshot, bool, error)
	GetByID(ctx context.Context, threadID, checkpointID string) (Snapshot, bool, error)
	List(ctx context.Context, threadID string, beforeRevision, limit int64) ([]SnapshotMeta, error)
	DeleteThread(ctx context.Context, threadID string) error
	Prune(ctx context.Context, threadID string, keepLast int) error
	// GetAgentSession returns messages JSON for (checkpoint, agent), or false if absent.
	GetAgentSession(ctx context.Context, threadID, checkpointID, agentID string) (json.RawMessage, bool, error)
}

// NewID returns a 128-bit hex id (32 chars) for checkpoints / sessions / runs.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Extremely unlikely; fall back to timestamp-ish entropy.
		now := time.Now().UnixNano()
		for i := 0; i < 8; i++ {
			b[i] = byte(now >> (8 * i))
		}
	}
	return hex.EncodeToString(b[:])
}
