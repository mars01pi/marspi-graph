package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mars/marspi-graph/graph"
)

// DurableMemory is an in-process DurableCheckpointer with append history,
// parent CAS, agent session refs, and prune — semantic twin of MySQL.
type DurableMemory struct {
	mu      sync.Mutex
	threads map[string]*memThread
}

type memThread struct {
	latestID  string
	revision  int64
	history   []graph.Snapshot
	sessions  map[string]json.RawMessage            // session_id -> messages
	agentMeta map[string]memAgentMeta               // session_id -> meta
	refs      map[string]map[string]string          // checkpoint_id -> agent_id -> session_id
}

type memAgentMeta struct {
	threadID string
	agentID  string
}

// NewDurableMemory creates an empty durable memory store.
func NewDurableMemory() *DurableMemory {
	return &DurableMemory{threads: map[string]*memThread{}}
}

func (m *DurableMemory) threadLocked(threadID string) *memThread {
	t, ok := m.threads[threadID]
	if !ok {
		t = &memThread{
			sessions:  map[string]json.RawMessage{},
			agentMeta: map[string]memAgentMeta{},
			refs:      map[string]map[string]string{},
		}
		m.threads[threadID] = t
	}
	return t
}

// CommitStep appends a checkpoint and applies agent_session artifacts atomically.
func (m *DurableMemory) CommitStep(ctx context.Context, snap graph.Snapshot, artifacts []graph.StepArtifact) error {
	_ = ctx
	if snap.ThreadID == "" {
		return fmt.Errorf("checkpoint: CommitStep requires ThreadID")
	}
	if snap.CheckpointID == "" {
		return fmt.Errorf("checkpoint: CommitStep requires CheckpointID")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.threadLocked(snap.ThreadID)

	// Idempotent retry: same id + matching parent/thread.
	for _, existing := range t.history {
		if existing.CheckpointID == snap.CheckpointID {
			if existing.ParentCheckpointID == snap.ParentCheckpointID && existing.ThreadID == snap.ThreadID {
				return nil
			}
			return fmt.Errorf("%w: checkpoint %q exists with different parent", graph.ErrCheckpointConflict, snap.CheckpointID)
		}
	}

	if t.latestID != snap.ParentCheckpointID {
		return fmt.Errorf("%w: want parent %q, have latest %q", graph.ErrCheckpointConflict, snap.ParentCheckpointID, t.latestID)
	}

	rev := t.revision + 1
	snap.Revision = rev
	if snap.CreatedAt.IsZero() {
		snap.CreatedAt = time.Now().UTC()
	}
	snap.State = snap.State.Clone()

	// Copy parent refs then overlay changed agent sessions.
	newRefs := map[string]string{}
	if snap.ParentCheckpointID != "" {
		if parentRefs, ok := t.refs[snap.ParentCheckpointID]; ok {
			for a, s := range parentRefs {
				newRefs[a] = s
			}
		}
	}
	for _, art := range artifacts {
		if art.Kind != graph.ArtifactAgentSession {
			continue
		}
		sid := graph.NewID()
		payload := append(json.RawMessage(nil), art.Payload...)
		t.sessions[sid] = payload
		t.agentMeta[sid] = memAgentMeta{threadID: snap.ThreadID, agentID: art.Key}
		newRefs[art.Key] = sid
	}

	t.history = append(t.history, snap)
	t.refs[snap.CheckpointID] = newRefs
	t.latestID = snap.CheckpointID
	t.revision = rev
	return nil
}

// GetLatest returns the latest snapshot for threadID.
func (m *DurableMemory) GetLatest(ctx context.Context, threadID string) (graph.Snapshot, bool, error) {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.threads[threadID]
	if !ok || t.latestID == "" {
		return graph.Snapshot{}, false, nil
	}
	for i := len(t.history) - 1; i >= 0; i-- {
		if t.history[i].CheckpointID == t.latestID {
			return cloneSnap(t.history[i]), true, nil
		}
	}
	return graph.Snapshot{}, false, nil
}

// GetByID returns a specific checkpoint.
func (m *DurableMemory) GetByID(ctx context.Context, threadID, checkpointID string) (graph.Snapshot, bool, error) {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.threads[threadID]
	if !ok {
		return graph.Snapshot{}, false, nil
	}
	for _, s := range t.history {
		if s.CheckpointID == checkpointID {
			return cloneSnap(s), true, nil
		}
	}
	return graph.Snapshot{}, false, nil
}

// List returns history newest-first, optionally before beforeRevision (exclusive).
func (m *DurableMemory) List(ctx context.Context, threadID string, beforeRevision, limit int64) ([]graph.SnapshotMeta, error) {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.threads[threadID]
	if !ok {
		return nil, nil
	}
	var out []graph.SnapshotMeta
	for i := len(t.history) - 1; i >= 0; i-- {
		s := t.history[i]
		if beforeRevision > 0 && s.Revision >= beforeRevision {
			continue
		}
		out = append(out, graph.SnapshotMeta{
			CheckpointID: s.CheckpointID,
			ThreadID:     s.ThreadID,
			Node:         s.Node,
			Step:         s.Step,
			Revision:     s.Revision,
			Interrupt:    s.Interrupt,
			CreatedAt:    s.CreatedAt,
		})
		if limit > 0 && int64(len(out)) >= limit {
			break
		}
	}
	return out, nil
}

// DeleteThread removes all durable data for a thread.
func (m *DurableMemory) DeleteThread(ctx context.Context, threadID string) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.threads, threadID)
	return nil
}

// Prune keeps the newest keepLast checkpoints; deletes orphan sessions.
func (m *DurableMemory) Prune(ctx context.Context, threadID string, keepLast int) error {
	_ = ctx
	if keepLast < 0 {
		return fmt.Errorf("checkpoint: keepLast must be >= 0")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.threads[threadID]
	if !ok {
		return nil
	}
	if keepLast == 0 {
		delete(m.threads, threadID)
		return nil
	}
	if len(t.history) <= keepLast {
		return nil
	}
	sort.SliceStable(t.history, func(i, j int) bool {
		return t.history[i].Revision < t.history[j].Revision
	})
	drop := t.history[:len(t.history)-keepLast]
	keep := t.history[len(t.history)-keepLast:]
	for _, s := range drop {
		delete(t.refs, s.CheckpointID)
	}
	t.history = keep

	live := map[string]struct{}{}
	for _, refs := range t.refs {
		for _, sid := range refs {
			live[sid] = struct{}{}
		}
	}
	for sid := range t.sessions {
		if _, ok := live[sid]; !ok {
			delete(t.sessions, sid)
			delete(t.agentMeta, sid)
		}
	}
	if len(t.history) > 0 {
		t.latestID = t.history[len(t.history)-1].CheckpointID
		t.revision = t.history[len(t.history)-1].Revision
	} else {
		t.latestID = ""
		t.revision = 0
	}
	return nil
}

// GetAgentSession returns messages JSON for a checkpoint+agent ref.
func (m *DurableMemory) GetAgentSession(ctx context.Context, threadID, checkpointID, agentID string) (json.RawMessage, bool, error) {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.threads[threadID]
	if !ok {
		return nil, false, nil
	}
	refs, ok := t.refs[checkpointID]
	if !ok {
		return nil, false, nil
	}
	sid, ok := refs[agentID]
	if !ok {
		return nil, false, nil
	}
	msg, ok := t.sessions[sid]
	if !ok {
		return nil, false, nil
	}
	out := append(json.RawMessage(nil), msg...)
	return out, true, nil
}

// Put implements legacy Checkpointer (append with empty parent CAS chain).
func (m *DurableMemory) Put(ctx context.Context, threadID string, snap graph.Snapshot) error {
	if threadID != "" {
		snap.ThreadID = threadID
	}
	if snap.CheckpointID == "" {
		snap.CheckpointID = graph.NewID()
	}
	m.mu.Lock()
	t := m.threadLocked(snap.ThreadID)
	snap.ParentCheckpointID = t.latestID
	m.mu.Unlock()
	return m.CommitStep(ctx, snap, nil)
}

// Get implements legacy Checkpointer.
func (m *DurableMemory) Get(ctx context.Context, threadID string) (graph.Snapshot, bool, error) {
	return m.GetLatest(ctx, threadID)
}

func cloneSnap(s graph.Snapshot) graph.Snapshot {
	s.State = s.State.Clone()
	return s
}
