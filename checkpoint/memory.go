package checkpoint

import (
	"context"
	"sync"

	"github.com/mars/marspi-graph/graph"
)

// Memory is an in-process checkpointer (tests / single-process runs).
type Memory struct {
	mu   sync.RWMutex
	data map[string]graph.Snapshot
}

// NewMemory creates an empty memory checkpointer.
func NewMemory() *Memory {
	return &Memory{data: map[string]graph.Snapshot{}}
}

func (m *Memory) Put(_ context.Context, threadID string, snap graph.Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if threadID == "" {
		threadID = snap.ThreadID
	}
	snap.ThreadID = threadID
	m.data[threadID] = snap
	return nil
}

func (m *Memory) Get(_ context.Context, threadID string) (graph.Snapshot, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.data[threadID]
	return s, ok, nil
}
