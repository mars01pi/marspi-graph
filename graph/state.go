package graph

import "context"

// State is the shared workflow state passed between nodes.
// Prefer small typed helpers over dumping entire agent transcripts here.
type State map[string]any

// Clone returns a shallow copy of the state map.
func (s State) Clone() State {
	out := make(State, len(s))
	for k, v := range s {
		out[k] = v
	}
	return out
}

// GetString returns a string field or empty.
func (s State) GetString(key string) string {
	v, _ := s[key].(string)
	return v
}

// Update is a partial state patch returned by a node.
// Keys present in Update overwrite State (last-write-wins for MVP).
// Future: pluggable reducers per key.
type Update map[string]any

// Apply merges u into s and returns the new state.
func Apply(s State, u Update) State {
	out := s.Clone()
	if out == nil {
		out = State{}
	}
	for k, v := range u {
		out[k] = v
	}
	return out
}

// NodeFunc is one graph computation step.
type NodeFunc func(ctx context.Context, s State) (Update, error)

// RouteFunc chooses the next node name from state (conditional edge).
// Return END to finish.
type RouteFunc func(ctx context.Context, s State) string

// Special node names.
const (
	START = "__start__"
	END   = "__end__"
)
