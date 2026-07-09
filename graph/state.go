package graph

import "context"

// State is the shared workflow state passed between nodes.
// Prefer small typed helpers over dumping entire agent transcripts here.
type State map[string]any

// Clone returns a shallow copy of the state map.
func (s State) Clone() State {
	if s == nil {
		return State{}
	}
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
type Update map[string]any

// Reducer merges an existing state value with an incoming update for one key.
// If no reducer is registered for a key, last-write-wins is used.
type Reducer func(existing, update any) any

// LastValue is the default reducer (overwrite).
func LastValue(_, update any) any { return update }

// AppendSlice appends update onto an existing []any (or wraps scalars).
func AppendSlice(existing, update any) any {
	var out []any
	switch e := existing.(type) {
	case nil:
		out = nil
	case []any:
		out = append([]any(nil), e...)
	default:
		out = []any{e}
	}
	switch u := update.(type) {
	case nil:
		return out
	case []any:
		return append(out, u...)
	default:
		return append(out, u)
	}
}

// Apply merges u into s using reducers (nil map => last-write-wins for all keys).
func Apply(s State, u Update, reducers map[string]Reducer) State {
	out := s.Clone()
	if out == nil {
		out = State{}
	}
	for k, v := range u {
		if r, ok := reducers[k]; ok && r != nil {
			out[k] = r(out[k], v)
			continue
		}
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
