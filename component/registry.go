// Package component defines code-first workflow node types (SDK).
// A Component's declared inputs/outputs are the source of truth for the canvas;
// workflow Spec only stores topology + per-instance bindings.
package component

import (
	"fmt"

	"github.com/mars/marspi-graph/graph"
)

// Type IDs registered in the default catalog.
const (
	TypeAgent    = "agent"
	TypeApproval = "approval"
)

// Meta is stable identity for a component type.
type Meta struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// InputDef describes one configurable field (canvas property panel).
type InputDef struct {
	Name          string   `json:"name"`
	Label         string   `json:"label,omitempty"`
	Types         []string `json:"types,omitempty"`
	Widget        string   `json:"widget,omitempty"` // textarea, text, multiselect, bool, ...
	Default       any      `json:"default,omitempty"`
	Advanced      bool     `json:"advanced,omitempty"`
	OptionsSource string   `json:"options_source,omitempty"`
}

// OutputDef describes one published port / state field.
type OutputDef struct {
	Name  string   `json:"name"`
	Label string   `json:"label,omitempty"`
	Types []string `json:"types,omitempty"`
}

// Schema is the UI/runtime contract derived from a component type (+ bindings).
type Schema struct {
	Meta    Meta        `json:"meta"`
	Inputs  []InputDef  `json:"inputs"`
	Outputs []OutputDef `json:"outputs"`
}

// Runtime holds host-injected deps for building graph nodes.
type Runtime struct {
	NodeID string
	// FailIncKey is incremented when FailCheckKey's value looks like FAIL
	// (typically edge count_key for pass_fail_rework).
	FailIncKey string
	// FailCheckKey is the state field inspected for FAIL (typically edge key).
	FailCheckKey string
	ExpandRefs   RefExpander
	Host         Host
}

// RefExpander expands {{refs}} in binding strings against graph.State.
type RefExpander func(tmpl string, s graph.State, extras map[string]string) string

// Host is workflow-provided runtime services (LLM, tools, HITL hooks live outside).
type Host any

// Component is a code entity that can be bound and turned into a graph node.
type Component interface {
	Meta() Meta
	// TypeSchema returns static inputs/outputs before instance bindings.
	TypeSchema() Schema
	// Bind applies instance bindings from the thin flow Spec.
	Bind(bindings map[string]any) error
	// InstanceSchema returns schema after Bind (may add dynamic outputs).
	InstanceSchema() Schema
	// BuildNode constructs the graph node function.
	BuildNode(rt Runtime) (graph.NodeFunc, error)
}

// Factory constructs a fresh unbound component.
type Factory func() Component

// Registry maps component type id → factory.
type Registry struct {
	byType map[string]Factory
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{byType: map[string]Factory{}}
}

// Register adds or replaces a component type.
func (r *Registry) Register(typeID string, f Factory) error {
	if r == nil {
		return fmt.Errorf("component: nil registry")
	}
	if typeID == "" || f == nil {
		return fmt.Errorf("component: type and factory required")
	}
	if r.byType == nil {
		r.byType = map[string]Factory{}
	}
	r.byType[typeID] = f
	return nil
}

// New instantiates an unbound component by type id.
func (r *Registry) New(typeID string) (Component, error) {
	if r == nil {
		return nil, fmt.Errorf("component: nil registry")
	}
	f, ok := r.byType[typeID]
	if !ok {
		return nil, fmt.Errorf("component: unknown type %q", typeID)
	}
	return f(), nil
}

// Schemas returns type-level schemas for all registered components (canvas palette).
func (r *Registry) Schemas() []Schema {
	if r == nil {
		return nil
	}
	out := make([]Schema, 0, len(r.byType))
	for _, f := range r.byType {
		out = append(out, f().TypeSchema())
	}
	return out
}

// Default is the process-wide component catalog.
var Default = NewRegistry()

func init() {
	_ = Default.Register(TypeAgent, func() Component { return NewAgent() })
	_ = Default.Register(TypeApproval, func() Component { return NewApproval() })
}
