package workflow

import (
	"fmt"
	"os"
	"strings"

	"github.com/mars/marspi-graph/component"
	"gopkg.in/yaml.v3"
)

// Named route ids (closures stay in Go; Spec only references names).
const (
	RoutePassFail       = "pass_fail"
	RoutePassFailRework = "pass_fail_rework"
	RouteApproval       = "approval"
)

// Spec is a thin workflow instance: topology + per-node component bindings.
// Node *types* live in package component (code entities), not in this file.
type Spec struct {
	ID       string            `json:"id" yaml:"id"`
	Entry    string            `json:"entry" yaml:"entry"`
	Reducers map[string]string `json:"reducers,omitempty" yaml:"reducers,omitempty"`
	Nodes    []NodeSpec        `json:"nodes" yaml:"nodes"`
	Edges    []EdgeSpec        `json:"edges" yaml:"edges"`
}

// NodeSpec places one component instance on the canvas.
type NodeSpec struct {
	ID        string         `json:"id" yaml:"id"`
	Component string         `json:"component" yaml:"component"`
	Bindings  map[string]any `json:"bindings,omitempty" yaml:"bindings,omitempty"`
	// Kind is a deprecated alias for Component (interrupt_gate → approval).
	Kind string `json:"kind,omitempty" yaml:"kind,omitempty"`
}

// EdgeSpec is a fixed edge or a named conditional route.
type EdgeSpec struct {
	From string `json:"from" yaml:"from"`
	To   string `json:"to,omitempty" yaml:"to,omitempty"`

	Route string `json:"route,omitempty" yaml:"route,omitempty"`
	Key   string `json:"key,omitempty" yaml:"key,omitempty"`

	Pass string `json:"pass,omitempty" yaml:"pass,omitempty"`
	Fail string `json:"fail,omitempty" yaml:"fail,omitempty"`

	OnFailInc string `json:"on_fail_inc,omitempty" yaml:"on_fail_inc,omitempty"`
	MaxKey    string `json:"max_key,omitempty" yaml:"max_key,omitempty"`
	CountKey  string `json:"count_key,omitempty" yaml:"count_key,omitempty"`
	Exhausted string `json:"exhausted,omitempty" yaml:"exhausted,omitempty"`

	Approved string `json:"approved,omitempty" yaml:"approved,omitempty"`
	Denied   string `json:"denied,omitempty" yaml:"denied,omitempty"`
}

// ParseSpec unmarshals YAML or JSON bytes into Spec.
func ParseSpec(data []byte) (Spec, error) {
	var s Spec
	if err := yaml.Unmarshal(data, &s); err != nil {
		return Spec{}, fmt.Errorf("workflow: parse spec: %w", err)
	}
	s.normalize()
	if err := s.Validate(); err != nil {
		return Spec{}, err
	}
	return s, nil
}

// LoadSpec reads a YAML/JSON file from disk.
func LoadSpec(path string) (Spec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Spec{}, fmt.Errorf("workflow: load %q: %w", path, err)
	}
	return ParseSpec(data)
}

func (s *Spec) normalize() {
	for i := range s.Nodes {
		n := &s.Nodes[i]
		if n.Component == "" {
			n.Component = n.Kind
		}
		if n.Component == "interrupt_gate" {
			n.Component = component.TypeApproval
		}
		n.Kind = ""
	}
}

// Validate checks structural constraints.
func (s Spec) Validate() error {
	if strings.TrimSpace(s.Entry) == "" {
		return fmt.Errorf("workflow: entry is required")
	}
	if len(s.Nodes) == 0 {
		return fmt.Errorf("workflow: at least one node is required")
	}
	nodes := make(map[string]NodeSpec, len(s.Nodes))
	for _, n := range s.Nodes {
		id := strings.TrimSpace(n.ID)
		if id == "" {
			return fmt.Errorf("workflow: node id must be non-empty")
		}
		if id == "START" || id == "END" {
			return fmt.Errorf("workflow: node id %q is reserved", id)
		}
		if _, ok := nodes[id]; ok {
			return fmt.Errorf("workflow: duplicate node %q", id)
		}
		ctype := n.Component
		if ctype == "" {
			return fmt.Errorf("workflow: node %q missing component", id)
		}
		c, err := component.Default.New(ctype)
		if err != nil {
			return fmt.Errorf("workflow: node %q: %w", id, err)
		}
		if err := c.Bind(n.Bindings); err != nil {
			return fmt.Errorf("workflow: node %q bindings: %w", id, err)
		}
		nodes[id] = n
	}
	if _, ok := nodes[s.Entry]; !ok {
		return fmt.Errorf("workflow: entry %q not found", s.Entry)
	}
	for key, r := range s.Reducers {
		if r != "append_slice" {
			return fmt.Errorf("workflow: unknown reducer %q for key %q", r, key)
		}
	}
	if err := validateRefs(s); err != nil {
		return err
	}
	fromEdge := map[string]int{}
	for i, e := range s.Edges {
		if strings.TrimSpace(e.From) == "" {
			return fmt.Errorf("workflow: edges[%d] missing from", i)
		}
		if _, ok := nodes[e.From]; !ok {
			return fmt.Errorf("workflow: edges[%d] unknown from %q", i, e.From)
		}
		fromEdge[e.From]++
		if fromEdge[e.From] > 1 {
			return fmt.Errorf("workflow: node %q has multiple outgoing edges", e.From)
		}
		if e.Route == "" {
			if e.To == "" {
				return fmt.Errorf("workflow: edges[%d] needs to or route", i)
			}
			if err := checkTarget(nodes, e.To); err != nil {
				return fmt.Errorf("workflow: edges[%d]: %w", i, err)
			}
			continue
		}
		switch e.Route {
		case RoutePassFail:
			if e.Key == "" || e.Pass == "" || e.Fail == "" {
				return fmt.Errorf("workflow: edges[%d] pass_fail needs key/pass/fail", i)
			}
			if err := checkTarget(nodes, e.Pass); err != nil {
				return fmt.Errorf("workflow: edges[%d] pass: %w", i, err)
			}
			if err := checkTarget(nodes, e.Fail); err != nil {
				return fmt.Errorf("workflow: edges[%d] fail: %w", i, err)
			}
		case RoutePassFailRework:
			if e.Key == "" || e.Pass == "" || e.Fail == "" {
				return fmt.Errorf("workflow: edges[%d] pass_fail_rework needs key/pass/fail", i)
			}
			if e.CountKey == "" || e.MaxKey == "" {
				return fmt.Errorf("workflow: edges[%d] pass_fail_rework needs count_key/max_key", i)
			}
			exhausted := e.Exhausted
			if exhausted == "" {
				exhausted = "END"
			}
			if err := checkTarget(nodes, e.Pass); err != nil {
				return fmt.Errorf("workflow: edges[%d] pass: %w", i, err)
			}
			if err := checkTarget(nodes, e.Fail); err != nil {
				return fmt.Errorf("workflow: edges[%d] fail: %w", i, err)
			}
			if err := checkTarget(nodes, exhausted); err != nil {
				return fmt.Errorf("workflow: edges[%d] exhausted: %w", i, err)
			}
		case RouteApproval:
			if e.Approved == "" || e.Denied == "" {
				return fmt.Errorf("workflow: edges[%d] approval needs approved/denied", i)
			}
			if err := checkTarget(nodes, e.Approved); err != nil {
				return fmt.Errorf("workflow: edges[%d] approved: %w", i, err)
			}
			if err := checkTarget(nodes, e.Denied); err != nil {
				return fmt.Errorf("workflow: edges[%d] denied: %w", i, err)
			}
		default:
			return fmt.Errorf("workflow: edges[%d] unknown route %q", i, e.Route)
		}
	}
	return nil
}

func checkTarget(nodes map[string]NodeSpec, to string) error {
	if to == "END" || to == "START" {
		return nil
	}
	if _, ok := nodes[to]; !ok {
		return fmt.Errorf("unknown target %q", to)
	}
	return nil
}

// ComponentSchemas returns palette schemas from the default component registry.
func ComponentSchemas() []component.Schema {
	return component.Default.Schemas()
}
