package workflow

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/mars/marspi-graph/component"
	"github.com/mars/marspi-graph/graph"
)

// CatalogEntry is one selectable variable for authoring UIs.
type CatalogEntry struct {
	NodeID   string `json:"node_id"`
	Name     string `json:"name"`
	StateKey string `json:"state_key"`
	Label    string `json:"label"`
	Ref      string `json:"ref"`
}

var refPattern = regexp.MustCompile(`\{\{\s*([^{}]+?)\s*\}\}`)

// OutputCatalog lists start fields + each node's published outputs (from bound components).
func (s Spec) OutputCatalog() []CatalogEntry {
	var out []CatalogEntry
	seen := map[string]bool{}
	for _, e := range []CatalogEntry{
		{NodeID: "start", Name: "goal", StateKey: "goal", Label: "Goal", Ref: "start.goal"},
		{NodeID: "start", Name: "max_rework", StateKey: "max_rework", Label: "Max rework", Ref: "start.max_rework"},
	} {
		seen[e.Ref] = true
		out = append(out, e)
	}
	for _, n := range s.Nodes {
		for _, o := range nodeOutputs(n) {
			sk := o.StateKey
			if sk == "" {
				sk = o.Name
			}
			ref := n.ID + "." + o.Name
			if seen[ref] {
				continue
			}
			seen[ref] = true
			label := o.Label
			if label == "" {
				label = o.Name
			}
			out = append(out, CatalogEntry{
				NodeID: n.ID, Name: o.Name, StateKey: sk, Label: label, Ref: ref,
			})
		}
	}
	return out
}

func nodeOutputs(n NodeSpec) []component.OutputSpec {
	c, err := component.Default.New(n.Component)
	if err != nil {
		return nil
	}
	_ = c.Bind(n.Bindings)
	if a, ok := c.(*component.Agent); ok {
		return a.PublishedOutputs()
	}
	// Approval / others: map InstanceSchema outputs.
	var out []component.OutputSpec
	for _, o := range c.InstanceSchema().Outputs {
		out = append(out, component.OutputSpec{Name: o.Name, Label: o.Label, StateKey: o.Name})
	}
	return out
}

type refIndex struct {
	alias map[string]string
}

func buildRefIndex(s Spec) refIndex {
	idx := refIndex{alias: map[string]string{
		"goal": "goal", "start.goal": "goal",
		"max_rework": "max_rework", "rework_count": "rework_count",
		"status": "status", "phase": "phase", "approval": "approval",
		"output": "output",
	}}
	for _, n := range s.Nodes {
		for _, o := range nodeOutputs(n) {
			sk := o.StateKey
			if sk == "" {
				sk = o.Name
			}
			idx.alias[sk] = sk
			idx.alias[o.Name] = sk
			idx.alias[n.ID+"."+o.Name] = sk
			if sk != o.Name {
				idx.alias[n.ID+"."+sk] = sk
			}
		}
	}
	return idx
}

// ExpandRefs replaces {{key}} / {{node.field}} using State (+ optional extras).
func ExpandRefs(tmpl string, s graph.State, idx refIndex, extras map[string]string) string {
	if tmpl == "" || !strings.Contains(tmpl, "{{") {
		return tmpl
	}
	return refPattern.ReplaceAllStringFunc(tmpl, func(m string) string {
		inner := strings.TrimSpace(refPattern.FindStringSubmatch(m)[1])
		if extras != nil {
			if v, ok := extras[inner]; ok {
				return v
			}
		}
		sk, ok := idx.alias[inner]
		if !ok {
			sk = inner
		}
		if sk == "output" && extras != nil {
			if v, ok := extras["output"]; ok {
				return v
			}
		}
		if extras != nil {
			if v, ok := extras[sk]; ok {
				return v
			}
		}
		return stateString(s, sk)
	})
}

func stateString(s graph.State, key string) string {
	if s == nil {
		return ""
	}
	if v, ok := s[key]; ok && v != nil {
		switch t := v.(type) {
		case string:
			return t
		default:
			return fmt.Sprint(t)
		}
	}
	return ""
}

func collectBindingStrings(bindings map[string]any) []string {
	var out []string
	var walk func(any)
	walk = func(v any) {
		switch t := v.(type) {
		case string:
			out = append(out, t)
		case map[string]any:
			for _, x := range t {
				walk(x)
			}
		case []any:
			for _, x := range t {
				walk(x)
			}
		}
	}
	walk(bindings)
	return out
}

func validateRefs(s Spec) error {
	idx := buildRefIndex(s)
	for _, n := range s.Nodes {
		for _, tmpl := range collectBindingStrings(n.Bindings) {
			for _, m := range refPattern.FindAllStringSubmatch(tmpl, -1) {
				inner := strings.TrimSpace(m[1])
				if _, ok := idx.alias[inner]; ok {
					continue
				}
				return fmt.Errorf("workflow: node %q unknown ref {{%s}}", n.ID, inner)
			}
		}
	}
	return nil
}
