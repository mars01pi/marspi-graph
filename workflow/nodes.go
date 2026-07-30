package workflow

import (
	"github.com/mars/marspi-graph/component"
	"github.com/mars/marspi-graph/graph"
)

func buildNode(n NodeSpec, b Bindings, fail failHook, idx refIndex) (graph.NodeFunc, error) {
	c, err := component.Default.New(n.Component)
	if err != nil {
		return nil, err
	}
	if err := c.Bind(n.Bindings); err != nil {
		return nil, err
	}
	deps := &component.ExecDeps{
		Provider:      b.Provider,
		Registry:      b.Registry,
		Reporter:      b.Reporter,
		Events:        b.Events,
		SystemPrompt:  b.SystemPrompt,
		MaxContext:    b.MaxContext,
		MaxIterAgent:  b.MaxIterAgent,
		Stream:        b.Stream,
		Durable:       b.Durable,
		SpawnChildren: b.SpawnChildren,
	}
	expand := func(tmpl string, s graph.State, extras map[string]string) string {
		return ExpandRefs(tmpl, s, idx, extras)
	}
	return c.BuildNode(component.Runtime{
		NodeID:       n.ID,
		FailIncKey:   fail.IncKey,
		FailCheckKey: fail.CheckKey,
		ExpandRefs:   expand,
		Host:         deps,
	})
}
