package agentspec

import (
	"context"

	"github.com/mars/marspi-core/agent"
	"github.com/mars/marspi-core/agentctx"
	"github.com/mars/marspi-core/console"
	"github.com/mars/marspi-core/llm"
	"github.com/mars/marspi-core/tool"
	"github.com/mars/marspi-graph/graph"
)

// Spec describes a named agent that can be used as a graph node.
type Spec struct {
	ID           string
	SystemPrompt string
	Provider     llm.Provider
	Registry     *tool.Registry
	AllowTools   []string // empty = all tools from Registry
	MaxContext   int
	MaxIter      int
	Stream       bool
	Reporter     console.Reporter
	Events       *agent.Emitter
}

// Instance is a runnable agent with private conversation state.
type Instance struct {
	spec     Spec
	registry *tool.Registry
	runner   *agent.Runner
	ctx      *agentctx.Manager
}

// New builds an Instance from Spec.
func New(spec Spec) *Instance {
	if spec.MaxContext <= 0 {
		spec.MaxContext = 1_000_000
	}
	if spec.MaxIter <= 0 {
		spec.MaxIter = 100
	}
	reg := spec.Registry
	if reg != nil && len(spec.AllowTools) > 0 {
		reg = reg.View(spec.AllowTools)
	}
	events := spec.Events
	if events == nil {
		events = agent.NewEmitter()
	}
	runner := &agent.Runner{
		Provider:   spec.Provider,
		Registry:   reg,
		Events:     events,
		MaxContext: spec.MaxContext,
		MaxIter:    spec.MaxIter,
		Stream:     spec.Stream,
	}
	mgr := agentctx.New(spec.MaxContext, spec.Provider, nil, spec.Reporter)
	if spec.SystemPrompt != "" {
		mgr.AppendSystem(spec.SystemPrompt)
	}
	return &Instance{spec: spec, registry: reg, runner: runner, ctx: mgr}
}

// ID returns the agent id.
func (a *Instance) ID() string { return a.spec.ID }

// RunOnce appends userInput, runs the ReAct loop, and returns the last assistant/tool completion text.
func (a *Instance) RunOnce(ctx context.Context, userInput string) string {
	schemas := []map[string]any(nil)
	if a.registry != nil {
		schemas = a.registry.Schemas()
	}
	// Refresh tools on manager for compact paths.
	_ = schemas
	a.runner.LoopCtx(ctx, a.ctx, "", userInput)
	return lastCompletion(a.ctx)
}

// AsNode returns a graph node that reads State["input"] (or key) and writes State["output"].
func (a *Instance) AsNode(inputKey, outputKey string) graph.NodeFunc {
	if inputKey == "" {
		inputKey = "input"
	}
	if outputKey == "" {
		outputKey = "output"
	}
	return func(ctx context.Context, s graph.State) (graph.Update, error) {
		in := s.GetString(inputKey)
		out := a.RunOnce(ctx, in)
		return graph.Update{
			outputKey:    out,
			"last_agent": a.spec.ID,
		}, nil
	}
}

func lastCompletion(m *agentctx.Manager) string {
	for i := len(m.Messages) - 1; i >= 0; i-- {
		msg := m.Messages[i]
		role, _ := msg["role"].(string)
		if role != "assistant" && role != "tool" {
			continue
		}
		if c, ok := msg["content"].(string); ok && c != "" {
			return c
		}
	}
	return ""
}
