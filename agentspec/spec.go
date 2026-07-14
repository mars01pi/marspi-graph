package agentspec

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/mars/marspi-core/agent"
	"github.com/mars/marspi-core/agentctx"
	"github.com/mars/marspi-core/console"
	"github.com/mars/marspi-core/llm"
	"github.com/mars/marspi-core/tool"
	"github.com/mars/marspi-graph/graph"
)

// PersistPolicy controls whether agent messages are bound to checkpoints.
type PersistPolicy uint8

const (
	// PersistNone never loads/saves sessions (default).
	PersistNone PersistPolicy = iota
	// PersistSession loads from parent checkpoint and registers a step artifact.
	PersistSession
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
	// Persist controls durable session recovery (ADR 0005). Default PersistNone.
	Persist PersistPolicy
	// Store is required for PersistSession (typically a DurableCheckpointer).
	Store graph.DurableCheckpointer
}

// Instance is a runnable agent with private conversation state.
type Instance struct {
	spec     Spec
	registry *tool.Registry
	runner   *agent.Runner
	ctx      *agentctx.Manager

	mu   sync.Mutex
	corr correlation
}

type correlation struct {
	runID   string
	nodeID  string
	agentID string
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
	outer := spec.Events
	if outer == nil {
		outer = agent.NewEmitter()
	}
	inner := agent.NewEmitter()
	inst := &Instance{spec: spec, registry: reg}
	inner.Subscribe(func(ev agent.Event) {
		inst.mu.Lock()
		c := inst.corr
		inst.mu.Unlock()
		if c.runID != "" {
			ev.RunID = c.runID
		}
		if c.nodeID != "" {
			ev.NodeID = c.nodeID
		}
		if c.agentID != "" {
			ev.AgentID = c.agentID
		}
		outer.Emit(ev)
	})
	runner := &agent.Runner{
		Provider:   spec.Provider,
		Registry:   reg,
		Events:     inner,
		MaxContext: spec.MaxContext,
		MaxIter:    spec.MaxIter,
		Stream:     spec.Stream,
		ToolMeta: func(ctx context.Context, toolName, toolCallID string) tool.ExecutionMeta {
			return toolMetaFromGraph(ctx, toolName, toolCallID)
		},
	}
	schemas := []map[string]any(nil)
	if reg != nil {
		schemas = reg.Schemas()
	}
	mgr := agentctx.New(spec.MaxContext, spec.Provider, schemas, spec.Reporter)
	// PersistSession defers system prompt until RunOnce (after optional load).
	if spec.Persist != PersistSession && spec.SystemPrompt != "" {
		mgr.AppendSystem(spec.SystemPrompt)
	}
	inst.runner = runner
	inst.ctx = mgr
	inst.corr.agentID = spec.ID
	return inst
}

// ID returns the agent id.
func (a *Instance) ID() string { return a.spec.ID }

// Manager exposes the private conversation (for tests / advanced presets).
func (a *Instance) Manager() *agentctx.Manager { return a.ctx }

// RunOnce appends userInput, runs the ReAct loop, and returns the last completion text.
func (a *Instance) RunOnce(ctx context.Context, userInput string) (string, error) {
	a.setCorrelation(ctx)
	if err := a.ensureSession(ctx); err != nil {
		return "", err
	}
	if err := a.runner.LoopCtx(ctx, a.ctx, "", userInput); err != nil {
		return lastCompletion(a.ctx), err
	}
	if err := a.registerSessionArtifact(ctx); err != nil {
		return lastCompletion(a.ctx), err
	}
	return lastCompletion(a.ctx), nil
}

func (a *Instance) setCorrelation(ctx context.Context) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.corr.runID = graph.RunID(ctx)
	a.corr.nodeID = graph.NodeID(ctx)
	if a.corr.agentID == "" {
		a.corr.agentID = a.spec.ID
	}
}

func (a *Instance) ensureSession(ctx context.Context) error {
	if a.spec.Persist != PersistSession {
		return nil
	}
	if len(a.ctx.Messages) > 0 {
		return nil
	}
	parent := graph.ParentCheckpointID(ctx)
	thread := graph.ThreadID(ctx)
	if parent != "" && thread != "" && a.spec.Store != nil {
		raw, ok, err := a.spec.Store.GetAgentSession(ctx, thread, parent, a.spec.ID)
		if err != nil {
			return fmt.Errorf("agentspec %q: load session: %w", a.spec.ID, err)
		}
		if ok && len(raw) > 0 {
			var msgs []llm.Message
			if err := json.Unmarshal(raw, &msgs); err != nil {
				return fmt.Errorf("agentspec %q: decode session: %w", a.spec.ID, err)
			}
			a.ctx.Messages = msgs
			return nil
		}
	}
	if a.spec.SystemPrompt != "" {
		a.ctx.AppendSystem(a.spec.SystemPrompt)
	}
	return nil
}

func (a *Instance) registerSessionArtifact(ctx context.Context) error {
	if a.spec.Persist != PersistSession {
		return nil
	}
	payload, err := json.Marshal(a.ctx.Messages)
	if err != nil {
		return fmt.Errorf("agentspec %q: encode session: %w", a.spec.ID, err)
	}
	if err := graph.RegisterStepArtifact(ctx, graph.StepArtifact{
		Kind:    graph.ArtifactAgentSession,
		Key:     a.spec.ID,
		Payload: payload,
	}); err != nil {
		// No collector (node not under durable run) — ignore only if store unset path;
		// PersistSession under graph nodes always has a collector.
		return fmt.Errorf("agentspec %q: register session: %w", a.spec.ID, err)
	}
	return nil
}

// AsNode returns a graph node that reads State[inputKey] and writes State[outputKey].
// Agent failures surface as node errors.
func (a *Instance) AsNode(inputKey, outputKey string) graph.NodeFunc {
	if inputKey == "" {
		inputKey = "input"
	}
	if outputKey == "" {
		outputKey = "output"
	}
	return func(ctx context.Context, s graph.State) (graph.Update, error) {
		in := s.GetString(inputKey)
		out, err := a.RunOnce(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("agentspec %q: %w", a.spec.ID, err)
		}
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

// toolMetaFromGraph maps graph context into core ExecutionMeta for tool calls.
// OperationID defaults to toolCallID (stable within one LLM turn).
func toolMetaFromGraph(ctx context.Context, toolName, toolCallID string) tool.ExecutionMeta {
	op := toolCallID
	if op == "" {
		op = toolName
	}
	exec, err := graph.ToolExecutionFrom(ctx, toolName, op)
	if err != nil {
		return tool.ExecutionMeta{
			ThreadID:           graph.ThreadID(ctx),
			RunID:              graph.RunID(ctx),
			NodeID:             graph.NodeID(ctx),
			ParentCheckpointID: graph.ParentCheckpointID(ctx),
			LeaseEpoch:         graph.LeaseEpoch(ctx),
			ToolCallID:         toolCallID,
		}
	}
	return tool.ExecutionMeta{
		ThreadID:           exec.ThreadID,
		RunID:              exec.RunID,
		NodeID:             exec.NodeID,
		ParentCheckpointID: exec.ParentCheckpointID,
		LeaseEpoch:         exec.LeaseEpoch,
		ToolCallID:         toolCallID,
		IdempotencyKey:     exec.IdempotencyKey(),
	}
}
