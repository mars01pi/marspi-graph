package workflow

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mars/marspi-core/agent"
	"github.com/mars/marspi-core/console"
	"github.com/mars/marspi-core/llm"
	"github.com/mars/marspi-core/tool"
	"github.com/mars/marspi-graph/agentspec"
	"github.com/mars/marspi-graph/checkpoint"
	"github.com/mars/marspi-graph/graph"
)

// ErrApprovalDenied is returned when an interrupt_gate rejects approval.
var ErrApprovalDenied = fmt.Errorf("workflow: approval denied")

// InterruptInfo is passed to OnInterrupt for HITL gates.
type InterruptInfo struct {
	ThreadID string
	Node     string
	Value    any
}

// OnInterruptFunc handles graph interrupts. Return approve=true to Resume.
type OnInterruptFunc func(ctx context.Context, info InterruptInfo) (approve bool, err error)

// Bindings are runtime dependencies that cannot live in a serializable Spec.
type Bindings struct {
	Provider     llm.Provider
	Registry     *tool.Registry
	Reporter     console.Reporter
	Events       *agent.Emitter
	SystemPrompt string
	MaxContext   int
	MaxIterAgent int
	Stream       bool
	ThreadID     string

	Checkpointer graph.Checkpointer
	Durable      graph.DurableCheckpointer
	Lease        graph.ExecutionLease
	LeaseTTL     time.Duration
	EventHandler graph.EventHandler

	ResumeFromCheckpoint bool
	OnInterrupt          OnInterruptFunc
	AllowPush            bool // false: test PASS → END (skip push_gate/push)

	Goal      string
	MaxRework int
	MaxSteps  int

	// NodeRun overrides agent/gate bodies by node id (tests / custom hosts).
	NodeRun map[string]graph.NodeFunc

	// SpawnChildren supplies ChildSpec for agent nodes with spawn: [...].
	// When empty, builtin explore child is used for listed spawn ids.
	SpawnChildren []agentspec.ChildSpec
}

// Result is the outcome of Run.
type Result struct {
	Message string
	State   graph.State
}

// CompileWorkflow builds a Compiled graph from Spec + Bindings.
func CompileWorkflow(spec Spec, b Bindings) (*graph.Compiled, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	spec = applyAllowPush(spec, b.AllowPush)

	if b.Reporter == nil {
		b.Reporter = console.Nop{}
	}
	if b.Events == nil {
		b.Events = agent.NewEmitter()
	}
	if b.MaxContext <= 0 {
		b.MaxContext = 1_000_000
	}
	if b.MaxIterAgent <= 0 {
		b.MaxIterAgent = 100
	}

	failHooks := failHooksByNode(spec.Edges)
	idx := buildRefIndex(spec)
	builder := graph.NewBuilder()
	for key, name := range spec.Reducers {
		if name == "append_slice" {
			builder.AddReducer(key, graph.AppendSlice)
		}
	}

	for _, n := range spec.Nodes {
		n := n
		if override, ok := b.NodeRun[n.ID]; ok && override != nil {
			builder.AddNode(n.ID, override)
			continue
		}
		h := failHooks[n.ID]
		fn, err := buildNode(n, b, h, idx)
		if err != nil {
			return nil, err
		}
		builder.AddNode(n.ID, fn)
	}

	builder.SetEntry(spec.Entry)
	for _, e := range spec.Edges {
		e := e
		if e.Route == "" {
			to := e.To
			if to == "END" {
				to = graph.END
			}
			builder.AddEdge(e.From, to)
			continue
		}
		route, err := buildRoute(e)
		if err != nil {
			return nil, err
		}
		builder.AddConditionalEdges(e.From, route)
	}

	opts, err := compileOpts(b)
	if err != nil {
		return nil, err
	}
	return builder.Compile(opts...)
}

func applyAllowPush(spec Spec, allowPush bool) Spec {
	if allowPush {
		return spec
	}
	skip := map[string]bool{"push_gate": true, "push": true}
	var nodes []NodeSpec
	for _, n := range spec.Nodes {
		if skip[n.ID] {
			continue
		}
		nodes = append(nodes, n)
	}
	var edges []EdgeSpec
	for _, e := range spec.Edges {
		if skip[e.From] {
			continue
		}
		e = rewriteEdgeTargets(e, skip, "END")
		edges = append(edges, e)
	}
	spec.Nodes = nodes
	spec.Edges = edges
	return spec
}

func rewriteEdgeTargets(e EdgeSpec, skip map[string]bool, replacement string) EdgeSpec {
	rew := func(t string) string {
		if skip[t] {
			return replacement
		}
		return t
	}
	e.To = rew(e.To)
	e.Pass = rew(e.Pass)
	e.Fail = rew(e.Fail)
	e.Exhausted = rew(e.Exhausted)
	e.Approved = rew(e.Approved)
	e.Denied = rew(e.Denied)
	return e
}

type failHook struct {
	IncKey   string
	CheckKey string
}

func failHooksByNode(edges []EdgeSpec) map[string]failHook {
	out := map[string]failHook{}
	for _, e := range edges {
		h := out[e.From]
		if e.OnFailInc != "" {
			h.IncKey = e.OnFailInc
		}
		if e.Route == RoutePassFailRework {
			if e.CountKey != "" {
				h.IncKey = e.CountKey
			}
			if e.Key != "" {
				h.CheckKey = e.Key
			}
		}
		if h.IncKey != "" || h.CheckKey != "" {
			out[e.From] = h
		}
	}
	return out
}

func compileOpts(b Bindings) ([]graph.CompileOption, error) {
	var opts []graph.CompileOption
	if b.Durable != nil {
		opts = append(opts, graph.WithDurableCheckpointer(b.Durable))
		if b.Lease != nil {
			leaseOpts := []graph.CompileOption{graph.WithExecutionLease(b.Lease)}
			if b.LeaseTTL > 0 {
				leaseOpts = append(leaseOpts, graph.WithLeaseTTL(b.LeaseTTL))
			}
			opts = append(opts, leaseOpts...)
		}
		return opts, nil
	}
	if b.Lease != nil {
		return nil, fmt.Errorf("workflow: Lease requires Durable checkpointer")
	}
	cp := b.Checkpointer
	if cp == nil {
		cp = checkpoint.NewMemory()
	}
	opts = append(opts, graph.WithCheckpointer(cp))
	return opts, nil
}

// Run compiles Spec (if needed path via Builtin/Load) and Invoke/Resume.
func Run(ctx context.Context, spec Spec, b Bindings) (Result, error) {
	if b.ThreadID == "" {
		b.ThreadID = fmt.Sprintf("workflow-%d", time.Now().UnixNano())
	}
	if b.MaxRework <= 0 {
		b.MaxRework = 3
	}
	if b.MaxSteps <= 0 {
		b.MaxSteps = 40
	}

	g, err := CompileWorkflow(spec, b)
	if err != nil {
		return Result{}, err
	}

	runOpts := []graph.RunOption{
		graph.WithThreadID(b.ThreadID),
		graph.WithMaxSteps(b.MaxSteps),
	}
	if b.EventHandler != nil {
		runOpts = append(runOpts, graph.WithEventHandler(b.EventHandler))
	}

	var out graph.State
	if b.ResumeFromCheckpoint {
		if b.Checkpointer == nil && b.Durable == nil {
			return Result{}, fmt.Errorf("workflow: ResumeFromCheckpoint requires Checkpointer or Durable")
		}
		out, err = resumeWithHITL(ctx, g, b, runOpts)
	} else {
		init := graph.State{
			"goal":         b.Goal,
			"max_rework":   b.MaxRework,
			"rework_count": 0,
			"status":       "running",
			"messages":     []any{},
		}
		out, err = g.Invoke(ctx, init, runOpts...)
	}

	for graph.IsInterrupted(err) {
		ie, _ := graph.AsInterrupt(err)
		if b.OnInterrupt == nil {
			return Result{State: out}, err
		}
		approve, herr := b.OnInterrupt(ctx, InterruptInfo{
			ThreadID: b.ThreadID,
			Node:     ie.Node,
			Value:    ie.Value,
		})
		if herr != nil {
			return Result{State: out}, herr
		}
		if !approve {
			return Result{State: out, Message: "Approval denied"}, ErrApprovalDenied
		}
		out, err = g.Resume(ctx, b.ThreadID, append(runOpts, graph.WithCommand(graph.Command{
			Resume: true,
		}))...)
	}
	if err != nil {
		return Result{State: out}, err
	}

	msg := out.GetString("status")
	if msg == "" {
		msg = out.GetString("last_agent")
	}
	if out.GetString("push_result") != "" {
		msg = out.GetString("push_result")
	}
	return Result{State: out, Message: msg}, nil
}

func resumeWithHITL(ctx context.Context, g *graph.Compiled, b Bindings, runOpts []graph.RunOption) (graph.State, error) {
	var snap graph.Snapshot
	var ok bool
	var gerr error
	switch {
	case b.Durable != nil:
		snap, ok, gerr = b.Durable.GetLatest(ctx, b.ThreadID)
	case b.Checkpointer != nil:
		snap, ok, gerr = b.Checkpointer.Get(ctx, b.ThreadID)
	default:
		return nil, fmt.Errorf("workflow: ResumeFromCheckpoint requires Checkpointer or Durable")
	}
	if gerr != nil {
		return nil, gerr
	}
	if !ok {
		return nil, fmt.Errorf("workflow: no checkpoint for thread %q", b.ThreadID)
	}
	if snap.Interrupt {
		if b.OnInterrupt == nil {
			return snap.State, &graph.InterruptError{Node: snap.Node, Value: snap.InterruptValue}
		}
		approve, herr := b.OnInterrupt(ctx, InterruptInfo{
			ThreadID: b.ThreadID,
			Node:     snap.Node,
			Value:    snap.InterruptValue,
		})
		if herr != nil {
			return snap.State, herr
		}
		if !approve {
			return snap.State, ErrApprovalDenied
		}
		return g.Resume(ctx, b.ThreadID, append(runOpts, graph.WithCommand(graph.Command{Resume: true}))...)
	}
	return g.Resume(ctx, b.ThreadID, runOpts...)
}

func isPass(verdict string) bool {
	u := strings.ToUpper(verdict)
	return strings.Contains(u, "PASS") && !strings.Contains(u, "FAIL")
}

func isFail(verdict string) bool {
	return strings.Contains(strings.ToUpper(verdict), "FAIL")
}

func stateInt(s graph.State, key string) int {
	switch v := s[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

func approvalTruthy(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "1", "true", "yes", "y", "approve", "approved":
			return true
		}
	case float64:
		return x != 0
	case int:
		return x != 0
	}
	return false
}
