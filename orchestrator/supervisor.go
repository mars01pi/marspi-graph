// Package orchestrator provides high-level agent orchestration patterns.
// Star-topology supervisor preset.
package orchestrator

import (
	"context"
	"errors"
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

const supervisorNode = "supervisor"

// ErrApprovalDenied is returned when HITL rejects a handoff to a gated worker.
var ErrApprovalDenied = errors.New("orchestrator: approval denied")

// WorkerSpec describes one worker agent under a supervisor.
type WorkerSpec struct {
	ID           string
	Description  string // shown to supervisor for routing
	SystemPrompt string
	AllowTools   []string // empty = all tools from Registry
	// Run overrides the default ReAct worker (tests / custom nodes).
	Run func(ctx context.Context, s graph.State, task string) (string, error)
}

// DecideFunc chooses the next worker (or END). When nil, the supervisor LLM is used.
type DecideFunc func(ctx context.Context, s graph.State) (Decision, error)

// InterruptInfo is passed to OnInterrupt when a gated worker pauses for approval.
type InterruptInfo struct {
	ThreadID string
	Node     string // worker id
	Value    any    // payload map: worker, task, reason, goal
}

// OnInterruptFunc is a blocking HITL hook. Return approve=true to Resume.
type OnInterruptFunc func(ctx context.Context, info InterruptInfo) (approve bool, err error)

// SupervisorConfig configures a star-topology supervisor workflow.
// Invoke-oriented: graph Resume does not restore worker agentctx (ADR 0004).
type SupervisorConfig struct {
	Goal         string
	Workers      []WorkerSpec
	MaxSteps     int // supervisor+worker hops; default 12
	SystemPrompt string
	Provider     llm.Provider
	Registry     *tool.Registry
	Reporter     console.Reporter
	Events       *agent.Emitter
	MaxContext   int
	MaxIterAgent int
	Stream       bool
	ThreadID     string // empty → supervisor-<unixnano>
	Decide       DecideFunc
	// RequireApprovalFor lists worker IDs that must be approved before RunOnce.
	RequireApprovalFor []string
	// OnInterrupt handles graph interrupts. If nil and a gated worker interrupts,
	// RunSupervisor returns the interrupt error to the caller.
	OnInterrupt OnInterruptFunc
}

// SupervisorResult is the outcome of a supervisor run.
type SupervisorResult struct {
	Message string
	State   graph.State
}

// RunSupervisor builds and invokes a supervisor ↔ workers star graph.
func RunSupervisor(ctx context.Context, cfg SupervisorConfig) (SupervisorResult, error) {
	if cfg.Decide == nil && cfg.Provider == nil {
		return SupervisorResult{}, fmt.Errorf("orchestrator: supervisor requires Provider or Decide")
	}
	if len(cfg.Workers) == 0 {
		return SupervisorResult{}, fmt.Errorf("orchestrator: supervisor requires at least one worker")
	}
	if cfg.MaxSteps <= 0 {
		cfg.MaxSteps = 12
	}
	if cfg.MaxContext <= 0 {
		cfg.MaxContext = 1_000_000
	}
	if cfg.MaxIterAgent <= 0 {
		cfg.MaxIterAgent = 100
	}
	if cfg.Reporter == nil {
		cfg.Reporter = console.Nop{}
	}
	if cfg.Events == nil {
		cfg.Events = agent.NewEmitter()
	}
	if cfg.ThreadID == "" {
		cfg.ThreadID = fmt.Sprintf("supervisor-%d", time.Now().UnixNano())
	}

	workerSet := make(map[string]struct{}, len(cfg.Workers))
	for _, w := range cfg.Workers {
		if w.ID == "" || w.ID == supervisorNode || w.ID == graph.END || w.ID == graph.START {
			return SupervisorResult{}, fmt.Errorf("orchestrator: invalid worker id %q", w.ID)
		}
		if _, ok := workerSet[w.ID]; ok {
			return SupervisorResult{}, fmt.Errorf("orchestrator: duplicate worker id %q", w.ID)
		}
		workerSet[w.ID] = struct{}{}
	}
	approvalSet := make(map[string]struct{}, len(cfg.RequireApprovalFor))
	for _, id := range cfg.RequireApprovalFor {
		approvalSet[id] = struct{}{}
	}

	b := graph.NewBuilder()
	b.AddReducer("messages", graph.AppendSlice)

	b.AddNode(supervisorNode, func(runCtx context.Context, s graph.State) (graph.Update, error) {
		step := stateInt(s, "step")
		maxSteps := stateInt(s, "max_steps")
		if maxSteps > 0 && step >= maxSteps {
			return graph.Update{
				"next":       graph.END,
				"last_agent": supervisorNode,
			}, nil
		}

		decision, err := decideNext(runCtx, cfg, s)
		if err != nil {
			return nil, fmt.Errorf("supervisor decide: %w", err)
		}
		next := NormalizeNext(decision.Next)
		if next != graph.END {
			if _, ok := workerSet[next]; !ok {
				return nil, fmt.Errorf("orchestrator: unknown worker %q", decision.Next)
			}
		}
		task := decision.Task
		if task == "" {
			task = s.GetString("goal")
		}
		h := Handoff{
			From:   supervisorNode,
			To:     next,
			Reason: decision.Reason,
			Task:   task,
		}
		summary := fmt.Sprintf("[supervisor → %s] %s", next, decision.Reason)
		return graph.Update{
			"next":       next,
			"handoff":    handoffToMap(h),
			"messages":   summary,
			"last_agent": supervisorNode,
			"step":       step + 1,
		}, nil
	})

	for _, w := range cfg.Workers {
		w := w
		needApproval := false
		if _, ok := approvalSet[w.ID]; ok {
			needApproval = true
		}
		b.AddNode(w.ID, func(runCtx context.Context, s graph.State) (graph.Update, error) {
			h := HandoffFromMap(s["handoff"])
			task := h.Task
			if task == "" {
				task = s.GetString("goal")
			}
			if needApproval {
				payload := map[string]any{
					"worker": w.ID,
					"task":   task,
					"reason": h.Reason,
					"goal":   s.GetString("goal"),
				}
				v, err := graph.InterruptOrResume(runCtx, payload)
				if err != nil {
					return nil, err
				}
				if !approvalTruthy(v) {
					return nil, ErrApprovalDenied
				}
			}
			var out string
			if w.Run != nil {
				text, err := w.Run(runCtx, s, task)
				if err != nil {
					return nil, err
				}
				out = text
			} else {
				if cfg.Provider == nil {
					return nil, fmt.Errorf("orchestrator: worker %q needs Provider or Run", w.ID)
				}
				sys := w.SystemPrompt
				if sys == "" {
					sys = cfg.SystemPrompt
				}
				inst := agentspec.New(agentspec.Spec{
					ID:           w.ID,
					SystemPrompt: sys,
					Provider:     cfg.Provider,
					Registry:     cfg.Registry,
					AllowTools:   w.AllowTools,
					MaxContext:   cfg.MaxContext,
					MaxIter:      cfg.MaxIterAgent,
					Stream:       cfg.Stream,
					Reporter:     cfg.Reporter,
					Events:       cfg.Events,
				})
				text, err := inst.RunOnce(runCtx, formatWorkerPrompt(w, s.GetString("goal"), task, s))
				if err != nil {
					return nil, fmt.Errorf("worker %q: %w", w.ID, err)
				}
				out = text
			}
			summary := fmt.Sprintf("[%s] %s", w.ID, truncate(out, 400))
			return graph.Update{
				"last_agent":  w.ID,
				"last_output": out,
				"messages":    summary,
			}, nil
		})
		b.AddEdge(w.ID, supervisorNode)
	}

	b.SetEntry(supervisorNode)
	b.AddConditionalEdges(supervisorNode, func(_ context.Context, s graph.State) string {
		next := s.GetString("next")
		if next == "" || next == graph.END {
			return graph.END
		}
		return next
	})

	cp := checkpoint.NewMemory()
	g, err := b.Compile(graph.WithCheckpointer(cp))
	if err != nil {
		return SupervisorResult{}, err
	}

	runOpts := []graph.RunOption{
		graph.WithThreadID(cfg.ThreadID),
		graph.WithMaxSteps(cfg.MaxSteps*2 + 2),
	}
	out, err := g.Invoke(ctx, graph.State{
		"goal":      cfg.Goal,
		"max_steps": cfg.MaxSteps,
		"step":      0,
		"messages":  []any{},
	}, runOpts...)

	for graph.IsInterrupted(err) {
		ie, _ := graph.AsInterrupt(err)
		if cfg.OnInterrupt == nil {
			return SupervisorResult{State: out}, err
		}
		if cerr := ctx.Err(); cerr != nil {
			return SupervisorResult{State: out}, cerr
		}
		info := InterruptInfo{
			ThreadID: cfg.ThreadID,
			Node:     ie.Node,
			Value:    ie.Value,
		}
		approve, herr := cfg.OnInterrupt(ctx, info)
		if herr != nil {
			return SupervisorResult{State: out}, herr
		}
		if !approve {
			return SupervisorResult{State: out, Message: "Approval denied"}, ErrApprovalDenied
		}
		if cerr := ctx.Err(); cerr != nil {
			return SupervisorResult{State: out}, cerr
		}
		out, err = g.Resume(ctx, cfg.ThreadID, append(runOpts, graph.WithCommand(graph.Command{
			Resume: true,
		}))...)
	}
	if err != nil {
		return SupervisorResult{State: out}, err
	}

	msg := out.GetString("last_output")
	if msg == "" {
		msg = "Supervisor finished"
	}
	return SupervisorResult{Message: msg, State: out}, nil
}

func approvalTruthy(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "true", "yes", "y", "approve", "1":
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func decideNext(ctx context.Context, cfg SupervisorConfig, s graph.State) (Decision, error) {
	if cfg.Decide != nil {
		return cfg.Decide(ctx, s)
	}
	prompt := buildSupervisorPrompt(cfg, s)
	text, err := callLLMOnce(ctx, cfg.Provider, prompt)
	if err != nil {
		return Decision{}, err
	}
	d, err := ParseDecision(text)
	if err != nil {
		retry := prompt + "\n\nPrevious output was invalid. Reply with ONLY a JSON object: {\"next\":\"...\",\"reason\":\"...\",\"task\":\"...\"}"
		text2, err2 := callLLMOnce(ctx, cfg.Provider, retry)
		if err2 != nil {
			return Decision{}, err
		}
		return ParseDecision(text2)
	}
	return d, nil
}

func buildSupervisorPrompt(cfg SupervisorConfig, s graph.State) string {
	var b strings.Builder
	b.WriteString("You are a Supervisor agent. Route work to specialist workers or finish.\n")
	b.WriteString("Reply with ONLY a JSON object (no markdown):\n")
	b.WriteString(`{"next":"<worker_id|END>","reason":"<short>","task":"<instructions for worker>"` + "}\n\n")
	b.WriteString("Workers:\n")
	for _, w := range cfg.Workers {
		desc := w.Description
		if desc == "" {
			desc = w.ID
		}
		fmt.Fprintf(&b, "- %s: %s\n", w.ID, desc)
	}
	b.WriteString("\nGoal:\n")
	b.WriteString(s.GetString("goal"))
	b.WriteString("\n\nHistory:\n")
	b.WriteString(formatMessages(s["messages"]))
	if out := s.GetString("last_output"); out != "" {
		b.WriteString("\n\nLast worker output:\n")
		b.WriteString(truncate(out, 1200))
	}
	b.WriteString("\n\nWhen the goal is satisfied, set next to END.")
	return b.String()
}

func formatMessages(v any) string {
	switch x := v.(type) {
	case []any:
		if len(x) == 0 {
			return "(none)"
		}
		var parts []string
		for _, item := range x {
			parts = append(parts, fmt.Sprint(item))
		}
		return strings.Join(parts, "\n")
	case nil:
		return "(none)"
	default:
		return fmt.Sprint(x)
	}
}

func formatWorkerPrompt(w WorkerSpec, goal, task string, s graph.State) string {
	var b strings.Builder
	fmt.Fprintf(&b, "You are worker %q.\n", w.ID)
	b.WriteString("Overall goal:\n")
	b.WriteString(goal)
	b.WriteString("\n\nYour assigned task:\n")
	b.WriteString(task)
	if hist := formatMessages(s["messages"]); hist != "(none)" {
		b.WriteString("\n\nPrior handoffs:\n")
		b.WriteString(hist)
	}
	b.WriteString("\n\nComplete the task. Be concise in your final answer.")
	return b.String()
}

func callLLMOnce(ctx context.Context, p llm.Provider, userText string) (string, error) {
	msgs := []llm.Message{
		{"role": "user", "content": userText},
	}
	raw, err := llm.RequestContext(ctx, p.APIURL(), p.BuildBody(msgs, nil), p.Headers(), 120*time.Second, 2)
	if err != nil {
		return "", err
	}
	resp := p.ParseResponse(raw)
	return strings.TrimSpace(resp.Content), nil
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
