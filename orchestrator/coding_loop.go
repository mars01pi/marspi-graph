package orchestrator

import (
	"context"
	"fmt"
	"strings"

	"github.com/mars/marspi-core/agent"
	"github.com/mars/marspi-core/agentctx"
	"github.com/mars/marspi-core/console"
	"github.com/mars/marspi-core/llm"
	"github.com/mars/marspi-core/tool"
	"github.com/mars/marspi-graph/checkpoint"
	"github.com/mars/marspi-graph/graph"
)

// CodingLoopConfig configures the Implementer → Verifier → Updater workflow.
type CodingLoopConfig struct {
	Goal         string
	MaxIter      int
	SystemPrompt string
	Provider     llm.Provider
	Registry     *tool.Registry
	Reporter     console.Reporter
	Events       *agent.Emitter
	MaxContext   int
	MaxIterAgent int
	Stream       bool
}

// CodingLoopResult is the outcome of a coding loop run.
type CodingLoopResult struct {
	Success    bool
	Iterations int
	Message    string
	State      graph.State
}

// RunCodingLoop executes the classic 3-role coding loop on a StateGraph.
// Implementer conversation persists across iterations; verifier/updater are fresh each time.
func RunCodingLoop(ctx context.Context, cfg CodingLoopConfig) (CodingLoopResult, error) {
	if cfg.MaxIter <= 0 {
		cfg.MaxIter = 5
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

	impl := newRoleAgent(cfg, "implementer", nil)
	impl.ctx.AppendSystem(cfg.SystemPrompt)

	b := graph.NewBuilder()
	b.AddNode("implementer", func(runCtx context.Context, s graph.State) (graph.Update, error) {
		iter := stateInt(s, "iteration")
		if iter == 0 {
			iter = 1
		}
		maxIter := stateInt(s, "max_iter")
		goal := s.GetString("goal")
		prompt := fmt.Sprintf(
			"[Loop iter %d/%d]\n"+
				"GOAL: %s\n\n"+
				"You are the IMPLEMENTER. Design + write code.\n"+
				"1. Read relevant code (read/grep).\n"+
				"2. Plan your design.\n"+
				"3. Implement progressively: smallest working change first.\n"+
				"4. Self-review: read back your changes, verify no logic errors.\n"+
				"5. Design for extensibility: clear abstractions, hooks, avoid hard-coding.\n"+
				"6. When calling edit/write, briefly explain WHY in your thinking.\n"+
				"7. Call `attempt_completion` tool when done.\n\n"+
				"DO NOT run tests or verify your own code. That's the Verifier's job.",
			iter, maxIter, goal)
		impl.runner.LoopCtx(runCtx, impl.ctx, "", prompt)
		return graph.Update{
			"changed_files": changedFiles(impl.ctx),
			"iteration":     iter,
			"last_agent":    "implementer",
		}, nil
	})

	b.AddNode("verifier", func(runCtx context.Context, s graph.State) (graph.Update, error) {
		ver := newRoleAgent(cfg, "verifier", nil)
		ver.ctx.AppendSystem(cfg.SystemPrompt)
		iter := stateInt(s, "iteration")
		goal := s.GetString("goal")
		files := s.GetString("changed_files")
		prompt := fmt.Sprintf(
			"[Verify iter %d]\n"+
				"GOAL: %s\n"+
				"Files changed by implementer: \n%s\n\n"+
				"You are an OBJECTIVE Verifier. Independent judgment required.\n"+
				"1. Inspect the changed files (read tool).\n"+
				"2. Determine the right test command(inspect project:package.json/pyproject.toml/go.mod).\n"+
				"3. Run tests with bash tool.\n"+
				"4. Judge PASS/FAIL based on exit code AND tests actually verify the goal.\n"+
				"5. If FAIL: explain which test, why, and suggested fix.\n"+
				"6. Call `attempt_completion` tool with one line:\n"+
				"   - 'VERIFY: PASS'\n"+
				"   - 'VERIFY: FAIL: <reason>'\n"+
				"   - 'VERIFY: PASS, NO_ISSUES'\n"+
				"   - 'VERIFY: PASS, ISSUES: <list>'\n\n"+
				"Explain at architecture-level (module/flow), not function-level.",
			iter, goal, files)
		ver.runner.LoopCtx(runCtx, ver.ctx, "", prompt)
		result := completionResult(ver.ctx)
		passed := result != "" && strings.Contains(result, "VERIFY: PASS")
		status := "failed"
		if passed {
			status = "success"
		}
		return graph.Update{
			"verify_result": result,
			"status":        status,
			"last_agent":    "verifier",
		}, nil
	})

	b.AddNode("updater", func(runCtx context.Context, s graph.State) (graph.Update, error) {
		// Updater: read-only tools preferred
		allow := []string{"read", "grep", "search", "use_skill", "search_memory", "attempt_completion"}
		upd := newRoleAgent(cfg, "updater", allow)
		upd.ctx.AppendSystem(cfg.SystemPrompt)
		iter := stateInt(s, "iteration")
		goal := s.GetString("goal")
		verifyResult := s.GetString("verify_result")
		prompt := fmt.Sprintf(
			"[Updater iter %d]\n"+
				"GOAL: %s\n"+
				"Verifier FAILED: %s\n\n"+
				"You are an UPDATER.\n"+
				"Refine the user's prompt for the next implementer iteration.\n"+
				"You MUST NOT write code or call write/edit/bash. Only read/grep for context.\n\n"+
				"Read the verifier's failure analysis above.\n"+
				"Identify what's missing or unclear in the original goal.\n\n"+
				"Output: a single prompt, 50-200 words, specific constraints.\n"+
				"Call `attempt_completion` tool to return the refined prompt.",
			iter, goal, verifyResult)
		upd.runner.LoopCtx(runCtx, upd.ctx, "", prompt)
		refined := completionResult(upd.ctx)
		if refined != "" {
			impl.ctx.AppendUser(fmt.Sprintf("[Refined prompt from updater iter %d]\n%s", iter, refined))
		}
		nextIter := iter + 1
		return graph.Update{
			"refined_prompt": refined,
			"iteration":      nextIter,
			"last_agent":     "updater",
		}, nil
	})

	b.SetEntry("implementer")
	b.AddEdge("implementer", "verifier")
	b.AddConditionalEdges("verifier", func(_ context.Context, s graph.State) string {
		if s.GetString("status") == "success" {
			return graph.END
		}
		iter := stateInt(s, "iteration")
		maxIter := stateInt(s, "max_iter")
		if iter >= maxIter {
			return graph.END
		}
		return "updater"
	})
	b.AddEdge("updater", "implementer")

	g, err := b.Compile(graph.WithCheckpointer(checkpoint.NewMemory()))
	if err != nil {
		return CodingLoopResult{}, err
	}

	out, err := g.Invoke(ctx, graph.State{
		"goal":      cfg.Goal,
		"max_iter":  cfg.MaxIter,
		"iteration": 1,
		"status":    "running",
	}, graph.WithThreadID("coding-loop"), graph.WithMaxSteps(cfg.MaxIter*4+2))
	if err != nil {
		return CodingLoopResult{}, err
	}

	iters := stateInt(out, "iteration")
	if out.GetString("status") == "success" {
		return CodingLoopResult{
			Success:    true,
			Iterations: iters,
			Message:    fmt.Sprintf("Loop succeeded at iter %d", iters),
			State:      out,
		}, nil
	}
	return CodingLoopResult{
		Success:    false,
		Iterations: cfg.MaxIter,
		Message:    fmt.Sprintf("Loop failed after %d iterations", cfg.MaxIter),
		State:      out,
	}, nil
}

type roleAgent struct {
	runner *agent.Runner
	ctx    *agentctx.Manager
}

func newRoleAgent(cfg CodingLoopConfig, id string, allow []string) *roleAgent {
	reg := cfg.Registry
	if reg != nil && len(allow) > 0 {
		reg = reg.View(allow)
	}
	schemas := []map[string]any(nil)
	if reg != nil {
		schemas = reg.Schemas()
	}
	runner := &agent.Runner{
		Provider:   cfg.Provider,
		Registry:   reg,
		Events:     cfg.Events,
		MaxContext: cfg.MaxContext,
		MaxIter:    cfg.MaxIterAgent,
		Stream:     cfg.Stream,
	}
	mgr := agentctx.New(cfg.MaxContext, cfg.Provider, schemas, cfg.Reporter)
	return &roleAgent{runner: runner, ctx: mgr}
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

func changedFiles(ctx *agentctx.Manager) string {
	seen := map[string]bool{}
	var files []string
	for _, m := range ctx.Messages {
		if r, _ := m["role"].(string); r != "tool" {
			continue
		}
		tn, _ := m["tool_name"].(string)
		if tn != "edit" && tn != "write" {
			continue
		}
		content := contentStr(m["content"])
		if !seen[content] {
			seen[content] = true
			files = append(files, content)
		}
	}
	if len(files) == 0 {
		return "(unknown — Verifier inspect project to find)"
	}
	return strings.Join(files, ",\n ")
}

func completionResult(ctx *agentctx.Manager) string {
	msgs := ctx.Messages
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		if r, _ := m["role"].(string); r != "tool" {
			continue
		}
		if tn, _ := m["tool_name"].(string); tn == "attempt_completion" {
			return contentStr(m["content"])
		}
	}
	return ""
}

func contentStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", x)
	}
}
