package orchestrator

import (
	"fmt"
	"strings"

	"github.com/mars/marspi-core/llm"
	"github.com/mars/marspi-graph/graph"
)

const handoffToolName = "handoff"

// handoffToolSchema builds an OpenAI tools array with a single handoff function.
// to enum = worker IDs + END.
func handoffToolSchema(workers []WorkerSpec) []map[string]any {
	enum := make([]string, 0, len(workers)+1)
	for _, w := range workers {
		enum = append(enum, w.ID)
	}
	enum = append(enum, "END")
	return []map[string]any{
		{
			"type": "function",
			"function": map[string]any{
				"name":        handoffToolName,
				"description": "Route work to a specialist worker, or END when the goal is satisfied.",
				"parameters": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"to": map[string]any{
							"type":        "string",
							"description": "Worker id to run next, or END to finish",
							"enum":        enum,
						},
						"reason": map[string]any{
							"type":        "string",
							"description": "Short reason for this routing choice",
						},
						"task": map[string]any{
							"type":        "string",
							"description": "Concrete instructions for the worker (ignored when to=END)",
						},
					},
					"required":             []string{"to", "reason", "task"},
					"additionalProperties": false,
				},
			},
		},
	}
}

// DecisionFromToolCalls extracts a Decision from a handoff tool call.
// Allowed destinations are worker IDs plus END aliases.
func DecisionFromToolCalls(resp llm.Response, allowed map[string]struct{}) (Decision, error) {
	if !resp.HasToolCalls || len(resp.ToolCalls) == 0 {
		return Decision{}, fmt.Errorf("orchestrator: supervisor response missing handoff tool call")
	}
	var tc *llm.ToolCall
	for i := range resp.ToolCalls {
		if resp.ToolCalls[i].Name == handoffToolName {
			tc = &resp.ToolCalls[i]
			break
		}
	}
	if tc == nil {
		return Decision{}, fmt.Errorf("orchestrator: expected tool %q, got %q", handoffToolName, resp.ToolCalls[0].Name)
	}
	to, _ := tc.Arguments["to"].(string)
	reason, _ := tc.Arguments["reason"].(string)
	task, _ := tc.Arguments["task"].(string)
	to = strings.TrimSpace(to)
	if to == "" {
		return Decision{}, fmt.Errorf("orchestrator: handoff missing to")
	}
	next := NormalizeNext(to)
	if next != graph.END {
		if _, ok := allowed[next]; !ok {
			return Decision{}, fmt.Errorf("orchestrator: handoff to unknown worker %q", to)
		}
	}
	return Decision{
		Next:   next,
		Reason: strings.TrimSpace(reason),
		Task:   strings.TrimSpace(task),
	}, nil
}

func workerAllowSet(workers []WorkerSpec) map[string]struct{} {
	out := make(map[string]struct{}, len(workers))
	for _, w := range workers {
		out[w.ID] = struct{}{}
	}
	return out
}
