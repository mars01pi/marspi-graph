package orchestrator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mars/marspi-core/llm"
	"github.com/mars/marspi-graph/graph"
	"github.com/mars/marspi-graph/orchestrator"
)

func TestDecisionFromToolCalls(t *testing.T) {
	allowed := map[string]struct{}{"coder": {}, "researcher": {}}
	resp := llm.Response{
		HasToolCalls: true,
		ToolCalls: []llm.ToolCall{{
			Name: "handoff",
			Arguments: map[string]any{
				"to":     "coder",
				"reason": "implement",
				"task":   "edit file",
			},
		}},
	}
	d, err := orchestrator.DecisionFromToolCalls(resp, allowed)
	if err != nil {
		t.Fatal(err)
	}
	if d.Next != "coder" || d.Task != "edit file" {
		t.Fatalf("%+v", d)
	}

	resp.ToolCalls[0].Arguments["to"] = "END"
	d, err = orchestrator.DecisionFromToolCalls(resp, allowed)
	if err != nil {
		t.Fatal(err)
	}
	if d.Next != graph.END {
		t.Fatalf("next=%q", d.Next)
	}
}

func TestDecisionFromToolCallsUnknownWorker(t *testing.T) {
	allowed := map[string]struct{}{"coder": {}}
	_, err := orchestrator.DecisionFromToolCalls(llm.Response{
		HasToolCalls: true,
		ToolCalls: []llm.ToolCall{{
			Name:      "handoff",
			Arguments: map[string]any{"to": "ghost", "reason": "x", "task": "y"},
		}},
	}, allowed)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDecisionFromToolCallsMissing(t *testing.T) {
	_, err := orchestrator.DecisionFromToolCalls(llm.Response{}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func writeHandoffCompletion(w http.ResponseWriter, to, reason, task string) {
	args, _ := json.Marshal(map[string]string{"to": to, "reason": reason, "task": task})
	_ = json.NewEncoder(w).Encode(map[string]any{
		"model": "mock",
		"choices": []any{map[string]any{
			"finish_reason": "tool_calls",
			"message": map[string]any{
				"role":    "assistant",
				"content": "I will route now", // prose must be ignored
				"tool_calls": []any{map[string]any{
					"id":   "call_1",
					"type": "function",
					"function": map[string]any{
						"name":      "handoff",
						"arguments": string(args),
					},
				}},
			},
		}},
		"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
	})
}

func TestRunSupervisorViaHandoffTool(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := int(n.Add(1))
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		tools, _ := body["tools"].([]any)
		if len(tools) == 0 {
			t.Errorf("call %d: expected handoff tools", call)
		}
		switch call {
		case 1:
			writeHandoffCompletion(w, "researcher", "gather", "find APIs")
		default:
			writeHandoffCompletion(w, "END", "done", "")
		}
	}))
	defer srv.Close()

	prov := llm.NewProvider("gpt-4o", srv.URL, "test-key")
	var workerCalls int
	res, err := orchestrator.RunSupervisor(context.Background(), orchestrator.SupervisorConfig{
		Goal:     "research X",
		MaxSteps: 6,
		Provider: prov,
		Workers: []orchestrator.WorkerSpec{
			{
				ID: "researcher",
				Run: func(_ context.Context, s graph.State, task string) (string, error) {
					workerCalls++
					if task != "find APIs" {
						t.Fatalf("task=%q", task)
					}
					return "found", nil
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if workerCalls != 1 {
		t.Fatalf("workerCalls=%d", workerCalls)
	}
	if res.State.GetString("last_output") != "found" {
		t.Fatalf("last_output=%q", res.State.GetString("last_output"))
	}
	if n.Load() < 2 {
		t.Fatalf("expected >=2 LLM calls, got %d", n.Load())
	}
}

func TestRunSupervisorHandoffRepair(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := int(n.Add(1))
		switch call {
		case 1:
			// Invalid: prose JSON, no tool_calls
			_ = json.NewEncoder(w).Encode(map[string]any{
				"model": "mock",
				"choices": []any{map[string]any{
					"finish_reason": "stop",
					"message": map[string]any{
						"role":    "assistant",
						"content": `{"next":"researcher","reason":"x","task":"y"}`,
					},
				}},
			})
		case 2:
			writeHandoffCompletion(w, "researcher", "retry", "find APIs")
		default:
			writeHandoffCompletion(w, "END", "done", "")
		}
	}))
	defer srv.Close()

	prov := llm.NewProvider("gpt-4o", srv.URL, "test-key")
	res, err := orchestrator.RunSupervisor(context.Background(), orchestrator.SupervisorConfig{
		Goal:     "research X",
		MaxSteps: 6,
		Provider: prov,
		Workers: []orchestrator.WorkerSpec{
			{
				ID: "researcher",
				Run: func(context.Context, graph.State, string) (string, error) {
					return "ok", nil
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.State.GetString("last_output") != "ok" {
		t.Fatalf("last_output=%q", res.State.GetString("last_output"))
	}
	if n.Load() < 3 {
		t.Fatalf("expected repair+end calls, got %d", n.Load())
	}
}
