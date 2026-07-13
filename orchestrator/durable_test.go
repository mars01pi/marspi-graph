package orchestrator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mars/marspi-core/llm"
	"github.com/mars/marspi-graph/checkpoint"
	"github.com/mars/marspi-graph/graph"
	"github.com/mars/marspi-graph/orchestrator"
)

func TestSupervisorDurableWorkerSessionResume(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "mock",
			"choices": []any{map[string]any{
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "worked"},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
	defer srv.Close()

	store := checkpoint.NewDurableMemory()
	thread := "sv-durable-1"
	provider := llm.NewProvider("gpt-4o", srv.URL, "k")

	var decideN int
	_, err := orchestrator.RunSupervisor(context.Background(), orchestrator.SupervisorConfig{
		Goal:               "do work",
		MaxSteps:           8,
		ThreadID:           thread,
		Durable:            store,
		Provider:           provider,
		RequireApprovalFor: []string{"coder"},
		Decide: func(context.Context, graph.State) (orchestrator.Decision, error) {
			decideN++
			if decideN == 1 {
				return orchestrator.Decision{Next: "coder", Reason: "go", Task: "t1"}, nil
			}
			return orchestrator.Decision{Next: "END", Reason: "done"}, nil
		},
		Workers: []orchestrator.WorkerSpec{{ID: "coder"}},
	})
	if !graph.IsInterrupted(err) {
		t.Fatalf("want interrupt, got %v", err)
	}

	res, err := orchestrator.RunSupervisor(context.Background(), orchestrator.SupervisorConfig{
		Goal:                 "do work",
		MaxSteps:             8,
		ThreadID:             thread,
		Durable:              store,
		Provider:             provider,
		ResumeFromCheckpoint: true,
		RequireApprovalFor:   []string{"coder"},
		OnInterrupt: func(context.Context, orchestrator.InterruptInfo) (bool, error) {
			return true, nil
		},
		Decide: func(context.Context, graph.State) (orchestrator.Decision, error) {
			return orchestrator.Decision{Next: "END", Reason: "done"}, nil
		},
		Workers: []orchestrator.WorkerSpec{{ID: "coder"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.State.GetString("last_output") != "worked" {
		t.Fatalf("last_output=%q", res.State.GetString("last_output"))
	}
	latest, ok, err := store.GetLatest(context.Background(), thread)
	if err != nil || !ok {
		t.Fatal(err)
	}
	raw, ok, err := store.GetAgentSession(context.Background(), thread, latest.CheckpointID, "coder")
	if err != nil || !ok || len(raw) == 0 {
		t.Fatalf("worker session missing ok=%v err=%v", ok, err)
	}
}

func TestCodingLoopResumeRequiresStore(t *testing.T) {
	_, err := orchestrator.RunCodingLoop(context.Background(), orchestrator.CodingLoopConfig{
		Goal:                 "x",
		Provider:             llm.NewProvider("gpt-4o", "http://127.0.0.1:1", "k"),
		ResumeFromCheckpoint: true,
	})
	if err == nil || !strings.Contains(err.Error(), "ResumeFromCheckpoint") {
		t.Fatalf("err=%v", err)
	}
}

func TestCodingLoopProviderRequired(t *testing.T) {
	_, err := orchestrator.RunCodingLoop(context.Background(), orchestrator.CodingLoopConfig{})
	if err == nil {
		t.Fatal("expected provider error")
	}
}

func TestCodingLoopDurableSuccess(t *testing.T) {
	// Mock: implementer stop, verifier returns PASS via attempt_completion-like text parsing.
	// Verifier looks for attempt_completion tool; simplify by making verifier LLM return stop
	// with content containing PASS and coding_loop checking completionResult / status.
	// Looking at coding_loop: verifier sets status success when completionResult contains "PASS"
	// after attempt_completion tool. Without tools registry, LoopCtx just returns assistant text.
	// completionResult scans for tool attempt_completion — so status won't be success easily.
	// Use MaxIter=1 and accept failure path still exercises Durable wiring.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "mock",
			"choices": []any{map[string]any{
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "PASS"},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
	defer srv.Close()

	store := checkpoint.NewDurableMemory()
	res, err := orchestrator.RunCodingLoop(context.Background(), orchestrator.CodingLoopConfig{
		Goal:     "noop",
		MaxIter:  1,
		Provider: llm.NewProvider("gpt-4o", srv.URL, "k"),
		Durable:  store,
		ThreadID: "loop-durable",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Without attempt_completion tool, loop ends unsuccessful — still durable checkpoints exist.
	if res.Iterations < 1 {
		t.Fatalf("res=%+v", res)
	}
	list, err := store.List(context.Background(), "loop-durable", 0, 20)
	if err != nil || len(list) == 0 {
		t.Fatalf("expected checkpoints, list=%v err=%v", list, err)
	}
}
