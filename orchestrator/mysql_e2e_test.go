package orchestrator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/mars/marspi-core/llm"
	"github.com/mars/marspi-graph/checkpoint"
	"github.com/mars/marspi-graph/graph"
	"github.com/mars/marspi-graph/orchestrator"
)

// TestMySQLSupervisorHITLSessionE2E runs Supervisor against real MySQL:
// Invoke → HITL interrupt → reopen DB → ResumeFromCheckpoint → worker PersistSession.
func TestMySQLSupervisorHITLSessionE2E(t *testing.T) {
	store := openOrchestratorE2EMySQL(t)

	ctx := context.Background()
	thread := "e2e-sv-" + graph.NewID()
	defer func() {
		_ = store.DeleteThread(ctx, thread)
		_ = store.Close()
	}()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "mock",
			"choices": []any{map[string]any{
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "patched-in-mysql"},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
	defer srv.Close()
	provider := llm.NewProvider("gpt-4o", srv.URL, "k")

	var decideN int
	_, err := orchestrator.RunSupervisor(ctx, orchestrator.SupervisorConfig{
		Goal:               "fix bug",
		MaxSteps:           8,
		ThreadID:           thread,
		Durable:            store,
		Provider:           provider,
		RequireApprovalFor: []string{"coder"},
		Decide: func(context.Context, graph.State) (orchestrator.Decision, error) {
			decideN++
			if decideN == 1 {
				return orchestrator.Decision{Next: "coder", Reason: "need patch", Task: "edit"}, nil
			}
			return orchestrator.Decision{Next: "END", Reason: "done"}, nil
		},
		Workers: []orchestrator.WorkerSpec{{ID: "coder"}},
	})
	if !graph.IsInterrupted(err) {
		t.Fatalf("want HITL interrupt, got %v", err)
	}

	// Cross-process: new connection, same thread.
	_ = store.Close()
	store = openOrchestratorE2EMySQL(t)

	res, err := orchestrator.RunSupervisor(ctx, orchestrator.SupervisorConfig{
		Goal:                 "fix bug",
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
	if res.State.GetString("last_output") != "patched-in-mysql" {
		t.Fatalf("last_output=%q state=%v", res.State.GetString("last_output"), res.State)
	}

	latest, ok, err := store.GetLatest(ctx, thread)
	if err != nil || !ok {
		t.Fatalf("latest ok=%v err=%v", ok, err)
	}
	raw, ok, err := store.GetAgentSession(ctx, thread, latest.CheckpointID, "coder")
	if err != nil || !ok || len(raw) == 0 {
		t.Fatalf("coder session missing in MySQL ok=%v err=%v", ok, err)
	}
	list, err := store.List(ctx, thread, 0, 50)
	if err != nil || len(list) < 2 {
		t.Fatalf("history=%+v err=%v", list, err)
	}
}

func openOrchestratorE2EMySQL(t *testing.T) *checkpoint.MySQL {
	t.Helper()
	dsn := os.Getenv("MARSPI_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:123456@tcp(127.0.0.1:3306)/marspi?parseTime=true&loc=UTC"
	}
	m, err := checkpoint.OpenMySQL(dsn)
	if err != nil {
		t.Skipf("MySQL unavailable (%v); set MARSPI_MYSQL_DSN or start docker/mysql.sh", err)
	}
	return m
}
