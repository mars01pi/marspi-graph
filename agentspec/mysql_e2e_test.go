package agentspec_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/mars/marspi-core/llm"
	"github.com/mars/marspi-graph/agentspec"
	"github.com/mars/marspi-graph/checkpoint"
	"github.com/mars/marspi-graph/graph"
)

// TestMySQLGraphPersistInterruptResumeE2E exercises:
// real StateGraph Invoke → PersistSession Save → Interrupt →
// reopen MySQL (cross-process) → Resume → PersistSession Load.
//
// Requires a reachable MySQL. Set MARSPI_MYSQL_DSN, or rely on local default
// (root:123456@127.0.0.1:3306/marspi). Skips if MySQL is unavailable.
func TestMySQLGraphPersistInterruptResumeE2E(t *testing.T) {
	store := openE2EMySQL(t)

	ctx := context.Background()
	thread := "e2e-graph-" + graph.NewID()
	defer func() {
		_ = store.DeleteThread(ctx, thread)
		_ = store.Close()
	}()

	var llmCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		llmCalls++
		content := "research-1"
		if llmCalls > 1 {
			content = "research-2"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "mock",
			"choices": []any{map[string]any{
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": content},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
	defer srv.Close()
	provider := llm.NewProvider("gpt-4o", srv.URL, "k")

	build := func() *graph.Compiled {
		b := graph.NewBuilder()
		b.AddNode("researcher", func(runCtx context.Context, s graph.State) (graph.Update, error) {
			inst := agentspec.New(agentspec.Spec{
				ID:           "researcher",
				SystemPrompt: "you are researcher",
				Provider:     provider,
				MaxIter:      2,
				Persist:      agentspec.PersistSession,
				Store:        store,
			})
			out, err := inst.RunOnce(runCtx, "find clues")
			if err != nil {
				return nil, err
			}
			return graph.Update{"out": out, "msgs": len(inst.Manager().Messages)}, nil
		})
		b.AddNode("gate", func(runCtx context.Context, s graph.State) (graph.Update, error) {
			if _, err := graph.InterruptOrResume(runCtx, map[string]any{"need": "approve"}); err != nil {
				return nil, err
			}
			return graph.Update{"approved": true}, nil
		})
		b.SetEntry("researcher")
		b.AddEdge("researcher", "gate")
		b.AddEdge("gate", graph.END)
		g, err := b.Compile(graph.WithDurableCheckpointer(store))
		if err != nil {
			t.Fatal(err)
		}
		return g
	}

	// Process A: run until HITL gate.
	g1 := build()
	_, err := g1.Invoke(ctx, graph.State{"goal": "e2e"}, graph.WithThreadID(thread))
	if !graph.IsInterrupted(err) {
		t.Fatalf("want interrupt after researcher, got %v", err)
	}

	latest, ok, err := store.GetLatest(ctx, thread)
	if err != nil || !ok || !latest.Interrupt || latest.Node != "gate" {
		t.Fatalf("interrupt checkpoint=%+v ok=%v err=%v", latest, ok, err)
	}
	// Session must already be bound to some checkpoint in the parent chain.
	// After researcher success, CommitStep wrote session; interrupt checkpoint copies refs.
	raw, ok, err := store.GetAgentSession(ctx, thread, latest.CheckpointID, "researcher")
	if err != nil || !ok || len(raw) == 0 {
		t.Fatalf("researcher session missing at interrupt ok=%v err=%v raw=%s", ok, err, raw)
	}
	var msgs []map[string]any
	if err := json.Unmarshal(raw, &msgs); err != nil {
		t.Fatal(err)
	}
	if len(msgs) < 2 {
		t.Fatalf("expected system+user(+assistant), got %v", msgs)
	}

	// Process B: new MySQL handle + new Compiled graph, same thread.
	_ = store.Close()
	store = openE2EMySQL(t)

	g2 := build()
	out, err := g2.Resume(ctx, thread, graph.WithCommand(graph.Command{Resume: true}))
	if err != nil {
		t.Fatal(err)
	}
	if out["approved"] != true {
		t.Fatalf("state=%v", out)
	}

	latest, ok, err = store.GetLatest(ctx, thread)
	if err != nil || !ok || latest.Interrupt {
		t.Fatalf("final checkpoint=%+v ok=%v err=%v", latest, ok, err)
	}
	if latest.Node != graph.END {
		t.Fatalf("want END, node=%q", latest.Node)
	}
	raw, ok, err = store.GetAgentSession(ctx, thread, latest.CheckpointID, "researcher")
	if err != nil || !ok {
		t.Fatalf("session after resume ok=%v err=%v", ok, err)
	}

	// Fresh agent must Load restored history (no duplicate system from new Spec prompt).
	loadCtx := graph.WithThreadIDCtx(ctx, thread)
	loadCtx = graph.WithParentCheckpointID(loadCtx, latest.CheckpointID)
	loadCtx, _ = graph.WithStepArtifacts(loadCtx)
	inst := agentspec.New(agentspec.Spec{
		ID:           "researcher",
		SystemPrompt: "should-not-appear-twice",
		Provider:     provider,
		MaxIter:      2,
		Persist:      agentspec.PersistSession,
		Store:        store,
	})
	if _, err := inst.RunOnce(loadCtx, "follow-up"); err != nil {
		t.Fatal(err)
	}
	sys := 0
	for _, m := range inst.Manager().Messages {
		if m["role"] == "system" {
			sys++
			if m["content"] == "should-not-appear-twice" {
				t.Fatal("loaded session should keep original system prompt")
			}
		}
	}
	if sys != 1 {
		t.Fatalf("sys count=%d msgs=%v", sys, inst.Manager().Messages)
	}
	if llmCalls < 2 {
		t.Fatalf("expected researcher+follow-up LLM calls, got %d", llmCalls)
	}

	list, err := store.List(ctx, thread, 0, 20)
	if err != nil || len(list) < 2 {
		t.Fatalf("want history >=2, list=%+v err=%v", list, err)
	}
}

func openE2EMySQL(t *testing.T) *checkpoint.MySQL {
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
