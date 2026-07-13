package agentspec_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mars/marspi-core/agent"
	"github.com/mars/marspi-core/llm"
	"github.com/mars/marspi-graph/agentspec"
	"github.com/mars/marspi-graph/checkpoint"
	"github.com/mars/marspi-graph/graph"
)

func TestPersistSessionRoundTrip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "mock",
			"choices": []any{map[string]any{
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "turn-ok"},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
	defer srv.Close()

	store := checkpoint.NewDurableMemory()
	provider := llm.NewProvider("gpt-4o", srv.URL, "k")

	b := graph.NewBuilder()
	b.AddNode("worker", func(ctx context.Context, s graph.State) (graph.Update, error) {
		inst := agentspec.New(agentspec.Spec{
			ID:           "worker",
			SystemPrompt: "sys",
			Provider:     provider,
			MaxIter:      2,
			Persist:      agentspec.PersistSession,
			Store:        store,
		})
		out, err := inst.RunOnce(ctx, "hello")
		if err != nil {
			return nil, err
		}
		return graph.Update{"out": out}, nil
	})
	b.SetEntry("worker")
	b.AddEdge("worker", graph.END)

	g, err := b.Compile(graph.WithDurableCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}
	out, err := g.Invoke(context.Background(), graph.State{}, graph.WithThreadID("p1"))
	if err != nil {
		t.Fatal(err)
	}
	if out.GetString("out") != "turn-ok" {
		t.Fatalf("out=%v", out)
	}

	latest, ok, err := store.GetLatest(context.Background(), "p1")
	if err != nil || !ok {
		t.Fatalf("latest ok=%v err=%v", ok, err)
	}
	raw, ok, err := store.GetAgentSession(context.Background(), "p1", latest.CheckpointID, "worker")
	if err != nil || !ok {
		t.Fatalf("session ok=%v err=%v", ok, err)
	}
	var msgs []map[string]any
	if err := json.Unmarshal(raw, &msgs); err != nil {
		t.Fatal(err)
	}
	if len(msgs) < 2 {
		t.Fatalf("msgs=%v", msgs)
	}

	ctx := graph.WithThreadIDCtx(context.Background(), "p1")
	ctx = graph.WithParentCheckpointID(ctx, latest.CheckpointID)
	ctx, _ = graph.WithStepArtifacts(ctx)
	inst := agentspec.New(agentspec.Spec{
		ID:           "worker",
		SystemPrompt: "sys-should-not-dup",
		Provider:     provider,
		MaxIter:      2,
		Persist:      agentspec.PersistSession,
		Store:        store,
	})
	if _, err := inst.RunOnce(ctx, "again"); err != nil {
		t.Fatal(err)
	}
	sysCount := 0
	for _, m := range inst.Manager().Messages {
		if m["role"] == "system" {
			sysCount++
			if m["content"] == "sys-should-not-dup" {
				t.Fatal("system prompt duplicated after load")
			}
		}
	}
	if sysCount != 1 {
		t.Fatalf("sysCount=%d msgs=%v", sysCount, inst.Manager().Messages)
	}
}

func TestCorrelationIDsOnAgentEvents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "mock",
			"choices": []any{map[string]any{
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "ok"},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
	defer srv.Close()

	em := agent.NewEmitter()
	var seen agent.Event
	em.Subscribe(func(ev agent.Event) {
		if ev.RunID != "" {
			seen = ev
		}
	})

	store := checkpoint.NewDurableMemory()
	b := graph.NewBuilder()
	b.AddNode("coder", func(ctx context.Context, s graph.State) (graph.Update, error) {
		inst := agentspec.New(agentspec.Spec{
			ID:       "coder",
			Provider: llm.NewProvider("gpt-4o", srv.URL, "k"),
			MaxIter:  2,
			Events:   em,
		})
		_, err := inst.RunOnce(ctx, "x")
		return graph.Update{"ok": true}, err
	})
	b.SetEntry("coder")
	b.AddEdge("coder", graph.END)
	g, err := b.Compile(graph.WithDurableCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.Invoke(context.Background(), graph.State{}, graph.WithThreadID("c1"))
	if err != nil {
		t.Fatal(err)
	}
	if seen.RunID == "" || seen.NodeID != "coder" || seen.AgentID != "coder" {
		t.Fatalf("correlation missing: %+v", seen)
	}
}
