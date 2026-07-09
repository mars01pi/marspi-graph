package agentspec_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mars/marspi-core/llm"
	"github.com/mars/marspi-graph/agentspec"
	"github.com/mars/marspi-graph/graph"
)

func TestRunOnceSuccess(t *testing.T) {
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

	inst := agentspec.New(agentspec.Spec{
		ID:       "t",
		Provider: llm.NewProvider("gpt-4o", srv.URL, "k"),
		MaxIter:  3,
	})
	out, err := inst.RunOnce(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	if out != "ok" {
		t.Fatalf("out=%q", out)
	}
}

func TestRunOnceCancel(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	defer srv.Close()
	defer close(block)

	inst := agentspec.New(agentspec.Spec{
		ID:       "t",
		Provider: llm.NewProvider("gpt-4o", srv.URL, "k"),
		MaxIter:  3,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := inst.RunOnce(ctx, "slow")
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled, got %v", err)
	}
}

func TestAsNodePropagatesError(t *testing.T) {
	inst := agentspec.New(agentspec.Spec{
		ID:       "t",
		Provider: llm.NewProvider("gpt-4o", "http://127.0.0.1:1", ""), // missing key
		MaxIter:  1,
	})
	fn := inst.AsNode("input", "output")
	_, err := fn(context.Background(), graph.State{"input": "x"})
	if err == nil {
		t.Fatal("expected error")
	}
}
