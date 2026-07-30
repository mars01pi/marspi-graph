package graph_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mars/marspi-graph/checkpoint"
	"github.com/mars/marspi-graph/graph"
)

func TestRetryEventuallySucceeds(t *testing.T) {
	var calls atomic.Int32
	b := graph.NewBuilder()
	b.AddNode("flaky", func(ctx context.Context, s graph.State) (graph.Update, error) {
		n := calls.Add(1)
		if n < 3 {
			return nil, fmt.Errorf("transient %d", n)
		}
		return graph.Update{"ok": true, "attempts": int(n)}, nil
	}, graph.WithRetry(graph.RetryPolicy{
		MaxAttempts:     3,
		InitialInterval: time.Millisecond,
		Jitter:          false,
	}))
	b.SetEntry("flaky")
	b.AddEdge("flaky", graph.END)

	g, err := b.Compile(graph.WithCheckpointer(checkpoint.NewMemory()))
	if err != nil {
		t.Fatal(err)
	}
	out, err := g.Invoke(context.Background(), graph.State{})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls=%d", calls.Load())
	}
	if out["ok"] != true {
		t.Fatalf("state=%v", out)
	}
}

func TestRetryExhaustedBubbles(t *testing.T) {
	b := graph.NewBuilder()
	b.AddNode("bad", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return nil, errors.New("boom")
	}, graph.WithRetry(graph.RetryPolicy{
		MaxAttempts:     2,
		InitialInterval: time.Millisecond,
		Jitter:          false,
	}))
	b.SetEntry("bad")
	b.AddEdge("bad", graph.END)

	g, err := b.Compile(graph.WithCheckpointer(checkpoint.NewMemory()))
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.Invoke(context.Background(), graph.State{})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("want bubble boom, got %v", err)
	}
}

func TestOnErrorDivertSoftComplete(t *testing.T) {
	b := graph.NewBuilder()
	b.AddNode("bad", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return nil, errors.New("hard fail")
	}, graph.WithRetry(graph.RetryPolicy{
		MaxAttempts:     2,
		InitialInterval: time.Millisecond,
		Jitter:          false,
	}))
	b.AddNode("sink", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"handled": true}, nil
	})
	b.SetEntry("bad")
	b.AddEdge("bad", "sink")
	b.SetOnError("bad", "sink")
	b.AddEdge("sink", graph.END)

	g, err := b.Compile(graph.WithCheckpointer(checkpoint.NewMemory()))
	if err != nil {
		t.Fatal(err)
	}
	out, err := g.Invoke(context.Background(), graph.State{})
	if err != nil {
		t.Fatalf("soft complete want nil err, got %v", err)
	}
	if out.GetString(graph.StateKeyStatus) != graph.StatusFailed {
		t.Fatalf("status=%q", out.GetString(graph.StateKeyStatus))
	}
	if out.GetString(graph.StateKeyErrorNode) != "bad" {
		t.Fatalf("error_node=%q", out.GetString(graph.StateKeyErrorNode))
	}
	if !strings.Contains(out.GetString(graph.StateKeyLastError), "hard fail") {
		t.Fatalf("last_error=%q", out.GetString(graph.StateKeyLastError))
	}
	if out["handled"] != true {
		t.Fatalf("sink not run: %v", out)
	}
}

func TestOnErrorToEND(t *testing.T) {
	b := graph.NewBuilder()
	b.AddNode("bad", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return nil, errors.New("x")
	})
	b.SetEntry("bad")
	b.AddEdge("bad", graph.END)
	b.SetOnError("bad", graph.END)

	g, err := b.Compile(graph.WithCheckpointer(checkpoint.NewMemory()))
	if err != nil {
		t.Fatal(err)
	}
	out, err := g.Invoke(context.Background(), graph.State{})
	if err != nil {
		t.Fatal(err)
	}
	if out.GetString(graph.StateKeyStatus) != graph.StatusFailed {
		t.Fatalf("status=%q", out.GetString(graph.StateKeyStatus))
	}
}

func TestNeverRetryInterrupt(t *testing.T) {
	var calls atomic.Int32
	b := graph.NewBuilder()
	b.AddNode("gate", func(ctx context.Context, s graph.State) (graph.Update, error) {
		calls.Add(1)
		return nil, graph.Interrupt("need")
	}, graph.WithRetry(graph.RetryPolicy{
		MaxAttempts:     5,
		InitialInterval: time.Millisecond,
	}))
	b.SetEntry("gate")
	b.AddEdge("gate", graph.END)

	g, err := b.Compile(graph.WithCheckpointer(checkpoint.NewMemory()))
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.Invoke(context.Background(), graph.State{}, graph.WithThreadID("t-int"))
	if !graph.IsInterrupted(err) {
		t.Fatalf("want interrupt, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("interrupt must not retry, calls=%d", calls.Load())
	}
}

func TestNeverRetryCanceled(t *testing.T) {
	var calls atomic.Int32
	b := graph.NewBuilder()
	b.AddNode("n", func(ctx context.Context, s graph.State) (graph.Update, error) {
		calls.Add(1)
		return nil, context.Canceled
	}, graph.WithRetry(graph.RetryPolicy{MaxAttempts: 4, InitialInterval: time.Millisecond}))
	b.SetEntry("n")
	b.AddEdge("n", graph.END)

	g, err := b.Compile(graph.WithCheckpointer(checkpoint.NewMemory()))
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.Invoke(context.Background(), graph.State{})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls.Load() != 1 {
		t.Fatalf("canceled must not retry, calls=%d", calls.Load())
	}
}

func TestRetryDoesNotBurnMaxSteps(t *testing.T) {
	var calls atomic.Int32
	b := graph.NewBuilder()
	b.AddNode("flaky", func(ctx context.Context, s graph.State) (graph.Update, error) {
		n := calls.Add(1)
		if n < 3 {
			return nil, fmt.Errorf("again")
		}
		return graph.Update{"done": true}, nil
	}, graph.WithRetry(graph.RetryPolicy{
		MaxAttempts:     3,
		InitialInterval: time.Millisecond,
		Jitter:          false,
	}))
	b.SetEntry("flaky")
	b.AddEdge("flaky", graph.END)

	g, err := b.Compile(graph.WithCheckpointer(checkpoint.NewMemory()))
	if err != nil {
		t.Fatal(err)
	}
	// MaxSteps 2 allows one successful leave; 3 attempts must not each burn a step.
	out, err := g.Invoke(context.Background(), graph.State{}, graph.WithMaxSteps(2))
	if err != nil {
		t.Fatal(err)
	}
	if out["done"] != true {
		t.Fatalf("%v", out)
	}
}

func TestAttemptContext(t *testing.T) {
	var seen []int
	b := graph.NewBuilder()
	b.AddNode("flaky", func(ctx context.Context, s graph.State) (graph.Update, error) {
		seen = append(seen, graph.Attempt(ctx))
		if len(seen) < 2 {
			return nil, errors.New("once")
		}
		return nil, nil
	}, graph.WithRetry(graph.RetryPolicy{MaxAttempts: 2, InitialInterval: time.Millisecond, Jitter: false}))
	b.SetEntry("flaky")
	b.AddEdge("flaky", graph.END)

	g, err := b.Compile(graph.WithCheckpointer(checkpoint.NewMemory()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Invoke(context.Background(), graph.State{}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[0] != 1 || seen[1] != 2 {
		t.Fatalf("attempts=%v", seen)
	}
}

func TestDefaultRetryOn(t *testing.T) {
	if graph.DefaultRetryOn(errors.New("x")) != true {
		t.Fatal("transient should retry")
	}
	if graph.DefaultRetryOn(context.Canceled) != false {
		t.Fatal("canceled must not retry")
	}
	if graph.NeverRetry(graph.Interrupt("x")) != true {
		t.Fatal("interrupt never")
	}
}
