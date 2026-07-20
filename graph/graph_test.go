package graph_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mars/marspi-graph/checkpoint"
	"github.com/mars/marspi-graph/graph"
)

func TestLinearGraph(t *testing.T) {
	b := graph.NewBuilder()
	b.AddNode("a", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"v": s.GetString("v") + "A"}, nil
	})
	b.AddNode("b", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"v": s.GetString("v") + "B"}, nil
	})
	b.SetEntry("a")
	b.AddEdge("a", "b")
	b.AddEdge("b", graph.END)

	g, err := b.Compile(graph.WithCheckpointer(checkpoint.NewMemory()))
	if err != nil {
		t.Fatal(err)
	}
	out, err := g.Invoke(context.Background(), graph.State{"v": ""}, graph.WithThreadID("t1"))
	if err != nil {
		t.Fatal(err)
	}
	if out.GetString("v") != "AB" {
		t.Fatalf("got %q", out.GetString("v"))
	}
}

func TestConditionalGraph(t *testing.T) {
	b := graph.NewBuilder()
	b.AddNode("decide", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"n": 1}, nil
	})
	b.AddNode("ok", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"path": "ok"}, nil
	})
	b.AddNode("fail", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"path": "fail"}, nil
	})
	b.SetEntry("decide")
	b.AddConditionalEdges("decide", func(ctx context.Context, s graph.State) string {
		if n, ok := s["n"].(int); ok && n > 0 {
			return "ok"
		}
		return "fail"
	})
	b.AddEdge("ok", graph.END)
	b.AddEdge("fail", graph.END)

	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := g.Invoke(context.Background(), graph.State{})
	if err != nil {
		t.Fatal(err)
	}
	if out.GetString("path") != "ok" {
		t.Fatalf("path=%q", out.GetString("path"))
	}
}

func TestAppendReducer(t *testing.T) {
	b := graph.NewBuilder()
	b.AddReducer("logs", graph.AppendSlice)
	b.AddNode("a", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"logs": "a"}, nil
	})
	b.AddNode("b", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"logs": []any{"b", "c"}}, nil
	})
	b.SetEntry("a")
	b.AddEdge("a", "b")
	b.AddEdge("b", graph.END)

	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := g.Invoke(context.Background(), graph.State{})
	if err != nil {
		t.Fatal(err)
	}
	logs, ok := out["logs"].([]any)
	if !ok {
		t.Fatalf("logs type %T", out["logs"])
	}
	if len(logs) != 3 || logs[0] != "a" || logs[1] != "b" || logs[2] != "c" {
		t.Fatalf("logs=%v", logs)
	}
}

func TestInterruptAndResume(t *testing.T) {
	cp := checkpoint.NewMemory()
	b := graph.NewBuilder()
	b.AddNode("ask", func(ctx context.Context, s graph.State) (graph.Update, error) {
		v, err := graph.InterruptOrResume(ctx, "need-approval")
		if err != nil {
			return nil, err
		}
		approved, _ := v.(string)
		return graph.Update{"decision": approved}, nil
	})
	b.AddNode("done", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"ok": true}, nil
	})
	b.SetEntry("ask")
	b.AddEdge("ask", "done")
	b.AddEdge("done", graph.END)

	g, err := b.Compile(graph.WithCheckpointer(cp))
	if err != nil {
		t.Fatal(err)
	}

	_, err = g.Invoke(context.Background(), graph.State{}, graph.WithThreadID("hitl-001"))
	if !graph.IsInterrupted(err) {
		t.Fatalf("want interrupt, got %v", err)
	}
	ie, ok := graph.AsInterrupt(err)
	if !ok || ie.Value != "need-approval" || ie.Node != "ask" {
		t.Fatalf("interrupt=%#v ok=%v", ie, ok)
	}

	out, err := g.Resume(context.Background(), "hitl-001", graph.WithCommand(graph.Command{
		Resume: "approve",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if out.GetString("decision") != "approve" {
		t.Fatalf("decision=%q", out.GetString("decision"))
	}
	if out["ok"] != true {
		t.Fatalf("ok=%v", out["ok"])
	}
}

func TestExpectedCheckpointMismatch(t *testing.T) {
	cp := checkpoint.NewMemory()
	b := graph.NewBuilder()
	b.AddNode("ask", func(ctx context.Context, s graph.State) (graph.Update, error) {
		v, err := graph.InterruptOrResume(ctx, "need-approval")
		if err != nil {
			return nil, err
		}
		return graph.Update{"decision": v}, nil
	})
	b.AddNode("done", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"ok": true}, nil
	})
	b.SetEntry("ask")
	b.AddEdge("ask", "done")
	b.AddEdge("done", graph.END)

	g, err := b.Compile(graph.WithCheckpointer(cp))
	if err != nil {
		t.Fatal(err)
	}

	_, err = g.Invoke(context.Background(), graph.State{}, graph.WithThreadID("exp-cp"))
	if !graph.IsInterrupted(err) {
		t.Fatalf("want interrupt, got %v", err)
	}
	snap, ok, err := cp.Get(context.Background(), "exp-cp")
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	staleID := snap.CheckpointID

	// Advance the tip with a second writer so expected becomes stale.
	_, err = g.Resume(context.Background(), "exp-cp", graph.WithCommand(graph.Command{Resume: "first"}))
	if err != nil {
		t.Fatal(err)
	}

	_, err = g.Resume(context.Background(), "exp-cp",
		graph.WithCommand(graph.Command{Resume: "second"}),
		graph.WithExpectedCheckpointID(staleID),
	)
	if !errors.Is(err, graph.ErrExpectedCheckpointMismatch) {
		t.Fatalf("want ErrExpectedCheckpointMismatch, got %v", err)
	}
}

func TestResumeWithoutCheckpoint(t *testing.T) {
	b := graph.NewBuilder()
	b.AddNode("a", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{}, nil
	})
	b.SetEntry("a")
	b.AddEdge("a", graph.END)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.Resume(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestResumeFromMidRun(t *testing.T) {
	cp := checkpoint.NewMemory()
	b := graph.NewBuilder()
	b.AddNode("a", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"v": "A"}, nil
	})
	b.AddNode("b", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"v": s.GetString("v") + "B"}, nil
	})
	b.SetEntry("a")
	b.AddEdge("a", "b")
	b.AddEdge("b", graph.END)

	g, err := b.Compile(graph.WithCheckpointer(cp))
	if err != nil {
		t.Fatal(err)
	}

	// Simulate crash after node a: checkpoint says next is b.
	if err := cp.Put(context.Background(), "mid", graph.Snapshot{
		ThreadID: "mid",
		Node:     "b",
		State:    graph.State{"v": "A"},
		Step:     1,
	}); err != nil {
		t.Fatal(err)
	}

	out, err := g.Resume(context.Background(), "mid")
	if err != nil {
		t.Fatal(err)
	}
	if out.GetString("v") != "AB" {
		t.Fatalf("got %q", out.GetString("v"))
	}
}

func TestInterruptRequiresCheckpointer(t *testing.T) {
	b := graph.NewBuilder()
	b.AddNode("ask", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return nil, graph.Interrupt("need")
	})
	b.SetEntry("ask")
	b.AddEdge("ask", graph.END)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.Invoke(context.Background(), graph.State{})
	if err == nil || !strings.Contains(err.Error(), "requires a checkpointer") {
		t.Fatalf("want checkpointer error, got %v", err)
	}
}

func TestResumeInterruptedWithoutCommand(t *testing.T) {
	cp := checkpoint.NewMemory()
	b := graph.NewBuilder()
	b.AddNode("ask", func(ctx context.Context, s graph.State) (graph.Update, error) {
		v, err := graph.InterruptOrResume(ctx, "need")
		if err != nil {
			return nil, err
		}
		return graph.Update{"v": v}, nil
	})
	b.SetEntry("ask")
	b.AddEdge("ask", graph.END)
	g, err := b.Compile(graph.WithCheckpointer(cp))
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.Invoke(context.Background(), graph.State{}, graph.WithThreadID("hitl2"))
	if !graph.IsInterrupted(err) {
		t.Fatalf("want interrupt, got %v", err)
	}
	_, err = g.Resume(context.Background(), "hitl2")
	if err == nil || !strings.Contains(err.Error(), "Resume") {
		t.Fatalf("want resume command error, got %v", err)
	}
}

func TestCommandUpdateAndGoto(t *testing.T) {
	cp := checkpoint.NewMemory()
	b := graph.NewBuilder()
	b.AddNode("a", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"v": "A"}, nil
	})
	b.AddNode("b", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"v": s.GetString("v") + "B"}, nil
	})
	b.AddNode("c", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"v": s.GetString("v") + "C"}, nil
	})
	b.SetEntry("a")
	b.AddEdge("a", "b")
	b.AddEdge("b", "c")
	b.AddEdge("c", graph.END)

	g, err := b.Compile(graph.WithCheckpointer(cp))
	if err != nil {
		t.Fatal(err)
	}
	if err := cp.Put(context.Background(), "cmd", graph.Snapshot{
		ThreadID: "cmd",
		Node:     "b",
		State:    graph.State{"v": "A"},
		Step:     1,
	}); err != nil {
		t.Fatal(err)
	}
	out, err := g.Resume(context.Background(), "cmd", graph.WithCommand(graph.Command{
		Update: graph.Update{"v": "X"},
		Goto:   "c",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if out.GetString("v") != "XC" {
		t.Fatalf("got %q", out.GetString("v"))
	}
}

func TestMaxSteps(t *testing.T) {
	b := graph.NewBuilder()
	b.AddNode("a", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"n": stateIntTest(s) + 1}, nil
	})
	b.SetEntry("a")
	b.AddEdge("a", "a") // self-loop
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.Invoke(context.Background(), graph.State{}, graph.WithMaxSteps(3))
	if err == nil || !strings.Contains(err.Error(), "max steps") {
		t.Fatalf("want max steps error, got %v", err)
	}
}

func stateIntTest(s graph.State) int {
	switch v := s["n"].(type) {
	case int:
		return v
	default:
		return 0
	}
}
