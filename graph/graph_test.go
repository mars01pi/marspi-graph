package graph_test

import (
	"context"
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
