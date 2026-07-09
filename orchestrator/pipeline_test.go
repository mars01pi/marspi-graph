package orchestrator_test

import (
	"context"
	"testing"

	"github.com/mars/marspi-graph/graph"
	"github.com/mars/marspi-graph/orchestrator"
)

func TestPipeline(t *testing.T) {
	g, err := orchestrator.Pipeline(map[string]graph.NodeFunc{
		"a": func(ctx context.Context, s graph.State) (graph.Update, error) {
			return graph.Update{"v": "A"}, nil
		},
		"b": func(ctx context.Context, s graph.State) (graph.Update, error) {
			return graph.Update{"v": s.GetString("v") + "B"}, nil
		},
	}, []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := g.Invoke(context.Background(), graph.State{})
	if err != nil {
		t.Fatal(err)
	}
	if out.GetString("v") != "AB" {
		t.Fatalf("got %q", out.GetString("v"))
	}
}

func TestCodingLoopRequiresProvider(t *testing.T) {
	_, err := orchestrator.RunCodingLoop(context.Background(), orchestrator.CodingLoopConfig{
		Goal: "x",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}
