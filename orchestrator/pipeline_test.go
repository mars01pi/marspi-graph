package orchestrator_test

import (
	"context"
	"testing"

	"github.com/mars/marspi-graph/checkpoint"
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

// TestPipelineInterruptResume shows HITL mid-pipeline:
// read → approve(gate) → write. Invoke pauses at approve; Resume continues to write.
func TestPipelineInterruptResume(t *testing.T) {
	cp := checkpoint.NewMemory()
	g, err := orchestrator.Pipeline(map[string]graph.NodeFunc{
		"read": func(ctx context.Context, s graph.State) (graph.Update, error) {
			return graph.Update{"draft": "hello"}, nil
		},
		"approve": func(ctx context.Context, s graph.State) (graph.Update, error) {
			// First visit: Interrupt. After Resume(Command{Resume: true}): continue.
			v, err := graph.InterruptOrResume(ctx, map[string]any{
				"action": "publish",
				"draft":  s.GetString("draft"),
			})
			if err != nil {
				return nil, err
			}
			if v != true {
				return nil, graph.Interrupt(map[string]any{"denied": true})
			}
			return graph.Update{"approved": true}, nil
		},
		"write": func(ctx context.Context, s graph.State) (graph.Update, error) {
			return graph.Update{"out": s.GetString("draft") + "!"}, nil
		},
	}, []string{"read", "approve", "write"}, graph.WithCheckpointer(cp))
	if err != nil {
		t.Fatal(err)
	}

	thread := "pipeline-hitl"
	_, err = g.Invoke(context.Background(), graph.State{}, graph.WithThreadID(thread))
	if !graph.IsInterrupted(err) {
		t.Fatalf("want interrupt at approve, got %v", err)
	}

	snap, ok, err := cp.Get(context.Background(), thread)
	if err != nil || !ok || snap.Node != "approve" || !snap.Interrupt {
		t.Fatalf("checkpoint=%+v ok=%v err=%v", snap, ok, err)
	}
	if snap.State.GetString("draft") != "hello" {
		t.Fatalf("draft should survive interrupt, state=%v", snap.State)
	}

	out, err := g.Resume(context.Background(), thread, graph.WithCommand(graph.Command{Resume: true}))
	if err != nil {
		t.Fatal(err)
	}
	if out.GetString("out") != "hello!" || out["approved"] != true {
		t.Fatalf("after resume got %v", out)
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
