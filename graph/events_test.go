package graph_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/mars/marspi-graph/checkpoint"
	"github.com/mars/marspi-graph/graph"
)

func TestEventOrderingSuccessAndInterrupt(t *testing.T) {
	var mu sync.Mutex
	var types []graph.EventType

	b := graph.NewBuilder()
	b.AddNode("a", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"n": 1}, nil
	})
	b.AddNode("gate", func(ctx context.Context, s graph.State) (graph.Update, error) {
		_, err := graph.InterruptOrResume(ctx, map[string]any{"need": "approve"})
		if err != nil {
			return nil, err
		}
		return graph.Update{"ok": true}, nil
	})
	b.SetEntry("a")
	b.AddEdge("a", "gate")
	b.AddEdge("gate", graph.END)

	cp := checkpoint.NewDurableMemory()
	g, err := b.Compile(graph.WithDurableCheckpointer(cp))
	if err != nil {
		t.Fatal(err)
	}

	handler := func(_ context.Context, e graph.Event) {
		mu.Lock()
		types = append(types, e.Type)
		mu.Unlock()
	}

	_, err = g.Invoke(context.Background(), graph.State{}, graph.WithThreadID("ev1"), graph.WithEventHandler(handler))
	if !graph.IsInterrupted(err) {
		t.Fatalf("want interrupt, got %v", err)
	}

	mu.Lock()
	got := append([]graph.EventType(nil), types...)
	mu.Unlock()

	wantPrefix := []graph.EventType{
		graph.EventRunStart,
		graph.EventNodeStart, // a
		graph.EventNodeEnd,
		graph.EventRoute,
		graph.EventCheckpoint,
		graph.EventNodeStart, // gate
		graph.EventCheckpoint,
		graph.EventInterrupt,
		graph.EventRunEnd,
	}
	if len(got) < len(wantPrefix) {
		t.Fatalf("events=%v", got)
	}
	for i, w := range wantPrefix {
		if got[i] != w {
			t.Fatalf("event[%d]=%s want %s full=%v", i, got[i], w, got)
		}
	}
	if got[len(got)-1] != graph.EventRunEnd {
		t.Fatalf("last=%s", got[len(got)-1])
	}

	types = nil
	_, err = g.Resume(context.Background(), "ev1",
		graph.WithCommand(graph.Command{Resume: true}),
		graph.WithEventHandler(handler),
	)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got = append([]graph.EventType(nil), types...)
	mu.Unlock()
	if got[0] != graph.EventRunResume {
		t.Fatalf("resume first=%s full=%v", got[0], got)
	}
	if got[len(got)-1] != graph.EventRunEnd {
		t.Fatalf("resume last=%s", got[len(got)-1])
	}
}

func TestEventHandlerPanicRecovered(t *testing.T) {
	b := graph.NewBuilder()
	b.AddNode("a", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"ok": true}, nil
	})
	b.SetEntry("a")
	b.AddEdge("a", graph.END)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.Invoke(context.Background(), graph.State{}, graph.WithEventHandler(func(context.Context, graph.Event) {
		panic("boom")
	}))
	if err != nil {
		t.Fatalf("panic should not fail run: %v", err)
	}
}

func TestDurableResumeParentChain(t *testing.T) {
	cp := checkpoint.NewDurableMemory()
	b := graph.NewBuilder()
	b.AddNode("a", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"v": "a"}, nil
	})
	b.AddNode("b", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return graph.Update{"v": s.GetString("v") + "b"}, nil
	})
	b.SetEntry("a")
	b.AddEdge("a", "b")
	b.AddEdge("b", graph.END)
	g, err := b.Compile(graph.WithDurableCheckpointer(cp))
	if err != nil {
		t.Fatal(err)
	}
	out, err := g.Invoke(context.Background(), graph.State{}, graph.WithThreadID("d1"))
	if err != nil {
		t.Fatal(err)
	}
	if out.GetString("v") != "ab" {
		t.Fatalf("v=%q", out.GetString("v"))
	}
	list, err := cp.List(context.Background(), "d1", 0, 0)
	if err != nil || len(list) < 2 {
		t.Fatalf("list=%v err=%v", list, err)
	}
}

func TestNodeErrorEmitsRunEnd(t *testing.T) {
	var last graph.Event
	b := graph.NewBuilder()
	b.AddNode("a", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return nil, errors.New("boom")
	})
	b.SetEntry("a")
	b.AddEdge("a", graph.END)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.Invoke(context.Background(), graph.State{}, graph.WithEventHandler(func(_ context.Context, e graph.Event) {
		last = e
	}))
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err=%v", err)
	}
	if last.Type != graph.EventRunEnd || last.Metadata["status"] != "error" {
		t.Fatalf("last=%+v", last)
	}
}
