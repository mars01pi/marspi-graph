package checkpoint_test

import (
	"context"
	"testing"

	"github.com/mars/marspi-graph/checkpoint"
	"github.com/mars/marspi-graph/graph"
)

func TestMemoryPutGet(t *testing.T) {
	m := checkpoint.NewMemory()
	_, ok, err := m.Get(context.Background(), "t")
	if err != nil || ok {
		t.Fatalf("empty get: ok=%v err=%v", ok, err)
	}
	if err := m.Put(context.Background(), "t", graph.Snapshot{
		Node:  "a",
		State: graph.State{"x": 1},
		Step:  2,
	}); err != nil {
		t.Fatal(err)
	}
	snap, ok, err := m.Get(context.Background(), "t")
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if snap.Node != "a" || snap.Step != 2 || snap.ThreadID != "t" {
		t.Fatalf("%+v", snap)
	}
}
