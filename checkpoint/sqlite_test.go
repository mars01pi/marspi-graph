package checkpoint_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mars/marspi-graph/checkpoint"
	"github.com/mars/marspi-graph/graph"
)

func TestSQLitePutGetRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.db")
	s, err := checkpoint.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	_, ok, err := s.Get(ctx, "t1")
	if err != nil || ok {
		t.Fatalf("empty: ok=%v err=%v", ok, err)
	}

	in := graph.Snapshot{
		ThreadID: "t1",
		Node:     "coder",
		Step:     3,
		Interrupt: true,
		InterruptValue: map[string]any{
			"worker": "coder",
			"task":   "add comment",
			"n":      2,
		},
		State: graph.State{
			"goal":     "ship it",
			"messages": []any{"a", "b"},
			"handoff": map[string]any{
				"from": "supervisor",
				"to":   "coder",
			},
			"step": 1,
		},
	}
	if err := s.Put(ctx, "t1", in); err != nil {
		t.Fatal(err)
	}

	got, ok, err := s.Get(ctx, "t1")
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if got.Node != "coder" || got.Step != 3 || !got.Interrupt || got.ThreadID != "t1" {
		t.Fatalf("meta: %+v", got)
	}
	if got.State.GetString("goal") != "ship it" {
		t.Fatalf("state=%v", got.State)
	}
	iv, ok := got.InterruptValue.(map[string]any)
	if !ok || iv["worker"] != "coder" {
		t.Fatalf("interrupt_value=%T %#v", got.InterruptValue, got.InterruptValue)
	}
	// JSON numbers become float64
	if n, ok := iv["n"].(float64); !ok || n != 2 {
		t.Fatalf("n=%v (%T)", iv["n"], iv["n"])
	}
}

func TestSQLiteOverwriteAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.db")
	ctx := context.Background()

	s1, err := checkpoint.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Put(ctx, "t", graph.Snapshot{
		Node:  "a",
		Step:  1,
		State: graph.State{"x": "1"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s1.Put(ctx, "t", graph.Snapshot{
		Node:  "b",
		Step:  2,
		State: graph.State{"x": "2"},
	}); err != nil {
		t.Fatal(err)
	}
	_ = s1.Close()

	s2, err := checkpoint.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	got, ok, err := s2.Get(ctx, "t")
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if got.Node != "b" || got.Step != 2 || got.State.GetString("x") != "2" {
		t.Fatalf("%+v", got)
	}
}

func TestSQLiteDeleteAndListInterrupted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.db")
	s, err := checkpoint.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	_ = s.Put(ctx, "ok", graph.Snapshot{Node: "n", Step: 1, State: graph.State{}})
	_ = s.Put(ctx, "hitl", graph.Snapshot{
		Node: "coder", Step: 2, Interrupt: true,
		InterruptValue: map[string]any{"worker": "coder"},
		State:          graph.State{"goal": "x"},
	})

	list, err := s.ListInterrupted(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ThreadID != "hitl" || list[0].Node != "coder" {
		t.Fatalf("%+v", list)
	}

	if err := s.Delete(ctx, "hitl"); err != nil {
		t.Fatal(err)
	}
	_, ok, err := s.Get(ctx, "hitl")
	if err != nil || ok {
		t.Fatalf("deleted: ok=%v err=%v", ok, err)
	}
	list, err = s.ListInterrupted(ctx)
	if err != nil || len(list) != 0 {
		t.Fatalf("after delete: %+v err=%v", list, err)
	}
}

func TestSQLiteEmptyPath(t *testing.T) {
	_, err := checkpoint.OpenSQLite("")
	if err == nil {
		t.Fatal("expected error")
	}
}
