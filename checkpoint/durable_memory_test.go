package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/mars/marspi-graph/graph"
)

func TestDurableMemoryHistoryCASAndPrune(t *testing.T) {
	ctx := context.Background()
	m := NewDurableMemory()

	s1 := graph.Snapshot{
		CheckpointID: "c1",
		ThreadID:     "t1",
		Node:         "a",
		Step:         1,
		State:        graph.State{"v": 1},
	}
	if err := m.CommitStep(ctx, s1, nil); err != nil {
		t.Fatal(err)
	}

	// Stale parent must conflict.
	bad := graph.Snapshot{
		CheckpointID:       "c2",
		ParentCheckpointID: "",
		ThreadID:           "t1",
		Node:               "b",
		Step:               2,
		State:              graph.State{"v": 2},
	}
	if err := m.CommitStep(ctx, bad, nil); !errors.Is(err, graph.ErrCheckpointConflict) {
		t.Fatalf("want conflict, got %v", err)
	}

	msgs, _ := json.Marshal([]map[string]any{{"role": "user", "content": "hi"}})
	s2 := graph.Snapshot{
		CheckpointID:       "c2",
		ParentCheckpointID: "c1",
		ThreadID:           "t1",
		Node:               "b",
		Step:               2,
		State:              graph.State{"v": 2},
	}
	if err := m.CommitStep(ctx, s2, []graph.StepArtifact{{
		Kind: graph.ArtifactAgentSession, Key: "worker", Payload: msgs,
	}}); err != nil {
		t.Fatal(err)
	}

	// Idempotent retry.
	if err := m.CommitStep(ctx, s2, nil); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}

	got, ok, err := m.GetLatest(ctx, "t1")
	if err != nil || !ok || got.CheckpointID != "c2" {
		t.Fatalf("latest=%v ok=%v err=%v", got.CheckpointID, ok, err)
	}

	raw, ok, err := m.GetAgentSession(ctx, "t1", "c2", "worker")
	if err != nil || !ok {
		t.Fatalf("session ok=%v err=%v", ok, err)
	}
	if string(raw) != string(msgs) {
		t.Fatalf("messages=%s", raw)
	}

	// Parent refs copied when agent unchanged.
	s3 := graph.Snapshot{
		CheckpointID:       "c3",
		ParentCheckpointID: "c2",
		ThreadID:           "t1",
		Node:               graph.END,
		Step:               3,
		State:              graph.State{"v": 3},
	}
	if err := m.CommitStep(ctx, s3, nil); err != nil {
		t.Fatal(err)
	}
	raw, ok, err = m.GetAgentSession(ctx, "t1", "c3", "worker")
	if err != nil || !ok || string(raw) != string(msgs) {
		t.Fatalf("copied session ok=%v raw=%s err=%v", ok, raw, err)
	}

	list, err := m.List(ctx, "t1", 0, 10)
	if err != nil || len(list) != 3 {
		t.Fatalf("list len=%d err=%v", len(list), err)
	}

	if err := m.Prune(ctx, "t1", 1); err != nil {
		t.Fatal(err)
	}
	list, err = m.List(ctx, "t1", 0, 10)
	if err != nil || len(list) != 1 || list[0].CheckpointID != "c3" {
		t.Fatalf("after prune list=%+v err=%v", list, err)
	}
	// Session still referenced by c3.
	raw, ok, err = m.GetAgentSession(ctx, "t1", "c3", "worker")
	if err != nil || !ok {
		t.Fatalf("pruned live session missing ok=%v err=%v", ok, err)
	}
	_ = raw
}

func TestDurableMemoryLegacyPutGet(t *testing.T) {
	m := NewDurableMemory()
	ctx := context.Background()
	if err := m.Put(ctx, "t", graph.Snapshot{Node: "n", Step: 1, State: graph.State{"a": 1}}); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, "t", graph.Snapshot{Node: "m", Step: 2, State: graph.State{"a": 2}}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := m.Get(ctx, "t")
	if err != nil || !ok || got.Step != 2 || got.Node != "m" {
		t.Fatalf("got=%+v ok=%v err=%v", got, ok, err)
	}
	list, err := m.List(ctx, "t", 0, 0)
	if err != nil || len(list) != 2 {
		t.Fatalf("history len=%d err=%v", len(list), err)
	}
}
