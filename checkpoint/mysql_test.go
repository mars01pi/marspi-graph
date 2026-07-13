package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/mars/marspi-graph/graph"
)

func TestMySQLDurableIntegration(t *testing.T) {
	dsn := os.Getenv("MARSPI_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:123456@tcp(127.0.0.1:3306)/marspi?parseTime=true&loc=UTC"
	}
	m, err := OpenMySQL(dsn)
	if err != nil {
		t.Skipf("MySQL unavailable (%v); set MARSPI_MYSQL_DSN or start docker/mysql.sh", err)
	}
	defer m.Close()
	ctx := context.Background()
	thread := "test-" + graph.NewID()
	defer func() { _ = m.DeleteThread(ctx, thread) }()

	s1 := graph.Snapshot{CheckpointID: graph.NewID(), ThreadID: thread, Node: "a", Step: 1, State: graph.State{"v": 1}}
	if err := m.CommitStep(ctx, s1, nil); err != nil {
		t.Fatal(err)
	}
	bad := graph.Snapshot{CheckpointID: graph.NewID(), ParentCheckpointID: "", ThreadID: thread, Node: "b", Step: 2, State: graph.State{}}
	if err := m.CommitStep(ctx, bad, nil); !errors.Is(err, graph.ErrCheckpointConflict) {
		t.Fatalf("want conflict, got %v", err)
	}

	msgs, _ := json.Marshal([]map[string]any{{"role": "assistant", "content": "ok"}})
	s2 := graph.Snapshot{
		CheckpointID: graph.NewID(), ParentCheckpointID: s1.CheckpointID,
		ThreadID: thread, Node: "b", Step: 2, State: graph.State{"v": 2},
	}
	if err := m.CommitStep(ctx, s2, []graph.StepArtifact{{
		Kind: graph.ArtifactAgentSession, Key: "coder", Payload: msgs,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := m.CommitStep(ctx, s2, nil); err != nil {
		t.Fatalf("idempotent: %v", err)
	}

	got, ok, err := m.GetLatest(ctx, thread)
	if err != nil || !ok || got.CheckpointID != s2.CheckpointID {
		t.Fatalf("latest=%+v ok=%v err=%v", got, ok, err)
	}
	raw, ok, err := m.GetAgentSession(ctx, thread, s2.CheckpointID, "coder")
	if err != nil || !ok {
		t.Fatalf("session ok=%v err=%v", ok, err)
	}
	if !jsonEqual(t, raw, msgs) {
		t.Fatalf("session mismatch raw=%s want=%s", raw, msgs)
	}

	s3 := graph.Snapshot{
		CheckpointID: graph.NewID(), ParentCheckpointID: s2.CheckpointID,
		ThreadID: thread, Node: graph.END, Step: 3, State: graph.State{"v": 3},
	}
	if err := m.CommitStep(ctx, s3, nil); err != nil {
		t.Fatal(err)
	}
	raw, ok, err = m.GetAgentSession(ctx, thread, s3.CheckpointID, "coder")
	if err != nil || !ok {
		t.Fatalf("copied session missing ok=%v err=%v", ok, err)
	}

	if err := m.Prune(ctx, thread, 1); err != nil {
		t.Fatal(err)
	}
	list, err := m.List(ctx, thread, 0, 10)
	if err != nil || len(list) != 1 || list[0].CheckpointID != s3.CheckpointID {
		t.Fatalf("prune list=%+v err=%v", list, err)
	}
}

// jsonEqual compares JSON semantically (MySQL JSON columns may normalize spacing/order).
func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		t.Fatalf("unmarshal a: %v", err)
	}
	if err := json.Unmarshal(b, &y); err != nil {
		t.Fatalf("unmarshal b: %v", err)
	}
	ab, _ := json.Marshal(x)
	bb, _ := json.Marshal(y)
	return string(ab) == string(bb)
}
