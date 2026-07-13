package checkpoint

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/mars/marspi-graph/graph"
)

func TestMySQLLeaseAcquireFence(t *testing.T) {
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
	thread := "lease-" + graph.NewID()
	defer func() { _ = m.DeleteThread(ctx, thread) }()

	g1, err := m.Acquire(ctx, thread, "run-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if g1.Epoch < 1 {
		t.Fatalf("epoch=%d", g1.Epoch)
	}
	_, err = m.Acquire(ctx, thread, "run-b", time.Second)
	if !errors.Is(err, graph.ErrLeaseHeld) {
		t.Fatalf("want held, got %v", err)
	}

	snap := graph.Snapshot{
		CheckpointID: graph.NewID(),
		ThreadID:     thread,
		Node:         "a",
		State:        graph.State{"v": 1},
	}
	if err := m.CommitStepFenced(ctx, snap, nil, g1); err != nil {
		t.Fatal(err)
	}

	if err := m.Release(ctx, g1); err != nil {
		t.Fatal(err)
	}
	g2, err := m.Acquire(ctx, thread, "run-b", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if g2.Epoch <= g1.Epoch {
		t.Fatalf("epoch did not advance: %d -> %d", g1.Epoch, g2.Epoch)
	}
	err = m.CommitStepFenced(ctx, graph.Snapshot{
		CheckpointID:       graph.NewID(),
		ParentCheckpointID: snap.CheckpointID,
		ThreadID:           thread,
		Node:               "b",
		State:              graph.State{"v": 2},
	}, nil, g1)
	if !errors.Is(err, graph.ErrLeaseLost) {
		t.Fatalf("stale fenced commit: %v", err)
	}
	_, err = m.Renew(ctx, g1, time.Second)
	if !errors.Is(err, graph.ErrLeaseLost) {
		t.Fatalf("stale renew: %v", err)
	}
	if err := m.Release(ctx, g1); err != nil {
		t.Fatal(err)
	}
	_, err = m.Renew(ctx, g2, time.Second)
	if err != nil {
		t.Fatalf("owner renew after stale release: %v", err)
	}
}
