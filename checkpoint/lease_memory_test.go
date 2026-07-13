package checkpoint_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mars/marspi-graph/checkpoint"
	"github.com/mars/marspi-graph/graph"
)

func TestMemoryLeaseHeldAndRelease(t *testing.T) {
	lease := checkpoint.NewMemoryLease()
	ctx := context.Background()
	g1, err := lease.Acquire(ctx, "t1", "run-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if g1.Epoch != 1 {
		t.Fatalf("epoch=%d", g1.Epoch)
	}
	_, err = lease.Acquire(ctx, "t1", "run-b", time.Second)
	if !errors.Is(err, graph.ErrLeaseHeld) {
		t.Fatalf("want ErrLeaseHeld, got %v", err)
	}
	if err := lease.Release(ctx, g1); err != nil {
		t.Fatal(err)
	}
	g2, err := lease.Acquire(ctx, "t1", "run-b", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if g2.Epoch != 2 {
		t.Fatalf("epoch after release+acquire=%d want 2", g2.Epoch)
	}
	if err := lease.Release(ctx, g1); err != nil {
		t.Fatal(err)
	}
	_, err = lease.Renew(ctx, g2, time.Second)
	if err != nil {
		t.Fatalf("new owner renew after stale release: %v", err)
	}
}

func TestMemoryLeaseExpiryAndFence(t *testing.T) {
	fenced, lease := checkpoint.NewFencedMemory()
	var now atomic.Int64
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lease.SetClock(func() time.Time {
		return base.Add(time.Duration(now.Load()))
	})
	ctx := context.Background()
	old, err := lease.Acquire(ctx, "t1", "old", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	now.Store(int64(200 * time.Millisecond))
	neu, err := lease.Acquire(ctx, "t1", "new", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if neu.Epoch <= old.Epoch {
		t.Fatalf("new epoch %d <= old %d", neu.Epoch, old.Epoch)
	}
	_, err = lease.Renew(ctx, old, time.Second)
	if !errors.Is(err, graph.ErrLeaseLost) {
		t.Fatalf("stale renew: %v", err)
	}
	snap := graph.Snapshot{
		CheckpointID: "cp1",
		ThreadID:     "t1",
		Node:         "n",
		State:        graph.State{},
	}
	err = fenced.CommitStepFenced(ctx, snap, nil, old)
	if !errors.Is(err, graph.ErrLeaseLost) {
		t.Fatalf("stale fenced commit: %v", err)
	}
	if err := fenced.CommitStepFenced(ctx, snap, nil, neu); err != nil {
		t.Fatal(err)
	}
}

func TestGraphConcurrentLeaseHeld(t *testing.T) {
	fenced, lease := checkpoint.NewFencedMemory()
	b := graph.NewBuilder()
	started := make(chan struct{})
	release := make(chan struct{})
	b.AddNode("slow", func(ctx context.Context, s graph.State) (graph.Update, error) {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return graph.Update{"ok": true}, nil
	})
	b.SetEntry("slow")
	g, err := b.Compile(
		graph.WithDurableCheckpointer(fenced),
		graph.WithExecutionLease(lease),
		graph.WithLeaseTTL(2*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan error, 2)
	go func() {
		defer wg.Done()
		_, err := g.Invoke(context.Background(), graph.State{}, graph.WithThreadID("same"))
		errCh <- err
	}()
	<-started
	go func() {
		defer wg.Done()
		_, err := g.Invoke(context.Background(), graph.State{}, graph.WithThreadID("same"))
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	close(errCh)

	var held, ok int
	for e := range errCh {
		if e == nil {
			ok++
			continue
		}
		if errors.Is(e, graph.ErrLeaseHeld) {
			held++
			continue
		}
		t.Fatalf("unexpected err: %v", e)
	}
	if ok != 1 || held != 1 {
		t.Fatalf("ok=%d held=%d", ok, held)
	}
}

func TestGraphHeartbeatKeepsLongNode(t *testing.T) {
	fenced, lease := checkpoint.NewFencedMemory()
	b := graph.NewBuilder()
	b.AddNode("long", func(ctx context.Context, s graph.State) (graph.Update, error) {
		select {
		case <-time.After(350 * time.Millisecond):
			return graph.Update{"done": true}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	b.SetEntry("long")
	ttl := 120 * time.Millisecond
	g, err := b.Compile(
		graph.WithDurableCheckpointer(fenced),
		graph.WithExecutionLease(lease),
		graph.WithLeaseTTL(ttl),
	)
	if err != nil {
		t.Fatal(err)
	}
	out, err := g.Invoke(context.Background(), graph.State{}, graph.WithThreadID("hb"))
	if err != nil {
		t.Fatal(err)
	}
	if out["done"] != true {
		t.Fatalf("state=%v", out)
	}
}

func TestGraphInterruptReleasesLeaseForResume(t *testing.T) {
	fenced, lease := checkpoint.NewFencedMemory()
	b := graph.NewBuilder()
	b.AddNode("gate", func(ctx context.Context, s graph.State) (graph.Update, error) {
		_, err := graph.InterruptOrResume(ctx, "approve?")
		if err != nil {
			return nil, err
		}
		return graph.Update{"approved": true}, nil
	})
	b.SetEntry("gate")
	g, err := b.Compile(
		graph.WithDurableCheckpointer(fenced),
		graph.WithExecutionLease(lease),
		graph.WithLeaseTTL(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	thread := "hitl"
	_, err = g.Invoke(context.Background(), graph.State{}, graph.WithThreadID(thread))
	if !graph.IsInterrupted(err) {
		t.Fatalf("want interrupt, got %v", err)
	}
	out, err := g.Resume(context.Background(), thread, graph.WithCommand(graph.Command{Resume: true}))
	if err != nil {
		t.Fatal(err)
	}
	if out["approved"] != true {
		t.Fatalf("state=%v", out)
	}
}

func TestCompileLeaseRequiresFenced(t *testing.T) {
	store := checkpoint.NewDurableMemory()
	lease := checkpoint.NewMemoryLease()
	b := graph.NewBuilder()
	b.AddNode("n", func(ctx context.Context, s graph.State) (graph.Update, error) {
		return nil, nil
	})
	b.SetEntry("n")
	_, err := b.Compile(graph.WithDurableCheckpointer(store), graph.WithExecutionLease(lease))
	if err == nil {
		t.Fatal("expected compile error")
	}
}
