package graph

import (
	"context"
	"testing"
)

func TestToolExecutionStableKeyIgnoresRunAndEpoch(t *testing.T) {
	ctx := context.Background()
	ctx = WithThreadIDCtx(ctx, "thr")
	ctx = WithParentCheckpointID(ctx, "cp1")
	ctx = WithNodeID(ctx, "coder")
	ctx = WithRunID(ctx, "run-a")
	ctx = WithLeaseGrant(ctx, LeaseGrant{ThreadID: "thr", RunID: "run-a", Epoch: 3})

	a, err := ToolExecutionFrom(ctx, "charge", "order-1")
	if err != nil {
		t.Fatal(err)
	}
	ctxB := WithRunID(ctx, "run-b")
	ctxB = WithLeaseGrant(ctxB, LeaseGrant{ThreadID: "thr", RunID: "run-b", Epoch: 9})
	b, err := ToolExecutionFrom(ctxB, "charge", "order-1")
	if err != nil {
		t.Fatal(err)
	}
	if a.IdempotencyKey() != b.IdempotencyKey() {
		t.Fatalf("key changed across run/epoch: %s vs %s", a.IdempotencyKey(), b.IdempotencyKey())
	}
	if a.FenceToken() != 3 || b.FenceToken() != 9 {
		t.Fatalf("fence tokens a=%d b=%d", a.FenceToken(), b.FenceToken())
	}
}

func TestToolExecutionKeyChangesWithCoords(t *testing.T) {
	base := context.Background()
	base = WithThreadIDCtx(base, "thr")
	base = WithParentCheckpointID(base, "cp1")
	base = WithNodeID(base, "coder")

	e1, _ := ToolExecutionFrom(base, "write", "op1")
	key := e1.IdempotencyKey()

	cases := []struct {
		name string
		ctx  context.Context
		tool string
		op   string
	}{
		{"checkpoint", WithParentCheckpointID(base, "cp2"), "write", "op1"},
		{"node", WithNodeID(base, "other"), "write", "op1"},
		{"tool", base, "bash", "op1"},
		{"operation", base, "write", "op2"},
		{"thread", WithThreadIDCtx(base, "other"), "write", "op1"},
	}
	for _, tc := range cases {
		e, err := ToolExecutionFrom(tc.ctx, tc.tool, tc.op)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if e.IdempotencyKey() == key {
			t.Fatalf("%s: key should change", tc.name)
		}
	}
}

func TestToolExecutionNoLeaseFenceZero(t *testing.T) {
	ctx := WithThreadIDCtx(context.Background(), "t")
	e, err := ToolExecutionFrom(ctx, "read", "x")
	if err != nil {
		t.Fatal(err)
	}
	if e.FenceToken() != 0 {
		t.Fatalf("want 0, got %d", e.FenceToken())
	}
}

func TestToolExecutionRejectsEmpty(t *testing.T) {
	if _, err := ToolExecutionFrom(context.Background(), "", "op"); err == nil {
		t.Fatal("want toolName error")
	}
	if _, err := ToolExecutionFrom(context.Background(), "t", ""); err == nil {
		t.Fatal("want operationID error")
	}
	if _, err := ToolExecutionFrom(context.Background(), "  ", "op"); err == nil {
		t.Fatal("want trimmed toolName error")
	}
}
