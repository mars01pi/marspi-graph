package orchestrator_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mars/marspi-graph/checkpoint"
	"github.com/mars/marspi-graph/graph"
	"github.com/mars/marspi-graph/orchestrator"
)

func TestRunSupervisorRoutesAndFinishes(t *testing.T) {
	var decideCalls int
	res, err := orchestrator.RunSupervisor(context.Background(), orchestrator.SupervisorConfig{
		Goal:     "research topic X",
		MaxSteps: 6,
		Decide: func(_ context.Context, s graph.State) (orchestrator.Decision, error) {
			decideCalls++
			if decideCalls == 1 {
				return orchestrator.Decision{
					Next:   "researcher",
					Reason: "gather facts",
					Task:   "find APIs",
				}, nil
			}
			return orchestrator.Decision{Next: "END", Reason: "enough"}, nil
		},
		Workers: []orchestrator.WorkerSpec{
			{
				ID:          "researcher",
				Description: "looks things up",
				Run: func(_ context.Context, s graph.State, task string) (string, error) {
					if task != "find APIs" {
						return "", fmt.Errorf("unexpected task %q", task)
					}
					return "found api docs", nil
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if decideCalls < 2 {
		t.Fatalf("decideCalls=%d", decideCalls)
	}
	if res.State.GetString("last_output") != "found api docs" {
		t.Fatalf("last_output=%q", res.State.GetString("last_output"))
	}
	msgs, ok := res.State["messages"].([]any)
	if !ok || len(msgs) < 2 {
		t.Fatalf("messages=%v", res.State["messages"])
	}
	joined := joinMsgs(msgs)
	if !strings.Contains(joined, "supervisor → researcher") {
		t.Fatalf("missing handoff: %s", joined)
	}
	if !strings.Contains(joined, "[researcher]") {
		t.Fatalf("missing worker: %s", joined)
	}
}

func TestRunSupervisorUnknownWorkerEnds(t *testing.T) {
	_, err := orchestrator.RunSupervisor(context.Background(), orchestrator.SupervisorConfig{
		Goal: "x",
		Decide: func(context.Context, graph.State) (orchestrator.Decision, error) {
			return orchestrator.Decision{Next: "ghost", Reason: "bad"}, nil
		},
		Workers: []orchestrator.WorkerSpec{
			{ID: "researcher", Run: func(context.Context, graph.State, string) (string, error) {
				return "should not run", nil
			}},
		},
	})
	if err == nil {
		t.Fatal("expected error for unknown worker")
	}
}

func TestRunSupervisorHITLApprove(t *testing.T) {
	var decideCalls, workerCalls, interruptCalls int
	res, err := orchestrator.RunSupervisor(context.Background(), orchestrator.SupervisorConfig{
		Goal:               "edit something",
		MaxSteps:           6,
		RequireApprovalFor: []string{"coder"},
		Decide: func(_ context.Context, s graph.State) (orchestrator.Decision, error) {
			decideCalls++
			if decideCalls == 1 {
				return orchestrator.Decision{
					Next:   "coder",
					Reason: "need code change",
					Task:   "patch file",
				}, nil
			}
			return orchestrator.Decision{Next: "END", Reason: "done"}, nil
		},
		OnInterrupt: func(_ context.Context, info orchestrator.InterruptInfo) (bool, error) {
			interruptCalls++
			if info.Node != "coder" {
				t.Fatalf("node=%q", info.Node)
			}
			m, ok := info.Value.(map[string]any)
			if !ok || m["task"] != "patch file" {
				t.Fatalf("payload=%#v", info.Value)
			}
			return true, nil
		},
		Workers: []orchestrator.WorkerSpec{
			{
				ID: "coder",
				Run: func(_ context.Context, s graph.State, task string) (string, error) {
					workerCalls++
					return "patched", nil
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if interruptCalls != 1 {
		t.Fatalf("interruptCalls=%d", interruptCalls)
	}
	if workerCalls != 1 {
		t.Fatalf("workerCalls=%d", workerCalls)
	}
	if res.State.GetString("last_output") != "patched" {
		t.Fatalf("last_output=%q", res.State.GetString("last_output"))
	}
}

func TestRunSupervisorHITLDeny(t *testing.T) {
	var workerCalls int
	_, err := orchestrator.RunSupervisor(context.Background(), orchestrator.SupervisorConfig{
		Goal:               "edit something",
		MaxSteps:           4,
		RequireApprovalFor: []string{"coder"},
		Decide: func(context.Context, graph.State) (orchestrator.Decision, error) {
			return orchestrator.Decision{Next: "coder", Reason: "r", Task: "t"}, nil
		},
		OnInterrupt: func(context.Context, orchestrator.InterruptInfo) (bool, error) {
			return false, nil
		},
		Workers: []orchestrator.WorkerSpec{
			{
				ID: "coder",
				Run: func(context.Context, graph.State, string) (string, error) {
					workerCalls++
					return "should not run", nil
				},
			},
		},
	})
	if !errors.Is(err, orchestrator.ErrApprovalDenied) {
		t.Fatalf("want ErrApprovalDenied, got %v", err)
	}
	if workerCalls != 0 {
		t.Fatalf("worker should not run, calls=%d", workerCalls)
	}
}

func TestRunSupervisorHITLNoHookBubblesInterrupt(t *testing.T) {
	_, err := orchestrator.RunSupervisor(context.Background(), orchestrator.SupervisorConfig{
		Goal:               "edit",
		MaxSteps:           4,
		RequireApprovalFor: []string{"coder"},
		Decide: func(context.Context, graph.State) (orchestrator.Decision, error) {
			return orchestrator.Decision{Next: "coder", Reason: "r", Task: "t"}, nil
		},
		Workers: []orchestrator.WorkerSpec{
			{ID: "coder", Run: func(context.Context, graph.State, string) (string, error) {
				return "x", nil
			}},
		},
	})
	if !graph.IsInterrupted(err) {
		t.Fatalf("want interrupt, got %v", err)
	}
}

func TestRunSupervisorSQLiteCrossProcessResume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sv.db")
	threadID := "sv-cross-proc"
	workers := []orchestrator.WorkerSpec{
		{
			ID: "coder",
			Run: func(_ context.Context, s graph.State, task string) (string, error) {
				return "patched", nil
			},
		},
	}
	decide := func(context.Context, graph.State) (orchestrator.Decision, error) {
		return orchestrator.Decision{Next: "coder", Reason: "r", Task: "patch"}, nil
	}

	cp1, err := checkpoint.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = orchestrator.RunSupervisor(context.Background(), orchestrator.SupervisorConfig{
		Goal:               "edit",
		MaxSteps:           6,
		ThreadID:           threadID,
		Checkpointer:       cp1,
		RequireApprovalFor: []string{"coder"},
		Decide:             decide,
		Workers:            workers,
		// No OnInterrupt → interrupt bubbles; checkpoint remains.
	})
	if !graph.IsInterrupted(err) {
		t.Fatalf("want interrupt, got %v", err)
	}
	_ = cp1.Close()

	// Simulate new process: reopen DB and resume with approval.
	cp2, err := checkpoint.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer cp2.Close()

	var interruptCalls, decideCalls int
	res, err := orchestrator.RunSupervisor(context.Background(), orchestrator.SupervisorConfig{
		Goal:                 "edit",
		MaxSteps:             6,
		ThreadID:             threadID,
		Checkpointer:         cp2,
		ResumeFromCheckpoint: true,
		RequireApprovalFor:   []string{"coder"},
		Decide: func(ctx context.Context, s graph.State) (orchestrator.Decision, error) {
			decideCalls++
			return orchestrator.Decision{Next: "END", Reason: "done"}, nil
		},
		OnInterrupt: func(context.Context, orchestrator.InterruptInfo) (bool, error) {
			interruptCalls++
			return true, nil
		},
		Workers: workers,
	})
	if err != nil {
		t.Fatal(err)
	}
	if interruptCalls != 1 {
		t.Fatalf("interruptCalls=%d", interruptCalls)
	}
	if decideCalls < 1 {
		t.Fatalf("decideCalls=%d", decideCalls)
	}
	if res.State.GetString("last_output") != "patched" {
		t.Fatalf("last_output=%q", res.State.GetString("last_output"))
	}
}

func joinMsgs(msgs []any) string {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString(fmt.Sprint(m))
		b.WriteByte('\n')
	}
	return b.String()
}
