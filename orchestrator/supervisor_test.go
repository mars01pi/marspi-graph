package orchestrator_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

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
	res, err := orchestrator.RunSupervisor(context.Background(), orchestrator.SupervisorConfig{
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
	if err != nil {
		t.Fatal(err)
	}
	if res.State.GetString("error") == "" {
		t.Fatalf("expected error in state: %+v", res.State)
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
