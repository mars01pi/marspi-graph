package workflow_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mars/marspi-graph/checkpoint"
	"github.com/mars/marspi-graph/graph"
	"github.com/mars/marspi-graph/workflow"
)

func TestBuiltinDevflowLoads(t *testing.T) {
	spec, err := workflow.Builtin("devflow")
	if err != nil {
		t.Fatal(err)
	}
	if spec.ID != "devflow" || spec.Entry != "design" {
		t.Fatalf("spec=%+v", spec)
	}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateUnknownComponent(t *testing.T) {
	_, err := workflow.ParseSpec([]byte(`
id: x
entry: a
nodes:
  - id: a
    component: wizard
edges:
  - from: a
    to: END
`))
	if err == nil || !strings.Contains(err.Error(), "unknown type") {
		t.Fatalf("err=%v", err)
	}
}

func TestValidateUnknownRoute(t *testing.T) {
	_, err := workflow.ParseSpec([]byte(`
id: x
entry: a
nodes:
  - id: a
    component: agent
edges:
  - from: a
    route: magic
    pass: END
    fail: END
    key: k
`))
	if err == nil || !strings.Contains(err.Error(), "unknown route") {
		t.Fatalf("err=%v", err)
	}
}

func TestDevflowHappyAllowPushFalse(t *testing.T) {
	spec, err := workflow.Builtin("devflow")
	if err != nil {
		t.Fatal(err)
	}
	res, err := workflow.Run(context.Background(), spec, workflow.Bindings{
		Goal:      "add feature",
		AllowPush: false,
		MaxRework: 3,
		MaxSteps:  20,
		NodeRun: map[string]graph.NodeFunc{
			"design": func(_ context.Context, _ graph.State) (graph.Update, error) {
				return graph.Update{"design_doc": "plan", "phase": "design", "last_agent": "design"}, nil
			},
			"develop": func(_ context.Context, _ graph.State) (graph.Update, error) {
				return graph.Update{"changed_files": "a.go", "phase": "develop", "last_agent": "develop"}, nil
			},
			"review": func(_ context.Context, _ graph.State) (graph.Update, error) {
				return graph.Update{"review_result": "REVIEW: PASS", "phase": "review", "last_agent": "review"}, nil
			},
			"test": func(_ context.Context, _ graph.State) (graph.Update, error) {
				return graph.Update{"test_result": "TEST: PASS", "phase": "test", "last_agent": "test", "status": "success"}, nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.State.GetString("last_agent") != "test" {
		t.Fatalf("last_agent=%q want test (push skipped)", res.State.GetString("last_agent"))
	}
	if _, ok := res.State["approval"]; ok {
		t.Fatal("push_gate should not run when AllowPush=false")
	}
}

func TestDevflowReviewReworkThenPass(t *testing.T) {
	spec, err := workflow.Builtin("devflow")
	if err != nil {
		t.Fatal(err)
	}
	var reviews int
	res, err := workflow.Run(context.Background(), spec, workflow.Bindings{
		Goal:      "fix",
		AllowPush: false,
		MaxRework: 3,
		MaxSteps:  30,
		NodeRun: map[string]graph.NodeFunc{
			"design": stubPhase("design", graph.Update{"design_doc": "d"}),
			"develop": func(_ context.Context, s graph.State) (graph.Update, error) {
				return graph.Update{"phase": "develop", "last_agent": "develop", "changed_files": "x.go"}, nil
			},
			"review": func(_ context.Context, s graph.State) (graph.Update, error) {
				reviews++
				upd := graph.Update{"phase": "review", "last_agent": "review"}
				if reviews == 1 {
					upd["review_result"] = "REVIEW: FAIL: missing i18n"
					upd["rework_count"] = stateInt(s, "rework_count") + 1
					return upd, nil
				}
				upd["review_result"] = "REVIEW: PASS"
				return upd, nil
			},
			"test": stubPhase("test", graph.Update{"test_result": "TEST: PASS", "status": "success"}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if reviews != 2 {
		t.Fatalf("reviews=%d", reviews)
	}
	if stateInt(res.State, "rework_count") < 1 {
		t.Fatalf("rework_count=%v", res.State["rework_count"])
	}
}

func TestDevflowMaxReworkExhausted(t *testing.T) {
	spec, err := workflow.Builtin("devflow")
	if err != nil {
		t.Fatal(err)
	}
	res, err := workflow.Run(context.Background(), spec, workflow.Bindings{
		Goal:      "x",
		AllowPush: false,
		MaxRework: 1,
		MaxSteps:  20,
		NodeRun: map[string]graph.NodeFunc{
			"design":  stubPhase("design", graph.Update{"design_doc": "d"}),
			"develop": stubPhase("develop", graph.Update{"changed_files": "a"}),
			"review": func(_ context.Context, s graph.State) (graph.Update, error) {
				return graph.Update{
					"phase":         "review",
					"last_agent":    "review",
					"review_result": "REVIEW: FAIL: always",
					"rework_count":  stateInt(s, "rework_count") + 1,
				}, nil
			},
			"test": stubPhase("test", graph.Update{"test_result": "TEST: PASS"}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// After rework_count exceeds max_rework, route to END without reaching test.
	if res.State.GetString("last_agent") == "test" {
		t.Fatal("should not reach test after rework exhausted")
	}
	if stateInt(res.State, "rework_count") <= 1 {
		t.Fatalf("expected rework_count > max, got %d", stateInt(res.State, "rework_count"))
	}
}

func TestDevflowHITLApprovePush(t *testing.T) {
	spec, err := workflow.Builtin("devflow")
	if err != nil {
		t.Fatal(err)
	}
	cp := checkpoint.NewMemory()
	res, err := workflow.Run(context.Background(), spec, workflow.Bindings{
		Goal:         "ship",
		AllowPush:    true,
		MaxRework:    2,
		MaxSteps:     20,
		Checkpointer: cp,
		ThreadID:     "wf-hitl-ok",
		OnInterrupt: func(ctx context.Context, info workflow.InterruptInfo) (bool, error) {
			if info.Node != "push_gate" {
				t.Fatalf("node=%s", info.Node)
			}
			return true, nil
		},
		NodeRun: map[string]graph.NodeFunc{
			"design":  stubPhase("design", graph.Update{"design_doc": "d"}),
			"develop": stubPhase("develop", graph.Update{}),
			"review":  stubPhase("review", graph.Update{"review_result": "REVIEW: PASS"}),
			"test":    stubPhase("test", graph.Update{"test_result": "TEST: PASS"}),
			"push": stubPhase("push", graph.Update{
				"push_result": "PR https://example.com/1",
				"status":      "success",
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.State.GetString("push_result") != "PR https://example.com/1" {
		t.Fatalf("push_result=%q", res.State.GetString("push_result"))
	}
	if res.State.GetString("status") != "success" {
		t.Fatalf("status=%q", res.State.GetString("status"))
	}
}

func TestDevflowHITLDeny(t *testing.T) {
	spec, err := workflow.Builtin("devflow")
	if err != nil {
		t.Fatal(err)
	}
	_, err = workflow.Run(context.Background(), spec, workflow.Bindings{
		Goal:         "ship",
		AllowPush:    true,
		MaxSteps:     20,
		Checkpointer: checkpoint.NewMemory(),
		ThreadID:     "wf-hitl-deny",
		OnInterrupt: func(context.Context, workflow.InterruptInfo) (bool, error) {
			return false, nil
		},
		NodeRun: map[string]graph.NodeFunc{
			"design":  stubPhase("design", graph.Update{"design_doc": "d"}),
			"develop": stubPhase("develop", graph.Update{}),
			"review":  stubPhase("review", graph.Update{"review_result": "REVIEW: PASS"}),
			"test":    stubPhase("test", graph.Update{"test_result": "TEST: PASS"}),
			"push":    stubPhase("push", graph.Update{"push_result": "should not run"}),
		},
	})
	if !errors.Is(err, workflow.ErrApprovalDenied) {
		t.Fatalf("err=%v", err)
	}
}

func TestRetryOnErrorSoftComplete(t *testing.T) {
	var calls int
	spec, err := workflow.ParseSpec([]byte(`
id: ft
entry: work
nodes:
  - id: work
    component: agent
    retry:
      max_attempts: 2
      initial_interval: 1ms
      jitter: false
    on_error: sink
  - id: sink
    component: agent
edges:
  - from: work
    to: END
  - from: sink
    to: END
`))
	if err != nil {
		t.Fatal(err)
	}
	res, err := workflow.Run(context.Background(), spec, workflow.Bindings{
		Goal:     "x",
		MaxSteps: 10,
		NodeRun: map[string]graph.NodeFunc{
			"work": func(_ context.Context, _ graph.State) (graph.Update, error) {
				calls++
				return nil, errors.New("hard")
			},
			"sink": func(_ context.Context, s graph.State) (graph.Update, error) {
				return graph.Update{"handled": true}, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("soft complete want nil, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls=%d want 2", calls)
	}
	if res.State.GetString(graph.StateKeyStatus) != graph.StatusFailed {
		t.Fatalf("status=%q message=%q", res.State.GetString(graph.StateKeyStatus), res.Message)
	}
	if res.Message != graph.StatusFailed {
		t.Fatalf("Message=%q want failed", res.Message)
	}
	if res.State["handled"] != true {
		t.Fatalf("sink not run: %v", res.State)
	}
	if res.State.GetString(graph.StateKeyErrorNode) != "work" {
		t.Fatalf("error_node=%q", res.State.GetString(graph.StateKeyErrorNode))
	}
}

func TestValidateOnErrorUnknown(t *testing.T) {
	_, err := workflow.ParseSpec([]byte(`
id: x
entry: a
nodes:
  - id: a
    component: agent
    on_error: missing
edges:
  - from: a
    to: END
`))
	if err == nil || !strings.Contains(err.Error(), "on_error") {
		t.Fatalf("err=%v", err)
	}
}

func TestRetryWithoutOnErrorBubbles(t *testing.T) {
	spec, err := workflow.ParseSpec([]byte(`
id: ft
entry: work
nodes:
  - id: work
    component: agent
    retry:
      max_attempts: 2
      initial_interval: 1ms
      jitter: false
edges:
  - from: work
    to: END
`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = workflow.Run(context.Background(), spec, workflow.Bindings{
		MaxSteps: 5,
		NodeRun: map[string]graph.NodeFunc{
			"work": func(_ context.Context, _ graph.State) (graph.Update, error) {
				return nil, errors.New("boom")
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("want bubble, got %v", err)
	}
}

func stubPhase(id string, extra graph.Update) graph.NodeFunc {
	return func(_ context.Context, _ graph.State) (graph.Update, error) {
		upd := graph.Update{"phase": id, "last_agent": id}
		for k, v := range extra {
			upd[k] = v
		}
		return upd, nil
	}
}

func stateInt(s graph.State, key string) int {
	switch v := s[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}
