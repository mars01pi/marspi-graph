package component

import (
	"testing"

	"github.com/mars/marspi-core/agentctx"
	"github.com/mars/marspi-core/llm"
	"github.com/mars/marspi-graph/graph"
)

func TestApplyExtractorsCompletion(t *testing.T) {
	mgr := &agentctx.Manager{
		Messages: []llm.Message{
			{"role": "assistant", "content": "drafting…"},
			{"role": "tool", "tool_name": "attempt_completion", "content": "REVIEW: PASS"},
		},
	}
	upd := graph.Update{}
	applyExtractors([]ExtractorSpec{
		{To: "review_result", From: ExtractCompletion},
	}, mgr, "ignored RunOnce text", upd)

	if got := upd["review_result"]; got != "REVIEW: PASS" {
		t.Fatalf("review_result=%v want REVIEW: PASS", got)
	}
}

func TestApplyExtractorsCompletionFallsBackToAssistant(t *testing.T) {
	mgr := &agentctx.Manager{
		Messages: []llm.Message{
			{"role": "assistant", "content": "TEST: FAIL: timeout"},
		},
	}
	upd := graph.Update{}
	applyExtractors([]ExtractorSpec{
		{To: "test_result", From: ExtractCompletion},
	}, mgr, "run once", upd)

	if got := upd["test_result"]; got != "TEST: FAIL: timeout" {
		t.Fatalf("test_result=%v", got)
	}
}

func TestApplyExtractorsCompletionFallsBackToOutput(t *testing.T) {
	upd := graph.Update{}
	applyExtractors([]ExtractorSpec{
		{To: "x", From: ExtractCompletion},
	}, &agentctx.Manager{}, "design doc body", upd)
	if got := upd["x"]; got != "design doc body" {
		t.Fatalf("x=%v want output fallback", got)
	}
}

func TestApplyExtractorsCompletionEmptySkipped(t *testing.T) {
	upd := graph.Update{}
	applyExtractors([]ExtractorSpec{
		{To: "x", From: ExtractCompletion},
	}, &agentctx.Manager{}, "", upd)
	if _, ok := upd["x"]; ok {
		t.Fatal("empty completion+output should not write")
	}
}

func TestWithVerdictCompletion(t *testing.T) {
	got := withVerdictCompletion(nil, "review_result")
	if len(got) != 1 || got[0].From != ExtractCompletion || got[0].To != "review_result" {
		t.Fatalf("got=%v", got)
	}
	existing := []ExtractorSpec{{To: "review_result", From: ExtractCompletion}}
	got = withVerdictCompletion(existing, "review_result")
	if len(got) != 1 {
		t.Fatalf("should not duplicate: %v", got)
	}
}

func TestApplyFailInc(t *testing.T) {
	upd := graph.Update{"review_result": "REVIEW: FAIL: gaps"}
	s := graph.State{"rework_count": 1}
	applyFailInc(upd, s, "rework_count", "review_result", "")
	if upd["rework_count"] != 2 {
		t.Fatalf("rework_count=%v", upd["rework_count"])
	}
}

func TestApplyExtractorsOutputAndConst(t *testing.T) {
	upd := graph.Update{}
	applyExtractors([]ExtractorSpec{
		{To: "push_result", From: ExtractOutput},
		{To: "status", From: ExtractConst, Value: "success"},
	}, nil, "pr opened", upd)
	if upd["push_result"] != "pr opened" || upd["status"] != "success" {
		t.Fatalf("upd=%v", upd)
	}
}
