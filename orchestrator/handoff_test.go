package orchestrator_test

import (
	"testing"

	"github.com/mars/marspi-graph/graph"
	"github.com/mars/marspi-graph/orchestrator"
)

func TestParseDecision(t *testing.T) {
	d, err := orchestrator.ParseDecision(`{"next":"coder","reason":"implement","task":"write foo"}`)
	if err != nil {
		t.Fatal(err)
	}
	if d.Next != "coder" || d.Task != "write foo" {
		t.Fatalf("%+v", d)
	}

	d, err = orchestrator.ParseDecision("Here:\n```json\n{\"next\":\"END\",\"reason\":\"done\",\"task\":\"\"}\n```")
	if err != nil {
		t.Fatal(err)
	}
	if orchestrator.NormalizeNext(d.Next) != graph.END {
		t.Fatalf("next=%q", d.Next)
	}
}

func TestParseDecisionMissingNext(t *testing.T) {
	_, err := orchestrator.ParseDecision(`{"reason":"x","task":"y"}`)
	if err == nil {
		t.Fatal("expected error")
	}
}
