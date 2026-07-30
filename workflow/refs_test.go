package workflow

import (
	"strings"
	"testing"

	"github.com/mars/marspi-graph/component"
	"github.com/mars/marspi-graph/graph"
)

func TestExpandRefs(t *testing.T) {
	spec, err := Builtin("devflow")
	if err != nil {
		t.Fatal(err)
	}
	idx := buildRefIndex(spec)
	s := graph.State{
		"goal":          "add tabs",
		"design_doc":    "use Pinia",
		"changed_files": "a.ts",
		"review_result": "REVIEW: PASS",
	}
	got := ExpandRefs("Goal={{start.goal}}; Doc={{design.design_doc}}; Files={{develop.changed_files}}", s, idx, nil)
	if !strings.Contains(got, "add tabs") || !strings.Contains(got, "use Pinia") || !strings.Contains(got, "a.ts") {
		t.Fatalf("got=%q", got)
	}
	got = ExpandRefs("out={{output}}", s, idx, map[string]string{"output": "hello"})
	if got != "out=hello" {
		t.Fatalf("got=%q", got)
	}
}

func TestValidateUnknownRef(t *testing.T) {
	_, err := ParseSpec([]byte(`
id: x
entry: a
nodes:
  - id: a
    component: agent
    bindings:
      system_prompt: "see {{ghost.field}}"
edges:
  - from: a
    to: END
`))
	if err == nil || !strings.Contains(err.Error(), "unknown ref") {
		t.Fatalf("err=%v", err)
	}
}

func TestOutputCatalog(t *testing.T) {
	spec, err := Builtin("devflow")
	if err != nil {
		t.Fatal(err)
	}
	cat := spec.OutputCatalog()
	if len(cat) < 3 {
		t.Fatalf("catalog too small: %+v", cat)
	}
	var foundDesign bool
	for _, e := range cat {
		if e.Ref == "design.design_doc" {
			foundDesign = true
		}
	}
	if !foundDesign {
		t.Fatalf("missing design.design_doc in %+v", cat)
	}
}

func TestGateTitleRefs(t *testing.T) {
	spec, err := Builtin("devflow")
	if err != nil {
		t.Fatal(err)
	}
	var title string
	for _, n := range spec.Nodes {
		if n.ID != "push_gate" {
			continue
		}
		if s, ok := n.Bindings["title"].(string); ok {
			title = s
		}
	}
	if title == "" {
		t.Fatal("push_gate missing title")
	}
	idx := buildRefIndex(spec)
	got := ExpandRefs(title, graph.State{"goal": "ship it"}, idx, nil)
	if got != "Push approval: ship it" {
		t.Fatalf("title=%q", got)
	}
}

func TestComponentSchemas(t *testing.T) {
	schemas := ComponentSchemas()
	if len(schemas) < 2 {
		t.Fatalf("schemas=%d", len(schemas))
	}
	var agent, approval bool
	for _, s := range schemas {
		switch s.Meta.Type {
		case component.TypeAgent:
			agent = true
		case component.TypeApproval:
			approval = true
		}
	}
	if !agent || !approval {
		t.Fatalf("missing types in %+v", schemas)
	}
}
