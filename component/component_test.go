package component_test

import (
	"testing"

	"github.com/mars/marspi-graph/component"
)

func TestAgentBindAndSchema(t *testing.T) {
	a := component.NewAgent()
	err := a.Bind(map[string]any{
		"system_prompt": "hi",
		"extractors": []any{
			map[string]any{"to": "changed_files", "from": "tool_calls", "tools": []any{"edit", "write"}},
			map[string]any{"to": "status", "from": "const", "value": "success"},
		},
		"outputs": []any{
			map[string]any{"name": "changed_files", "label": "Files"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.Meta().Type != component.TypeAgent {
		t.Fatal(a.Meta().Type)
	}
	found := false
	for _, o := range a.PublishedOutputs() {
		if o.Name == "changed_files" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected changed_files output")
	}
	sch := a.TypeSchema()
	if len(sch.Inputs) == 0 || len(sch.Outputs) == 0 {
		t.Fatalf("schema=%+v", sch)
	}
}

func TestApprovalSchema(t *testing.T) {
	a := component.NewApproval()
	_ = a.Bind(map[string]any{
		"title":   "Approve {{goal}}",
		"summary": "ok",
	})
	sch := a.InstanceSchema()
	if sch.Meta.Type != component.TypeApproval {
		t.Fatal(sch.Meta.Type)
	}
}

func TestRegistrySchemas(t *testing.T) {
	schemas := component.Default.Schemas()
	if len(schemas) < 2 {
		t.Fatalf("len=%d", len(schemas))
	}
}
