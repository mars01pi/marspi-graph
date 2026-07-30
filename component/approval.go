package component

import (
	"context"
	"strings"

	"github.com/mars/marspi-graph/graph"
)

// Approval is the HITL gate component (canvas "Approval" node).
type Approval struct {
	cfg approvalConfig
}

type approvalConfig struct {
	Title           string `yaml:"title"`
	Summary         string `yaml:"summary"`
	RequireApproval *bool  `yaml:"require_approval"`
}

// NewApproval returns an unbound Approval component.
func NewApproval() *Approval { return &Approval{} }

func (a *Approval) Meta() Meta {
	return Meta{Type: TypeApproval, Name: "Approval", Description: "Human-in-the-loop gate with approve/reject"}
}

func (a *Approval) TypeSchema() Schema {
	return Schema{
		Meta: a.Meta(),
		Inputs: []InputDef{
			{Name: "title", Label: "标题", Types: []string{"str"}, Widget: "text"},
			{Name: "summary", Label: "摘要", Types: []string{"str"}, Widget: "textarea"},
			{Name: "require_approval", Label: "需要审批", Types: []string{"bool"}, Default: true, Widget: "bool"},
		},
		Outputs: []OutputDef{
			{Name: "approval", Label: "是否通过", Types: []string{"bool"}},
			{Name: "title", Label: "标题", Types: []string{"str"}},
			{Name: "summary", Label: "摘要", Types: []string{"str"}},
		},
	}
}

func (a *Approval) Bind(bindings map[string]any) error {
	return decodeBindings(bindings, &a.cfg)
}

func (a *Approval) InstanceSchema() Schema { return a.TypeSchema() }

func (a *Approval) BuildNode(rt Runtime) (graph.NodeFunc, error) {
	cfg := a.cfg
	nodeID := rt.NodeID
	expand := rt.ExpandRefs
	require := true
	if cfg.RequireApproval != nil {
		require = *cfg.RequireApproval
	}

	return func(runCtx context.Context, s graph.State) (graph.Update, error) {
		title, summary := cfg.Title, cfg.Summary
		if expand != nil {
			title = expand(title, s, nil)
			summary = expand(summary, s, nil)
		}
		if !require {
			return graph.Update{
				"phase":    nodeID,
				"approval": true,
				"title":    title,
				"summary":  summary,
			}, nil
		}
		payload := map[string]any{
			"gate":    nodeID,
			"goal":    s.GetString("goal"),
			"title":   title,
			"summary": summary,
		}
		v, err := graph.InterruptOrResume(runCtx, payload)
		if err != nil {
			return nil, err
		}
		ok := approvalTruthy(v)
		upd := graph.Update{
			"phase":    nodeID,
			"approval": ok,
			"title":    title,
			"summary":  summary,
		}
		if !ok {
			upd["status"] = "denied"
		}
		return upd, nil
	}, nil
}

func approvalTruthy(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "1", "true", "yes", "y", "approve", "approved":
			return true
		}
	case float64:
		return x != 0
	case int:
		return x != 0
	}
	return false
}
