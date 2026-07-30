package component

import (
	"encoding/json"
	"strings"

	"github.com/mars/marspi-core/agentctx"
	"github.com/mars/marspi-graph/graph"
)

// ExtractorSpec declares how an agent writes derived values into State.
type ExtractorSpec struct {
	To    string   `json:"to" yaml:"to"`
	From  string   `json:"from" yaml:"from"`
	Tools []string `json:"tools,omitempty" yaml:"tools,omitempty"`
	Value string   `json:"value,omitempty" yaml:"value,omitempty"`
}

const (
	ExtractToolCalls  = "tool_calls"
	ExtractOutput     = "output"
	ExtractConst      = "const"
	ExtractCompletion = "completion"
)

// OutputSpec is an instance-level published field (canvas variable).
type OutputSpec struct {
	Name     string `json:"name" yaml:"name"`
	StateKey string `json:"state_key,omitempty" yaml:"state_key,omitempty"`
	Label    string `json:"label,omitempty" yaml:"label,omitempty"`
}

func (o OutputSpec) stateKey() string {
	if o.StateKey != "" {
		return o.StateKey
	}
	return o.Name
}

func applyExtractors(extractors []ExtractorSpec, mgr *agentctx.Manager, output string, upd graph.Update) {
	for _, ex := range extractors {
		if strings.TrimSpace(ex.To) == "" {
			continue
		}
		switch ex.From {
		case ExtractToolCalls:
			tools := ex.Tools
			if len(tools) == 0 {
				tools = []string{"edit", "write"}
			}
			files := changedFilesFromTools(mgr, tools)
			if files != "" {
				upd[ex.To] = files
			}
		case ExtractOutput:
			upd[ex.To] = output
		case ExtractConst:
			upd[ex.To] = ex.Value
		case ExtractCompletion:
			v := completionResult(mgr)
			if v == "" {
				v = output
			}
			if v != "" {
				upd[ex.To] = v
			}
		}
	}
}

// completionResult returns the latest attempt_completion tool result,
// falling back to the latest non-empty assistant content.
func completionResult(ctx *agentctx.Manager) string {
	if ctx == nil {
		return ""
	}
	msgs := ctx.Messages
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		if r, _ := m["role"].(string); r != "tool" {
			continue
		}
		if tn, _ := m["tool_name"].(string); tn == "attempt_completion" {
			if c, ok := m["content"].(string); ok {
				return c
			}
		}
	}
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		if r, _ := m["role"].(string); r != "assistant" {
			continue
		}
		if c, ok := m["content"].(string); ok && c != "" {
			return c
		}
	}
	return ""
}

func changedFilesFromTools(ctx *agentctx.Manager, tools []string) string {
	allow := map[string]bool{}
	for _, t := range tools {
		allow[t] = true
	}
	seen := map[string]bool{}
	var files []string
	for _, m := range ctx.Messages {
		if r, _ := m["role"].(string); r != "assistant" {
			continue
		}
		tcs, ok := m["tool_calls"].([]any)
		if !ok {
			continue
		}
		for _, raw := range tcs {
			tc, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			fn, _ := tc["function"].(map[string]any)
			if fn == nil {
				continue
			}
			name, _ := fn["name"].(string)
			if !allow[name] {
				continue
			}
			path := pathFromToolArgs(fn["arguments"])
			if path == "" || seen[path] {
				continue
			}
			seen[path] = true
			files = append(files, path)
		}
	}
	if len(files) == 0 {
		return ""
	}
	return strings.Join(files, ",\n ")
}

func pathFromToolArgs(args any) string {
	switch a := args.(type) {
	case string:
		var m map[string]any
		if err := json.Unmarshal([]byte(a), &m); err != nil {
			return ""
		}
		p, _ := m["path"].(string)
		return p
	case map[string]any:
		p, _ := a["path"].(string)
		return p
	default:
		return ""
	}
}
