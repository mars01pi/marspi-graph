package orchestrator

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Handoff is the transfer record between supervisor and a worker (or END).
// It is a State protocol, not a core tool.
type Handoff struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Reason string `json:"reason"`
	Task   string `json:"task"`
}

// Decision is the structured routing output expected from the supervisor LLM.
type Decision struct {
	Next   string `json:"next"` // worker id or "END" / "__end__"
	Reason string `json:"reason"`
	Task   string `json:"task"`
}

// ParseDecision extracts a Decision from model text (raw JSON or fenced block).
func ParseDecision(text string) (Decision, error) {
	raw := strings.TrimSpace(text)
	if raw == "" {
		return Decision{}, fmt.Errorf("orchestrator: empty supervisor decision")
	}
	if i := strings.Index(raw, "{"); i >= 0 {
		if j := strings.LastIndex(raw, "}"); j > i {
			raw = raw[i : j+1]
		}
	}
	var d Decision
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		return Decision{}, fmt.Errorf("orchestrator: parse decision: %w", err)
	}
	d.Next = strings.TrimSpace(d.Next)
	d.Reason = strings.TrimSpace(d.Reason)
	d.Task = strings.TrimSpace(d.Task)
	if d.Next == "" {
		return Decision{}, fmt.Errorf("orchestrator: decision missing next")
	}
	return d, nil
}

// NormalizeNext maps END aliases to graph.END and leaves worker ids as-is.
func NormalizeNext(next string) string {
	n := strings.TrimSpace(next)
	switch strings.ToLower(n) {
	case "end", "__end__", "finish", "done", "stop":
		return "__end__"
	}
	return n
}

// HandoffFromMap reads a Handoff stored in State["handoff"].
func HandoffFromMap(v any) Handoff {
	switch x := v.(type) {
	case Handoff:
		return x
	case map[string]any:
		return Handoff{
			From:   strField(x, "from"),
			To:     strField(x, "to"),
			Reason: strField(x, "reason"),
			Task:   strField(x, "task"),
		}
	default:
		return Handoff{}
	}
}

func strField(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

// handoffToMap stores Handoff as a plain map for checkpoint-friendly State.
func handoffToMap(h Handoff) map[string]any {
	return map[string]any{
		"from":   h.From,
		"to":     h.To,
		"reason": h.Reason,
		"task":   h.Task,
	}
}
