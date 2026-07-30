package component

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// decodeBindings maps a loose bindings map into a typed struct via YAML round-trip.
func decodeBindings(bindings map[string]any, dst any) error {
	if bindings == nil {
		return nil
	}
	raw, err := yaml.Marshal(bindings)
	if err != nil {
		return fmt.Errorf("component: encode bindings: %w", err)
	}
	if err := yaml.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("component: decode bindings: %w", err)
	}
	return nil
}

func strList(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, x := range t {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}
