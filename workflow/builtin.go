package workflow

import (
	"embed"
	"fmt"
)

//go:embed templates/*.yaml
var templatesFS embed.FS

// Builtin loads a named embedded template (e.g. "devflow").
func Builtin(name string) (Spec, error) {
	if name == "" {
		return Spec{}, fmt.Errorf("workflow: builtin name required")
	}
	data, err := templatesFS.ReadFile("templates/" + name + ".yaml")
	if err != nil {
		return Spec{}, fmt.Errorf("workflow: builtin %q: %w", name, err)
	}
	return ParseSpec(data)
}
