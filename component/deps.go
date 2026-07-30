package component

import (
	"github.com/mars/marspi-core/agent"
	"github.com/mars/marspi-core/console"
	"github.com/mars/marspi-core/llm"
	"github.com/mars/marspi-core/tool"
	"github.com/mars/marspi-graph/agentspec"
	"github.com/mars/marspi-graph/graph"
)

// ExecDeps are host-injected runtime services for component BuildNode.
type ExecDeps struct {
	Provider      llm.Provider
	Registry      *tool.Registry
	Reporter      console.Reporter
	Events        *agent.Emitter
	SystemPrompt  string
	MaxContext    int
	MaxIterAgent  int
	Stream        bool
	Durable       graph.DurableCheckpointer
	SpawnChildren []agentspec.ChildSpec
}

func depsFromHost(host Host) *ExecDeps {
	if d, ok := host.(*ExecDeps); ok {
		return d
	}
	return nil
}
