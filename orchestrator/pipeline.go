// Package orchestrator provides preset multi-agent workflows built on graph.
package orchestrator

import (
	"fmt"

	"github.com/mars/marspi-graph/graph"
)

// Pipeline builds a linear node1→node2→…→END graph.
// order lists node names in execution order; nodes must contain each name.
func Pipeline(nodes map[string]graph.NodeFunc, order []string) (*graph.Compiled, error) {
	if len(order) == 0 {
		return nil, fmt.Errorf("orchestrator: empty pipeline order")
	}
	b := graph.NewBuilder()
	for _, name := range order {
		fn, ok := nodes[name]
		if !ok {
			return nil, fmt.Errorf("orchestrator: missing node %q", name)
		}
		b.AddNode(name, fn)
	}
	b.SetEntry(order[0])
	for i := 0; i < len(order)-1; i++ {
		b.AddEdge(order[i], order[i+1])
	}
	b.AddEdge(order[len(order)-1], graph.END)
	return b.Compile()
}
