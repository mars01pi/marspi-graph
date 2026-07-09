package graph

import (
	"context"
	"fmt"
	"sync"
)

// Builder constructs a StateGraph before Compile.
type Builder struct {
	mu     sync.Mutex
	nodes  map[string]NodeFunc
	edges  map[string]string // fixed: from -> to
	routes map[string]RouteFunc
	entry  string
}

// NewBuilder creates an empty graph builder.
func NewBuilder() *Builder {
	return &Builder{
		nodes:  map[string]NodeFunc{},
		edges:  map[string]string{},
		routes: map[string]RouteFunc{},
	}
}

// AddNode registers a named node. Panics on duplicate names.
func (b *Builder) AddNode(name string, fn NodeFunc) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()
	if name == "" || name == START || name == END {
		panic("graph: invalid node name: " + name)
	}
	if _, ok := b.nodes[name]; ok {
		panic("graph: duplicate node: " + name)
	}
	if fn == nil {
		panic("graph: nil node func: " + name)
	}
	b.nodes[name] = fn
	return b
}

// SetEntry sets the first node after START.
func (b *Builder) SetEntry(name string) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entry = name
	return b
}

// AddEdge adds a fixed transition from -> to (to may be END).
func (b *Builder) AddEdge(from, to string) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.routes[from]; ok {
		panic("graph: node already has conditional route: " + from)
	}
	b.edges[from] = to
	return b
}

// AddConditionalEdges registers a router for from.
func (b *Builder) AddConditionalEdges(from string, route RouteFunc) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()
	if route == nil {
		panic("graph: nil route for " + from)
	}
	if _, ok := b.edges[from]; ok {
		panic("graph: node already has fixed edge: " + from)
	}
	b.routes[from] = route
	return b
}

// Compile validates topology and returns a runnable graph.
func (b *Builder) Compile(opts ...CompileOption) (*Compiled, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cfg := compileConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	if b.entry == "" {
		return nil, fmt.Errorf("graph: entry node not set")
	}
	if _, ok := b.nodes[b.entry]; !ok {
		return nil, fmt.Errorf("graph: entry node %q not registered", b.entry)
	}
	for from, to := range b.edges {
		if _, ok := b.nodes[from]; !ok && from != START {
			return nil, fmt.Errorf("graph: edge from unknown node %q", from)
		}
		if to != END {
			if _, ok := b.nodes[to]; !ok {
				return nil, fmt.Errorf("graph: edge to unknown node %q", to)
			}
		}
	}
	for from := range b.routes {
		if _, ok := b.nodes[from]; !ok {
			return nil, fmt.Errorf("graph: route from unknown node %q", from)
		}
	}

	nodes := make(map[string]NodeFunc, len(b.nodes))
	for k, v := range b.nodes {
		nodes[k] = v
	}
	edges := make(map[string]string, len(b.edges))
	for k, v := range b.edges {
		edges[k] = v
	}
	routes := make(map[string]RouteFunc, len(b.routes))
	for k, v := range b.routes {
		routes[k] = v
	}

	return &Compiled{
		nodes:        nodes,
		edges:        edges,
		routes:       routes,
		entry:        b.entry,
		checkpointer: cfg.checkpointer,
	}, nil
}

type compileConfig struct {
	checkpointer Checkpointer
}

// CompileOption configures Compile.
type CompileOption func(*compileConfig)

// WithCheckpointer attaches a checkpointer used by Invoke/Resume.
func WithCheckpointer(cp Checkpointer) CompileOption {
	return func(c *compileConfig) { c.checkpointer = cp }
}

// Checkpointer persists graph snapshots between super-steps.
type Checkpointer interface {
	Put(ctx context.Context, threadID string, snap Snapshot) error
	Get(ctx context.Context, threadID string) (Snapshot, bool, error)
}

// Snapshot is one checkpoint of graph execution.
type Snapshot struct {
	ThreadID  string
	Node      string // next node to run (or END)
	State     State
	Step      int
	Interrupt bool
}

// Compiled is an immutable runnable graph.
type Compiled struct {
	nodes        map[string]NodeFunc
	edges        map[string]string
	routes       map[string]RouteFunc
	entry        string
	checkpointer Checkpointer
}

// Invoke runs the graph to completion (or interrupt) starting from entry.
func (g *Compiled) Invoke(ctx context.Context, in State, opts ...RunOption) (State, error) {
	cfg := runConfig{threadID: "default"}
	for _, o := range opts {
		o(&cfg)
	}
	state := in.Clone()
	if state == nil {
		state = State{}
	}
	node := g.entry
	step := 0

	for node != "" && node != END {
		if err := ctx.Err(); err != nil {
			return state, err
		}
		fn, ok := g.nodes[node]
		if !ok {
			return state, fmt.Errorf("graph: unknown node %q", node)
		}
		upd, err := fn(ctx, state)
		if err != nil {
			return state, fmt.Errorf("graph: node %q: %w", node, err)
		}
		state = Apply(state, upd)
		step++

		next, err := g.next(ctx, node, state)
		if err != nil {
			return state, err
		}
		if g.checkpointer != nil {
			if err := g.checkpointer.Put(ctx, cfg.threadID, Snapshot{
				ThreadID: cfg.threadID,
				Node:     next,
				State:    state.Clone(),
				Step:     step,
			}); err != nil {
				return state, err
			}
		}
		node = next
		if cfg.maxSteps > 0 && step >= cfg.maxSteps {
			return state, fmt.Errorf("graph: max steps %d exceeded", cfg.maxSteps)
		}
	}
	return state, nil
}

func (g *Compiled) next(ctx context.Context, from string, s State) (string, error) {
	if route, ok := g.routes[from]; ok {
		to := route(ctx, s)
		if to == "" {
			return END, nil
		}
		if to != END {
			if _, ok := g.nodes[to]; !ok {
				return "", fmt.Errorf("graph: route from %q to unknown %q", from, to)
			}
		}
		return to, nil
	}
	to, ok := g.edges[from]
	if !ok {
		return END, nil
	}
	return to, nil
}

type runConfig struct {
	threadID string
	maxSteps int
}

// RunOption configures a single Invoke/Resume.
type RunOption func(*runConfig)

// WithThreadID sets the checkpoint thread id.
func WithThreadID(id string) RunOption {
	return func(c *runConfig) {
		if id != "" {
			c.threadID = id
		}
	}
}

// WithMaxSteps caps super-steps (0 = unlimited).
func WithMaxSteps(n int) RunOption {
	return func(c *runConfig) { c.maxSteps = n }
}
