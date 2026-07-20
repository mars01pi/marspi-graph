package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Builder constructs a StateGraph before Compile.
type Builder struct {
	mu       sync.Mutex
	nodes    map[string]NodeFunc
	edges    map[string]string // fixed: from -> to
	routes   map[string]RouteFunc
	reducers map[string]Reducer
	entry    string
}

// NewBuilder creates an empty graph builder.
func NewBuilder() *Builder {
	return &Builder{
		nodes:    map[string]NodeFunc{},
		edges:    map[string]string{},
		routes:   map[string]RouteFunc{},
		reducers: map[string]Reducer{},
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

// AddReducer registers a per-key merge function for state updates.
// Keys without a reducer use last-write-wins.
func (b *Builder) AddReducer(key string, r Reducer) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()
	if key == "" {
		panic("graph: empty reducer key")
	}
	if r == nil {
		panic("graph: nil reducer for " + key)
	}
	b.reducers[key] = r
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

	if cfg.lease != nil && cfg.durable != nil {
		if _, ok := cfg.durable.(LeaseFencedCommitter); !ok {
			return nil, fmt.Errorf("graph: ExecutionLease with durable store requires LeaseFencedCommitter")
		}
	}
	if cfg.lease != nil && cfg.durable == nil && cfg.checkpointer != nil {
		return nil, fmt.Errorf("graph: ExecutionLease requires durable checkpointer with fenced commit, not legacy Checkpointer")
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
	reducers := make(map[string]Reducer, len(b.reducers))
	for k, v := range b.reducers {
		reducers[k] = v
	}

	leaseTTL := cfg.leaseTTL
	if leaseTTL <= 0 {
		leaseTTL = DefaultLeaseTTL
	}
	releaseTO := cfg.leaseReleaseTimeout
	if releaseTO <= 0 {
		releaseTO = DefaultLeaseReleaseTimeout
	}

	return &Compiled{
		nodes:               nodes,
		edges:               edges,
		routes:              routes,
		reducers:            reducers,
		entry:               b.entry,
		checkpointer:        cfg.checkpointer,
		durable:             cfg.durable,
		lease:               cfg.lease,
		leaseTTL:            leaseTTL,
		leaseReleaseTimeout: releaseTO,
	}, nil
}

type compileConfig struct {
	checkpointer         Checkpointer
	durable             DurableCheckpointer
	lease               ExecutionLease
	leaseTTL            time.Duration
	leaseReleaseTimeout time.Duration
}

// CompileOption configures Compile.
type CompileOption func(*compileConfig)

// WithCheckpointer attaches a legacy checkpointer used by Invoke/Resume.
func WithCheckpointer(cp Checkpointer) CompileOption {
	return func(c *compileConfig) { c.checkpointer = cp }
}

// WithDurableCheckpointer attaches the P1 durable store (MySQL / Memory).
// When set, CommitStep/GetLatest are preferred over legacy Put/Get.
func WithDurableCheckpointer(d DurableCheckpointer) CompileOption {
	return func(c *compileConfig) { c.durable = d }
}

// WithExecutionLease enables exclusive Invoke/Resume for a thread.
// When paired with a durable store, that store must implement LeaseFencedCommitter.
func WithExecutionLease(l ExecutionLease) CompileOption {
	return func(c *compileConfig) { c.lease = l }
}

// WithLeaseTTL sets the default lease lifetime (default 30s).
func WithLeaseTTL(ttl time.Duration) CompileOption {
	return func(c *compileConfig) {
		if ttl > 0 {
			c.leaseTTL = ttl
		}
	}
}

// WithLeaseReleaseTimeout bounds Release after run exit (default 3s).
func WithLeaseReleaseTimeout(d time.Duration) CompileOption {
	return func(c *compileConfig) {
		if d > 0 {
			c.leaseReleaseTimeout = d
		}
	}
}

// Checkpointer persists graph snapshots between super-steps (latest-only).
type Checkpointer interface {
	Put(ctx context.Context, threadID string, snap Snapshot) error
	Get(ctx context.Context, threadID string) (Snapshot, bool, error)
}

// Snapshot is one checkpoint of graph execution.
type Snapshot struct {
	CheckpointID       string
	ParentCheckpointID string
	Revision           int64
	CreatedAt          time.Time
	ThreadID           string
	Node               string // next node to run (or current if Interrupt)
	State              State
	Step               int
	Interrupt          bool
	InterruptValue     any
}

// Compiled is an immutable runnable graph.
type Compiled struct {
	nodes               map[string]NodeFunc
	edges               map[string]string
	routes              map[string]RouteFunc
	reducers            map[string]Reducer
	entry               string
	checkpointer        Checkpointer
	durable             DurableCheckpointer
	lease               ExecutionLease
	leaseTTL            time.Duration
	leaseReleaseTimeout time.Duration
}

// Invoke runs the graph to completion (or interrupt) starting from entry.
func (g *Compiled) Invoke(ctx context.Context, in State, opts ...RunOption) (State, error) {
	cfg := g.prepareRun(opts...)
	state := in.Clone()
	if state == nil {
		state = State{}
	}
	return g.withLease(ctx, &cfg, func(ctx context.Context) (State, error) {
		ctx = g.enrichCtx(ctx, cfg, "")
		g.emit(ctx, cfg, Event{Type: EventRunStart, Step: 0})
		out, err := g.run(ctx, state, g.entry, 0, "", cfg)
		g.emitRunEnd(ctx, cfg, err)
		return out, err
	})
}

// Resume continues from the latest checkpoint for threadID.
// Pass WithCommand to inject a resume value / state patch after interrupt.
func (g *Compiled) Resume(ctx context.Context, threadID string, opts ...RunOption) (State, error) {
	cfg := g.prepareRun(opts...)
	if threadID != "" {
		cfg.threadID = threadID
	}
	if g.durable == nil && g.checkpointer == nil {
		return nil, fmt.Errorf("graph: resume requires a checkpointer")
	}
	return g.withLease(ctx, &cfg, func(ctx context.Context) (State, error) {
		snap, ok, err := g.loadLatest(ctx, cfg.threadID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("graph: no checkpoint for thread %q", cfg.threadID)
		}
		if cfg.expectedCheckpointID != "" && snap.CheckpointID != cfg.expectedCheckpointID {
			return nil, fmt.Errorf("%w: want %q, latest %q",
				ErrExpectedCheckpointMismatch, cfg.expectedCheckpointID, snap.CheckpointID)
		}

		state := snap.State.Clone()
		if state == nil {
			state = State{}
		}
		if cfg.command != nil && cfg.command.Update != nil {
			state = Apply(state, cfg.command.Update, g.reducers)
		}

		node := snap.Node
		if cfg.command != nil && cfg.command.Goto != "" {
			node = cfg.command.Goto
		}
		if node == "" || node == END {
			ctx = g.enrichCtx(ctx, cfg, snap.CheckpointID)
			g.emit(ctx, cfg, Event{Type: EventRunResume, CheckpointID: snap.CheckpointID, Step: snap.Step})
			g.emit(ctx, cfg, Event{Type: EventRunEnd, CheckpointID: snap.CheckpointID, Step: snap.Step, Metadata: map[string]string{"status": "completed"}})
			return state, nil
		}

		if snap.Interrupt {
			if cfg.command == nil || cfg.command.Resume == nil {
				return state, fmt.Errorf("graph: interrupted at %q; resume requires WithCommand(Command{Resume: ...})", node)
			}
			ctx = WithResumeValue(ctx, cfg.command.Resume)
		}

		parentID := snap.CheckpointID
		ctx = g.enrichCtx(ctx, cfg, parentID)
		g.emit(ctx, cfg, Event{Type: EventRunResume, CheckpointID: parentID, NodeID: node, Step: snap.Step})
		out, err := g.run(ctx, state, node, snap.Step, parentID, cfg)
		g.emitRunEnd(ctx, cfg, err)
		return out, err
	})
}

func (g *Compiled) prepareRun(opts ...RunOption) runConfig {
	cfg := runConfig{threadID: "default", runID: NewID()}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.runID == "" {
		cfg.runID = NewID()
	}
	if cfg.leaseTTL <= 0 {
		cfg.leaseTTL = g.leaseTTL
	}
	return cfg
}

func (g *Compiled) enrichCtx(ctx context.Context, cfg runConfig, parentCheckpointID string) context.Context {
	ctx = WithThreadIDCtx(ctx, cfg.threadID)
	ctx = WithRunID(ctx, cfg.runID)
	ctx = WithParentCheckpointID(ctx, parentCheckpointID)
	if cfg.events != nil {
		ctx = withEventHandler(ctx, cfg.events)
	}
	if cfg.leaseSess != nil {
		ctx = WithLeaseGrant(ctx, cfg.leaseSess.get())
	}
	return ctx
}

// withLease acquires an execution lease, runs body under a heartbeat, then releases.
func (g *Compiled) withLease(ctx context.Context, cfg *runConfig, body func(context.Context) (State, error)) (State, error) {
	if g.lease == nil {
		return body(ctx)
	}
	ttl := cfg.leaseTTL
	if ttl <= 0 {
		ttl = g.leaseTTL
	}
	if ttl <= 0 {
		ttl = DefaultLeaseTTL
	}

	grant, err := g.lease.Acquire(ctx, cfg.threadID, cfg.runID, ttl)
	if err != nil {
		return nil, err
	}
	cfg.leaseSess = &leaseSession{grant: grant}

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	runCtx = WithThreadIDCtx(runCtx, cfg.threadID)
	runCtx = WithRunID(runCtx, cfg.runID)
	if cfg.events != nil {
		runCtx = withEventHandler(runCtx, cfg.events)
	}
	runCtx = WithLeaseGrant(runCtx, grant)

	done := make(chan struct{})
	go g.leaseHeartbeat(runCtx, cancel, cfg, ttl, done)

	g.emit(runCtx, *cfg, Event{
		Type: EventLeaseAcquired,
		Metadata: map[string]string{
			"epoch": fmt.Sprintf("%d", grant.Epoch),
			"ttl":   ttl.String(),
		},
	})

	out, err := body(runCtx)

	cancel(nil)
	<-done

	status := "completed"
	if err != nil {
		if IsInterrupted(err) {
			status = "paused"
		} else if errors.Is(err, ErrLeaseLost) || errors.Is(context.Cause(runCtx), ErrLeaseLost) {
			status = "lease_lost"
		} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			status = "cancelled"
		} else {
			status = "error"
		}
	}
	final := cfg.leaseSess.get()
	g.emit(ctx, *cfg, Event{
		Type: EventLeaseReleased,
		Metadata: map[string]string{
			"epoch":  fmt.Sprintf("%d", final.Epoch),
			"status": status,
		},
	})

	releaseTO := g.leaseReleaseTimeout
	if releaseTO <= 0 {
		releaseTO = DefaultLeaseReleaseTimeout
	}
	releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTO)
	_ = g.lease.Release(releaseCtx, final)
	releaseCancel()
	return out, err
}

func (g *Compiled) leaseHeartbeat(runCtx context.Context, cancel context.CancelCauseFunc, cfg *runConfig, ttl time.Duration, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(heartbeatInterval(ttl))
	defer ticker.Stop()
	for {
		select {
		case <-runCtx.Done():
			return
		case <-ticker.C:
			grant := cfg.leaseSess.get()
			next, err := g.lease.Renew(runCtx, grant, ttl)
			if err != nil {
				if runCtx.Err() != nil {
					return
				}
				g.emit(runCtx, *cfg, Event{
					Type: EventLeaseRenewFailed,
					Err:  err,
					Metadata: map[string]string{
						"epoch": fmt.Sprintf("%d", grant.Epoch),
						"class": leaseErrClass(err),
					},
				})
				if errors.Is(err, ErrLeaseLost) {
					cancel(ErrLeaseLost)
				} else {
					cancel(fmt.Errorf("%w: renew: %v", ErrLeaseLost, err))
				}
				return
			}
			cfg.leaseSess.set(next)
		}
	}
}

func heartbeatInterval(ttl time.Duration) time.Duration {
	iv := ttl / 3
	if iv <= 0 {
		iv = time.Millisecond
	}
	if max := ttl / 2; max > 0 && iv >= max {
		iv = max - time.Millisecond
		if iv <= 0 {
			iv = time.Millisecond
		}
	}
	return iv
}

func leaseErrClass(err error) string {
	if errors.Is(err, ErrLeaseLost) {
		return "lost"
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "canceled"
	}
	return "error"
}

func (g *Compiled) emitRunEnd(ctx context.Context, cfg runConfig, err error) {
	meta := map[string]string{"status": "completed"}
	if err != nil {
		if IsInterrupted(err) {
			meta["status"] = "paused"
		} else if errors.Is(err, ErrLeaseLost) || errors.Is(context.Cause(ctx), ErrLeaseLost) {
			meta["status"] = "lease_lost"
			meta["error"] = FormatEventErr(err)
		} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			meta["status"] = "cancelled"
		} else {
			meta["status"] = "error"
			meta["error"] = FormatEventErr(err)
		}
	}
	g.emit(ctx, cfg, Event{Type: EventRunEnd, Err: err, Metadata: meta})
}

func (g *Compiled) loadLatest(ctx context.Context, threadID string) (Snapshot, bool, error) {
	if g.durable != nil {
		return g.durable.GetLatest(ctx, threadID)
	}
	return g.checkpointer.Get(ctx, threadID)
}

func (g *Compiled) persist(ctx context.Context, cfg runConfig, snap Snapshot, artifacts []StepArtifact) error {
	if err := leaseLostFrom(ctx); err != nil {
		return err
	}
	if g.durable != nil {
		if g.lease != nil {
			fc, ok := g.durable.(LeaseFencedCommitter)
			if !ok {
				return fmt.Errorf("graph: ExecutionLease with durable store requires LeaseFencedCommitter")
			}
			if cfg.leaseSess == nil {
				return fmt.Errorf("graph: missing lease grant for fenced commit")
			}
			return fc.CommitStepFenced(ctx, snap, artifacts, cfg.leaseSess.get())
		}
		return g.durable.CommitStep(ctx, snap, artifacts)
	}
	if g.checkpointer != nil {
		return g.checkpointer.Put(ctx, cfg.threadID, snap)
	}
	return fmt.Errorf("graph: no checkpointer")
}

func leaseLostFrom(ctx context.Context) error {
	if cause := context.Cause(ctx); errors.Is(cause, ErrLeaseLost) {
		return ErrLeaseLost
	}
	if err := ctx.Err(); err != nil && errors.Is(context.Cause(ctx), ErrLeaseLost) {
		return ErrLeaseLost
	}
	return nil
}

func (g *Compiled) hasStore() bool {
	return g.durable != nil || g.checkpointer != nil
}

func (g *Compiled) run(ctx context.Context, state State, node string, step int, parentCheckpointID string, cfg runConfig) (State, error) {
	parent := parentCheckpointID
	for node != "" && node != END {
		if err := leaseLostFrom(ctx); err != nil {
			return state, err
		}
		if err := ctx.Err(); err != nil {
			return state, err
		}
		fn, ok := g.nodes[node]
		if !ok {
			return state, fmt.Errorf("graph: unknown node %q", node)
		}

		nodeCtx := WithNodeID(ctx, node)
		nodeCtx = WithParentCheckpointID(nodeCtx, parent)
		if cfg.leaseSess != nil {
			nodeCtx = WithLeaseGrant(nodeCtx, cfg.leaseSess.get())
		}
		nodeCtx, collector := WithStepArtifacts(nodeCtx)

		g.emit(nodeCtx, cfg, Event{Type: EventNodeStart, NodeID: node, Step: step, CheckpointID: parent})

		upd, err := fn(nodeCtx, state)
		if lost := leaseLostFrom(ctx); lost != nil {
			// Discard update/artifacts after heartbeat loss.
			return state, lost
		}
		if ie, ok := AsInterrupt(err); ok {
			ie.Node = node
			if !g.hasStore() {
				g.emit(nodeCtx, cfg, Event{Type: EventNodeError, NodeID: node, Step: step, Err: ie})
				return state, fmt.Errorf("graph: interrupt at %q requires a checkpointer: %w", node, ie)
			}
			snap := Snapshot{
				CheckpointID:       NewID(),
				ParentCheckpointID: parent,
				ThreadID:           cfg.threadID,
				Node:               node, // re-enter same node on Resume
				State:              state.Clone(),
				Step:               step,
				Interrupt:          true,
				InterruptValue:     ie.Value,
				CreatedAt:          time.Now().UTC(),
			}
			arts := collector.snapshot()
			if putErr := g.persist(nodeCtx, cfg, snap, arts); putErr != nil {
				g.emit(nodeCtx, cfg, Event{Type: EventCheckpointError, NodeID: node, Step: step, CheckpointID: snap.CheckpointID, Err: putErr})
				return state, putErr
			}
			parent = snap.CheckpointID
			g.emit(nodeCtx, cfg, Event{Type: EventCheckpoint, NodeID: node, Step: step, CheckpointID: snap.CheckpointID, Metadata: map[string]string{"interrupt": "true"}})
			g.emit(nodeCtx, cfg, Event{Type: EventInterrupt, NodeID: node, Step: step, CheckpointID: snap.CheckpointID})
			return state, ie
		}
		if err != nil {
			g.emit(nodeCtx, cfg, Event{Type: EventNodeError, NodeID: node, Step: step, Err: err})
			return state, fmt.Errorf("graph: node %q: %w", node, err)
		}

		g.emit(nodeCtx, cfg, Event{Type: EventNodeEnd, NodeID: node, Step: step, CheckpointID: parent})

		state = Apply(state, upd, g.reducers)
		step++

		// Clear one-shot resume value after a successful node re-entry.
		ctx = context.WithValue(ctx, resumeCtxKey{}, nil)

		next, err := g.next(ctx, node, state)
		if err != nil {
			return state, err
		}
		g.emit(nodeCtx, cfg, Event{
			Type:         EventRoute,
			NodeID:       node,
			Step:         step,
			CheckpointID: parent,
			Metadata:     map[string]string{"from": node, "to": next},
		})

		if g.hasStore() {
			snap := Snapshot{
				CheckpointID:       NewID(),
				ParentCheckpointID: parent,
				ThreadID:           cfg.threadID,
				Node:               next,
				State:              state.Clone(),
				Step:               step,
				Interrupt:          false,
				CreatedAt:          time.Now().UTC(),
			}
			arts := collector.snapshot()
			if err := g.persist(nodeCtx, cfg, snap, arts); err != nil {
				g.emit(nodeCtx, cfg, Event{Type: EventCheckpointError, NodeID: node, Step: step, CheckpointID: snap.CheckpointID, Err: err})
				return state, err
			}
			parent = snap.CheckpointID
			ctx = WithParentCheckpointID(ctx, parent)
			g.emit(nodeCtx, cfg, Event{Type: EventCheckpoint, NodeID: node, Step: step, CheckpointID: snap.CheckpointID, Metadata: map[string]string{"next": next}})
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

type leaseSession struct {
	mu    sync.Mutex
	grant LeaseGrant
}

func (s *leaseSession) get() LeaseGrant {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.grant
}

func (s *leaseSession) set(g LeaseGrant) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grant = g
}

type runConfig struct {
	threadID             string
	maxSteps             int
	command              *Command
	runID                string // 实例id
	events               EventHandler
	leaseTTL             time.Duration
	leaseSess            *leaseSession
	expectedCheckpointID string
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

// WithCommand attaches a Resume command (resume value / update / goto).
func WithCommand(cmd Command) RunOption {
	return func(c *runConfig) {
		c.command = &cmd
	}
}

// WithEventHandler attaches a per-run lifecycle event handler.
func WithEventHandler(h EventHandler) RunOption {
	return func(c *runConfig) { c.events = h }
}

// WithRunLeaseTTL overrides the compile-time lease TTL for one run.
func WithRunLeaseTTL(ttl time.Duration) RunOption {
	return func(c *runConfig) {
		if ttl > 0 {
			c.leaseTTL = ttl
		}
	}
}

// WithExpectedCheckpointID requires Resume to load exactly this latest checkpoint.
// Checked after lease acquisition to close stale-approval TOCTOU windows.
func WithExpectedCheckpointID(checkpointID string) RunOption {
	return func(c *runConfig) {
		c.expectedCheckpointID = checkpointID
	}
}

// IsInterrupted reports whether err is (or wraps) an interrupt.
func IsInterrupted(err error) bool {
	return errors.Is(err, ErrInterrupted)
}
