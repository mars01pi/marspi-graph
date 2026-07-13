package graph

import (
	"context"
	"fmt"
	"sync"
)

type artifactCollectorKey struct{}
type threadIDKey struct{}
type runIDKey struct{}
type nodeIDKey struct{}
type parentCheckpointKey struct{}

type artifactCollector struct {
	mu   sync.Mutex
	list []StepArtifact
	seen map[string]struct{}
}

func newArtifactCollector() *artifactCollector {
	return &artifactCollector{seen: map[string]struct{}{}}
}

// WithStepArtifacts attaches a per-node artifact collector to ctx.
func WithStepArtifacts(ctx context.Context) (context.Context, *artifactCollector) {
	c := newArtifactCollector()
	return context.WithValue(ctx, artifactCollectorKey{}, c), c
}

// RegisterStepArtifact records an artifact for the current node step.
// Duplicate (Kind, Key) in the same step returns an error.
func RegisterStepArtifact(ctx context.Context, artifact StepArtifact) error {
	v := ctx.Value(artifactCollectorKey{})
	c, ok := v.(*artifactCollector)
	if !ok || c == nil {
		return fmt.Errorf("graph: no step artifact collector in context")
	}
	if artifact.Kind == "" || artifact.Key == "" {
		return fmt.Errorf("graph: artifact kind and key are required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	sk := artifact.Kind + "\x00" + artifact.Key
	if _, exists := c.seen[sk]; exists {
		return fmt.Errorf("graph: duplicate artifact %q/%q", artifact.Kind, artifact.Key)
	}
	c.seen[sk] = struct{}{}
	c.list = append(c.list, artifact)
	return nil
}

func (c *artifactCollector) snapshot() []StepArtifact {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]StepArtifact, len(c.list))
	copy(out, c.list)
	return out
}

// WithThreadIDCtx stores the run thread id for nodes / agentspec.
func WithThreadIDCtx(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, threadIDKey{}, id)
}

// ThreadID returns the current run thread id, if set.
func ThreadID(ctx context.Context) string {
	v, _ := ctx.Value(threadIDKey{}).(string)
	return v
}

// WithRunID stores the current Invoke/Resume run id.
func WithRunID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, runIDKey{}, id)
}

// RunID returns the current run id, if set.
func RunID(ctx context.Context) string {
	v, _ := ctx.Value(runIDKey{}).(string)
	return v
}

// WithNodeID stores the current node name.
func WithNodeID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, nodeIDKey{}, id)
}

// NodeID returns the current node name, if set.
func NodeID(ctx context.Context) string {
	v, _ := ctx.Value(nodeIDKey{}).(string)
	return v
}

// WithParentCheckpointID stores the checkpoint that this step continues from.
func WithParentCheckpointID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, parentCheckpointKey{}, id)
}

// ParentCheckpointID returns the parent checkpoint id, if set.
func ParentCheckpointID(ctx context.Context) string {
	v, _ := ctx.Value(parentCheckpointKey{}).(string)
	return v
}
