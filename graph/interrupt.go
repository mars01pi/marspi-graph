package graph

import (
	"context"
	"errors"
	"fmt"
)

// ErrInterrupted is returned by Invoke/Resume when a node requested a pause.
var ErrInterrupted = errors.New("graph: interrupted")

// InterruptError carries the payload exposed to the caller / UI.
type InterruptError struct {
	Value any
	Node  string
}

func (e *InterruptError) Error() string {
	if e.Node == "" {
		return "graph: interrupted"
	}
	return fmt.Sprintf("graph: interrupted at %q", e.Node)
}

func (e *InterruptError) Is(target error) bool {
	return target == ErrInterrupted
}

// Interrupt pauses graph execution after the current node is checkpointed.
// On Resume with Command{Resume: v}, ResumeValue(ctx) returns v inside the
// re-entered node (LangGraph-style interrupt/Command subset).
func Interrupt(value any) error {
	return &InterruptError{Value: value}
}

// InterruptOrResume returns the Command.Resume value when re-entering after
// an interrupt; otherwise pauses with the given payload (LangGraph pattern).
func InterruptOrResume(ctx context.Context, value any) (any, error) {
	if v, ok := ResumeValue(ctx); ok {
		return v, nil
	}
	return nil, Interrupt(value)
}

type resumeCtxKey struct{}

// WithResumeValue stores the Command.Resume payload for the re-entered node.
func WithResumeValue(ctx context.Context, v any) context.Context {
	return context.WithValue(ctx, resumeCtxKey{}, v)
}

// ResumeValue returns the value provided via Command.Resume, if any.
func ResumeValue(ctx context.Context) (any, bool) {
	v := ctx.Value(resumeCtxKey{})
	if v == nil {
		return nil, false
	}
	return v, true
}

// AsInterrupt unwraps an InterruptError.
func AsInterrupt(err error) (*InterruptError, bool) {
	var ie *InterruptError
	if errors.As(err, &ie) {
		return ie, true
	}
	return nil, false
}
