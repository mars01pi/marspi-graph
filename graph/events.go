package graph

import (
	"context"
	"fmt"
	"time"
)

// EventType identifies a graph lifecycle event.
type EventType string

const (
	EventRunStart         EventType = "run_start"
	EventRunResume        EventType = "run_resume"
	EventNodeStart        EventType = "node_start"
	EventNodeEnd          EventType = "node_end"
	EventNodeError        EventType = "node_error"
	EventRoute            EventType = "route"
	EventCheckpoint       EventType = "checkpoint"
	EventCheckpointError  EventType = "checkpoint_error"
	EventInterrupt        EventType = "interrupt"
	EventRunEnd           EventType = "run_end"
	EventCustom           EventType = "custom"
)

// Event is a metadata-only graph lifecycle notification.
type Event struct {
	Type         EventType
	RunID        string
	ThreadID     string
	CheckpointID string
	NodeID       string
	AgentID      string
	Step         int
	Timestamp    time.Time
	Err          error
	Metadata     map[string]string
}

// EventHandler receives lifecycle events synchronously.
// Handlers must return quickly; panics are recovered by the engine.
type EventHandler func(context.Context, Event)

// EmitCustom sends a custom event through the run's handler, if any.
func EmitCustom(ctx context.Context, e Event) {
	h := eventHandlerFrom(ctx)
	if h == nil {
		return
	}
	e.Type = EventCustom
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	if e.RunID == "" {
		e.RunID = RunID(ctx)
	}
	if e.ThreadID == "" {
		e.ThreadID = ThreadID(ctx)
	}
	if e.NodeID == "" {
		e.NodeID = NodeID(ctx)
	}
	safeEmit(h, ctx, e)
}

type eventHandlerKey struct{}

func withEventHandler(ctx context.Context, h EventHandler) context.Context {
	if h == nil {
		return ctx
	}
	return context.WithValue(ctx, eventHandlerKey{}, h)
}

func eventHandlerFrom(ctx context.Context) EventHandler {
	h, _ := ctx.Value(eventHandlerKey{}).(EventHandler)
	return h
}

func safeEmit(h EventHandler, ctx context.Context, e Event) {
	if h == nil {
		return
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	defer func() {
		_ = recover()
	}()
	h(ctx, e)
}

func (g *Compiled) emit(ctx context.Context, cfg runConfig, e Event) {
	h := cfg.events
	if h == nil {
		h = eventHandlerFrom(ctx)
	}
	if e.RunID == "" {
		e.RunID = cfg.runID
	}
	if e.ThreadID == "" {
		e.ThreadID = cfg.threadID
	}
	safeEmit(h, ctx, e)
}

// FormatEventErr returns a short metadata value for terminal run_end errors.
func FormatEventErr(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%v", err)
}
