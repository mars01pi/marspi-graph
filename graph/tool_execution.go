package graph

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// IdempotencyKeyVersion prefixes ToolExecution.IdempotencyKey payloads.
// Bump only when the stable field set or encoding changes.
const IdempotencyKeyVersion = "v1"

// ToolExecution is stable execution metadata for downstream tool side effects.
// Callers (custom NodeFunc or agentspec→core wiring) use it to build
// idempotency keys and lease fencing tokens for external systems.
type ToolExecution struct {
	ThreadID           string
	RunID              string
	NodeID             string
	ParentCheckpointID string
	LeaseEpoch         int64
	ToolName           string
	OperationID        string
}

// ToolExecutionFrom builds ToolExecution from the current graph context.
// toolName identifies the tool; operationID must be a stable business or
// call-level id (not runID). Empty toolName or operationID returns an error.
func ToolExecutionFrom(ctx context.Context, toolName, operationID string) (ToolExecution, error) {
	toolName = strings.TrimSpace(toolName)
	operationID = strings.TrimSpace(operationID)
	if toolName == "" {
		return ToolExecution{}, fmt.Errorf("graph: ToolExecution requires toolName")
	}
	if operationID == "" {
		return ToolExecution{}, fmt.Errorf("graph: ToolExecution requires operationID")
	}
	return ToolExecution{
		ThreadID:           ThreadID(ctx),
		RunID:              RunID(ctx),
		NodeID:             NodeID(ctx),
		ParentCheckpointID: ParentCheckpointID(ctx),
		LeaseEpoch:         LeaseEpoch(ctx),
		ToolName:           toolName,
		OperationID:        operationID,
	}, nil
}

// IdempotencyKey returns a versioned SHA-256 hex digest suitable for
// HTTP Idempotency-Key headers or unique DB indexes.
//
// Stable coordinates: thread, parent checkpoint, node, tool name, operation ID.
// RunID and LeaseEpoch are intentionally excluded so retry/takeover of the
// same logical operation reuses the same downstream dedup key.
func (e ToolExecution) IdempotencyKey() string {
	payload := strings.Join([]string{
		IdempotencyKeyVersion,
		e.ThreadID,
		e.ParentCheckpointID,
		e.NodeID,
		e.ToolName,
		e.OperationID,
	}, "\x00")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// FenceToken returns the lease epoch for downstream monotonic fencing (0 if none).
func (e ToolExecution) FenceToken() int64 {
	return e.LeaseEpoch
}
