package checkpoint

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mars/marspi-graph/graph"
	_ "modernc.org/sqlite"
)

// SQLite persists the latest Snapshot per thread_id (Memory-compatible semantics).
// Graph-only: does not store agentctx (ADR 0004).
type SQLite struct {
	db *sql.DB
}

// SnapshotMeta is a lightweight row for listing interrupted threads.
type SnapshotMeta struct {
	ThreadID  string
	Node      string
	Step      int
	Interrupt bool
	UpdatedAt time.Time
}

// OpenSQLite opens (or creates) a SQLite checkpointer at path.
// Uses WAL mode. Caller should Close when done.
func OpenSQLite(path string) (*SQLite, error) {
	if path == "" {
		return nil, fmt.Errorf("checkpoint: sqlite path is empty")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: open sqlite: %w", err)
	}
	// Single-writer CLI usage; avoid busy surprises.
	db.SetMaxOpenConns(1)
	s := &SQLite{db: db}
	if err := s.setup(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLite) setup() error {
	stmts := []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			thread_id       TEXT PRIMARY KEY,
			node            TEXT NOT NULL,
			step            INTEGER NOT NULL,
			interrupt       INTEGER NOT NULL DEFAULT 0,
			interrupt_value TEXT,
			state_json      TEXT NOT NULL,
			updated_at      INTEGER NOT NULL
		)`,
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			return fmt.Errorf("checkpoint: setup: %w", err)
		}
	}
	return nil
}

// Close releases the database handle.
func (s *SQLite) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Put upserts the latest snapshot for threadID.
func (s *SQLite) Put(ctx context.Context, threadID string, snap graph.Snapshot) error {
	if threadID == "" {
		threadID = snap.ThreadID
	}
	if threadID == "" {
		return fmt.Errorf("checkpoint: put requires threadID")
	}
	snap.ThreadID = threadID

	stateJSON, err := json.Marshal(snap.State)
	if err != nil {
		return fmt.Errorf("checkpoint: marshal state: %w", err)
	}
	var ivJSON []byte
	if snap.InterruptValue != nil {
		ivJSON, err = json.Marshal(snap.InterruptValue)
		if err != nil {
			return fmt.Errorf("checkpoint: marshal interrupt_value: %w", err)
		}
	}
	interrupt := 0
	if snap.Interrupt {
		interrupt = 1
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO checkpoints (thread_id, node, step, interrupt, interrupt_value, state_json, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(thread_id) DO UPDATE SET
			node = excluded.node,
			step = excluded.step,
			interrupt = excluded.interrupt,
			interrupt_value = excluded.interrupt_value,
			state_json = excluded.state_json,
			updated_at = excluded.updated_at
	`, threadID, snap.Node, snap.Step, interrupt, nullableJSON(ivJSON), string(stateJSON), time.Now().Unix())
	if err != nil {
		return fmt.Errorf("checkpoint: put: %w", err)
	}
	return nil
}

func nullableJSON(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

// Get loads the latest snapshot for threadID.
func (s *SQLite) Get(ctx context.Context, threadID string) (graph.Snapshot, bool, error) {
	var (
		node, stateJSON string
		step, interrupt int
		ivSQL           sql.NullString
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT node, step, interrupt, interrupt_value, state_json
		FROM checkpoints WHERE thread_id = ?
	`, threadID).Scan(&node, &step, &interrupt, &ivSQL, &stateJSON)
	if err == sql.ErrNoRows {
		return graph.Snapshot{}, false, nil
	}
	if err != nil {
		return graph.Snapshot{}, false, fmt.Errorf("checkpoint: get: %w", err)
	}

	var state graph.State
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		return graph.Snapshot{}, false, fmt.Errorf("checkpoint: unmarshal state: %w", err)
	}
	if state == nil {
		state = graph.State{}
	}
	snap := graph.Snapshot{
		ThreadID:  threadID,
		Node:      node,
		State:     state,
		Step:      step,
		Interrupt: interrupt != 0,
	}
	if ivSQL.Valid && ivSQL.String != "" {
		var iv any
		if err := json.Unmarshal([]byte(ivSQL.String), &iv); err != nil {
			return graph.Snapshot{}, false, fmt.Errorf("checkpoint: unmarshal interrupt_value: %w", err)
		}
		snap.InterruptValue = iv
	}
	return snap, true, nil
}

// Delete removes a thread's checkpoint.
func (s *SQLite) Delete(ctx context.Context, threadID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM checkpoints WHERE thread_id = ?`, threadID)
	if err != nil {
		return fmt.Errorf("checkpoint: delete: %w", err)
	}
	return nil
}

// ListInterrupted returns threads currently paused on an interrupt.
func (s *SQLite) ListInterrupted(ctx context.Context) ([]SnapshotMeta, error) {
	return s.list(ctx, `interrupt = 1`)
}

// ListResumable returns threads that can continue: HITL interrupt, or
// cancelled mid-run (last Put with interrupt=0 and node != END).
func (s *SQLite) ListResumable(ctx context.Context) ([]SnapshotMeta, error) {
	return s.list(ctx, `interrupt = 1 OR node != ?`, graph.END)
}

func (s *SQLite) list(ctx context.Context, where string, args ...any) ([]SnapshotMeta, error) {
	q := `
		SELECT thread_id, node, step, interrupt, updated_at
		FROM checkpoints WHERE ` + where + `
		ORDER BY updated_at DESC`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: list: %w", err)
	}
	defer rows.Close()

	var out []SnapshotMeta
	for rows.Next() {
		var m SnapshotMeta
		var interrupt int
		var updated int64
		if err := rows.Scan(&m.ThreadID, &m.Node, &m.Step, &interrupt, &updated); err != nil {
			return nil, fmt.Errorf("checkpoint: list scan: %w", err)
		}
		m.Interrupt = interrupt != 0
		m.UpdatedAt = time.Unix(updated, 0)
		out = append(out, m)
	}
	return out, rows.Err()
}
