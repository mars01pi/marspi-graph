package checkpoint

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mars/marspi-graph/graph"
)

// Acquire implements graph.ExecutionLease using MySQL as the clock authority.
func (m *MySQL) Acquire(ctx context.Context, threadID, runID string, ttl time.Duration) (graph.LeaseGrant, error) {
	if threadID == "" || runID == "" {
		return graph.LeaseGrant{}, graph.ErrLeaseLost
	}
	if ttl <= 0 {
		ttl = graph.DefaultLeaseTTL
	}
	us := ttl.Microseconds()
	if us <= 0 {
		us = 1
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return graph.LeaseGrant{}, fmt.Errorf("checkpoint: lease begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO graph_threads (thread_id, latest_checkpoint_id, latest_revision, updated_at)
		VALUES (?, NULL, 0, UTC_TIMESTAMP(6))
		ON DUPLICATE KEY UPDATE thread_id = thread_id
	`, threadID)
	if err != nil {
		return graph.LeaseGrant{}, fmt.Errorf("checkpoint: lease upsert thread: %w", err)
	}

	var (
		leaseRun sql.NullString
		epoch    int64
		active   int
	)
	err = tx.QueryRowContext(ctx, `
		SELECT lease_run_id, lease_epoch,
		       CASE
		         WHEN lease_run_id IS NOT NULL AND lease_run_id != ''
		              AND lease_expires_at IS NOT NULL
		              AND lease_expires_at > UTC_TIMESTAMP(6)
		         THEN 1 ELSE 0
		       END
		FROM graph_threads WHERE thread_id = ? FOR UPDATE
	`, threadID).Scan(&leaseRun, &epoch, &active)
	if err != nil {
		return graph.LeaseGrant{}, fmt.Errorf("checkpoint: lease lock: %w", err)
	}

	if active == 1 && leaseRun.Valid && leaseRun.String == runID {
		_, err = tx.ExecContext(ctx, `
			UPDATE graph_threads
			SET lease_expires_at = UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND
			WHERE thread_id = ?
		`, us, threadID)
		if err != nil {
			return graph.LeaseGrant{}, fmt.Errorf("checkpoint: lease extend: %w", err)
		}
		var exp time.Time
		err = tx.QueryRowContext(ctx, `
			SELECT lease_expires_at FROM graph_threads WHERE thread_id = ?
		`, threadID).Scan(&exp)
		if err != nil {
			return graph.LeaseGrant{}, err
		}
		if err := tx.Commit(); err != nil {
			return graph.LeaseGrant{}, err
		}
		return graph.LeaseGrant{ThreadID: threadID, RunID: runID, Epoch: epoch, ExpiresAt: exp.UTC()}, nil
	}
	if active == 1 {
		return graph.LeaseGrant{}, graph.ErrLeaseHeld
	}

	newEpoch := epoch + 1
	_, err = tx.ExecContext(ctx, `
		UPDATE graph_threads
		SET lease_run_id = ?,
		    lease_epoch = ?,
		    lease_expires_at = UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND
		WHERE thread_id = ?
	`, runID, newEpoch, us, threadID)
	if err != nil {
		return graph.LeaseGrant{}, fmt.Errorf("checkpoint: lease take: %w", err)
	}
	var exp time.Time
	err = tx.QueryRowContext(ctx, `
		SELECT lease_expires_at FROM graph_threads WHERE thread_id = ?
	`, threadID).Scan(&exp)
	if err != nil {
		return graph.LeaseGrant{}, err
	}
	if err := tx.Commit(); err != nil {
		return graph.LeaseGrant{}, err
	}
	return graph.LeaseGrant{ThreadID: threadID, RunID: runID, Epoch: newEpoch, ExpiresAt: exp.UTC()}, nil
}

// Renew implements graph.ExecutionLease.
func (m *MySQL) Renew(ctx context.Context, grant graph.LeaseGrant, ttl time.Duration) (graph.LeaseGrant, error) {
	if ttl <= 0 {
		ttl = graph.DefaultLeaseTTL
	}
	us := ttl.Microseconds()
	if us <= 0 {
		us = 1
	}
	res, err := m.db.ExecContext(ctx, `
		UPDATE graph_threads
		SET lease_expires_at = UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND
		WHERE thread_id = ?
		  AND lease_run_id = ?
		  AND lease_epoch = ?
		  AND lease_expires_at > UTC_TIMESTAMP(6)
	`, us, grant.ThreadID, grant.RunID, grant.Epoch)
	if err != nil {
		return graph.LeaseGrant{}, fmt.Errorf("checkpoint: lease renew: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return graph.LeaseGrant{}, err
	}
	if n == 0 {
		return graph.LeaseGrant{}, graph.ErrLeaseLost
	}
	var exp time.Time
	err = m.db.QueryRowContext(ctx, `
		SELECT lease_expires_at FROM graph_threads WHERE thread_id = ?
	`, grant.ThreadID).Scan(&exp)
	if err != nil {
		return graph.LeaseGrant{}, err
	}
	grant.ExpiresAt = exp.UTC()
	return grant, nil
}

// Release implements graph.ExecutionLease (idempotent; never resets epoch).
func (m *MySQL) Release(ctx context.Context, grant graph.LeaseGrant) error {
	_, err := m.db.ExecContext(ctx, `
		UPDATE graph_threads
		SET lease_run_id = NULL, lease_expires_at = NULL
		WHERE thread_id = ?
		  AND lease_run_id = ?
		  AND lease_epoch = ?
	`, grant.ThreadID, grant.RunID, grant.Epoch)
	if err != nil {
		return fmt.Errorf("checkpoint: lease release: %w", err)
	}
	return nil
}
