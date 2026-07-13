package checkpoint

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mars/marspi-graph/graph"
)

// MySQL is the P1 durable checkpointer (history + agent sessions).
type MySQL struct {
	db *sql.DB
}

// OpenMySQL opens a MySQL DurableCheckpointer using DSN and runs migrations.
func OpenMySQL(dsn string) (*MySQL, error) {
	if dsn == "" {
		return nil, fmt.Errorf("checkpoint: mysql dsn is empty")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: parse dsn: %w", err)
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("checkpoint: open mysql: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)
	m := &MySQL{db: db}
	if err := m.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return m, nil
}

// Close releases the database handle.
func (m *MySQL) Close() error {
	if m == nil || m.db == nil {
		return nil
	}
	return m.db.Close()
}

func (m *MySQL) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS schema_migrations (
			version INT NOT NULL PRIMARY KEY COMMENT '迁移版本号',
			applied_at DATETIME(6) NOT NULL COMMENT '应用时间（UTC）'
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Schema 迁移版本记录'`,

		`CREATE TABLE IF NOT EXISTS graph_threads (
			thread_id VARCHAR(191) PRIMARY KEY COMMENT '执行线程 ID（一次 Invoke/Resume 会话）',
			latest_checkpoint_id CHAR(32) NULL COMMENT '该线程当前最新 checkpoint_id',
			latest_revision BIGINT NOT NULL DEFAULT 0 COMMENT '最新 checkpoint 的单调版本号',
			updated_at DATETIME(6) NOT NULL COMMENT '线程指针最后更新时间（UTC）',
			lease_run_id CHAR(32) NULL COMMENT '当前租约 owner runID',
			lease_epoch BIGINT NOT NULL DEFAULT 0 COMMENT '单调递增 fencing epoch，释放时不重置',
			lease_expires_at DATETIME(6) NULL COMMENT '租约过期时间（MySQL UTC）'
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='图执行线程：维护 latest checkpoint 指针，供 CAS 与 Resume'`,

		`CREATE TABLE IF NOT EXISTS graph_checkpoints (
			checkpoint_id CHAR(32) PRIMARY KEY COMMENT '检查点 ID（提交幂等键，128-bit hex）',
			thread_id VARCHAR(191) NOT NULL COMMENT '所属执行线程',
			parent_checkpoint_id CHAR(32) NULL COMMENT '父检查点 ID；首步为空，用于 CAS',
			revision BIGINT NOT NULL COMMENT '线程内单调递增版本（从 1 起）',
			node VARCHAR(191) NOT NULL COMMENT '下一步要跑的节点（中断时为当前节点）',
			step BIGINT NOT NULL COMMENT '已完成的 super-step 计数',
			interrupted BOOLEAN NOT NULL COMMENT '是否因 Interrupt/HITL 暂停',
			interrupt_value JSON NULL COMMENT '中断载荷（审批信息等）',
			state_json JSON NOT NULL COMMENT '图共享 State 快照（不含 agentctx）',
			created_at DATETIME(6) NOT NULL COMMENT '检查点创建时间（UTC）',
			UNIQUE KEY uq_thread_revision(thread_id, revision),
			KEY idx_thread_created(thread_id, created_at),
			CONSTRAINT fk_checkpoint_thread
				FOREIGN KEY(thread_id) REFERENCES graph_threads(thread_id) ON DELETE CASCADE
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='检查点历史：每个成功 super-step 追加一行'`,

		`CREATE TABLE IF NOT EXISTS graph_agent_sessions (
			session_id CHAR(32) PRIMARY KEY COMMENT '会话 blob ID（消息列表不可变副本）',
			thread_id VARCHAR(191) NOT NULL COMMENT '所属执行线程',
			agent_id VARCHAR(191) NOT NULL COMMENT 'Agent 节点 ID（如 implementer/coder）',
			messages_json JSON NOT NULL COMMENT 'agentctx 消息列表 JSON（[]llm.Message）',
			created_at DATETIME(6) NOT NULL COMMENT '会话 blob 创建时间（UTC）',
			KEY idx_session_thread_agent(thread_id, agent_id),
			CONSTRAINT fk_session_thread
				FOREIGN KEY(thread_id) REFERENCES graph_threads(thread_id) ON DELETE CASCADE
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Agent 会话内容：仅变化时新建 blob，供多 checkpoint 共享引用'`,

		`CREATE TABLE IF NOT EXISTS graph_checkpoint_agents (
			checkpoint_id CHAR(32) NOT NULL COMMENT '所属检查点',
			agent_id VARCHAR(191) NOT NULL COMMENT 'Agent 节点 ID',
			session_id CHAR(32) NOT NULL COMMENT '指向 graph_agent_sessions 的会话 blob',
			PRIMARY KEY(checkpoint_id, agent_id),
			KEY idx_checkpoint_agent_session(session_id),
			CONSTRAINT fk_ref_checkpoint
				FOREIGN KEY(checkpoint_id) REFERENCES graph_checkpoints(checkpoint_id) ON DELETE CASCADE,
			CONSTRAINT fk_ref_session
				FOREIGN KEY(session_id) REFERENCES graph_agent_sessions(session_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='检查点→Agent 会话引用：每 checkpoint 一份完整映射，未变更 agent 只复制引用'`,
	}
	for _, q := range stmts {
		if _, err := m.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("checkpoint: mysql migrate: %w", err)
		}
	}
	// 对已存在、无 COMMENT 的旧表补齐中文注释（幂等）。
	for _, q := range mysqlCommentAlters() {
		if _, err := m.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("checkpoint: mysql comment migrate: %w", err)
		}
	}
	_, err := m.db.ExecContext(ctx, `
		INSERT IGNORE INTO schema_migrations (version, applied_at) VALUES (1, UTC_TIMESTAMP(6))
	`)
	if err != nil {
		return err
	}
	return m.migrateLeaseV2(ctx)
}

func (m *MySQL) migrateLeaseV2(ctx context.Context) error {
	alters := []string{
		`ALTER TABLE graph_threads ADD COLUMN lease_run_id CHAR(32) NULL COMMENT '当前租约 owner runID'`,
		`ALTER TABLE graph_threads ADD COLUMN lease_epoch BIGINT NOT NULL DEFAULT 0 COMMENT '单调递增 fencing epoch，释放时不重置'`,
		`ALTER TABLE graph_threads ADD COLUMN lease_expires_at DATETIME(6) NULL COMMENT '租约过期时间（MySQL UTC）'`,
	}
	for _, q := range alters {
		if _, err := m.db.ExecContext(ctx, q); err != nil {
			var me *mysql.MySQLError
			if errors.As(err, &me) && me.Number == 1060 { // Duplicate column
				continue
			}
			return fmt.Errorf("checkpoint: mysql lease migrate: %w", err)
		}
	}
	_, err := m.db.ExecContext(ctx, `
		INSERT IGNORE INTO schema_migrations (version, applied_at) VALUES (2, UTC_TIMESTAMP(6))
	`)
	return err
}

func mysqlCommentAlters() []string {
	return []string{
		`ALTER TABLE schema_migrations COMMENT='Schema 迁移版本记录'`,
		`ALTER TABLE schema_migrations
			MODIFY COLUMN version INT NOT NULL COMMENT '迁移版本号',
			MODIFY COLUMN applied_at DATETIME(6) NOT NULL COMMENT '应用时间（UTC）'`,

		`ALTER TABLE graph_threads COMMENT='图执行线程：维护 latest checkpoint 指针，供 CAS 与 Resume'`,
		`ALTER TABLE graph_threads
			MODIFY COLUMN thread_id VARCHAR(191) NOT NULL COMMENT '执行线程 ID（一次 Invoke/Resume 会话）',
			MODIFY COLUMN latest_checkpoint_id CHAR(32) NULL COMMENT '该线程当前最新 checkpoint_id',
			MODIFY COLUMN latest_revision BIGINT NOT NULL DEFAULT 0 COMMENT '最新 checkpoint 的单调版本号',
			MODIFY COLUMN updated_at DATETIME(6) NOT NULL COMMENT '线程指针最后更新时间（UTC）'`,

		`ALTER TABLE graph_checkpoints COMMENT='检查点历史：每个成功 super-step 追加一行'`,
		`ALTER TABLE graph_checkpoints
			MODIFY COLUMN checkpoint_id CHAR(32) NOT NULL COMMENT '检查点 ID（提交幂等键，128-bit hex）',
			MODIFY COLUMN thread_id VARCHAR(191) NOT NULL COMMENT '所属执行线程',
			MODIFY COLUMN parent_checkpoint_id CHAR(32) NULL COMMENT '父检查点 ID；首步为空，用于 CAS',
			MODIFY COLUMN revision BIGINT NOT NULL COMMENT '线程内单调递增版本（从 1 起）',
			MODIFY COLUMN node VARCHAR(191) NOT NULL COMMENT '下一步要跑的节点（中断时为当前节点）',
			MODIFY COLUMN step BIGINT NOT NULL COMMENT '已完成的 super-step 计数',
			MODIFY COLUMN interrupted BOOLEAN NOT NULL COMMENT '是否因 Interrupt/HITL 暂停',
			MODIFY COLUMN interrupt_value JSON NULL COMMENT '中断载荷（审批信息等）',
			MODIFY COLUMN state_json JSON NOT NULL COMMENT '图共享 State 快照（不含 agentctx）',
			MODIFY COLUMN created_at DATETIME(6) NOT NULL COMMENT '检查点创建时间（UTC）'`,

		`ALTER TABLE graph_agent_sessions COMMENT='Agent 会话内容：仅变化时新建 blob，供多 checkpoint 共享引用'`,
		`ALTER TABLE graph_agent_sessions
			MODIFY COLUMN session_id CHAR(32) NOT NULL COMMENT '会话 blob ID（消息列表不可变副本）',
			MODIFY COLUMN thread_id VARCHAR(191) NOT NULL COMMENT '所属执行线程',
			MODIFY COLUMN agent_id VARCHAR(191) NOT NULL COMMENT 'Agent 节点 ID（如 implementer/coder）',
			MODIFY COLUMN messages_json JSON NOT NULL COMMENT 'agentctx 消息列表 JSON（[]llm.Message）',
			MODIFY COLUMN created_at DATETIME(6) NOT NULL COMMENT '会话 blob 创建时间（UTC）'`,

		`ALTER TABLE graph_checkpoint_agents COMMENT='检查点→Agent 会话引用：每 checkpoint 一份完整映射，未变更 agent 只复制引用'`,
		`ALTER TABLE graph_checkpoint_agents
			MODIFY COLUMN checkpoint_id CHAR(32) NOT NULL COMMENT '所属检查点',
			MODIFY COLUMN agent_id VARCHAR(191) NOT NULL COMMENT 'Agent 节点 ID',
			MODIFY COLUMN session_id CHAR(32) NOT NULL COMMENT '指向 graph_agent_sessions 的会话 blob'`,
	}
}

// CommitStep appends a checkpoint and agent session artifacts in one transaction.
func (m *MySQL) CommitStep(ctx context.Context, snap graph.Snapshot, artifacts []graph.StepArtifact) error {
	return m.commitStep(ctx, snap, artifacts, nil)
}

// CommitStepFenced appends a checkpoint only while grant is still the active lease.
func (m *MySQL) CommitStepFenced(ctx context.Context, snap graph.Snapshot, artifacts []graph.StepArtifact, grant graph.LeaseGrant) error {
	return m.commitStep(ctx, snap, artifacts, &grant)
}

func (m *MySQL) commitStep(ctx context.Context, snap graph.Snapshot, artifacts []graph.StepArtifact, grant *graph.LeaseGrant) error {
	if snap.ThreadID == "" {
		return fmt.Errorf("checkpoint: CommitStep requires ThreadID")
	}
	if snap.CheckpointID == "" {
		return fmt.Errorf("checkpoint: CommitStep requires CheckpointID")
	}
	if grant != nil && grant.ThreadID != "" && grant.ThreadID != snap.ThreadID {
		return graph.ErrLeaseLost
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("checkpoint: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Idempotent retry check.
	var existingParent sql.NullString
	var existingThread string
	err = tx.QueryRowContext(ctx, `
		SELECT thread_id, parent_checkpoint_id FROM graph_checkpoints WHERE checkpoint_id = ?
	`, snap.CheckpointID).Scan(&existingThread, &existingParent)
	if err == nil {
		parent := ""
		if existingParent.Valid {
			parent = existingParent.String
		}
		if existingThread == snap.ThreadID && parent == snap.ParentCheckpointID {
			return tx.Commit()
		}
		return fmt.Errorf("%w: checkpoint %q exists with different parent", graph.ErrCheckpointConflict, snap.CheckpointID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("checkpoint: idempotency check: %w", err)
	}

	now := time.Now().UTC()
	if snap.CreatedAt.IsZero() {
		snap.CreatedAt = now
	}

	// Upsert + lock thread row.
	_, err = tx.ExecContext(ctx, `
		INSERT INTO graph_threads (thread_id, latest_checkpoint_id, latest_revision, updated_at)
		VALUES (?, NULL, 0, ?)
		ON DUPLICATE KEY UPDATE updated_at = updated_at
	`, snap.ThreadID, now)
	if err != nil {
		return fmt.Errorf("checkpoint: upsert thread: %w", err)
	}

	var latestID sql.NullString
	var latestRev int64
	var leaseOK int
	if grant != nil {
		err = tx.QueryRowContext(ctx, `
			SELECT latest_checkpoint_id, latest_revision,
			       CASE
			         WHEN lease_run_id = ? AND lease_epoch = ?
			              AND lease_expires_at IS NOT NULL
			              AND lease_expires_at > UTC_TIMESTAMP(6)
			         THEN 1 ELSE 0
			       END
			FROM graph_threads WHERE thread_id = ? FOR UPDATE
		`, grant.RunID, grant.Epoch, snap.ThreadID).Scan(&latestID, &latestRev, &leaseOK)
	} else {
		err = tx.QueryRowContext(ctx, `
			SELECT latest_checkpoint_id, latest_revision
			FROM graph_threads WHERE thread_id = ? FOR UPDATE
		`, snap.ThreadID).Scan(&latestID, &latestRev)
	}
	if err != nil {
		return fmt.Errorf("checkpoint: lock thread: %w", err)
	}
	if grant != nil && leaseOK != 1 {
		return graph.ErrLeaseLost
	}

	curLatest := ""
	if latestID.Valid {
		curLatest = latestID.String
	}
	if curLatest != snap.ParentCheckpointID {
		return fmt.Errorf("%w: want parent %q, have latest %q", graph.ErrCheckpointConflict, snap.ParentCheckpointID, curLatest)
	}

	rev := latestRev + 1
	stateJSON, err := json.Marshal(snap.State)
	if err != nil {
		return fmt.Errorf("checkpoint: marshal state: %w", err)
	}
	var ivJSON any
	if snap.InterruptValue != nil {
		b, err := json.Marshal(snap.InterruptValue)
		if err != nil {
			return fmt.Errorf("checkpoint: marshal interrupt: %w", err)
		}
		ivJSON = string(b)
	}

	var parent any
	if snap.ParentCheckpointID != "" {
		parent = snap.ParentCheckpointID
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO graph_checkpoints (
			checkpoint_id, thread_id, parent_checkpoint_id, revision,
			node, step, interrupted, interrupt_value, state_json, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, snap.CheckpointID, snap.ThreadID, parent, rev, snap.Node, snap.Step,
		snap.Interrupt, ivJSON, string(stateJSON), snap.CreatedAt)
	if err != nil {
		return fmt.Errorf("checkpoint: insert checkpoint: %w", err)
	}

	// Copy parent agent refs.
	if snap.ParentCheckpointID != "" {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO graph_checkpoint_agents (checkpoint_id, agent_id, session_id)
			SELECT ?, agent_id, session_id FROM graph_checkpoint_agents WHERE checkpoint_id = ?
		`, snap.CheckpointID, snap.ParentCheckpointID)
		if err != nil {
			return fmt.Errorf("checkpoint: copy agent refs: %w", err)
		}
	}

	for _, art := range artifacts {
		if art.Kind != graph.ArtifactAgentSession {
			continue
		}
		sid := graph.NewID()
		_, err = tx.ExecContext(ctx, `
			INSERT INTO graph_agent_sessions (session_id, thread_id, agent_id, messages_json, created_at)
			VALUES (?, ?, ?, ?, ?)
		`, sid, snap.ThreadID, art.Key, string(art.Payload), now)
		if err != nil {
			return fmt.Errorf("checkpoint: insert session: %w", err)
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO graph_checkpoint_agents (checkpoint_id, agent_id, session_id)
			VALUES (?, ?, ?)
			ON DUPLICATE KEY UPDATE session_id = VALUES(session_id)
		`, snap.CheckpointID, art.Key, sid)
		if err != nil {
			return fmt.Errorf("checkpoint: upsert agent ref: %w", err)
		}
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE graph_threads
		SET latest_checkpoint_id = ?, latest_revision = ?, updated_at = ?
		WHERE thread_id = ?
	`, snap.CheckpointID, rev, now, snap.ThreadID)
	if err != nil {
		return fmt.Errorf("checkpoint: update latest: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("checkpoint: commit: %w", err)
	}
	return nil
}

// GetLatest returns the latest snapshot for threadID.
func (m *MySQL) GetLatest(ctx context.Context, threadID string) (graph.Snapshot, bool, error) {
	var latest sql.NullString
	err := m.db.QueryRowContext(ctx, `
		SELECT latest_checkpoint_id FROM graph_threads WHERE thread_id = ?
	`, threadID).Scan(&latest)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !latest.Valid) {
		return graph.Snapshot{}, false, nil
	}
	if err != nil {
		return graph.Snapshot{}, false, fmt.Errorf("checkpoint: get latest id: %w", err)
	}
	return m.GetByID(ctx, threadID, latest.String)
}

// GetByID returns a specific checkpoint.
func (m *MySQL) GetByID(ctx context.Context, threadID, checkpointID string) (graph.Snapshot, bool, error) {
	var (
		parent    sql.NullString
		revision  int64
		node      string
		step      int64
		interrupt bool
		ivSQL     sql.NullString
		stateJSON string
		created   time.Time
	)
	err := m.db.QueryRowContext(ctx, `
		SELECT parent_checkpoint_id, revision, node, step, interrupted,
		       interrupt_value, state_json, created_at
		FROM graph_checkpoints
		WHERE checkpoint_id = ? AND thread_id = ?
	`, checkpointID, threadID).Scan(&parent, &revision, &node, &step, &interrupt, &ivSQL, &stateJSON, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return graph.Snapshot{}, false, nil
	}
	if err != nil {
		return graph.Snapshot{}, false, fmt.Errorf("checkpoint: get by id: %w", err)
	}
	var state graph.State
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		return graph.Snapshot{}, false, fmt.Errorf("checkpoint: unmarshal state: %w", err)
	}
	if state == nil {
		state = graph.State{}
	}
	snap := graph.Snapshot{
		CheckpointID: checkpointID,
		ThreadID:     threadID,
		Revision:     revision,
		Node:         node,
		Step:         int(step),
		Interrupt:    interrupt,
		State:        state,
		CreatedAt:    created.UTC(),
	}
	if parent.Valid {
		snap.ParentCheckpointID = parent.String
	}
	if ivSQL.Valid && ivSQL.String != "" {
		var iv any
		if err := json.Unmarshal([]byte(ivSQL.String), &iv); err != nil {
			return graph.Snapshot{}, false, fmt.Errorf("checkpoint: unmarshal interrupt: %w", err)
		}
		snap.InterruptValue = iv
	}
	return snap, true, nil
}

// List returns history newest-first.
func (m *MySQL) List(ctx context.Context, threadID string, beforeRevision, limit int64) ([]graph.SnapshotMeta, error) {
	q := `
		SELECT checkpoint_id, thread_id, node, step, revision, interrupted, created_at
		FROM graph_checkpoints WHERE thread_id = ?`
	args := []any{threadID}
	if beforeRevision > 0 {
		q += ` AND revision < ?`
		args = append(args, beforeRevision)
	}
	q += ` ORDER BY revision DESC`
	if limit > 0 {
		q += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := m.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: list: %w", err)
	}
	defer rows.Close()
	var out []graph.SnapshotMeta
	for rows.Next() {
		var meta graph.SnapshotMeta
		var step int64
		if err := rows.Scan(&meta.CheckpointID, &meta.ThreadID, &meta.Node, &step, &meta.Revision, &meta.Interrupt, &meta.CreatedAt); err != nil {
			return nil, fmt.Errorf("checkpoint: list scan: %w", err)
		}
		meta.Step = int(step)
		meta.CreatedAt = meta.CreatedAt.UTC()
		out = append(out, meta)
	}
	return out, rows.Err()
}

// DeleteThread removes all durable data for a thread.
func (m *MySQL) DeleteThread(ctx context.Context, threadID string) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Break FK from refs -> sessions before deleting sessions via thread cascade.
	_, err = tx.ExecContext(ctx, `
		DELETE ca FROM graph_checkpoint_agents ca
		INNER JOIN graph_checkpoints c ON c.checkpoint_id = ca.checkpoint_id
		WHERE c.thread_id = ?
	`, threadID)
	if err != nil {
		return fmt.Errorf("checkpoint: delete refs: %w", err)
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM graph_agent_sessions WHERE thread_id = ?`, threadID)
	if err != nil {
		return fmt.Errorf("checkpoint: delete sessions: %w", err)
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM graph_threads WHERE thread_id = ?`, threadID)
	if err != nil {
		return fmt.Errorf("checkpoint: delete thread: %w", err)
	}
	return tx.Commit()
}

// Prune keeps the newest keepLast checkpoints and deletes orphan sessions.
func (m *MySQL) Prune(ctx context.Context, threadID string, keepLast int) error {
	if keepLast < 0 {
		return fmt.Errorf("checkpoint: keepLast must be >= 0")
	}
	if keepLast == 0 {
		return m.DeleteThread(ctx, threadID)
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, `
		SELECT checkpoint_id FROM graph_checkpoints
		WHERE thread_id = ? ORDER BY revision DESC
	`, threadID)
	if err != nil {
		return fmt.Errorf("checkpoint: prune list: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(ids) <= keepLast {
		return tx.Commit()
	}
	drop := ids[keepLast:]
	for _, id := range drop {
		if _, err := tx.ExecContext(ctx, `DELETE FROM graph_checkpoints WHERE checkpoint_id = ?`, id); err != nil {
			return fmt.Errorf("checkpoint: prune delete: %w", err)
		}
	}
	_, err = tx.ExecContext(ctx, `
		DELETE s FROM graph_agent_sessions s
		WHERE s.thread_id = ?
		  AND NOT EXISTS (
			SELECT 1 FROM graph_checkpoint_agents ca WHERE ca.session_id = s.session_id
		  )
	`, threadID)
	if err != nil {
		return fmt.Errorf("checkpoint: prune orphans: %w", err)
	}

	var latestID sql.NullString
	var latestRev sql.NullInt64
	err = tx.QueryRowContext(ctx, `
		SELECT checkpoint_id, revision FROM graph_checkpoints
		WHERE thread_id = ? ORDER BY revision DESC LIMIT 1
	`, threadID).Scan(&latestID, &latestRev)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, `
			UPDATE graph_threads SET latest_checkpoint_id = NULL, latest_revision = 0, updated_at = UTC_TIMESTAMP(6)
			WHERE thread_id = ?
		`, threadID)
	} else if err == nil {
		_, err = tx.ExecContext(ctx, `
			UPDATE graph_threads SET latest_checkpoint_id = ?, latest_revision = ?, updated_at = UTC_TIMESTAMP(6)
			WHERE thread_id = ?
		`, latestID.String, latestRev.Int64, threadID)
	}
	if err != nil {
		return fmt.Errorf("checkpoint: prune update latest: %w", err)
	}
	return tx.Commit()
}

// GetAgentSession returns messages JSON for a checkpoint+agent ref.
func (m *MySQL) GetAgentSession(ctx context.Context, threadID, checkpointID, agentID string) (json.RawMessage, bool, error) {
	var raw string
	err := m.db.QueryRowContext(ctx, `
		SELECT s.messages_json
		FROM graph_checkpoint_agents ca
		INNER JOIN graph_agent_sessions s ON s.session_id = ca.session_id
		INNER JOIN graph_checkpoints c ON c.checkpoint_id = ca.checkpoint_id
		WHERE ca.checkpoint_id = ? AND ca.agent_id = ? AND c.thread_id = ?
	`, checkpointID, agentID, threadID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("checkpoint: get agent session: %w", err)
	}
	return json.RawMessage(raw), true, nil
}

// Put implements legacy Checkpointer by appending with CAS against current latest.
func (m *MySQL) Put(ctx context.Context, threadID string, snap graph.Snapshot) error {
	if threadID != "" {
		snap.ThreadID = threadID
	}
	if snap.CheckpointID == "" {
		snap.CheckpointID = graph.NewID()
	}
	latest, ok, err := m.GetLatest(ctx, snap.ThreadID)
	if err != nil {
		return err
	}
	if ok {
		snap.ParentCheckpointID = latest.CheckpointID
	} else {
		snap.ParentCheckpointID = ""
	}
	return m.CommitStep(ctx, snap, nil)
}

// Get implements legacy Checkpointer.
func (m *MySQL) Get(ctx context.Context, threadID string) (graph.Snapshot, bool, error) {
	return m.GetLatest(ctx, threadID)
}
