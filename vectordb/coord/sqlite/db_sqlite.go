// Default: enabled (pure-Go SQLite). To disable, build with -tags no_sqlite.

package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // pure Go sqlite driver
)

// DB wraps *sql.DB with helpers.
type DB struct{ sql *sql.DB }

// Open opens or creates the coordination database and applies PRAGMAs.
func Open(path string) (*DB, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)", path)
	sqldb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// Basic PRAGMAs for WAL concurrency
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		// Optional cache/mmap tuning; safe to ignore errors if unsupported
		"PRAGMA cache_size=-65536;",
	}
	for _, p := range pragmas {
		if _, err := sqldb.Exec(p); err != nil {
			// continue on errors for optional pragmas
		}
	}
	return &DB{sql: sqldb}, nil
}

// EnsureSchema creates required tables and seeds meta if missing.
func (d *DB) EnsureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );`,
		`CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY,
            kind TEXT NOT NULL,
            payload BLOB NOT NULL,
            status TEXT NOT NULL,
            enqueued_at DATETIME NOT NULL,
            claimed_by TEXT,
            claimed_at DATETIME,
            done_at DATETIME,
            client_id TEXT,
            job_id TEXT,
            result BLOB
        );`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_status_time ON jobs(status, enqueued_at);`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_client ON jobs(client_id, job_id);`,
		`CREATE TABLE IF NOT EXISTS events (
            scn INTEGER PRIMARY KEY,
            kind TEXT NOT NULL,
            id TEXT NOT NULL,
            seg INTEGER,
            off INTEGER,
            len INTEGER,
            ts DATETIME NOT NULL,
            vec BLOB
        );`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
            reader_id TEXT PRIMARY KEY,
            last_ack INTEGER NOT NULL,
            ts DATETIME NOT NULL
        );`,
		`CREATE TABLE IF NOT EXISTS lease (
            key TEXT PRIMARY KEY,
            owner TEXT NOT NULL,
            heartbeat DATETIME NOT NULL
        );`,
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, s := range stmts {
		if _, err := tx.ExecContext(ctx, s); err != nil {
			return err
		}
	}
	// Forward-compatible schema updates
	_, _ = tx.ExecContext(ctx, `ALTER TABLE jobs ADD COLUMN result BLOB`)
	// Seed writer lease row if missing
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO lease(key, owner, heartbeat) VALUES('writer','', DATETIME(0))`); err != nil {
		return err
	}
	// Seed schema_version and last_scn if not present
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO meta(key,value) VALUES
        ('schema_version','1'),('last_scn','0')`); err != nil {
		return err
	}
	return tx.Commit()
}

// GetLastSCN returns the last committed SCN.
func (d *DB) GetLastSCN(ctx context.Context) (uint64, error) {
	var v string
	err := d.sql.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='last_scn'`).Scan(&v)
	if err != nil {
		return 0, err
	}
	var scn uint64
	if _, err := fmt.Sscanf(v, "%d", &scn); err != nil {
		return 0, err
	}
	return scn, nil
}

// SetLastSCN updates the last committed SCN.
func (d *DB) SetLastSCN(ctx context.Context, scn uint64) error {
	_, err := d.sql.ExecContext(ctx, `UPDATE meta SET value=? WHERE key='last_scn'`, fmt.Sprintf("%d", scn))
	return err
}

func (d *DB) Close() error { return d.sql.Close() }

// util to get now; kept for potential test overrides
func now() time.Time { return time.Now() }

// EnqueueJob inserts a pending job row and returns its rowid.
func (d *DB) EnqueueJob(ctx context.Context, kind string, payload []byte, clientID, jobID string) (int64, error) {
	res, err := d.sql.ExecContext(ctx, `INSERT INTO jobs(kind, payload, status, enqueued_at, client_id, job_id)
        VALUES(?, ?, 'pending', CURRENT_TIMESTAMP, ?, ?)`, kind, payload, clientID, jobID)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

type Job struct {
	ID      int64
	Kind    string
	Payload []byte
	JobID   string
}

// ClaimPending claims up to limit pending jobs atomically and returns them.
func (d *DB) ClaimPending(ctx context.Context, token string, limit int) ([]Job, error) {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	// Atomically set claimed_by on up to limit pending rows
	_, err = tx.ExecContext(ctx, `UPDATE jobs SET status='claimed', claimed_by=?, claimed_at=CURRENT_TIMESTAMP
        WHERE id IN (SELECT id FROM jobs WHERE status='pending' ORDER BY enqueued_at, id LIMIT ?)`, token, limit)
	if err != nil {
		return nil, err
	}
	// Fetch claimed rows for this token
	rows, err := tx.QueryContext(ctx, `SELECT id, kind, payload, job_id FROM jobs WHERE status='claimed' AND claimed_by=? ORDER BY enqueued_at, id`, token)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.Kind, &j.Payload, &j.JobID); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// MarkJobsDone marks the given job IDs as done.
func (d *DB) MarkJobsDone(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	// build placeholders
	q := "UPDATE jobs SET status='done', done_at=CURRENT_TIMESTAMP WHERE id IN (" + placeholders(len(ids)) + ")"
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	_, err := d.sql.ExecContext(ctx, q, args...)
	return err
}

type Event struct {
	Kind string
	ID   string
	Seg  uint32
	Off  uint64
	Len  uint32
	Ts   time.Time
	Vec  []float32
}

// InsertEvents inserts events with sequential SCNs and updates last_scn atomically.
func (d *DB) InsertEvents(ctx context.Context, evs []Event) (uint64, error) {
	if len(evs) == 0 {
		return 0, nil
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	var cur uint64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(scn),0) FROM events`).Scan(&cur); err != nil {
		return 0, err
	}
	start := cur + 1
	// Ensure vec column exists (noop if already present)
	_, _ = tx.ExecContext(ctx, `ALTER TABLE events ADD COLUMN vec BLOB`)
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO events(scn, kind, id, seg, off, len, ts, vec) VALUES(?,?,?,?,?,?,?,?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	scn := start
	nowTs := now()
	for i := range evs {
		e := evs[i]
		if e.Ts.IsZero() {
			e.Ts = nowTs
		}
		var vec any = nil
		if len(e.Vec) > 0 {
			b, _ := json.Marshal(e.Vec)
			vec = b
		}
		// Store ID as the SCN string to make it the canonical external ID
		idStr := fmt.Sprintf("%d", scn)
		if _, err := stmt.ExecContext(ctx, scn, e.Kind, idStr, e.Seg, e.Off, e.Len, e.Ts, vec); err != nil {
			return 0, err
		}
		scn++
	}
	// update last_scn in meta
	if _, err := tx.ExecContext(ctx, `UPDATE meta SET value=? WHERE key='last_scn'`, fmt.Sprintf("%d", scn-1)); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return start, nil
}

type EventRow struct {
	SCN uint64
	Ev  Event
}

// EventsSince returns events with scn > from, ordered by scn.
func (d *DB) EventsSince(ctx context.Context, from uint64) ([]EventRow, error) {
	rows, err := d.sql.QueryContext(ctx, `SELECT scn, kind, id, seg, off, len, ts, vec FROM events WHERE scn > ? ORDER BY scn`, from)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EventRow
	for rows.Next() {
		var r EventRow
		var vecBytes []byte
		if err := rows.Scan(&r.SCN, &r.Ev.Kind, &r.Ev.ID, &r.Ev.Seg, &r.Ev.Off, &r.Ev.Len, &r.Ev.Ts, &vecBytes); err != nil {
			return nil, err
		}
		if len(vecBytes) > 0 {
			_ = json.Unmarshal(vecBytes, &r.Ev.Vec)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SetJobResult stores a JSON-encoded result payload for a job identified by jobID.
func (d *DB) SetJobResult(ctx context.Context, jobID string, result []byte) error {
	_, err := d.sql.ExecContext(ctx, `UPDATE jobs SET result=?, done_at=COALESCE(done_at,CURRENT_TIMESTAMP), status='done' WHERE job_id=?`, result, jobID)
	return err
}

// GetJobResult returns the result payload (if any) and status for a job.
func (d *DB) GetJobResult(ctx context.Context, jobID string) (result []byte, status string, err error) {
	err = d.sql.QueryRowContext(ctx, `SELECT result, status FROM jobs WHERE job_id=?`, jobID).Scan(&result, &status)
	return
}

// TryAcquireWriter attempts to acquire the writer lease if it's free or expired (by ttl).
// Returns true if acquired.
func (d *DB) TryAcquireWriter(ctx context.Context, owner string, ttl time.Duration) (bool, error) {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	// Ensure row exists
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO lease(key, owner, heartbeat) VALUES('writer','', DATETIME(0))`); err != nil {
		return false, err
	}
	var curOwner string
	var hb time.Time
	if err := tx.QueryRowContext(ctx, `SELECT owner, heartbeat FROM lease WHERE key='writer'`).Scan(&curOwner, &hb); err != nil {
		return false, err
	}
	expired := time.Since(hb) > ttl
	if curOwner == "" || expired {
		if _, err := tx.ExecContext(ctx, `UPDATE lease SET owner=?, heartbeat=CURRENT_TIMESTAMP WHERE key='writer'`, owner); err != nil {
			return false, err
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return true, nil
	}
	// not acquired
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return false, nil
}

// RenewWriter updates the heartbeat for the current owner. Returns false if the lease is lost.
func (d *DB) RenewWriter(ctx context.Context, owner string) (bool, error) {
	res, err := d.sql.ExecContext(ctx, `UPDATE lease SET heartbeat=CURRENT_TIMESTAMP WHERE key='writer' AND owner=?`, owner)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ReleaseWriter clears the lease if held by this owner.
func (d *DB) ReleaseWriter(ctx context.Context, owner string) error {
	_, err := d.sql.ExecContext(ctx, `UPDATE lease SET owner='' WHERE key='writer' AND owner=?`, owner)
	return err
}

// UpsertCheckpoint records the reader's last acknowledged SCN.
func (d *DB) UpsertCheckpoint(ctx context.Context, readerID string, lastAck uint64) error {
	_, err := d.sql.ExecContext(ctx, `INSERT INTO checkpoints(reader_id,last_ack,ts)
        VALUES(?,?,CURRENT_TIMESTAMP)
        ON CONFLICT(reader_id) DO UPDATE SET last_ack=excluded.last_ack, ts=excluded.ts`, readerID, lastAck)
	return err
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	s := "?"
	for i := 1; i < n; i++ {
		s += ",?"
	}
	return s
}
