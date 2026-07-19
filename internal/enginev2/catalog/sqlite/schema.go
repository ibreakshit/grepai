package sqlite

import (
	"context"
	"database/sql"
	"errors"
)

// schemaVersion is the current catalog schema version. Bump it and append a
// migration when the schema changes.
const schemaVersion = 1

// migration0001 is the initial schema. Vectors are little-endian float32
// blobs whose byte length equals dimensions*4 (validated in Go, not SQL).
const migration0001 = `
CREATE TABLE schema_migrations (
  version    INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL
);
CREATE TABLE repositories (
  repository_id  TEXT PRIMARY KEY,
  root_path      TEXT NOT NULL,
  git_common_dir TEXT NOT NULL DEFAULT '',
  created_at     TEXT NOT NULL
);
CREATE TABLE worktrees (
  worktree_id             TEXT PRIMARY KEY,
  repository_id           TEXT NOT NULL REFERENCES repositories(repository_id),
  root_path               TEXT NOT NULL,
  registration_generation INTEGER NOT NULL,
  created_at              TEXT NOT NULL
);
CREATE TABLE index_generations (
  repository_id TEXT NOT NULL REFERENCES repositories(repository_id),
  generation    INTEGER NOT NULL,
  fingerprint   TEXT NOT NULL,
  status        TEXT NOT NULL, -- 'building' | 'active' | 'retired'
  created_at    TEXT NOT NULL,
  PRIMARY KEY (repository_id, generation)
);
CREATE TABLE file_artifacts (
  artifact_id   TEXT PRIMARY KEY,
  repository_id TEXT NOT NULL REFERENCES repositories(repository_id),
  relative_path TEXT NOT NULL,
  source_hash   TEXT NOT NULL,
  fingerprint   TEXT NOT NULL,
  dimensions    INTEGER NOT NULL,
  created_at    TEXT NOT NULL,
  UNIQUE (repository_id, relative_path, source_hash, fingerprint)
);
CREATE TABLE chunks (
  chunk_id      TEXT PRIMARY KEY,
  repository_id TEXT NOT NULL REFERENCES repositories(repository_id),
  fingerprint   TEXT NOT NULL,
  dimensions    INTEGER NOT NULL,
  vector        BLOB NOT NULL,
  created_at    TEXT NOT NULL
);
CREATE TABLE artifact_chunks (
  artifact_id TEXT NOT NULL REFERENCES file_artifacts(artifact_id),
  ordinal     INTEGER NOT NULL,
  chunk_id    TEXT NOT NULL REFERENCES chunks(chunk_id),
  PRIMARY KEY (artifact_id, ordinal)
);
CREATE TABLE worktree_files (
  worktree_id   TEXT NOT NULL REFERENCES worktrees(worktree_id),
  relative_path TEXT NOT NULL,
  artifact_id   TEXT NOT NULL REFERENCES file_artifacts(artifact_id),
  generation    INTEGER NOT NULL,
  updated_at    TEXT NOT NULL,
  PRIMARY KEY (worktree_id, relative_path)
);
CREATE TABLE index_jobs (
  job_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  worktree_id   TEXT NOT NULL,
  relative_path TEXT NOT NULL,
  desired_hash  TEXT NOT NULL,
  generation    INTEGER NOT NULL,
  operation     INTEGER NOT NULL,
  priority      INTEGER NOT NULL,
  attempts      INTEGER NOT NULL DEFAULT 0,
  claimed       INTEGER NOT NULL DEFAULT 0,
  created_at    TEXT NOT NULL,
  UNIQUE (worktree_id, relative_path)
);
CREATE INDEX idx_index_jobs_claim ON index_jobs(claimed, priority, job_id);
CREATE TABLE dead_letter_jobs (
  job_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  worktree_id   TEXT NOT NULL,
  relative_path TEXT NOT NULL,
  reason        TEXT NOT NULL,
  created_at    TEXT NOT NULL
);
CREATE TABLE symbols (
  artifact_id TEXT NOT NULL REFERENCES file_artifacts(artifact_id),
  name        TEXT NOT NULL,
  kind        TEXT NOT NULL,
  PRIMARY KEY (artifact_id, name, kind)
);
CREATE TABLE symbol_edges (
  artifact_id TEXT NOT NULL REFERENCES file_artifacts(artifact_id),
  caller      TEXT NOT NULL,
  callee      TEXT NOT NULL,
  PRIMARY KEY (artifact_id, caller, callee)
);
CREATE TABLE service_state (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
`

// migrations is the ordered list; index i applies version i+1.
var migrations = []string{migration0001}

// schemaVersion (method) returns the highest applied migration version, or 0.
func (c *Catalog) schemaVersion(ctx context.Context) (int, error) {
	var v sql.NullInt64
	err := c.db.QueryRowContext(ctx, "SELECT MAX(version) FROM schema_migrations").Scan(&v)
	if err != nil {
		return 0, err
	}
	if !v.Valid {
		return 0, nil
	}
	return int(v.Int64), nil
}

// migrate applies all pending migrations inside serialized write transactions.
func (c *Catalog) migrate(ctx context.Context) error {
	cur, err := c.currentVersion(ctx)
	if err != nil {
		return err
	}
	for i := cur; i < len(migrations); i++ {
		version := i + 1
		stmt := migrations[i]
		if err := c.withWriteTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx,
				"INSERT INTO schema_migrations(version, applied_at) VALUES(?, datetime('now'))", version)
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

// currentVersion returns the applied version, treating a missing
// schema_migrations table (fresh DB) as 0.
func (c *Catalog) currentVersion(ctx context.Context) (int, error) {
	var exists string
	err := c.db.QueryRowContext(ctx,
		"SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations'").Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return c.schemaVersion(ctx)
}
