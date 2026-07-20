package sqlite

import (
	"context"
	"database/sql"
	"errors"
)

// schemaVersion is the current catalog schema version. Bump it and append a
// migration when the schema changes.
const schemaVersion = 4

// LatestSchemaVersion is the schema version this binary migrates a catalog to.
// The daemon refuses to open a catalog stamped newer than this.
const LatestSchemaVersion = schemaVersion

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
// migration0002 adds chunk display metadata so search can return code snippets
// with line numbers: content is content-addressed (stable per chunk_id, in
// chunks); line ranges are per-artifact (identical content can sit at different
// lines in different files, so they live in artifact_chunks).
const migration0002 = `
ALTER TABLE chunks ADD COLUMN content TEXT NOT NULL DEFAULT '';
ALTER TABLE artifact_chunks ADD COLUMN start_line INTEGER NOT NULL DEFAULT 0;
ALTER TABLE artifact_chunks ADD COLUMN end_line INTEGER NOT NULL DEFAULT 0;
`

// migration0003 rebuilds the dormant symbol tables for trace. The v1 shapes
// keyed (artifact_id, name, kind) / (artifact_id, caller, callee), which would
// collapse same-named symbols (Go methods on different receivers, overloads)
// and repeated call sites at different lines under INSERT OR IGNORE — so the
// key must include line. DROP is safe: no pre-v3 code path ever wrote these
// tables (they are empty in every v2 catalog). Also adds the extraction marker
// to file_artifacts (0 = never extracted; SymbolsVersionCurrent = extracted by
// the current extractor).
const migration0003 = `
DROP TABLE symbols;
DROP TABLE symbol_edges;
CREATE TABLE symbols (
  artifact_id TEXT NOT NULL REFERENCES file_artifacts(artifact_id),
  name        TEXT NOT NULL,
  kind        TEXT NOT NULL,
  line        INTEGER NOT NULL DEFAULT 0,
  end_line    INTEGER NOT NULL DEFAULT 0,
  signature   TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (artifact_id, name, kind, line)
);
CREATE TABLE symbol_edges (
  artifact_id TEXT NOT NULL REFERENCES file_artifacts(artifact_id),
  caller      TEXT NOT NULL,
  callee      TEXT NOT NULL,
  line        INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (artifact_id, caller, callee, line)
);
ALTER TABLE file_artifacts ADD COLUMN symbols_version INTEGER NOT NULL DEFAULT 0;
CREATE INDEX idx_symbols_name ON symbols(name);
CREATE INDEX idx_symbol_edges_caller ON symbol_edges(caller);
CREATE INDEX idx_symbol_edges_callee ON symbol_edges(callee);
`

// migration0004 (issue #20, v1-parity trace output) widens the symbol tables
// with the fields the shared v1 extractor already produces: symbol identity
// detail (receiver/package/exported/language/docstring) and the call-site
// source line (context). Primary keys are unchanged (v3 keys already include
// line). SymbolsVersionCurrent bumps to 2 alongside this, so the daemon
// backfill re-extracts every artifact and populates the new columns.
const migration0004 = `
ALTER TABLE symbols ADD COLUMN receiver TEXT NOT NULL DEFAULT '';
ALTER TABLE symbols ADD COLUMN package TEXT NOT NULL DEFAULT '';
ALTER TABLE symbols ADD COLUMN exported INTEGER NOT NULL DEFAULT 0;
ALTER TABLE symbols ADD COLUMN language TEXT NOT NULL DEFAULT '';
ALTER TABLE symbols ADD COLUMN docstring TEXT NOT NULL DEFAULT '';
ALTER TABLE symbol_edges ADD COLUMN context TEXT NOT NULL DEFAULT '';
`

var migrations = []string{migration0001, migration0002, migration0003, migration0004}

// SymbolsVersionCurrent is the extractor version stamped on artifacts whose
// symbols have been extracted. Bump to force a fleet-wide re-backfill after an
// extractor upgrade.
const SymbolsVersionCurrent = 2

// SchemaVersion returns the highest applied migration version (0 on a fresh DB).
// Exported so the daemon can guard against opening a catalog written by a newer
// binary (schema too new -> skip rather than risk corruption).
func (c *Catalog) SchemaVersion(ctx context.Context) (int, error) {
	return c.schemaVersion(ctx)
}

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
