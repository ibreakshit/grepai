package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestMigrateAppliesSchemaOnce(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "c.db")

	c, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	v, err := c.schemaVersion(ctx)
	if err != nil {
		t.Fatalf("version: %v", err)
	}
	if v != schemaVersion {
		t.Fatalf("schema version = %d, want %d", v, schemaVersion)
	}
	// Every expected table exists.
	for _, tbl := range []string{
		"schema_migrations", "repositories", "worktrees", "index_generations",
		"file_artifacts", "chunks", "artifact_chunks", "worktree_files",
		"index_jobs", "dead_letter_jobs", "symbols", "symbol_edges", "service_state",
	} {
		var name string
		err := c.db.QueryRowContext(ctx,
			"SELECT name FROM sqlite_master WHERE type='table' AND name=?", tbl).Scan(&name)
		if err != nil {
			t.Fatalf("table %q missing: %v", tbl, err)
		}
	}
	// migration0002 columns exist (chunk display metadata).
	for _, col := range []struct{ table, column string }{
		{"chunks", "content"}, {"artifact_chunks", "start_line"}, {"artifact_chunks", "end_line"},
	} {
		var found int
		if err := c.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM pragma_table_info(?) WHERE name=?", col.table, col.column).Scan(&found); err != nil {
			t.Fatalf("pragma %s.%s: %v", col.table, col.column, err)
		}
		if found != 1 {
			t.Fatalf("column %s.%s missing after migration0002", col.table, col.column)
		}
	}
	c.Close()

	// Reopening applies no new migration and keeps the version.
	c2, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()
	v2, err := c2.schemaVersion(ctx)
	if err != nil {
		t.Fatalf("version2: %v", err)
	}
	if v2 != schemaVersion {
		t.Fatalf("reopened schema version = %d, want %d", v2, schemaVersion)
	}
}

func TestSchemaVersionMatchesLatestAfterOpen(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(context.Background(), filepath.Join(dir, "c.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()
	v, err := c.SchemaVersion(context.Background())
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != LatestSchemaVersion {
		t.Fatalf("freshly opened catalog is at schema %d, want LatestSchemaVersion=%d", v, LatestSchemaVersion)
	}
	if LatestSchemaVersion < 1 {
		t.Fatalf("LatestSchemaVersion must be >= 1, got %d", LatestSchemaVersion)
	}
}

// TestMigration0003UpgradesV2Catalog builds a genuine schema-v2 catalog (the
// shape every fleet repo had before trace) with raw SQL, then opens it through
// the normal path and asserts migration 0003 rebuilt the symbol tables with
// location-aware keys.
func TestMigration0003UpgradesV2Catalog(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "v2.db")

	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	for i, stmt := range []string{migration0001, migration0002} {
		if _, err := raw.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("apply migration %d raw: %v", i+1, err)
		}
		if _, err := raw.ExecContext(ctx,
			"INSERT INTO schema_migrations(version, applied_at) VALUES(?, datetime('now'))", i+1); err != nil {
			t.Fatal(err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	c, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("open v2 catalog with v3 binary: %v", err)
	}
	defer c.Close()
	v, err := c.schemaVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v != 3 {
		t.Fatalf("schema version after upgrade = %d, want 3", v)
	}
	// Location-aware keys: same (name, kind) at two lines and a repeated
	// caller->callee at two lines must all insert as distinct rows.
	for _, ins := range []string{
		"INSERT INTO repositories(repository_id, root_path, created_at) VALUES('r','/r',datetime('now'))",
		"INSERT INTO file_artifacts(artifact_id, repository_id, relative_path, source_hash, fingerprint, dimensions, created_at) VALUES('a','r','a.go','h','fp',4,datetime('now'))",
		"INSERT INTO symbols(artifact_id, name, kind, line) VALUES('a','Get','method',10)",
		"INSERT INTO symbols(artifact_id, name, kind, line) VALUES('a','Get','method',50)",
		"INSERT INTO symbol_edges(artifact_id, caller, callee, line) VALUES('a','Run','Get',3)",
		"INSERT INTO symbol_edges(artifact_id, caller, callee, line) VALUES('a','Run','Get',7)",
	} {
		if _, err := c.db.ExecContext(ctx, ins); err != nil {
			t.Fatalf("%s: %v", ins, err)
		}
	}
	var n int
	if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM symbols").Scan(&n); err != nil || n != 2 {
		t.Fatalf("symbols rows = %d (err %v), want 2", n, err)
	}
	if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM symbol_edges").Scan(&n); err != nil || n != 2 {
		t.Fatalf("symbol_edges rows = %d (err %v), want 2", n, err)
	}
	if err := c.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM pragma_table_info('file_artifacts') WHERE name='symbols_version'").Scan(&n); err != nil || n != 1 {
		t.Fatalf("file_artifacts.symbols_version missing (n=%d err=%v)", n, err)
	}
}
