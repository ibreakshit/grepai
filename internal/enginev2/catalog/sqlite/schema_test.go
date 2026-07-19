package sqlite

import (
	"context"
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
