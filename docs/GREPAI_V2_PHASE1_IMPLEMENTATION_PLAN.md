# GrepAI v2 — Phase 1 Implementation Plan (Durable SQLite Catalog)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Implement the durable, transactional SQLite catalog that backs the Phase 0 `catalog.Catalog` contract — the v2 source of truth for repositories, worktrees, generations, immutable artifacts, chunk vectors, worktree views, and jobs — so Gate 1 (rollback preserves the prior searchable view, incompatible fingerprints never cache-hit, repository/worktree isolation under `-race`) passes.

**Architecture:** A new `internal/enginev2/catalog/sqlite` subpackage provides `*sqlite.Catalog`, which implements `catalog.Catalog` plus the registration/generation/chunk helpers the interface methods and later phases require. It uses `modernc.org/sqlite` (pure Go) in WAL mode. Writes flow through one process-level `writeMu` (the "single serialized writer"); reads use WAL snapshot reads on the shared `*sql.DB`. Schema is versioned and applied by an idempotent migration runner. `CommitUpdate` is atomic: it stores any missing immutable artifact, switches the worktree view, and deletes the completed job in one transaction; any failure rolls the whole thing back, leaving the prior view searchable.

**Tech Stack:** Go 1.24.2, `database/sql`, `modernc.org/sqlite` (new dependency — pure Go, keeps CGO_ENABLED=0), `encoding/binary` + `math` for vector blobs, stdlib `testing`.

## Global Constraints

- Go version floor **go 1.24.2**.
- **CGO_ENABLED=0 must stay buildable.** `modernc.org/sqlite` is pure Go — verify `CGO_ENABLED=0 go build ./...` still works after adding it. No other cgo-requiring import.
- Module path **`github.com/yoanbernabeu/grepai`**.
- Phase 1 code lives under **`internal/enginev2/catalog/sqlite/`**. The interface package `internal/enginev2/catalog` stays interface-only (do not add the impl there — the subpackage imports it).
- `*sqlite.Catalog` MUST satisfy `catalog.Catalog` (compile assertion `var _ catalog.Catalog = (*Catalog)(nil)`).
- **`make test` runs `go test -race ./...`** — all tests pass under the race detector, including concurrent isolation tests.
- **`make lint` (golangci-lint v1.64.2) must stay green**; code `gofmt`-clean. gosec is enabled (annotate justified findings with `// #nosec GXXX - reason`, house style per `updater.go`); `_test.go` files are excluded from gosec/errcheck by `.golangci.yml`.
- Conventional commits, scope `enginev2` or `catalog`. Never push to `main`; work on the current feature branch.
- SQLite driver is **`modernc.org/sqlite` pinned at `v1.45.0`** (driver name `"sqlite"`, blank-imported for side effects). **Do NOT use `@latest`**: v1.46.2+ require **go 1.25.0**, which would bump the module's `go` directive off the 1.24.2 floor. v1.45.0 is the newest release whose `go` directive stays `1.24.2`; it is verified to build under `CGO_ENABLED=0` on host go 1.24.2 and to open a WAL DB with `foreign_keys` ON and round-trip a BLOB. Open in **WAL** mode with `busy_timeout` and `foreign_keys` ON. All writes serialized via `writeMu`.
- Vectors are stored as **little-endian float32 blobs** whose byte length MUST equal `dimensions*4`; validate on write and read (invariant 10: fingerprint correctness; never load a mis-dimensioned vector).
- Artifact cache lookups are **repository-scoped and fingerprint-exact** — a differing fingerprint MUST NOT return a cache hit (Gate 1).

## Consumed Phase 0 surfaces (already committed — do not modify)

- `core.RepositoryID`, `core.WorktreeID`, `core.ArtifactID`, `core.Generation` (int64).
- `core.ArtifactKey{RepositoryID, RelativePath, SourceHash, Fingerprint string}` with `.ArtifactID() core.ArtifactID`.
- `core.Artifact{ID core.ArtifactID; Key core.ArtifactKey; Dimensions int}`.
- `core.ViewEntry{WorktreeID core.WorktreeID; Path string; ArtifactID core.ArtifactID; Generation core.Generation}`.
- `core.CommitRequest{View core.ViewEntry; Artifact core.Artifact}`.
- `core.Job{WorktreeID core.WorktreeID; Path string; DesiredHash string; Generation core.Generation; Operation core.Operation; Priority core.Priority; Attempts int}`.
- `core.Operation` (`OpUpsert`=1, `OpDelete`=2); `core.Priority` (`PriorityInteractiveQuery`=1 … `PriorityBootstrap`=4, lower = higher priority).
- `catalog.Catalog` interface: `ActiveGeneration`, `GetArtifact`, `ResolveView`, `CommitUpdate(ctx, core.CommitRequest, core.Job)`, `UpsertJob`, `ClaimNextJob(ctx, minPriority)`.

---

## File Structure

```
internal/enginev2/catalog/sqlite/
  catalog.go        # Catalog struct, Open/Close, writeMu, withWriteTx, DSN/pragmas
  catalog_test.go   # Open/Close + pragma verification
  schema.go         # embedded migration SQL + idempotent migrate()
  schema_test.go    # migrate applies once, schema_version, tables/idempotency
  vector.go         # encodeVector / decodeVector + ErrVectorLength
  vector_test.go    # round-trip + wrong-length rejection
  registry.go       # RegisterRepository/RegisterWorktree/CreateGeneration/SetActiveGeneration/ActiveGeneration
  registry_test.go
  artifacts.go      # PutArtifact/GetArtifact + PutChunkVector/GetChunkVector (dim-validated)
  artifacts_test.go # fingerprint-scoped cache; no cross-fingerprint hit
  views.go          # ResolveView + CommitUpdate/commitUpdateTx (atomic) + UpsertJob + ClaimNextJob
  views_test.go     # commit→resolve, supersede, priority claim, isolation
  gate1_test.go     # Gate 1: rollback preserves prior view; no cross-fingerprint hit; -race isolation
```

---

## Task 1: Dependency, Catalog struct, Open/Close, write serialization

**Files:**
- Modify: `go.mod`, `go.sum` (add `modernc.org/sqlite`)
- Create: `internal/enginev2/catalog/sqlite/catalog.go`, `internal/enginev2/catalog/sqlite/catalog_test.go`

**Interfaces:**
- Consumes: nothing from later tasks.
- Produces: `type Catalog struct{...}`; `func Open(ctx context.Context, path string) (*Catalog, error)`; `func (c *Catalog) Close() error`; unexported `func (c *Catalog) withWriteTx(ctx context.Context, fn func(*sql.Tx) error) error`; `db *sql.DB` and `writeMu sync.Mutex` fields. Later tasks add methods on `*Catalog`.

- [ ] **Step 1: Add the dependency (pinned)**

Run:
```bash
GOFLAGS=-mod=mod go get modernc.org/sqlite@v1.45.0
```
Expected: `go.mod`/`go.sum` updated with `modernc.org/sqlite v1.45.0` and its deps; the `go` directive stays `go 1.24.2`. **Never `@latest`** — v1.46.2+ require go 1.25.0. (In this branch the dependency is already present in `go.mod`/`go.sum` from planning; if so, leave it and skip the `go get`.) After the catalog code imports it, it becomes a direct dependency; a later `go mod tidy` will drop the `// indirect` comment — that is expected.

- [ ] **Step 2: Write the failing test**

```go
// internal/enginev2/catalog/sqlite/catalog_test.go
package sqlite

import (
	"context"
	"path/filepath"
	"testing"
)

func TestOpenAppliesPragmas(t *testing.T) {
	ctx := context.Background()
	c, err := Open(ctx, filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()

	var journal string
	if err := c.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
		t.Fatalf("journal_mode: %v", err)
	}
	if journal != "wal" {
		t.Fatalf("journal_mode = %q, want wal", journal)
	}

	var fk int
	if err := c.db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&fk); err != nil {
		t.Fatalf("foreign_keys: %v", err)
	}
	if fk != 1 {
		t.Fatalf("foreign_keys = %d, want 1", fk)
	}
}

func TestCloseIsIdempotentEnough(t *testing.T) {
	c, err := Open(context.Background(), filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/enginev2/catalog/sqlite/ -run TestOpen`
Expected: FAIL — package/`Open` undefined.

- [ ] **Step 4: Write minimal implementation**

```go
// internal/enginev2/catalog/sqlite/catalog.go
// Package sqlite implements the enginev2 catalog.Catalog contract over a
// local SQLite database (modernc.org/sqlite, WAL mode). It is the durable
// source of truth for the v2 engine: repositories, worktrees, generations,
// immutable file artifacts, chunk vectors, worktree views, and jobs.
//
// All writes are serialized through a single process-level mutex (the
// "single serialized writer"); reads use WAL snapshot reads on the shared
// *sql.DB and never block commits.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	_ "modernc.org/sqlite" // pure-Go driver, registered as "sqlite"
)

// Catalog is a SQLite-backed durable catalog.
type Catalog struct {
	db      *sql.DB
	writeMu sync.Mutex
}

// Open opens (creating if needed) the SQLite database at path, applies WAL,
// busy_timeout, and foreign-keys pragmas, and runs pending migrations.
func Open(ctx context.Context, path string) (*Catalog, error) {
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
	// Establish the connection early so pragmas/migrations run against a live DB.
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite ping: %w", err)
	}
	c := &Catalog{db: db}
	if err := c.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return c, nil
}

// Close closes the underlying database.
func (c *Catalog) Close() error {
	return c.db.Close()
}

// withWriteTx runs fn inside a serialized write transaction. writeMu enforces
// the single-writer invariant; a non-nil error from fn rolls the whole
// transaction back, leaving prior state intact.
func (c *Catalog) withWriteTx(ctx context.Context, fn func(*sql.Tx) error) (err error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()
	if err = fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
```

Note: `migrate` is added in Task 2; until then this file will not compile on its own. Implement Task 2 immediately after so the package compiles. (Task 1 and Task 2 may be committed together if the reviewer prefers; keep the commits sequential.)

- [ ] **Step 5: Commit** (after Task 2 compiles — see Task 2 Step 5). If committing Task 1 alone, temporarily stub `func (c *Catalog) migrate(ctx context.Context) error { return nil }` and remove the stub in Task 2. Prefer committing Tasks 1+2 together.

```bash
git add go.mod go.sum internal/enginev2/catalog/sqlite/catalog.go internal/enginev2/catalog/sqlite/catalog_test.go
# commit performed in Task 2
```

---

## Task 2: Versioned schema and migration runner

**Files:**
- Create: `internal/enginev2/catalog/sqlite/schema.go`, `internal/enginev2/catalog/sqlite/schema_test.go`

**Interfaces:**
- Consumes: `*Catalog.db`, `*Catalog.withWriteTx` (Task 1).
- Produces: `func (c *Catalog) migrate(ctx context.Context) error`; `func (c *Catalog) schemaVersion(ctx context.Context) (int, error)`; `const schemaVersion = 1`.

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/catalog/sqlite/schema_test.go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/catalog/sqlite/ -run TestMigrate`
Expected: FAIL — `migrate`/`schemaVersion`/`schemaVersion const` undefined (or Task 1's stub returns nil so tables missing).

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/catalog/sqlite/schema.go
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
		// schema_migrations may not exist yet on a fresh DB.
		var sqlErr *sqliteNoTable
		if errors.As(err, &sqlErr) {
			return 0, nil
		}
		// Fall back: treat "no such table" as version 0.
		return 0, nil
	}
	if !v.Valid {
		return 0, nil
	}
	return int(v.Int64), nil
}

// sqliteNoTable is unused sentinel scaffolding; schemaVersion tolerates a
// missing table by returning 0 above.
type sqliteNoTable struct{}

func (*sqliteNoTable) Error() string { return "no such table" }

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
```

Note to implementer: the `sqliteNoTable` sentinel above is defensive scaffolding — `currentVersion` already guards the fresh-DB case via `sqlite_master`, so `schemaVersion` is only called once the table exists. If golangci-lint flags `sqliteNoTable` as unused, delete the type and simplify `schemaVersion` to just scan `MAX(version)` (the table is guaranteed to exist when it's called). Prefer the simpler form: remove `sqliteNoTable` and the `errors.As` branch.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/catalog/sqlite/ -run 'TestMigrate|TestOpen|TestClose'`
Expected: PASS.

- [ ] **Step 5: Commit (Tasks 1+2 together)**

```bash
git add go.mod go.sum internal/enginev2/catalog/sqlite/catalog.go internal/enginev2/catalog/sqlite/catalog_test.go internal/enginev2/catalog/sqlite/schema.go internal/enginev2/catalog/sqlite/schema_test.go
git commit -m "feat(catalog): SQLite catalog open + versioned schema migrations"
```

---

## Task 3: Vector encoding and validation

**Files:**
- Create: `internal/enginev2/catalog/sqlite/vector.go`, `internal/enginev2/catalog/sqlite/vector_test.go`

**Interfaces:**
- Produces: `func encodeVector(v []float32) []byte`; `func decodeVector(b []byte, dims int) ([]float32, error)`; `var ErrVectorLength = errors.New(...)`.

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/catalog/sqlite/vector_test.go
package sqlite

import (
	"errors"
	"testing"
)

func TestVectorRoundTrip(t *testing.T) {
	in := []float32{0, 1, -1, 3.14159, 2.71828, 1e-9, 1e9}
	b := encodeVector(in)
	if len(b) != len(in)*4 {
		t.Fatalf("encoded length = %d, want %d", len(b), len(in)*4)
	}
	out, err := decodeVector(b, len(in))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("decoded len = %d, want %d", len(out), len(in))
	}
	for i := range in {
		if out[i] != in[i] {
			t.Fatalf("index %d: got %v want %v", i, out[i], in[i])
		}
	}
}

func TestDecodeRejectsWrongLength(t *testing.T) {
	b := encodeVector([]float32{1, 2, 3})
	if _, err := decodeVector(b, 4); !errors.Is(err, ErrVectorLength) {
		t.Fatalf("expected ErrVectorLength for dims mismatch, got %v", err)
	}
	if _, err := decodeVector(b[:len(b)-1], 3); !errors.Is(err, ErrVectorLength) {
		t.Fatalf("expected ErrVectorLength for truncated blob, got %v", err)
	}
	if _, err := decodeVector(b, -1); !errors.Is(err, ErrVectorLength) {
		t.Fatalf("expected ErrVectorLength for negative dims, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/catalog/sqlite/ -run 'TestVector|TestDecode'`
Expected: FAIL — `encodeVector` undefined.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/catalog/sqlite/vector.go
package sqlite

import (
	"encoding/binary"
	"errors"
	"math"
)

// ErrVectorLength indicates a vector blob whose byte length does not equal
// dimensions*4, or a negative dimension count.
var ErrVectorLength = errors.New("catalog/sqlite: vector byte length does not match dimensions")

// encodeVector serializes a float32 slice as little-endian IEEE-754 bytes.
func encodeVector(v []float32) []byte {
	b := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

// decodeVector deserializes a little-endian float32 blob, validating that its
// byte length equals dims*4 (invariant 10: never load a mis-dimensioned vector).
func decodeVector(b []byte, dims int) ([]float32, error) {
	if dims < 0 || len(b) != dims*4 {
		return nil, ErrVectorLength
	}
	v := make([]float32, dims)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return v, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/catalog/sqlite/ -run 'TestVector|TestDecode'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/catalog/sqlite/vector.go internal/enginev2/catalog/sqlite/vector_test.go
git commit -m "feat(catalog): validated little-endian float32 vector encoding"
```

---

## Task 4: Repository/worktree registration and generations

**Files:**
- Create: `internal/enginev2/catalog/sqlite/registry.go`, `internal/enginev2/catalog/sqlite/registry_test.go`

**Interfaces:**
- Consumes: `core` types, `*Catalog.withWriteTx`, `*Catalog.db`.
- Produces on `*Catalog`:
  - `RegisterRepository(ctx, repo core.RepositoryID, rootPath, gitCommonDir string) error` (idempotent upsert)
  - `RegisterWorktree(ctx, wt core.WorktreeID, repo core.RepositoryID, rootPath string, regGen core.Generation) error`
  - `CreateGeneration(ctx, repo core.RepositoryID, gen core.Generation, fingerprint string) error` (status 'building')
  - `SetActiveGeneration(ctx, repo core.RepositoryID, gen core.Generation) error` (retire the previous active, set this active)
  - `ActiveGeneration(ctx, repo core.RepositoryID) (core.Generation, error)` (0 if none)

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/catalog/sqlite/registry_test.go
package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

func newTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	c, err := Open(context.Background(), filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestRegistrationAndGenerations(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)

	if err := c.RegisterRepository(ctx, "repo1", "/repo1", "/repo1/.git"); err != nil {
		t.Fatalf("register repo: %v", err)
	}
	// Idempotent: registering again does not error.
	if err := c.RegisterRepository(ctx, "repo1", "/repo1", "/repo1/.git"); err != nil {
		t.Fatalf("re-register repo: %v", err)
	}
	if err := c.RegisterWorktree(ctx, "wt1", "repo1", "/repo1", 1); err != nil {
		t.Fatalf("register worktree: %v", err)
	}

	// No active generation yet.
	if g, err := c.ActiveGeneration(ctx, "repo1"); err != nil || g != 0 {
		t.Fatalf("active gen = %d, err %v; want 0, nil", g, err)
	}

	if err := c.CreateGeneration(ctx, "repo1", 1, "fp-a"); err != nil {
		t.Fatalf("create gen 1: %v", err)
	}
	if err := c.SetActiveGeneration(ctx, "repo1", 1); err != nil {
		t.Fatalf("activate gen 1: %v", err)
	}
	if g, err := c.ActiveGeneration(ctx, "repo1"); err != nil || g != 1 {
		t.Fatalf("active gen = %d, err %v; want 1, nil", g, err)
	}

	// A second generation supersedes the first as active.
	if err := c.CreateGeneration(ctx, "repo1", 2, "fp-b"); err != nil {
		t.Fatalf("create gen 2: %v", err)
	}
	if err := c.SetActiveGeneration(ctx, "repo1", 2); err != nil {
		t.Fatalf("activate gen 2: %v", err)
	}
	if g, err := c.ActiveGeneration(ctx, "repo1"); err != nil || g != core.Generation(2) {
		t.Fatalf("active gen = %d; want 2", g)
	}
}

func TestRegisterWorktreeRequiresRepository(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	// Foreign key: worktree for an unregistered repo must fail.
	if err := c.RegisterWorktree(ctx, "wt1", "ghost", "/x", 1); err == nil {
		t.Fatal("expected FK error registering worktree for unknown repository")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/catalog/sqlite/ -run TestRegistration`
Expected: FAIL — `RegisterRepository` undefined.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/catalog/sqlite/registry.go
package sqlite

import (
	"context"
	"database/sql"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// RegisterRepository idempotently records a repository namespace.
func (c *Catalog) RegisterRepository(ctx context.Context, repo core.RepositoryID, rootPath, gitCommonDir string) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO repositories(repository_id, root_path, git_common_dir, created_at)
			VALUES(?, ?, ?, datetime('now'))
			ON CONFLICT(repository_id) DO UPDATE SET root_path=excluded.root_path, git_common_dir=excluded.git_common_dir`,
			string(repo), rootPath, gitCommonDir)
		return err
	})
}

// RegisterWorktree records a worktree bound to a repository namespace.
func (c *Catalog) RegisterWorktree(ctx context.Context, wt core.WorktreeID, repo core.RepositoryID, rootPath string, regGen core.Generation) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO worktrees(worktree_id, repository_id, root_path, registration_generation, created_at)
			VALUES(?, ?, ?, ?, datetime('now'))
			ON CONFLICT(worktree_id) DO UPDATE SET root_path=excluded.root_path, registration_generation=excluded.registration_generation`,
			string(wt), string(repo), rootPath, int64(regGen))
		return err
	})
}

// CreateGeneration records a new 'building' index generation.
func (c *Catalog) CreateGeneration(ctx context.Context, repo core.RepositoryID, gen core.Generation, fingerprint string) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO index_generations(repository_id, generation, fingerprint, status, created_at)
			VALUES(?, ?, ?, 'building', datetime('now'))`,
			string(repo), int64(gen), fingerprint)
		return err
	})
}

// SetActiveGeneration retires any current active generation and makes gen active.
func (c *Catalog) SetActiveGeneration(ctx context.Context, repo core.RepositoryID, gen core.Generation) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			UPDATE index_generations SET status='retired'
			WHERE repository_id=? AND status='active'`, string(repo)); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `
			UPDATE index_generations SET status='active'
			WHERE repository_id=? AND generation=?`, string(repo), int64(gen))
		return err
	})
}

// ActiveGeneration returns the repository's active generation, or 0 if none.
func (c *Catalog) ActiveGeneration(ctx context.Context, repo core.RepositoryID) (core.Generation, error) {
	var gen sql.NullInt64
	err := c.db.QueryRowContext(ctx, `
		SELECT generation FROM index_generations
		WHERE repository_id=? AND status='active'`, string(repo)).Scan(&gen)
	if err == sql.ErrNoRows || !gen.Valid {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return core.Generation(gen.Int64), nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/catalog/sqlite/ -run 'TestRegistration|TestRegisterWorktree'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/catalog/sqlite/registry.go internal/enginev2/catalog/sqlite/registry_test.go
git commit -m "feat(catalog): repository/worktree registration and generation lifecycle"
```

---

## Task 5: Artifact cache and chunk vectors

**Files:**
- Create: `internal/enginev2/catalog/sqlite/artifacts.go`, `internal/enginev2/catalog/sqlite/artifacts_test.go`

**Interfaces:**
- Produces on `*Catalog`:
  - `PutArtifact(ctx, core.Artifact) error` (insert-or-ignore immutable artifact; requires its repository registered)
  - `GetArtifact(ctx, core.ArtifactKey) (core.Artifact, bool, error)` (repository+fingerprint-exact cache lookup)
  - `PutChunkVector(ctx, chunkID string, repo core.RepositoryID, fingerprint string, vec []float32) error` (validates via encodeVector; stores dimensions)
  - `GetChunkVector(ctx, chunkID string) ([]float32, bool, error)` (decodes with stored dimensions; validates)

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/catalog/sqlite/artifacts_test.go
package sqlite

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

func seedRepo(t *testing.T, c *Catalog, repo core.RepositoryID) {
	t.Helper()
	if err := c.RegisterRepository(context.Background(), repo, "/"+string(repo), ""); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
}

func TestArtifactCacheFingerprintScoped(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepo(t, c, "repo1")

	keyA := core.ArtifactKey{RepositoryID: "repo1", RelativePath: "a.go", SourceHash: "oid", Fingerprint: "fp-a"}
	artA := core.Artifact{ID: keyA.ArtifactID(), Key: keyA, Dimensions: 8}
	if err := c.PutArtifact(ctx, artA); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Exact key hits.
	got, ok, err := c.GetArtifact(ctx, keyA)
	if err != nil || !ok {
		t.Fatalf("GetArtifact ok=%v err=%v", ok, err)
	}
	if got.ID != artA.ID || got.Dimensions != 8 {
		t.Fatalf("artifact mismatch: %+v", got)
	}

	// A differing fingerprint must NOT hit (Gate 1).
	keyB := keyA
	keyB.Fingerprint = "fp-b"
	if _, ok, _ := c.GetArtifact(ctx, keyB); ok {
		t.Fatal("incompatible fingerprint must not produce a cache hit")
	}
	// A differing source hash must NOT hit.
	keyC := keyA
	keyC.SourceHash = "oid2"
	if _, ok, _ := c.GetArtifact(ctx, keyC); ok {
		t.Fatal("differing source hash must not produce a cache hit")
	}
}

func TestPutArtifactIsImmutable(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepo(t, c, "repo1")
	key := core.ArtifactKey{RepositoryID: "repo1", RelativePath: "a.go", SourceHash: "oid", Fingerprint: "fp"}
	art := core.Artifact{ID: key.ArtifactID(), Key: key, Dimensions: 4}
	if err := c.PutArtifact(ctx, art); err != nil {
		t.Fatalf("put1: %v", err)
	}
	// Re-putting the same immutable artifact is a no-op, not an error.
	if err := c.PutArtifact(ctx, art); err != nil {
		t.Fatalf("put2: %v", err)
	}
}

func TestChunkVectorRoundTrip(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepo(t, c, "repo1")
	vec := []float32{1, 2, 3, 4}
	if err := c.PutChunkVector(ctx, "chunk1", "repo1", "fp", vec); err != nil {
		t.Fatalf("put chunk: %v", err)
	}
	got, ok, err := c.GetChunkVector(ctx, "chunk1")
	if err != nil || !ok {
		t.Fatalf("get chunk ok=%v err=%v", ok, err)
	}
	if len(got) != 4 || got[0] != 1 || got[3] != 4 {
		t.Fatalf("vector mismatch: %v", got)
	}
	// Missing chunk -> ok=false, no error.
	if _, ok, err := c.GetChunkVector(ctx, "nope"); ok || err != nil {
		t.Fatalf("missing chunk: ok=%v err=%v", ok, err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/catalog/sqlite/ -run 'TestArtifact|TestPutArtifact|TestChunkVector'`
Expected: FAIL — `PutArtifact` undefined.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/catalog/sqlite/artifacts.go
package sqlite

import (
	"context"
	"database/sql"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// PutArtifact stores an immutable artifact. Re-inserting an identical
// artifact_id is a no-op (INSERT OR IGNORE), preserving immutability.
func (c *Catalog) PutArtifact(ctx context.Context, a core.Artifact) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		return putArtifactTx(ctx, tx, a)
	})
}

// putArtifactTx is the transaction-scoped artifact insert, reused by
// CommitUpdate (Task 6) so the artifact store and view switch are atomic.
func putArtifactTx(ctx context.Context, tx *sql.Tx, a core.Artifact) error {
	_, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO file_artifacts(
			artifact_id, repository_id, relative_path, source_hash, fingerprint, dimensions, created_at)
		VALUES(?, ?, ?, ?, ?, ?, datetime('now'))`,
		string(a.ID), string(a.Key.RepositoryID), a.Key.RelativePath, a.Key.SourceHash,
		a.Key.Fingerprint, a.Dimensions)
	return err
}

// GetArtifact returns the artifact for an exact (repository, path, source hash,
// fingerprint) key. A differing fingerprint or source hash never matches.
func (c *Catalog) GetArtifact(ctx context.Context, key core.ArtifactKey) (core.Artifact, bool, error) {
	var id string
	var dims int
	err := c.db.QueryRowContext(ctx, `
		SELECT artifact_id, dimensions FROM file_artifacts
		WHERE repository_id=? AND relative_path=? AND source_hash=? AND fingerprint=?`,
		string(key.RepositoryID), key.RelativePath, key.SourceHash, key.Fingerprint).Scan(&id, &dims)
	if err == sql.ErrNoRows {
		return core.Artifact{}, false, nil
	}
	if err != nil {
		return core.Artifact{}, false, err
	}
	return core.Artifact{ID: core.ArtifactID(id), Key: key, Dimensions: dims}, true, nil
}

// PutChunkVector stores a chunk's validated float32 vector.
func (c *Catalog) PutChunkVector(ctx context.Context, chunkID string, repo core.RepositoryID, fingerprint string, vec []float32) error {
	blob := encodeVector(vec)
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO chunks(chunk_id, repository_id, fingerprint, dimensions, vector, created_at)
			VALUES(?, ?, ?, ?, ?, datetime('now'))`,
			chunkID, string(repo), fingerprint, len(vec), blob)
		return err
	})
}

// GetChunkVector returns a chunk's vector, validating the stored blob length
// against its stored dimension count.
func (c *Catalog) GetChunkVector(ctx context.Context, chunkID string) ([]float32, bool, error) {
	var dims int
	var blob []byte
	err := c.db.QueryRowContext(ctx, `
		SELECT dimensions, vector FROM chunks WHERE chunk_id=?`, chunkID).Scan(&dims, &blob)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	v, err := decodeVector(blob, dims)
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/catalog/sqlite/ -run 'TestArtifact|TestPutArtifact|TestChunkVector'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/catalog/sqlite/artifacts.go internal/enginev2/catalog/sqlite/artifacts_test.go
git commit -m "feat(catalog): fingerprint-scoped artifact cache and validated chunk vectors"
```

---

## Task 6: Worktree views, atomic CommitUpdate, and jobs

**Files:**
- Create: `internal/enginev2/catalog/sqlite/views.go`, `internal/enginev2/catalog/sqlite/views_test.go`

**Interfaces:**
- Produces on `*Catalog` (completing `catalog.Catalog`):
  - `ResolveView(ctx, wt core.WorktreeID, relPath string) (core.ArtifactID, bool, error)`
  - `CommitUpdate(ctx, req core.CommitRequest, job core.Job) error` wrapping unexported `commitUpdateTx(ctx, tx, req, job) error` (atomic: put artifact + switch view + delete job)
  - `UpsertJob(ctx, job core.Job) error` (supersede older/equal generation for same worktree+path)
  - `ClaimNextJob(ctx, minPriority core.Priority) (core.Job, bool, error)` (highest-priority unclaimed at/above minPriority; marks claimed)
- Also add the compile assertion `var _ catalog.Catalog = (*Catalog)(nil)` in this file.

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/catalog/sqlite/views_test.go
package sqlite

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

func seedRepoWorktree(t *testing.T, c *Catalog, repo core.RepositoryID, wt core.WorktreeID) {
	t.Helper()
	ctx := context.Background()
	if err := c.RegisterRepository(ctx, repo, "/"+string(repo), ""); err != nil {
		t.Fatalf("repo: %v", err)
	}
	if err := c.RegisterWorktree(ctx, wt, repo, "/"+string(wt), 1); err != nil {
		t.Fatalf("worktree: %v", err)
	}
}

func mkArtifact(repo core.RepositoryID, path, oid, fp string) core.Artifact {
	k := core.ArtifactKey{RepositoryID: repo, RelativePath: path, SourceHash: oid, Fingerprint: fp}
	return core.Artifact{ID: k.ArtifactID(), Key: k, Dimensions: 4}
}

func TestCommitThenResolve(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "repo1", "wt1")
	art := mkArtifact("repo1", "a.go", "oid1", "fp")
	req := core.CommitRequest{
		View:     core.ViewEntry{WorktreeID: "wt1", Path: "a.go", ArtifactID: art.ID, Generation: 1},
		Artifact: art,
	}
	job := core.Job{WorktreeID: "wt1", Path: "a.go", DesiredHash: "oid1", Generation: 1, Operation: core.OpUpsert}
	if err := c.CommitUpdate(ctx, req, job); err != nil {
		t.Fatalf("commit: %v", err)
	}
	id, ok, err := c.ResolveView(ctx, "wt1", "a.go")
	if err != nil || !ok || id != art.ID {
		t.Fatalf("resolve: id=%v ok=%v err=%v", id, ok, err)
	}
}

func TestCommitUpdateCompletesJob(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "repo1", "wt1")
	if err := c.UpsertJob(ctx, core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Priority: core.PriorityLiveChange, Operation: core.OpUpsert}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	art := mkArtifact("repo1", "a.go", "oid1", "fp")
	req := core.CommitRequest{View: core.ViewEntry{WorktreeID: "wt1", Path: "a.go", ArtifactID: art.ID, Generation: 1}, Artifact: art}
	job := core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Operation: core.OpUpsert}
	if err := c.CommitUpdate(ctx, req, job); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// The job is complete: nothing claimable remains.
	if _, ok, _ := c.ClaimNextJob(ctx, core.PriorityBootstrap); ok {
		t.Fatal("committed job must not remain claimable")
	}
}

func TestWorktreeViewIsolation(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "repo1", "wt1")
	seedRepoWorktree(t, c, "repo1", "wt2")
	art := mkArtifact("repo1", "a.go", "oid1", "fp")
	req := core.CommitRequest{View: core.ViewEntry{WorktreeID: "wt1", Path: "a.go", ArtifactID: art.ID, Generation: 1}, Artifact: art}
	if err := c.CommitUpdate(ctx, req, core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Operation: core.OpUpsert}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, ok, _ := c.ResolveView(ctx, "wt2", "a.go"); ok {
		t.Fatal("wt2 must not resolve a path only committed under wt1")
	}
}

func TestUpsertJobSupersedesOlderGeneration(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "repo1", "wt1")
	if err := c.UpsertJob(ctx, core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, DesiredHash: "old", Priority: core.PriorityLiveChange, Operation: core.OpUpsert}); err != nil {
		t.Fatalf("upsert1: %v", err)
	}
	if err := c.UpsertJob(ctx, core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 2, DesiredHash: "new", Priority: core.PriorityLiveChange, Operation: core.OpUpsert}); err != nil {
		t.Fatalf("upsert2: %v", err)
	}
	job, ok, err := c.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if job.Generation != 2 || job.DesiredHash != "new" {
		t.Fatalf("expected superseding gen 2/new, got gen %d/%q", job.Generation, job.DesiredHash)
	}
	// Only one row survived: no second claim.
	if _, ok, _ := c.ClaimNextJob(ctx, core.PriorityBootstrap); ok {
		t.Fatal("supersede must leave exactly one job per (worktree,path)")
	}
}

func TestClaimNextJobPriorityOrder(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "repo1", "wt1")
	_ = c.UpsertJob(ctx, core.Job{WorktreeID: "wt1", Path: "b.go", Generation: 1, Priority: core.PriorityBootstrap, Operation: core.OpUpsert})
	_ = c.UpsertJob(ctx, core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Priority: core.PriorityLiveChange, Operation: core.OpUpsert})
	job, ok, err := c.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if job.Priority != core.PriorityLiveChange {
		t.Fatalf("expected highest-priority (live change) first, got %v", job.Priority)
	}
	// minPriority gating: a claim at InteractiveQuery only sees priority<=1.
	if _, ok, _ := c.ClaimNextJob(ctx, core.PriorityInteractiveQuery); ok {
		t.Fatal("no job at/above interactive priority should be claimable")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/catalog/sqlite/ -run 'TestCommit|TestWorktree|TestUpsertJob|TestClaim'`
Expected: FAIL — `ResolveView`/`CommitUpdate` undefined.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/catalog/sqlite/views.go
package sqlite

import (
	"context"
	"database/sql"

	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// Compile-time assertion that the SQLite catalog satisfies the contract.
var _ catalog.Catalog = (*Catalog)(nil)

// ResolveView returns the artifact a worktree path currently resolves to.
func (c *Catalog) ResolveView(ctx context.Context, wt core.WorktreeID, relPath string) (core.ArtifactID, bool, error) {
	var id string
	err := c.db.QueryRowContext(ctx, `
		SELECT artifact_id FROM worktree_files WHERE worktree_id=? AND relative_path=?`,
		string(wt), relPath).Scan(&id)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return core.ArtifactID(id), true, nil
}

// CommitUpdate atomically stores the artifact, switches the worktree view, and
// completes the job. Any failure rolls the whole transaction back, leaving the
// prior view searchable (invariant 6).
func (c *Catalog) CommitUpdate(ctx context.Context, req core.CommitRequest, job core.Job) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		return commitUpdateTx(ctx, tx, req, job)
	})
}

// commitUpdateTx performs the artifact store + view switch + job completion
// within a caller-provided transaction. It is the internal seam used by both
// CommitUpdate and the Gate 1 rollback test.
func commitUpdateTx(ctx context.Context, tx *sql.Tx, req core.CommitRequest, job core.Job) error {
	if err := putArtifactTx(ctx, tx, req.Artifact); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO worktree_files(worktree_id, relative_path, artifact_id, generation, updated_at)
		VALUES(?, ?, ?, ?, datetime('now'))
		ON CONFLICT(worktree_id, relative_path) DO UPDATE SET
			artifact_id=excluded.artifact_id, generation=excluded.generation, updated_at=excluded.updated_at`,
		string(req.View.WorktreeID), req.View.Path, string(req.View.ArtifactID), int64(req.View.Generation)); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		DELETE FROM index_jobs WHERE worktree_id=? AND relative_path=?`,
		string(job.WorktreeID), job.Path)
	return err
}

// UpsertJob records desired file state, superseding an existing job for the
// same (worktree, path) only when the incoming generation is at least as new.
func (c *Catalog) UpsertJob(ctx context.Context, job core.Job) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO index_jobs(worktree_id, relative_path, desired_hash, generation, operation, priority, attempts, claimed, created_at)
			VALUES(?, ?, ?, ?, ?, ?, ?, 0, datetime('now'))
			ON CONFLICT(worktree_id, relative_path) DO UPDATE SET
				desired_hash=excluded.desired_hash,
				generation=excluded.generation,
				operation=excluded.operation,
				priority=excluded.priority,
				attempts=excluded.attempts,
				claimed=0
			WHERE excluded.generation >= index_jobs.generation`,
			string(job.WorktreeID), job.Path, job.DesiredHash, int64(job.Generation),
			int(job.Operation), int(job.Priority), job.Attempts)
		return err
	})
}

// ClaimNextJob claims the highest-priority unclaimed job at or above
// minPriority (lower Priority value = higher priority), marking it claimed so
// it is not handed out twice.
func (c *Catalog) ClaimNextJob(ctx context.Context, minPriority core.Priority) (core.Job, bool, error) {
	var job core.Job
	var found bool
	err := c.withWriteTx(ctx, func(tx *sql.Tx) error {
		var (
			jobID int64
			wt    string
			path  string
			hash  string
			gen   int64
			op    int
			prio  int
			att   int
		)
		row := tx.QueryRowContext(ctx, `
			SELECT job_id, worktree_id, relative_path, desired_hash, generation, operation, priority, attempts
			FROM index_jobs
			WHERE claimed=0 AND priority<=?
			ORDER BY priority ASC, job_id ASC
			LIMIT 1`, int(minPriority))
		if err := row.Scan(&jobID, &wt, &path, &hash, &gen, &op, &prio, &att); err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE index_jobs SET claimed=1 WHERE job_id=?`, jobID); err != nil {
			return err
		}
		job = core.Job{
			WorktreeID:  core.WorktreeID(wt),
			Path:        path,
			DesiredHash: hash,
			Generation:  core.Generation(gen),
			Operation:   core.Operation(op),
			Priority:    core.Priority(prio),
			Attempts:    att,
		}
		found = true
		return nil
	})
	if err != nil {
		return core.Job{}, false, err
	}
	return job, found, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/catalog/sqlite/ -run 'TestCommit|TestWorktree|TestUpsertJob|TestClaim'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/catalog/sqlite/views.go internal/enginev2/catalog/sqlite/views_test.go
git commit -m "feat(catalog): worktree views, atomic CommitUpdate, and durable jobs"
```

---

## Task 7: Gate 1 — rollback, fingerprint isolation, concurrency

**Files:**
- Create: `internal/enginev2/catalog/sqlite/gate1_test.go`

**Interfaces:**
- Consumes: everything above (uses the unexported `commitUpdateTx` seam and `withWriteTx`).
- Produces: no new production code — Gate 1 verification tests.

- [ ] **Step 1: Write the Gate 1 tests**

```go
// internal/enginev2/catalog/sqlite/gate1_test.go
package sqlite

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// Gate 1: a transaction rollback leaves the prior view searchable.
func TestGate1_RollbackPreservesPriorView(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "repo1", "wt1")

	// Commit v1 normally.
	v1 := mkArtifact("repo1", "a.go", "oid1", "fp")
	if err := c.CommitUpdate(ctx,
		core.CommitRequest{View: core.ViewEntry{WorktreeID: "wt1", Path: "a.go", ArtifactID: v1.ID, Generation: 1}, Artifact: v1},
		core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Operation: core.OpUpsert}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}

	// Attempt v2 inside a transaction that we force to roll back (simulating a
	// crash after the writes but before commit).
	v2 := mkArtifact("repo1", "a.go", "oid2", "fp")
	c.writeMu.Lock()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		c.writeMu.Unlock()
		t.Fatalf("begin: %v", err)
	}
	if err := commitUpdateTx(ctx,
		tx,
		core.CommitRequest{View: core.ViewEntry{WorktreeID: "wt1", Path: "a.go", ArtifactID: v2.ID, Generation: 2}, Artifact: v2},
		core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 2, Operation: core.OpUpsert}); err != nil {
		t.Fatalf("commitUpdateTx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	c.writeMu.Unlock()

	// The view must still resolve to v1, not the rolled-back v2.
	id, ok, err := c.ResolveView(ctx, "wt1", "a.go")
	if err != nil || !ok {
		t.Fatalf("resolve after rollback: ok=%v err=%v", ok, err)
	}
	if id != v1.ID {
		t.Fatalf("view = %v after rollback, want prior v1 %v", id, v1.ID)
	}
}

// Gate 1: incompatible fingerprints never produce a cache hit.
func TestGate1_NoCrossFingerprintCacheHit(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepo(t, c, "repo1")
	base := core.ArtifactKey{RepositoryID: "repo1", RelativePath: "a.go", SourceHash: "oid", Fingerprint: "fp-1536"}
	if err := c.PutArtifact(ctx, core.Artifact{ID: base.ArtifactID(), Key: base, Dimensions: 1536}); err != nil {
		t.Fatalf("put: %v", err)
	}
	other := base
	other.Fingerprint = "fp-768"
	if _, ok, _ := c.GetArtifact(ctx, other); ok {
		t.Fatal("Gate 1: differing fingerprint returned a cache hit")
	}
}

// Gate 1: repository and worktree isolation hold under concurrent writers
// (run the package with -race to exercise the single-writer serialization).
func TestGate1_ConcurrentWorktreeIsolation(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	if err := c.RegisterRepository(ctx, "repo1", "/repo1", ""); err != nil {
		t.Fatalf("repo: %v", err)
	}
	const n = 8
	for i := 0; i < n; i++ {
		if err := c.RegisterWorktree(ctx, core.WorktreeID(fmt.Sprintf("wt%d", i)), "repo1", "/w", 1); err != nil {
			t.Fatalf("worktree %d: %v", i, err)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wt := core.WorktreeID(fmt.Sprintf("wt%d", i))
			// Each worktree commits its own distinct artifact for the same path.
			art := mkArtifact("repo1", "shared.go", fmt.Sprintf("oid%d", i), "fp")
			_ = c.CommitUpdate(ctx,
				core.CommitRequest{View: core.ViewEntry{WorktreeID: wt, Path: "shared.go", ArtifactID: art.ID, Generation: 1}, Artifact: art},
				core.Job{WorktreeID: wt, Path: "shared.go", Generation: 1, Operation: core.OpUpsert})
		}(i)
	}
	wg.Wait()

	// Each worktree resolves to its own artifact — no cross-contamination.
	for i := 0; i < n; i++ {
		wt := core.WorktreeID(fmt.Sprintf("wt%d", i))
		want := mkArtifact("repo1", "shared.go", fmt.Sprintf("oid%d", i), "fp").ID
		id, ok, err := c.ResolveView(ctx, wt, "shared.go")
		if err != nil || !ok {
			t.Fatalf("resolve %s: ok=%v err=%v", wt, ok, err)
		}
		if id != want {
			t.Fatalf("worktree %s resolved %v, want its own %v", wt, id, want)
		}
	}
}
```

- [ ] **Step 2: Run the Gate 1 tests under the race detector**

Run: `go test -race ./internal/enginev2/catalog/sqlite/ -run TestGate1 -v`
Expected: `TestGate1_RollbackPreservesPriorView`, `TestGate1_NoCrossFingerprintCacheHit`, `TestGate1_ConcurrentWorktreeIsolation` all PASS, no data races.

- [ ] **Step 3: Full Gate 1 verification**

```bash
go build ./...
go vet ./internal/enginev2/...
go test -race ./internal/enginev2/...
gofmt -l internal/enginev2/catalog
make lint
CGO_ENABLED=0 go build ./...   # confirm pure-Go build still works with modernc.org/sqlite
```
Expected: build/vet/tests pass; gofmt prints nothing; `make lint` exit 0; CGO_ENABLED=0 build succeeds.

- [ ] **Step 4: Commit**

```bash
git add internal/enginev2/catalog/sqlite/gate1_test.go
git commit -m "test(catalog): Gate 1 — rollback preserves view, fingerprint isolation, -race concurrency"
```

---

## Gate 1 Exit Criteria (spec §9, Phase 1)

- [ ] `*sqlite.Catalog` satisfies `catalog.Catalog` (compile assertion present).
- [ ] Versioned schema + idempotent migration runner; reopening applies nothing new.
- [ ] Repository-scoped, fingerprint-exact artifact cache; incompatible fingerprints never hit.
- [ ] Validated float32 vector encoding (byte length == dims*4 enforced on read/write).
- [ ] Atomic `CommitUpdate`; a rolled-back transaction leaves the prior view searchable.
- [ ] Repository/worktree isolation holds under concurrent writers with `go test -race`.
- [ ] `go build ./...`, `go vet`, `go test -race ./internal/enginev2/...`, `gofmt -l`, `make lint`, and `CGO_ENABLED=0 go build ./...` all pass.

---

## Self-Review Notes

**Spec coverage (Phase 1 deliverables, arch plan §9):**
- versioned SQLite schema + migration runner → Task 2.
- repositories/worktrees/generations/artifacts/chunks/symbols/views/jobs tables → Task 2 schema (symbols/symbol_edges/dead_letter_jobs/service_state created but exercised in later phases).
- vector encoding validation → Task 3 + Task 5 (`PutChunkVector`/`GetChunkVector`).
- repository-scoped fingerprinted cache → Task 5 (`GetArtifact`).
- atomic worktree view switching → Task 6 (`CommitUpdate`/`commitUpdateTx`).
- Gate 1 (rollback, fingerprint, isolation `-race`) → Task 7.

**Type consistency:** `commitUpdateTx(ctx, *sql.Tx, core.CommitRequest, core.Job)` is defined in Task 6 and reused by the Task 7 rollback test. `putArtifactTx` (Task 5) is reused by `commitUpdateTx` (Task 6). `ActiveGeneration` is defined once (Task 4) and is the `catalog.Catalog` method — Task 6's `var _ catalog.Catalog` assertion covers all six interface methods (`ActiveGeneration` T4; `GetArtifact` T5; `ResolveView`/`CommitUpdate`/`UpsertJob`/`ClaimNextJob` T6). `core.Priority` lower-value-is-higher ordering drives `ClaimNextJob`'s `ORDER BY priority ASC` and the `priority<=minPriority` gate.

**Deferred to later phases (documented):** symbols/symbol_edges population (Phase 3 trace), dead_letter_jobs transitions (Phase 3/4), generation-scoped view filtering and retention/GC (Phase 5/§5.9), the single-writer/WAL read-concurrency benchmark on the real corpus (arch plan §13 risk — Phase 1 uses a write mutex; benchmarking is a follow-up).
