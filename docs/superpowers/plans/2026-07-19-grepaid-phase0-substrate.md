# grepaid Phase 0 — Multi-Repo Substrate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the multi-repo substrate that lets one process serve many per-repo SQLite catalogs — a `catalogset.Set` fan-out adapter (satisfying the four catalog-facing interfaces), a per-repo `BuilderRouter`, a host `registry` (registry.json), and an exported catalog schema-version accessor + guard.

**Architecture:** `Set` owns `map[RepositoryID]*sqlite.Catalog` and implements the union of `scheduler.Queue`, `service.Catalog`, `worker.Catalog`, and `reconcile.CatalogReader` by routing single-repo calls (keyed by an explicit repo id, a `WorktreeID` resolved through an internal worktree→repo map, a `Job.WorktreeID`, or an `ArtifactKey.RepositoryID`) and fanning out the five host-wide aggregate calls. Chunk-cache reads carry no repo id, so a separate `BuilderRouter` dispatches builds by `BuildRequest.Key.RepositoryID`. This is Phase 0 of `docs/superpowers/specs/2026-07-19-grepaid-daemon-design.md`; it is fully isolated — no daemon, no transport, no CLI.

**Tech Stack:** Go 1.24.2, `modernc.org/sqlite` (via the existing `internal/enginev2/catalog/sqlite`), standard library only for the substrate.

## Global Constraints

- Module path: `github.com/yoanbernabeu/grepai` (import internal packages under this path).
- Go version floor: 1.24.2 (per `go.mod`; do not use newer language features).
- New packages: `internal/enginev2/catalogset/` (Set + BuilderRouter) and `internal/enginev2/registry/`.
- Every per-repo routing method returns an error for an unregistered repo/worktree — **never** silently pick another repo (this is the cross-repo isolation guardrail).
- Gates (Gate 0, run before handoff to Phase A): `make build`, `make lint`, `go test ./... -race`, `go vet ./...`, `gofmt -l` (empty) — all green.
- Commit convention: `type(scope): description` (conventional commits). Commit at the end of each task.
- Reference existing assembly patterns in `internal/enginev2/runtime/runtime.go` (it injects `*sqlite.Catalog` into all four constructors already).

---

## File Structure

- `internal/enginev2/catalog/sqlite/schema.go` — **modify**: add exported `SchemaVersion(ctx)` + `LatestSchemaVersion`.
- `internal/enginev2/registry/registry.go` — **create**: `Entry`, `Registry`, `Load`, `Save` (atomic).
- `internal/enginev2/registry/registry_test.go` — **create**.
- `internal/enginev2/catalogset/catalogset.go` — **create**: `Set` struct, `Add`/`Close`/routing helpers + the worktree→repo map.
- `internal/enginev2/catalogset/methods.go` — **create**: the 25 interface methods + `SetActiveGeneration` + four `var _` assertions.
- `internal/enginev2/catalogset/builder.go` — **create**: `BuilderRouter`.
- `internal/enginev2/catalogset/catalogset_test.go` — **create**.
- `internal/enginev2/catalogset/builder_test.go` — **create**.

---

## Reference: the four interfaces `Set` must satisfy (verbatim from the tree)

```go
// scheduler.Queue
RepositoriesWithPendingJobs(ctx) ([]core.RepositoryID, error)
ClaimNextJobInRepo(ctx, repo core.RepositoryID, minPriority core.Priority) (core.Job, bool, error)
QueueDepthByPriority(ctx) (map[core.Priority]int, error)
UpsertJob(ctx, job core.Job) error
DeadLetterJob(ctx, job core.Job, reason string) error

// service.Catalog (adds)
RegisterRepository(ctx, repo core.RepositoryID, rootPath, gitCommonDir string) error
RegisterWorktree(ctx, wt core.WorktreeID, repo core.RepositoryID, rootPath string, regGen core.Generation) error
WorktreeInfo(ctx, wt core.WorktreeID) (string, core.RepositoryID, error)
ActiveGeneration(ctx, repo core.RepositoryID) (core.Generation, error)
GenerationFingerprint(ctx, repo core.RepositoryID, gen core.Generation) (string, error)
CreateGeneration(ctx, repo core.RepositoryID, gen core.Generation, fingerprint string) error
EnsureActiveGeneration(ctx, repo core.RepositoryID, gen core.Generation, fingerprint string) error
SearchWorktree(ctx, wt core.WorktreeID, query []float32, limit int) ([]core.SearchHit, error)
WorktreePendingCount(ctx, wt core.WorktreeID) (int, error)
WorktreePathsPending(ctx, wt core.WorktreeID, paths []string) (bool, error)
DeadLetterCount(ctx) (int, error)

// worker.Catalog (adds)
ClaimNextJob(ctx, minPriority core.Priority) (core.Job, bool, error)
GetArtifact(ctx, key core.ArtifactKey) (core.Artifact, bool, error)
PutChunkVector(ctx, chunkID string, repo core.RepositoryID, fingerprint string, vec []float32, content string) error
CommitUpdate(ctx, req core.CommitRequest, job core.Job) error
CommitDelete(ctx, wt core.WorktreeID, relPath string, gen core.Generation, job core.Job) error
FailJobAttempt(ctx, job core.Job) (int, error)
CurrentJob(ctx, wt core.WorktreeID, relPath string) (core.Generation, string, bool, error)
RequeueClaimedJobs(ctx) (int, error)

// reconcile.CatalogReader (adds)
WorktreeIndexedHashes(ctx, wt core.WorktreeID) (map[string]string, error)
```

**Routing table — implement every row in `methods.go`:**

| Method | Key | Strategy |
|---|---|---|
| ActiveGeneration, GenerationFingerprint, CreateGeneration, EnsureActiveGeneration, RegisterRepository, ClaimNextJobInRepo, PutChunkVector, SetActiveGeneration | explicit `repo` arg | `get(repo)` → delegate |
| RegisterWorktree | `repo` arg | `get(repo)` → delegate, **then** record `wt→repo` in the map |
| WorktreeInfo, SearchWorktree, WorktreePendingCount, WorktreePathsPending, CommitDelete, CurrentJob, WorktreeIndexedHashes | `wt` arg | `getByWT(wt)` → delegate |
| UpsertJob, DeadLetterJob, FailJobAttempt, CommitUpdate | `job.WorktreeID` | `getByWT(job.WorktreeID)` → delegate |
| GetArtifact | `key.RepositoryID` | `get(key.RepositoryID)` → delegate |
| RepositoriesWithPendingJobs | — | fan-out, concat unique |
| QueueDepthByPriority | — | fan-out, sum per priority |
| DeadLetterCount | — | fan-out, sum |
| RequeueClaimedJobs | — | fan-out, sum |
| ClaimNextJob | — | fan-out, first catalog returning a job wins (completeness only; daemon never calls it) |

---

## Task 1: Exported catalog schema-version accessor

**Files:**
- Modify: `internal/enginev2/catalog/sqlite/schema.go` (near `var migrations` and `func (c *Catalog) schemaVersion`)
- Test: `internal/enginev2/catalog/sqlite/schema_test.go` (create or append)

**Interfaces:**
- Produces: `func (c *Catalog) SchemaVersion(ctx context.Context) (int, error)` (highest applied migration, 0 on fresh DB) and `var LatestSchemaVersion = len(migrations)` (the version this binary migrates to; currently 2).

- [ ] **Step 1: Write the failing test**

Append to `internal/enginev2/catalog/sqlite/schema_test.go`:

```go
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
```

Ensure the test file imports `context`, `path/filepath`, `testing`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/catalog/sqlite/ -run TestSchemaVersionMatchesLatestAfterOpen -v`
Expected: FAIL — `c.SchemaVersion undefined` / `undefined: LatestSchemaVersion`.

- [ ] **Step 3: Add the accessor + constant**

In `schema.go`, immediately after `var migrations = []string{migration0001, migration0002}` add:

```go
// LatestSchemaVersion is the schema version this binary migrates a catalog to.
// The daemon refuses to open a catalog stamped newer than this.
var LatestSchemaVersion = len(migrations)

// SchemaVersion returns the highest applied migration version (0 on a fresh DB).
// Exported so the daemon can guard against opening a catalog written by a newer
// binary (schema too new -> skip rather than risk corruption).
func (c *Catalog) SchemaVersion(ctx context.Context) (int, error) {
	return c.schemaVersion(ctx)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/enginev2/catalog/sqlite/ -run TestSchemaVersionMatchesLatestAfterOpen -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/catalog/sqlite/schema.go internal/enginev2/catalog/sqlite/schema_test.go
git commit -m "feat(sqlite): export SchemaVersion + LatestSchemaVersion for the daemon schema guard"
```

---

## Task 2: Host registry (registry.json)

**Files:**
- Create: `internal/enginev2/registry/registry.go`
- Test: `internal/enginev2/registry/registry_test.go`

**Interfaces:**
- Produces:
  - `type Entry struct { RepositoryID, Root, CatalogPath string; ActiveGeneration int64; LastReconciledAt string; PendingCount int }`
  - `type Registry struct { Entries []Entry }`
  - `func Load(path string) (*Registry, error)` — a missing file returns an empty `&Registry{}`, nil.
  - `func (r *Registry) Save(path string) error` — atomic (temp + rename).
  - `func (r *Registry) Upsert(e Entry)` — replace by `RepositoryID`, else append.

- [ ] **Step 1: Write the failing test**

Create `internal/enginev2/registry/registry_test.go`:

```go
package registry

import (
	"path/filepath"
	"testing"
)

func TestLoadMissingReturnsEmpty(t *testing.T) {
	r, err := Load(filepath.Join(t.TempDir(), "registry.json"))
	if err != nil {
		t.Fatalf("Load missing: %v", err)
	}
	if len(r.Entries) != 0 {
		t.Fatalf("missing file should load empty, got %d entries", len(r.Entries))
	}
}

func TestSaveLoadRoundTripAndUpsert(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	r := &Registry{}
	r.Upsert(Entry{RepositoryID: "/a", Root: "/a", CatalogPath: "/a/.grepai/catalog_v2.db", ActiveGeneration: 1})
	r.Upsert(Entry{RepositoryID: "/b", Root: "/b", CatalogPath: "/b/.grepai/catalog_v2.db", ActiveGeneration: 3})
	r.Upsert(Entry{RepositoryID: "/a", Root: "/a", CatalogPath: "/a/.grepai/catalog_v2.db", ActiveGeneration: 2}) // replace /a
	if err := r.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got.Entries) != 2 {
		t.Fatalf("want 2 entries after upsert-replace, got %d", len(got.Entries))
	}
	var genA int64
	for _, e := range got.Entries {
		if e.RepositoryID == "/a" {
			genA = e.ActiveGeneration
		}
	}
	if genA != 2 {
		t.Fatalf("upsert should have replaced /a generation with 2, got %d", genA)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/registry/ -v`
Expected: FAIL — package/`Load`/`Registry`/`Entry` undefined.

- [ ] **Step 3: Implement the registry**

Create `internal/enginev2/registry/registry.go`:

```go
// Package registry persists the host daemon's set of registered repositories to
// a single JSON file (registry.json) so the daemon re-opens their catalogs on
// restart without opening every catalog to discover them.
package registry

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// Entry records one registered repository. CatalogPath is the absolute path to
// that repo's .grepai/catalog_v2.db. The generation/cursor fields are a cheap
// status cache (kept fresh on register/reconcile); the catalog remains the
// source of truth.
type Entry struct {
	RepositoryID     string `json:"repository_id"`
	Root             string `json:"root"`
	CatalogPath      string `json:"catalog_path"`
	ActiveGeneration int64  `json:"active_generation"`
	LastReconciledAt string `json:"last_reconciled_at,omitempty"`
	PendingCount     int    `json:"pending_count"`
}

// Registry is the full set of registered repositories.
type Registry struct {
	Entries []Entry `json:"entries"`
}

// Load reads the registry file. A missing file is not an error: it returns an
// empty registry (first run).
func Load(path string) (*Registry, error) {
	b, err := os.ReadFile(path) // #nosec G304 - operator's own state file
	if errors.Is(err, os.ErrNotExist) {
		return &Registry{}, nil
	}
	if err != nil {
		return nil, err
	}
	var r Registry
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// Upsert replaces the entry with the same RepositoryID, or appends it.
func (r *Registry) Upsert(e Entry) {
	for i := range r.Entries {
		if r.Entries[i].RepositoryID == e.RepositoryID {
			r.Entries[i] = e
			return
		}
	}
	r.Entries = append(r.Entries, e)
}

// Save writes the registry atomically (temp file in the same dir + rename), so a
// crash mid-write never truncates the live registry.
func (r *Registry) Save(path string) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".registry-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/enginev2/registry/ -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/registry/
git commit -m "feat(registry): host registry.json load/save/upsert with atomic write"
```

---

## Task 3: `catalogset.Set` — lifecycle, worktree→repo map, routing helpers

**Files:**
- Create: `internal/enginev2/catalogset/catalogset.go`
- Test: `internal/enginev2/catalogset/catalogset_test.go`

**Interfaces:**
- Consumes: `internal/enginev2/catalog/sqlite` (`Open`, `*Catalog`, `SchemaVersion`, `LatestSchemaVersion`), `internal/enginev2/core`.
- Produces:
  - `type Set struct { … }` with unexported `map[core.RepositoryID]*sqlite.Catalog`, `map[core.WorktreeID]core.RepositoryID`, and a `sync.RWMutex`.
  - `func New() *Set`
  - `func (s *Set) Add(ctx context.Context, repo core.RepositoryID, catalogPath string) error` — opens the catalog with the schema guard; returns `ErrSchemaTooNew` if the catalog is newer than `sqlite.LatestSchemaVersion`.
  - `func (s *Set) Close() error` — closes all catalogs.
  - `var ErrSchemaTooNew = errors.New("catalogset: catalog schema newer than supported")`
  - unexported helpers `get(repo) (*sqlite.Catalog, error)`, `getByWT(wt) (*sqlite.Catalog, error)`, `bindWorktree(wt, repo)` — used by Task 4.
  - `var ErrUnknownRepo`, `var ErrUnknownWorktree`.

- [ ] **Step 1: Write the failing test**

Create `internal/enginev2/catalogset/catalogset_test.go`:

```go
package catalogset

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

func TestAddOpensCatalogAndGetRoutes(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	p := filepath.Join(t.TempDir(), "a.db")
	if err := s.Add(ctx, core.RepositoryID("/a"), p); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := s.get(core.RepositoryID("/a")); err != nil {
		t.Fatalf("get registered repo: %v", err)
	}
	if _, err := s.get(core.RepositoryID("/nope")); !errors.Is(err, ErrUnknownRepo) {
		t.Fatalf("get unknown repo should be ErrUnknownRepo, got %v", err)
	}
}

func TestGetByWTRequiresBinding(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	if err := s.Add(ctx, core.RepositoryID("/a"), filepath.Join(t.TempDir(), "a.db")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := s.getByWT(core.WorktreeID("/a")); !errors.Is(err, ErrUnknownWorktree) {
		t.Fatalf("unbound worktree should be ErrUnknownWorktree, got %v", err)
	}
	s.bindWorktree(core.WorktreeID("/a"), core.RepositoryID("/a"))
	if _, err := s.getByWT(core.WorktreeID("/a")); err != nil {
		t.Fatalf("bound worktree should route: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/catalogset/ -run 'TestAdd|TestGetByWT' -v`
Expected: FAIL — package/`New`/`Set`/`Add` undefined.

- [ ] **Step 3: Implement the core**

Create `internal/enginev2/catalogset/catalogset.go`:

```go
// Package catalogset serves many per-repo SQLite catalogs from one process. Set
// implements the union of the catalog-facing interfaces (scheduler.Queue,
// service.Catalog, worker.Catalog, reconcile.CatalogReader) by routing each
// single-repo call to the owning catalog and fanning out the host-wide
// aggregates. Cross-repo isolation is structural: an op for an unregistered
// repo/worktree errors, it never touches another repo's catalog.
package catalogset

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog/sqlite"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

var (
	// ErrUnknownRepo is returned for an op targeting an unregistered repository.
	ErrUnknownRepo = errors.New("catalogset: unknown repository")
	// ErrUnknownWorktree is returned for an op whose worktree is not bound to a
	// registered repository.
	ErrUnknownWorktree = errors.New("catalogset: unknown worktree")
	// ErrSchemaTooNew is returned by Add when a catalog's schema is newer than
	// this binary supports (guard against corruption from an older daemon).
	ErrSchemaTooNew = errors.New("catalogset: catalog schema newer than supported")
)

// Set is the live map of registered repositories to their open catalogs, plus
// the worktree->repo routing map. Safe for concurrent use.
type Set struct {
	mu    sync.RWMutex
	cats  map[core.RepositoryID]*sqlite.Catalog
	wtToR map[core.WorktreeID]core.RepositoryID
}

// New returns an empty Set.
func New() *Set {
	return &Set{
		cats:  make(map[core.RepositoryID]*sqlite.Catalog),
		wtToR: make(map[core.WorktreeID]core.RepositoryID),
	}
}

// Add opens the repository's catalog at catalogPath and registers it. It applies
// the schema guard: a catalog stamped newer than sqlite.LatestSchemaVersion is
// closed and rejected with ErrSchemaTooNew (the daemon skips + logs it).
func (s *Set) Add(ctx context.Context, repo core.RepositoryID, catalogPath string) error {
	cat, err := sqlite.Open(ctx, catalogPath)
	if err != nil {
		return err
	}
	v, err := cat.SchemaVersion(ctx)
	if err != nil {
		_ = cat.Close()
		return err
	}
	if v > sqlite.LatestSchemaVersion {
		_ = cat.Close()
		return fmt.Errorf("%w: catalog %q at v%d, binary supports v%d", ErrSchemaTooNew, catalogPath, v, sqlite.LatestSchemaVersion)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cats[repo]; ok {
		_ = cat.Close() // already registered; keep the first handle, drop this one
		return nil
	}
	s.cats[repo] = cat
	return nil
}

// Close closes every catalog. The first error is returned; all are attempted.
func (s *Set) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var first error
	for repo, c := range s.cats {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
		delete(s.cats, repo)
	}
	return first
}

// bindWorktree records that wt belongs to repo (called from RegisterWorktree and
// at daemon startup rehydration). Idempotent.
func (s *Set) bindWorktree(wt core.WorktreeID, repo core.RepositoryID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wtToR[wt] = repo
}

// get returns the catalog for repo, or ErrUnknownRepo.
func (s *Set) get(repo core.RepositoryID) (*sqlite.Catalog, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.cats[repo]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownRepo, repo)
	}
	return c, nil
}

// getByWT resolves wt->repo then returns that repo's catalog.
func (s *Set) getByWT(wt core.WorktreeID) (*sqlite.Catalog, error) {
	s.mu.RLock()
	repo, ok := s.wtToR[wt]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownWorktree, wt)
	}
	return s.get(repo)
}

// snapshot returns the current catalogs for fan-out reads without holding the
// lock during delegation.
func (s *Set) snapshot() []*sqlite.Catalog {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*sqlite.Catalog, 0, len(s.cats))
	for _, c := range s.cats {
		out = append(out, c)
	}
	return out
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/enginev2/catalogset/ -run 'TestAdd|TestGetByWT' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/catalogset/catalogset.go internal/enginev2/catalogset/catalogset_test.go
git commit -m "feat(catalogset): Set lifecycle, worktree->repo routing map, schema guard"
```

---

## Task 4: `catalogset.Set` — all 25 interface methods + interface assertions

**Files:**
- Create: `internal/enginev2/catalogset/methods.go`
- Test: `internal/enginev2/catalogset/catalogset_test.go` (append)

**Interfaces:**
- Consumes: `Set` helpers from Task 3 (`get`, `getByWT`, `bindWorktree`, `snapshot`), `internal/enginev2/core`, `internal/enginev2/scheduler`, `internal/enginev2/service`, `internal/enginev2/worker`, `internal/enginev2/reconcile`.
- Produces: `Set` satisfies `scheduler.Queue`, `service.Catalog`, `worker.Catalog`, `reconcile.CatalogReader`, enforced by four compile-time `var _` assertions. Plus `func (s *Set) SetActiveGeneration(ctx, repo, gen) error` (routed by repo; used by the Phase B fingerprint-roll).

- [ ] **Step 1: Write the failing test**

Append to `catalogset_test.go` (this drives isolation + routing + fan-out through two real catalogs):

```go
func TestRoutingIsolationAndFanout(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()

	for _, id := range []string{"/a", "/b"} {
		if err := s.Add(ctx, core.RepositoryID(id), filepath.Join(t.TempDir(), "c.db")); err != nil {
			t.Fatalf("Add %s: %v", id, err)
		}
		// Register repo + worktree + active generation (mirrors service.Register).
		if err := s.RegisterRepository(ctx, core.RepositoryID(id), id, ""); err != nil {
			t.Fatalf("RegisterRepository %s: %v", id, err)
		}
		if err := s.RegisterWorktree(ctx, core.WorktreeID(id), core.RepositoryID(id), id, 1); err != nil {
			t.Fatalf("RegisterWorktree %s: %v", id, err)
		}
		if err := s.EnsureActiveGeneration(ctx, core.RepositoryID(id), 1, "fp-"+id); err != nil {
			t.Fatalf("EnsureActiveGeneration %s: %v", id, err)
		}
	}

	// Routing: WorktreeInfo resolves through the wt->repo map to the right repo.
	if _, repo, err := s.WorktreeInfo(ctx, core.WorktreeID("/a")); err != nil || repo != "/a" {
		t.Fatalf("WorktreeInfo(/a) = %q, %v; want /a, nil", repo, err)
	}
	// Per-repo fingerprint stays isolated.
	fpB, err := s.GenerationFingerprint(ctx, core.RepositoryID("/b"), 1)
	if err != nil || fpB != "fp-/b" {
		t.Fatalf("GenerationFingerprint(/b) = %q, %v; want fp-/b", fpB, err)
	}
	// Unknown worktree errors, never falls back to another repo.
	if _, _, err := s.WorktreeInfo(ctx, core.WorktreeID("/zzz")); !errors.Is(err, ErrUnknownWorktree) {
		t.Fatalf("WorktreeInfo(unknown) = %v; want ErrUnknownWorktree", err)
	}

	// Fan-out: enqueue one job in /a and one in /b, expect both repos pending.
	job := func(id string) core.Job {
		return core.Job{WorktreeID: core.WorktreeID(id), Path: "x.go", DesiredHash: "h", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}
	}
	if err := s.UpsertJob(ctx, job("/a")); err != nil {
		t.Fatalf("UpsertJob(/a): %v", err)
	}
	if err := s.UpsertJob(ctx, job("/b")); err != nil {
		t.Fatalf("UpsertJob(/b): %v", err)
	}
	repos, err := s.RepositoriesWithPendingJobs(ctx)
	if err != nil {
		t.Fatalf("RepositoriesWithPendingJobs: %v", err)
	}
	if len(repos) != 2 {
		t.Fatalf("want 2 repos with pending jobs (fan-out), got %d: %v", len(repos), repos)
	}
}
```

> Verified against the tree 2026-07-19: `core.OpUpsert` (`Operation`) and `core.PriorityReconcile` (`Priority`) exist with these spellings; the reconcile priority is the natural one for reconciliation-produced jobs.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/catalogset/ -run TestRoutingIsolationAndFanout -v`
Expected: FAIL — `s.RegisterRepository` (and the other methods) undefined.

- [ ] **Step 3: Implement `methods.go`**

Create `internal/enginev2/catalogset/methods.go`. Implement **every** method in the routing table. The four `var _` assertions below will not compile until all are present — that is the completeness guarantee.

Representative implementations (one per category); write the rest identically, routing by the key in the table:

```go
package catalogset

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/reconcile"
	"github.com/yoanbernabeu/grepai/internal/enginev2/scheduler"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
	"github.com/yoanbernabeu/grepai/internal/enginev2/worker"
)

// Compile-time proof that Set satisfies every catalog-facing interface. If any
// method is missing or mis-typed, the package fails to build.
var (
	_ scheduler.Queue          = (*Set)(nil)
	_ service.Catalog          = (*Set)(nil)
	_ worker.Catalog           = (*Set)(nil)
	_ reconcile.CatalogReader  = (*Set)(nil)
)

// --- routed by explicit repo id ---

func (s *Set) ActiveGeneration(ctx context.Context, repo core.RepositoryID) (core.Generation, error) {
	c, err := s.get(repo)
	if err != nil {
		return 0, err
	}
	return c.ActiveGeneration(ctx, repo)
}

func (s *Set) RegisterRepository(ctx context.Context, repo core.RepositoryID, rootPath, gitCommonDir string) error {
	c, err := s.get(repo)
	if err != nil {
		return err
	}
	return c.RegisterRepository(ctx, repo, rootPath, gitCommonDir)
}

// RegisterWorktree routes by repo AND records the worktree->repo binding so
// later wt-keyed calls resolve.
func (s *Set) RegisterWorktree(ctx context.Context, wt core.WorktreeID, repo core.RepositoryID, rootPath string, regGen core.Generation) error {
	c, err := s.get(repo)
	if err != nil {
		return err
	}
	if err := c.RegisterWorktree(ctx, wt, repo, rootPath, regGen); err != nil {
		return err
	}
	s.bindWorktree(wt, repo)
	return nil
}

// SetActiveGeneration is beyond the strict interface union; the Phase B
// fingerprint-roll uses it. Routed by repo.
func (s *Set) SetActiveGeneration(ctx context.Context, repo core.RepositoryID, gen core.Generation) error {
	c, err := s.get(repo)
	if err != nil {
		return err
	}
	return c.SetActiveGeneration(ctx, repo, gen)
}

// --- routed by worktree id (through the wt->repo map) ---

func (s *Set) WorktreeInfo(ctx context.Context, wt core.WorktreeID) (string, core.RepositoryID, error) {
	c, err := s.getByWT(wt)
	if err != nil {
		return "", "", err
	}
	return c.WorktreeInfo(ctx, wt)
}

// --- routed by job.WorktreeID ---

func (s *Set) UpsertJob(ctx context.Context, job core.Job) error {
	c, err := s.getByWT(job.WorktreeID)
	if err != nil {
		return err
	}
	return c.UpsertJob(ctx, job)
}

// --- routed by ArtifactKey.RepositoryID ---

func (s *Set) GetArtifact(ctx context.Context, key core.ArtifactKey) (core.Artifact, bool, error) {
	c, err := s.get(key.RepositoryID)
	if err != nil {
		return core.Artifact{}, false, err
	}
	return c.GetArtifact(ctx, key)
}

// --- fan-out aggregates ---

func (s *Set) RepositoriesWithPendingJobs(ctx context.Context) ([]core.RepositoryID, error) {
	var out []core.RepositoryID
	seen := map[core.RepositoryID]bool{}
	for _, c := range s.snapshot() {
		repos, err := c.RepositoriesWithPendingJobs(ctx)
		if err != nil {
			return nil, err
		}
		for _, r := range repos {
			if !seen[r] {
				seen[r] = true
				out = append(out, r)
			}
		}
	}
	return out, nil
}

func (s *Set) QueueDepthByPriority(ctx context.Context) (map[core.Priority]int, error) {
	out := map[core.Priority]int{}
	for _, c := range s.snapshot() {
		m, err := c.QueueDepthByPriority(ctx)
		if err != nil {
			return nil, err
		}
		for p, n := range m {
			out[p] += n
		}
	}
	return out, nil
}

func (s *Set) DeadLetterCount(ctx context.Context) (int, error) {
	total := 0
	for _, c := range s.snapshot() {
		n, err := c.DeadLetterCount(ctx)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func (s *Set) RequeueClaimedJobs(ctx context.Context) (int, error) {
	total := 0
	for _, c := range s.snapshot() {
		n, err := c.RequeueClaimedJobs(ctx)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// ClaimNextJob (host-wide, no repo arg) exists only to satisfy worker.Catalog.
// The daemon's scheduler claims per-repo via ClaimNextJobInRepo, so this is
// never called in the daemon path; implemented as a first-non-empty fan-out.
func (s *Set) ClaimNextJob(ctx context.Context, minPriority core.Priority) (core.Job, bool, error) {
	for _, c := range s.snapshot() {
		job, ok, err := c.ClaimNextJob(ctx, minPriority)
		if err != nil {
			return core.Job{}, false, err
		}
		if ok {
			return job, true, nil
		}
	}
	return core.Job{}, false, nil
}
```

Now implement the remaining methods, each a one-line delegation via the table's key:

- **Route by repo** (`get(repo)`): `GenerationFingerprint`, `CreateGeneration`, `EnsureActiveGeneration`, `ClaimNextJobInRepo`, `PutChunkVector`.
- **Route by worktree** (`getByWT(wt)`): `SearchWorktree`, `WorktreePendingCount`, `WorktreePathsPending`, `CommitDelete`, `CurrentJob`, `WorktreeIndexedHashes`.
- **Route by `job.WorktreeID`** (`getByWT(job.WorktreeID)`): `DeadLetterJob`, `FailJobAttempt`, `CommitUpdate`.

Each follows the exact shape of its category's example above (get the catalog, return its error, else delegate with the same args). Copy the signatures verbatim from the Reference section at the top of this plan.

- [ ] **Step 4: Run tests + interface assertions**

Run: `go build ./internal/enginev2/catalogset/ && go test ./internal/enginev2/catalogset/ -run TestRoutingIsolationAndFanout -v`
Expected: builds (all four `var _` assertions satisfied) and PASS. If the build complains a method is missing/mis-typed, add/fix it — that is the assertion doing its job.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/catalogset/methods.go internal/enginev2/catalogset/catalogset_test.go
git commit -m "feat(catalogset): implement the four-interface union with routing + fan-out"
```

---

## Task 5: `BuilderRouter` — per-repo build dispatch

**Files:**
- Create: `internal/enginev2/catalogset/builder.go`
- Test: `internal/enginev2/catalogset/builder_test.go`

**Interfaces:**
- Consumes: `internal/enginev2/artifacts` (`BuildRequest`, `EndpointResult`), `internal/enginev2/core`.
- Produces:
  - `type BuilderRouter struct { … }` implementing `Build(ctx, artifacts.BuildRequest) (core.Artifact, artifacts.EndpointResult, error)` (i.e. satisfies `worker.Builder`).
  - `func NewBuilderRouter() *BuilderRouter`
  - `func (r *BuilderRouter) Add(repo core.RepositoryID, b RepoBuilder)`
  - `type RepoBuilder interface { Build(ctx, artifacts.BuildRequest) (core.Artifact, artifacts.EndpointResult, error) }` (locally declared so `*artifacts.DefaultBuilder` satisfies it and tests can fake it).

Rationale: `artifacts.ChunkCache.GetChunkVector(chunkID)` carries no repo id, so a single shared builder cannot route chunk-cache reads. Each repo gets its own `artifacts.DefaultBuilder` (holding that repo's catalog as its `ChunkCache`); the router dispatches by `req.Key.RepositoryID`.

- [ ] **Step 1: Write the failing test**

Create `internal/enginev2/catalogset/builder_test.go`:

```go
package catalogset

import (
	"context"
	"errors"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/artifacts"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

type fakeBuilder struct{ id string }

func (f fakeBuilder) Build(_ context.Context, _ artifacts.BuildRequest) (core.Artifact, artifacts.EndpointResult, error) {
	return core.Artifact{ID: core.ArtifactID(f.id)}, 0, nil
}

func TestBuilderRouterDispatchesByRepo(t *testing.T) {
	r := NewBuilderRouter()
	r.Add(core.RepositoryID("/a"), fakeBuilder{id: "A"})
	r.Add(core.RepositoryID("/b"), fakeBuilder{id: "B"})

	art, _, err := r.Build(context.Background(), artifacts.BuildRequest{Key: core.ArtifactKey{RepositoryID: "/b"}})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if art.ID != "B" {
		t.Fatalf("routed to wrong builder: got artifact %q, want B", art.ID)
	}
	if _, _, err := r.Build(context.Background(), artifacts.BuildRequest{Key: core.ArtifactKey{RepositoryID: "/x"}}); !errors.Is(err, ErrUnknownRepo) {
		t.Fatalf("unknown repo build should be ErrUnknownRepo, got %v", err)
	}
}
```

> Verified against the tree 2026-07-19: `core.Artifact.ID` is `core.ArtifactID` (a string type), so `core.Artifact{ID: core.ArtifactID(f.id)}` and comparing `art.ID != "B"` both typecheck.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/catalogset/ -run TestBuilderRouterDispatchesByRepo -v`
Expected: FAIL — `NewBuilderRouter`/`BuilderRouter` undefined.

- [ ] **Step 3: Implement the router**

Create `internal/enginev2/catalogset/builder.go`:

```go
package catalogset

import (
	"context"
	"fmt"
	"sync"

	"github.com/yoanbernabeu/grepai/internal/enginev2/artifacts"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// RepoBuilder builds one artifact for a single repository (satisfied by
// *artifacts.DefaultBuilder). Declared locally so the router does not import
// worker and so tests can substitute a fake.
type RepoBuilder interface {
	Build(ctx context.Context, req artifacts.BuildRequest) (core.Artifact, artifacts.EndpointResult, error)
}

// BuilderRouter dispatches a build to the per-repo builder named by
// req.Key.RepositoryID. It satisfies worker.Builder. Needed because chunk-cache
// reads (ChunkCache.GetChunkVector) carry no repo id and so cannot route through
// a single shared cache.
type BuilderRouter struct {
	mu       sync.RWMutex
	builders map[core.RepositoryID]RepoBuilder
}

// NewBuilderRouter returns an empty router.
func NewBuilderRouter() *BuilderRouter {
	return &BuilderRouter{builders: make(map[core.RepositoryID]RepoBuilder)}
}

// Add registers repo's builder.
func (r *BuilderRouter) Add(repo core.RepositoryID, b RepoBuilder) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.builders[repo] = b
}

// Build routes by req.Key.RepositoryID. An unregistered repo errors (ErrUnknownRepo).
func (r *BuilderRouter) Build(ctx context.Context, req artifacts.BuildRequest) (core.Artifact, artifacts.EndpointResult, error) {
	r.mu.RLock()
	b, ok := r.builders[req.Key.RepositoryID]
	r.mu.RUnlock()
	if !ok {
		return core.Artifact{}, 0, fmt.Errorf("%w: %q", ErrUnknownRepo, req.Key.RepositoryID)
	}
	return b.Build(ctx, req)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/enginev2/catalogset/ -run TestBuilderRouterDispatchesByRepo -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/catalogset/builder.go internal/enginev2/catalogset/builder_test.go
git commit -m "feat(catalogset): BuilderRouter dispatches builds by repository id"
```

---

## Gate 0 (run before Phase A handoff)

- [ ] `gofmt -l internal/enginev2/catalogset internal/enginev2/registry internal/enginev2/catalog/sqlite` → empty output.
- [ ] `go vet ./internal/enginev2/...` → clean.
- [ ] `make build` → succeeds.
- [ ] `make lint` → clean (add `//nolint` with a reason only where an existing pattern in the tree already does, e.g. gosec file-path reads).
- [ ] `go test ./... -race` → all pass.
- [ ] Independent review: hand the Phase 0 diff to `codex-bg` (independent-review rule for git projects) and address findings before starting Phase A.

## Self-Review notes (author)

- **Spec coverage:** §4.4 catalogset (Tasks 3–4), builder router (Task 5), registry (Task 2), §4.6 schema accessor+guard (Tasks 1, 3). §4.2/§4.3/§4.5/§5 are Phases B/C — out of Phase 0 by design.
- **Completeness of the 25 methods** is enforced by the four `var _` interface assertions in Task 4 — a missing method is a build failure, not a silent gap.
- **All literals verified** against the tree 2026-07-19: `core.OpUpsert`, `core.PriorityReconcile`, `core.Artifact.ID`/`core.ArtifactID`. No unverified names remain in the task code.
