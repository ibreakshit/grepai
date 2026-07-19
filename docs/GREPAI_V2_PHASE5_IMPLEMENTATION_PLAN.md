# GrepAI v2 — Phase 5 Implementation Plan (Worktree-aware query service, TIGHT scope)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Implement the in-process `service.Service` (worktree-scoped search, freshness/status, wait-fresh, register/reconcile/dead-letters, and a rebuild handle) so Gate 5 passes: concurrent agents see only their own worktree's file versions, a query path never initiates indexing, and the active generation stays queryable while a rebuild generation is being built.

**Architecture:** A concrete `service.Server` implements the Phase 0 `service.Service` interface against the Phase 1 catalog, the Phase 2 reconciler, and a v2 `embedder.Embedder`. **Search** embeds the query once and ranks the worktree's active-view chunks by cosine similarity — the candidate set is exactly the chunks reachable from that worktree's `worktree_files` view (`worktree_files → artifact_chunks → chunks`), so isolation and generation-scoping fall out of the join: a different worktree's versions and a not-yet-referenced building generation's artifacts simply are not in the set. **Status/WaitFresh** read job state (a path is fresh when it has no pending `index_jobs` row). **Reconcile** runs the reconciler and durably enqueues its plan. Query paths (`Search`, `Status`, `WaitFresh`, `Trace`) only read + embed the query — they never enqueue index jobs or reconcile — which is invariant 3 (MCP is read/query oriented).

**Tech Stack:** Go 1.24.2, the Phase 1 `catalog/sqlite`, the Phase 2 `reconcile`, the Phase 3 `embedder` port, `crypto`/`math` for cosine, `enginetest.FakeEmbedder`.

## Global Constraints

- Go 1.24.2 floor; go.mod `go` directive stays `go 1.24.2`; `modernc.org/sqlite` stays `v1.45.0`.
- CGO_ENABLED=0 must stay buildable. **No new module dependency** (stdlib + already-vendored only).
- Module `github.com/yoanbernabeu/grepai`. New/changed code under `internal/enginev2/service/`, small additive reads under `internal/enginev2/catalog/sqlite/`, and one small type in `internal/enginev2/core/`.
- `go test -race ./...` passes; `gofmt`-clean; `make lint` (golangci-lint v1.64.2) green (`// #nosec GXXX - reason` house style; `_test.go` excluded from gosec/errcheck).
- Conventional commits (scope `service`, `catalog`, or `core`). Never push to `main`.
- **Worktree isolation (invariant 4):** a search from one worktree can never return a file version that exists only in another worktree — the candidate chunk set is drawn solely from that worktree's `worktree_files` rows.
- **MCP is read/query oriented (invariant 3):** `Search`/`Status`/`WaitFresh`/`Trace` issue zero index jobs and never call the reconciler. Only `Register`/`Reconcile`/`Rebuild` mutate.
- **Search availability (invariant 12):** the active generation's committed view stays searchable; a building generation's artifacts (present in `file_artifacts`/`chunks` but referenced by no active view row) never appear in results.

## Scope / Non-goals (this phase)

- **In:** the in-process `service.Server` implementing `service.Service` — worktree-scoped `Search` (query embed → cosine over the worktree's active-view chunks → top-k paths), `Status` (active generation + freshness), `WaitFresh` (block until the named paths have no pending job, or the context deadline), `Register`, `Reconcile` (reconciler plan → durable enqueue), `DeadLetters` (list), and a minimal `Rebuild` (create/cancel a building generation handle); the three catalog query/freshness reads; `core.SearchHit`; Gate 5 tests.
- **Out (deferred):** the `grepaid` process, Unix-socket JSON-RPC framing/dispatch, and systemd packaging (Phase 5b / paired with this Service); CLI and MCP client wiring (`cli/`, `mcp/` thin clients); **`Trace` and symbol/`symbol_edges` population** (needs artifact-scoped symbol extraction — `Trace` returns an empty result with a documented "not populated in this generation" note); the fsnotify watcher; RPG refresh; chunk **content/line-range** metadata in results (the v2 `chunks` table stores only vectors — results are file-scoped `{path, score}`; previews/line ranges are a later schema addition); and — importantly — the **full controlled-rebuild build→validate→activate flow with generation-scoped views** (see note below). `Rebuild` here only creates/cancels the building-generation record; it does not build, switch, or activate.
- **Deferred-rebuild design note (why the full flow is out):** correct "old generation stays queryable, new generation activates atomically" needs the worktree view to be **generation-scoped** (`worktree_files` keyed by `(worktree, path, generation)`), so a rebuild stages gen N+1 view rows without clobbering the active gen N rows, and activation flips a per-repo active pointer. That is a foundational schema/`commitUpdateTx` change touching Phases 1/3 and belongs with the Phase 6 migration/cutover work. Phase 5's Gate-5 crit 3 is satisfied by the weaker, sufficient property that a building generation's artifacts are referenced by no active view row and therefore never surface in search.

## Consumed surfaces (do not modify their existing behavior)

- Phase 0 `service.Service` interface and its request/response structs (`internal/enginev2/service/service.go`). This phase **implements** the interface and **extends** the response structs additively (new fields only).
- Phase 1 `catalog/sqlite.Catalog`: `ActiveGeneration`, `ResolveView`, `RegisterRepository`, `RegisterWorktree`, `CreateGeneration`, `SetActiveGeneration`, `UpsertJob`, `WorktreeInfo`, `DeadLetterCount`; internal `decodeVector`, `withWriteTx`, `db`. Adds reads in a new `query.go`.
- Phase 2 `reconcile.New(cat reconcile.CatalogReader) *reconcile.Engine` and `(*Engine).Reconcile(ctx, wt) (reconcile.Plan, error)`; `reconcile.Plan{Jobs []core.Job}`. `*sqlite.Catalog` already satisfies `reconcile.CatalogReader`.
- Phase 3 `embedder.Embedder` (`Embed`, `EmbedBatch`, `Dimensions`, `Close`).
- `core.RepositoryID`/`WorktreeID`/`Generation`/`Job`; `enginetest.FakeEmbedder`.
- Existing schema tables `worktree_files`, `file_artifacts`, `artifact_chunks`, `chunks`, `index_jobs`, `dead_letter_jobs`, `index_generations` (unchanged this phase).

---

## File Structure

```
internal/enginev2/core/
  search.go              # SearchHit{Path, Score}
  search_test.go
internal/enginev2/catalog/sqlite/
  query.go               # SearchWorktree, WorktreePendingCount, WorktreePathsPending
  query_test.go
internal/enginev2/service/
  service.go             # (modify) extend response structs additively (Search/Status results)
  server.go              # Server: New + all Service methods (in-process impl)
  server_test.go         # unit tests
  gate5_test.go          # Gate 5 integration: isolation, no-indexing on query, old-gen-queryable
```

---

## Chunk A — Catalog query & freshness reads (Tasks 1–2)

### Task 1: `core.SearchHit`

**Files:** Create `internal/enginev2/core/search.go`, `search_test.go`.

**Interfaces — Produces:** `type SearchHit struct { Path string; Score float32 }` — one ranked file result (best-matching chunk's score for that path).

- [ ] **Step 1: failing test**

```go
// core/search_test.go
package core

import "testing"

func TestSearchHitFields(t *testing.T) {
	h := SearchHit{Path: "a.go", Score: 0.9}
	if h.Path != "a.go" || h.Score != 0.9 {
		t.Fatalf("unexpected: %+v", h)
	}
}
```

- [ ] **Step 2:** `GOTOOLCHAIN=local go test ./internal/enginev2/core/ -run SearchHit` → FAIL. **Step 3:** implement:

```go
// core/search.go
package core

// SearchHit is one ranked file result from a worktree-scoped search: the file's
// relative path and the similarity score of its best-matching chunk.
type SearchHit struct {
	Path  string
	Score float32
}
```

- [ ] **Step 4:** `go test ./internal/enginev2/core/ -race` → PASS. **Step 5:** commit `feat(core): SearchHit result type`.

---

### Task 2: Worktree-scoped search + freshness catalog reads

**Files:** Create `internal/enginev2/catalog/sqlite/query.go`, `query_test.go`.

**Interfaces — Produces (on `*Catalog`):**
- `func (c *Catalog) SearchWorktree(ctx, wt core.WorktreeID, query []float32, limit int) ([]core.SearchHit, error)` — cosine-rank the chunks reachable from `wt`'s `worktree_files` view; return the top `limit` **distinct file paths** by their best chunk score, descending. A stored vector whose length differs from `len(query)` is skipped (incompatible fingerprint/dimension). `limit <= 0` returns all, ranked.
- `func (c *Catalog) WorktreePendingCount(ctx, wt core.WorktreeID) (int, error)` — count of `index_jobs` rows for the worktree (any claimed state).
- `func (c *Catalog) WorktreePathsPending(ctx, wt core.WorktreeID, paths []string) (bool, error)` — true if any of `paths` has an `index_jobs` row for the worktree (empty `paths` ⇒ false).

**SearchWorktree query & ranking:** join `worktree_files wf` (for `wt`) → `artifact_chunks ac` (via `wf.artifact_id`) → `chunks ch` (via `ac.chunk_id`), selecting `wf.relative_path, ch.dimensions, ch.vector`. Decode each vector (`decodeVector`), skip if `len != len(query)`, compute cosine, keep the **max score per path**, then sort by score desc and take `limit`.

- [ ] **Step 1: failing test** (uses `enginetest.FakeEmbedder` to produce comparable vectors)

```go
// query_test.go
package sqlite

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
)

func TestSearchWorktreeIsolationAndRanking(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	emb := enginetest.NewFakeEmbedder(4)
	seedRepoWorktree(t, c, "r", "w1")
	seedRepoWorktree(t, c, "r", "w2")
	seedGeneration(t, c, "r", 1, "fp")

	// Helper: build+commit a one-chunk artifact for (wt, path, content) at gen 1.
	put := func(wt core.WorktreeID, path, content string) {
		vec, _ := emb.Embed(ctx, content)
		key := core.ArtifactKey{RepositoryID: "r", RelativePath: path, SourceHash: path + content, Fingerprint: "fp"}
		chID := core.ChunkID("fp", content)
		must(t, c.PutChunkVector(ctx, chID, "r", "fp", vec))
		art := core.Artifact{ID: key.ArtifactID(), Key: key, Dimensions: 4, Chunks: []core.ArtifactChunk{{Ordinal: 0, ChunkID: chID, Vector: vec}}}
		req := core.CommitRequest{View: core.ViewEntry{WorktreeID: wt, Path: path, ArtifactID: key.ArtifactID(), Generation: 1}, Artifact: art, Chunks: art.Chunks}
		must(t, c.CommitUpdate(ctx, req, core.Job{WorktreeID: wt, Path: path, DesiredHash: path + content, Generation: 1, Operation: core.OpUpsert}))
	}
	put("w1", "a.go", "alpha")
	put("w2", "secret.go", "beta") // only in w2

	// A query embedded like "alpha" ranks a.go first in w1.
	q, _ := emb.Embed(ctx, "alpha")
	hits, err := c.SearchWorktree(ctx, "w1", q, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Path != "a.go" {
		t.Fatalf("w1 search wrong: %+v", hits)
	}
	// w1 must NEVER see w2's file (isolation).
	for _, h := range hits {
		if h.Path == "secret.go" {
			t.Fatal("worktree isolation violated: w1 saw w2's file")
		}
	}
	// w2 sees only its own file.
	h2, _ := c.SearchWorktree(ctx, "w2", q, 10)
	if len(h2) != 1 || h2[0].Path != "secret.go" {
		t.Fatalf("w2 search wrong: %+v", h2)
	}
}

func TestWorktreeFreshnessReads(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "r", "w")
	seedGeneration(t, c, "r", 1, "fp")
	if n, _ := c.WorktreePendingCount(ctx, "w"); n != 0 {
		t.Fatalf("empty worktree should have 0 pending, got %d", n)
	}
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	if n, _ := c.WorktreePendingCount(ctx, "w"); n != 1 {
		t.Fatalf("expected 1 pending, got %d", n)
	}
	pend, _ := c.WorktreePathsPending(ctx, "w", []string{"a.go", "b.go"})
	if !pend {
		t.Fatal("a.go is pending")
	}
	none, _ := c.WorktreePathsPending(ctx, "w", []string{"b.go"})
	if none {
		t.Fatal("b.go is not pending")
	}
}
```

- [ ] **Step 2:** run → FAIL. **Step 3: implement `query.go`**:

```go
// query.go
package sqlite

import (
	"context"
	"math"
	"sort"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// SearchWorktree ranks the chunks reachable from a worktree's current view by
// cosine similarity to query, returning the top `limit` distinct file paths by
// their best chunk score. The candidate set is scoped to this worktree's view,
// so results never include another worktree's file versions (invariant 4) nor a
// building generation's not-yet-referenced artifacts (invariant 12).
func (c *Catalog) SearchWorktree(ctx context.Context, wt core.WorktreeID, query []float32, limit int) ([]core.SearchHit, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT wf.relative_path, ch.dimensions, ch.vector
		FROM worktree_files wf
		JOIN artifact_chunks ac ON ac.artifact_id = wf.artifact_id
		JOIN chunks ch ON ch.chunk_id = ac.chunk_id
		WHERE wf.worktree_id=?`, string(wt))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	best := map[string]float32{}
	for rows.Next() {
		var path string
		var dims int
		var blob []byte
		if err := rows.Scan(&path, &dims, &blob); err != nil {
			return nil, err
		}
		if dims != len(query) {
			continue // incompatible fingerprint/dimension
		}
		vec, err := decodeVector(blob, dims)
		if err != nil {
			return nil, err
		}
		s := cosine(query, vec)
		if cur, ok := best[path]; !ok || s > cur {
			best[path] = s
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	hits := make([]core.SearchHit, 0, len(best))
	for p, s := range best {
		hits = append(hits, core.SearchHit{Path: p, Score: s})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].Path < hits[j].Path // stable tie-break
	})
	if limit > 0 && len(hits) > limit {
		hits = hits[:limit]
	}
	return hits, nil
}

func cosine(a, b []float32) float32 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return float32(dot / (math.Sqrt(na) * math.Sqrt(nb)))
}

// WorktreePendingCount returns the number of active index jobs for a worktree.
func (c *Catalog) WorktreePendingCount(ctx context.Context, wt core.WorktreeID) (int, error) {
	var n int
	err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM index_jobs WHERE worktree_id=?`, string(wt)).Scan(&n)
	return n, err
}

// WorktreePathsPending reports whether any of paths has a pending job.
func (c *Catalog) WorktreePathsPending(ctx context.Context, wt core.WorktreeID, paths []string) (bool, error) {
	for _, p := range paths {
		var n int
		if err := c.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM index_jobs WHERE worktree_id=? AND relative_path=?`,
			string(wt), p).Scan(&n); err != nil {
			return false, err
		}
		if n > 0 {
			return true, nil
		}
	}
	return false, nil
}
```

- [ ] **Step 4:** `go test ./internal/enginev2/catalog/sqlite/ -race && make lint` → PASS/clean. **Step 5:** commit `feat(catalog): worktree-scoped search and freshness reads`.

---

## Chunk B — The service.Server (Task 3)

### Task 3: `service.Server` implementing `service.Service`

**Files:** Modify `internal/enginev2/service/service.go` (extend response structs additively); create `internal/enginev2/service/server.go`, `server_test.go`.

**Additive response fields (service.go):**
- `SearchResponse`: add `Results []core.SearchHit`, `ActiveGeneration core.Generation`, `Fresh bool`.
- `StatusResponse` (already has `ActiveGeneration`): add `Pending int`, `Fresh bool`, `DeadLetters int`.
- `RebuildResponse` (already has `Generation`): no change.
- `TraceResponse`: add `Symbols []string` (empty this phase).

**Interfaces — Produces (server.go):**
- Narrow dependency interfaces so the Server is unit-testable and does not force a concrete catalog:
  ```go
  type Catalog interface {
      RegisterRepository(ctx context.Context, repo core.RepositoryID, rootPath, gitCommonDir string) error
      RegisterWorktree(ctx context.Context, wt core.WorktreeID, repo core.RepositoryID, rootPath string, regGen core.Generation) error
      WorktreeInfo(ctx context.Context, wt core.WorktreeID) (string, core.RepositoryID, error)
      ActiveGeneration(ctx context.Context, repo core.RepositoryID) (core.Generation, error)
      CreateGeneration(ctx context.Context, repo core.RepositoryID, gen core.Generation, fingerprint string) error
      SearchWorktree(ctx context.Context, wt core.WorktreeID, query []float32, limit int) ([]core.SearchHit, error)
      WorktreePendingCount(ctx context.Context, wt core.WorktreeID) (int, error)
      WorktreePathsPending(ctx context.Context, wt core.WorktreeID, paths []string) (bool, error)
      DeadLetterCount(ctx context.Context) (int, error)
      UpsertJob(ctx context.Context, job core.Job) error
  }
  type Reconciler interface {
      Reconcile(ctx context.Context, wt core.WorktreeID) (reconcile.Plan, error)
  }
  ```
  (`*sqlite.Catalog` satisfies `Catalog`; `*reconcile.Engine` satisfies `Reconciler`. `DeadLetterCount` is host-wide in Phase 1 — the DeadLetters listing is coarse this phase; a per-worktree dead-letter read is a later refinement, noted.)
- `func New(cat Catalog, rec Reconciler, emb embedder.Embedder, searchLimit int) *Server`
- `var _ Service = (*Server)(nil)`

**Method behavior:**
- `Search(req)`: `q, err := s.emb.Embed(ctx, req.Query)`; `hits, err := s.cat.SearchWorktree(ctx, req.WorktreeID, q, s.limit)`; resolve repo via `WorktreeInfo` → `ActiveGeneration`; `pending, _ := WorktreePendingCount`; return `SearchResponse{WorktreeID, Results: hits, ActiveGeneration: gen, Fresh: pending == 0}`. **Issues no jobs, no reconcile.**
- `Status(req)`: repo via `WorktreeInfo` → `ActiveGeneration`; `pending`; `dl, _ := DeadLetterCount`; `StatusResponse{ActiveGeneration, Pending: pending, Fresh: pending == 0, DeadLetters: dl}`.
- `WaitFresh(req)`: loop — if `!WorktreePathsPending(ctx, wt, req.Paths)` return `{Fresh: true}`; else wait on `ctx.Done()` or a short poll ticker; on `ctx.Done()` return `{Fresh: false}, nil` (deadline reached, not an error). Empty `Paths` ⇒ immediately fresh.
- `Register(req)`: derive ids from `req.Root` (this phase: `RepositoryID(req.Root)`, `WorktreeID(req.Root)` — a single-worktree registration; multi-worktree identity derivation via git-common-dir is a later refinement, noted), `RegisterRepository` then `RegisterWorktree` (ignore already-registered), return the ids.
- `Reconcile(req)`: `plan, err := s.rec.Reconcile(ctx, req.WorktreeID)`; for each `job` in `plan.Jobs`, `s.cat.UpsertJob(ctx, job)`; return `{JobsQueued: len(plan.Jobs)}`. **This is an admin path — enqueue is expected.**
- `Rebuild(req)`: if `req.Cancel` → no-op success (cancellation of the un-built handle); else compute `next := activeGen+1`, `CreateGeneration(repo, next, <fingerprint TBD>)`, return `{Generation: next}`. *(The fingerprint for a rebuild generation is the daemon's current indexing fingerprint; in this in-process phase the Server is constructed without one, so Rebuild records the generation with the active generation's fingerprint carried forward — a placeholder until the Phase 6 rebuild flow supplies a real new fingerprint. Building/validating/activating the generation is out of scope, per the deferred-rebuild note.)*
- `Trace(req)`: return `TraceResponse{WorktreeID: req.WorktreeID, Symbols: nil}` — symbol extraction is deferred; document that Trace is inert this phase.
- `DeadLetters(req)`: return `{Paths: nil}` with the count available via Status; a path-level per-worktree dead-letter listing is deferred (Phase 1 exposes only a host-wide count). Document it.

- [ ] **Step 1: failing tests** (`server_test.go`) — representative:

```go
// server_test.go
package service_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog/sqlite"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
	"github.com/yoanbernabeu/grepai/internal/enginev2/reconcile"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
)

func newServer(t *testing.T) (*sqlite.Catalog, *enginetest.FakeEmbedder, *service.Server) {
	t.Helper()
	c, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	emb := enginetest.NewFakeEmbedder(4)
	return c, emb, service.New(c, reconcile.New(c), emb, 10)
}

func TestSearchReturnsWorktreeResults(t *testing.T) {
	ctx := context.Background()
	c, emb, s := newServer(t)
	// seed r/w/gen1 and one committed artifact for "alpha" (see query_test helpers).
	seedWorktreeArtifact(t, c, emb, "r", "w", "a.go", "alpha")
	resp, err := s.Search(ctx, service.SearchRequest{WorktreeID: "w", Query: "alpha"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) != 1 || resp.Results[0].Path != "a.go" {
		t.Fatalf("search results wrong: %+v", resp.Results)
	}
	if resp.ActiveGeneration != 1 || !resp.Fresh {
		t.Fatalf("freshness/gen wrong: %+v", resp)
	}
}
```

(`seedWorktreeArtifact` mirrors `query_test.go`'s `put` helper — factor a shared test helper in `server_test.go`.)

- [ ] **Step 2:** run → FAIL. **Step 3:** implement `server.go` per the behavior above; extend `service.go` structs. **Step 4:** `go test ./internal/enginev2/service/ -race && go vet ./... && make lint` → PASS/clean. **Step 5:** commit `feat(service): in-process worktree-aware query server`.

---

## Chunk C — Gate 5 (Task 4)

### Task 4: Gate 5 integration tests

**Files:** Create `internal/enginev2/service/gate5_test.go` (package `service_test`, real `sqlite.Catalog` + `reconcile.Engine` + `FakeEmbedder`).

**The three Gate 5 criteria, each an explicit test:**

**(a) Concurrent agents see only their worktree's file versions.** Register `w1` and `w2` in the same repo; commit `a.go` (content "v1") into `w1` and `a.go` (content "v2") into `w2` — same path, different versions (different `SourceHash`/artifact/chunk). `Search(w1, "v1")` returns `w1`'s `a.go` and its result must rank `w1`'s vector (assert by committing a distinguishing extra file only in `w2` and confirming it never appears in `w1`'s results, plus that each worktree's `a.go` resolves to its own artifact via `ResolveView`).

**(b) A query path makes no indexing calls.** After seeding, snapshot `WorktreePendingCount` (0). Call `Search`, `Status`, `WaitFresh` (with fresh paths), and `Trace`. Assert `WorktreePendingCount` is still 0 (no jobs enqueued) and — using a **counting embedder wrapper** — that only `Search` embedded (one `Embed` call, the query) and none of the calls triggered `EmbedBatch` (no indexing). Also assert `Reconcile` is the only method that increases the pending count.

**(c) The active generation stays queryable during a rebuild.** Commit `a.go` at gen 1 into `w` and confirm `Search` returns it. Call `Rebuild` (creates gen 2, building) and **stage** a gen-2 artifact + chunk directly via `PutChunkVector`/`PutArtifact` (simulating rebuild progress) for a *different* content — WITHOUT committing a gen-2 view. Assert `Search(w, ...)` still returns exactly the gen-1 `a.go` result (the staged gen-2 artifact, referenced by no active view row, never appears), and `Status.ActiveGeneration` is still 1.

- [ ] **Step 1:** write the three tests (+ a `countingEmbedder` wrapping `FakeEmbedder` to count `Embed`/`EmbedBatch`). **Step 2:** `GOTOOLCHAIN=local go test ./internal/enginev2/service/ -race -run Gate5 -v` → PASS. **Step 3:** full gate — `go build ./... && CGO_ENABLED=0 go build ./... && go vet ./... && go test -race ./... && gofmt -l internal/enginev2 && make lint`. **Step 4:** commit `test(service): Gate 5 — worktree isolation, query issues no indexing, old-gen queryable`.

---

## Gate 5 Exit Criteria (spec §9, Phase 5)

1. **Worktree isolation** — `TestGate5_WorktreeIsolation`: a search from one worktree never returns another worktree's file version.
2. **Query issues no indexing** — `TestGate5_QueryMakesNoIndexingCalls`: `Search`/`Status`/`WaitFresh`/`Trace` enqueue zero jobs and trigger no `EmbedBatch`; only `Reconcile` enqueues.
3. **Old generation queryable during rebuild** — `TestGate5_ActiveGenerationQueryableDuringRebuild`: a building generation's staged artifacts never surface in search; the active generation keeps serving.
4. **No data races** — `go test -race ./...` clean.
5. **Build discipline** — `go build ./...`, `CGO_ENABLED=0 go build ./...`, `go vet ./...`, `gofmt -l internal/enginev2` empty, `make lint` 0; go.mod unchanged; no new module dependency.

## Self-Review Notes

- **Spec coverage:** explicit worktree view selection (`SearchRequest.WorktreeID` → view-scoped join) ✓; active-generation filtering (results drawn only from committed active-view rows; `ActiveGeneration` reported) ✓; freshness metadata + wait-fresh (`Status`/`Search.Fresh`, `WaitFresh`) ✓; MCP query paths never index (query methods are read+embed-only) ✓. **Deferred (documented in Scope/Non-goals):** CLI administration wiring, `Trace`/symbols, the daemon process + Unix-RPC + systemd, chunk content/line metadata, and the full controlled-rebuild build→validate→activate with generation-scoped views.
- **Isolation is structural, not filtered-after-the-fact:** `SearchWorktree`'s candidate set is the join from `worktree_files` for exactly one worktree, so another worktree's versions and a building generation's unreferenced artifacts are never in scope — the strongest form of invariant 4/12 available without loading everything and filtering.
- **Query-path purity (invariant 3):** verify in review that `Search`/`Status`/`WaitFresh`/`Trace` contain no `UpsertJob`/`Reconcile`/`CreateGeneration`/`CommitUpdate` call — only reads and `emb.Embed`. Gate 5 crit 2 asserts it behaviorally.
- **Type consistency:** `Catalog`/`Reconciler` interfaces in `server.go` match `*sqlite.Catalog` / `*reconcile.Engine` method sets; `core.SearchHit` is the single result type shared by `SearchWorktree` and `SearchResponse.Results`.
- **Known deferrals flagged (not placeholders):** `Rebuild`'s fingerprint-carry-forward and the coarse host-wide `DeadLetters`/`Register` identity are deliberately minimal and documented; they become real with the Phase 6 rebuild/migration flow and the daemon's fingerprint/registry wiring. `WaitFresh` uses a real-time poll (not the scheduler Clock) — it is an I/O wait, and tests exercise the fresh-immediately and deadline-reached paths deterministically via the context deadline.
