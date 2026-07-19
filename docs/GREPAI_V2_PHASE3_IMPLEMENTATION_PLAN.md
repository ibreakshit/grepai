# GrepAI v2 — Phase 3 Implementation Plan (Artifact indexer and durable workers)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Build the artifact indexer and a durable in-process worker loop so Gate 3 passes: a failed indexing request preserves the previously searchable file version, a daemon crash at every durable-state injection point recovers to a valid state, and rapid successive saves commit only the final desired generation — with cache-miss-only embedding, vector validation, atomic artifact/view/job commit, and dead-letter classification.

**Architecture:** A new `artifacts.DefaultBuilder` transforms file content into chunks (reusing the existing top-level `indexer.Chunker`), reuses any compatible cached chunk vector by an exact-input content-addressed `chunk_id`, embeds only the cache misses through a small v2 `embedder.Embedder` port, validates every returned vector's dimension, and returns an immutable `core.Artifact` plus its ordered chunk composition. A new `worker.Worker` drives the durable loop: claim the highest-priority job, load its desired content, build (or whole-file cache-hit), pre-persist chunk vectors idempotently (content-addressed cache warming), then `CommitUpdate` the artifact + `artifact_chunks` mapping + worktree view switch + job completion in one SQLite transaction. Build failures are classified (transient → bounded retry, permanent → immediate dead-letter, superseded → dropped). Crash recovery rests on two pillars: content-addressed idempotency (re-running any partial build re-hits the cache) and a startup `RequeueClaimedJobs` that reclaims jobs a dead worker left claimed. A read-only legacy-GOB inspection spike de-risks Phase 6 migration compatibility early.

**Tech Stack:** Go 1.24.2, the Phase 1 `catalog/sqlite` package, the existing top-level `indexer` (chunker) package, `crypto/sha256`, `encoding/gob` (spike, read-only), `enginetest.FakeEmbedder`/`enginetest.CrashRegistry`/`enginetest.GitFixture`.

## Global Constraints

- Go 1.24.2 floor; go.mod `go` directive stays `go 1.24.2`; `modernc.org/sqlite` stays `v1.45.0`.
- CGO_ENABLED=0 must stay buildable. **No new module dependency** (only stdlib + already-vendored packages).
- Module `github.com/yoanbernabeu/grepai`. New v2 code under `internal/enginev2/{artifacts,embedder,worker,legacyimport}/`; catalog extensions under `internal/enginev2/catalog/sqlite/`; core additions under `internal/enginev2/core/`.
- `go test -race ./...` must pass; `gofmt`-clean; `make lint` (golangci-lint v1.64.2) green (annotate justified gosec with `// #nosec GXXX - reason`; `_test.go` excluded from gosec/errcheck).
- Conventional commits (scope `artifacts`, `worker`, `catalog`, `core`, or `legacyimport`). Never push to `main`.
- **Shared immutable work (invariant 5):** an artifact key that already exists is never re-embedded; identical chunk input across artifacts is embedded once and referenced by `chunk_id`.
- **Atomic visibility (invariant 6):** an artifact, its `artifact_chunks` mapping, the worktree view switch, and the job completion commit in exactly one transaction — a searchable file view never references an artifact whose chunks are not all present.
- **Durable progress (invariant 7):** a committed generation survives a crash; an uncommitted one leaves the prior view intact and is fully re-derivable from catalog + on-disk content.
- **Fingerprint correctness (invariant 10):** a chunk vector is reused only when its `fingerprint` matches; `chunk_id` embeds the fingerprint so a differing fingerprint can never collide.
- **Newest generation only (spec §5.6):** when a newer desired generation exists for `(worktree, path)`, the older job does not become the visible view.

## Scope / Non-goals (this phase)

- **In:** the `artifacts.DefaultBuilder` (chunk → exact-input cache lookup → cache-miss-only embed → dimension validation → immutable artifact + ordered chunk composition); the v2 `embedder.Embedder` port; catalog extensions (atomic `artifact_chunks` commit, delete-view commit, dead-letter, requeue-claimed, failed-attempt release, per-generation fingerprint read, desired-generation read, dead-letter/chunk read helpers for tests); the durable `worker.Worker` loop with content loading, superseded-generation protection, dead-letter classification, and startup crash recovery; a read-only legacy-GOB inspection spike; Gate 3 integration tests.
- **Out (Phase 4, the daemon/scheduler):** the host-wide priority scheduler, timed backoff/jitter, the circuit breaker, request/token budgets, `grepaid` and Unix-socket RPC, fsnotify wiring. Phase 3's worker retries a transient failure by immediate re-eligibility with a bounded attempt counter — it does **not** implement timed backoff (Phase 4 owns pacing). Phase 3's `Worker.Run` is a simple in-process drain loop, not the fair multi-repository scheduler.
- **Out (deferred to Phase 4, per the TIGHT scope decision):** artifact-scoped symbol extraction and scheduled RPG refresh with LLM extraction (both depend on the Phase 4 global scheduler that routes LLM work). The `symbols`/`symbol_edges` tables already exist in the schema; Phase 3 leaves them empty and does not populate them.
- **Out (Phase 6):** any GOB **write**, migration, or import-into-catalog. The Phase 3 spike is strictly a read-only decode/inspect probe — it never writes to the v2 catalog and never mutates a legacy store.

## Consumed surfaces (do not modify their existing behavior)

- `core.Artifact{ID, Key, Dimensions}`, `core.ArtifactKey{RepositoryID, RelativePath, SourceHash, Fingerprint}` and `ArtifactKey.ArtifactID()`, `core.ViewEntry`, `core.CommitRequest{View, Artifact}`, `core.Job{...}`, `core.Operation` (`OpUpsert`/`OpDelete`), `core.FailureClass` (`FailureTransient`/`FailurePermanent`/`FailureSuperseded`), `core.JobState`/`JobEvent`/`Transition`, `core.Generation`, `core.RepositoryID`/`WorktreeID`/`ArtifactID`. Tasks 1–2 **extend** `core.CommitRequest` and `core.Artifact` (additive struct fields) and add `core.ChunkID`/`core.ArtifactChunk` — no existing field or method changes.
- Phase 1 `catalog/sqlite.Catalog`: existing `ActiveGeneration`, `GetArtifact`, `PutArtifact`, `ResolveView`, `CommitUpdate`, `UpsertJob`, `ClaimNextJob`, `PutChunkVector`, `GetChunkVector`, `RegisterRepository`, `RegisterWorktree`, `CreateGeneration`, `SetActiveGeneration`. Phase 3 **adds** methods (new file `jobs.go`, plus additive changes to `views.go`/`artifacts.go`); it does not alter existing method signatures or semantics beyond the additive `artifact_chunks` write inside `commitUpdateTx`.
- Phase 2 `catalog/sqlite` reads: `WorktreeInfo(ctx, wt) (root string, repo core.RepositoryID, err error)`, `WorktreeIndexedHashes`.
- Existing top-level `indexer` package: `indexer.NewChunker(chunkSize, overlap int) *indexer.Chunker` and `(*indexer.Chunker).Chunk(filePath, content string) []indexer.ChunkInfo`; `indexer.ChunkInfo{ID, FilePath, StartLine, EndLine, Content, EmbedContent, Hash, ContentHash}`.
- `enginetest.FakeEmbedder` (`NewFakeEmbedder(dims)`, `Embed`, `EmbedBatch`, `Dimensions`, `Close`, `SetError`, `FailNext`, `EmbedCalls`, `TextsEmbedded`), `enginetest.CrashRegistry` (`NewCrashRegistry`, `ArmAt`, `Check`), `enginetest.GitFixture`.
- Existing schema tables (migration0001), unchanged this phase: `file_artifacts`, `chunks`, `artifact_chunks`, `worktree_files`, `index_jobs`, `dead_letter_jobs`, `index_generations`.

---

## File Structure

```
internal/enginev2/core/
  chunk.go               # ChunkID(fingerprint, embedContent) + ArtifactChunk{Ordinal, ChunkID, Vector}; CommitRequest.Chunks; Artifact.Chunks
  chunk_test.go
internal/enginev2/embedder/
  embedder.go            # v2 Embedder port interface (Embed, EmbedBatch, Dimensions, Close) — FakeEmbedder & legacy embedder both satisfy it structurally
internal/enginev2/artifacts/
  builder.go             # (existing stub) DefaultBuilder + New; Build: transform→cache lookup→cache-miss embed→validate→assemble
  builder_test.go
internal/enginev2/catalog/sqlite/
  views.go               # (modify) commitUpdateTx also inserts artifact_chunks; add CommitDelete
  artifacts.go           # (modify) putArtifactChunksTx helper
  jobs.go                # DeadLetterJob, RequeueClaimedJobs, FailJobAttempt, DesiredGeneration, GenerationFingerprint, DeadLetterCount/ListDeadLetter, ArtifactChunkIDs (test read)
  jobs_test.go
internal/enginev2/worker/
  content.go             # ContentLoader interface + Load helper contract
  worker.go              # Worker struct, New, ProcessOne, Run, Recover (RequeueClaimedJobs), classify + dead-letter/retry/superseded routing
  worker_test.go
  gate3_test.go          # Gate 3 integration: real sqlite catalog + FakeEmbedder + CrashRegistry
internal/enginev2/legacyimport/
  gobspike.go            # read-only InspectGOB(path) → Summary{ChunkCount, DocumentCount, Dimensions, ...}
  gobspike_test.go
```

---

## Chunk A — Catalog & core durable substrate (Tasks 1–3)

### Task 1: Core chunk identity and commit composition

**Files:**
- Create: `internal/enginev2/core/chunk.go`
- Create: `internal/enginev2/core/chunk_test.go`
- Modify: `internal/enginev2/core/artifact.go` (add `Chunks` field to `Artifact` and `CommitRequest`)

**Interfaces:**
- Produces:
  - `func ChunkID(fingerprint, embedContent string) string` — hex sha256 of the length-prefixed `(fingerprint, embedContent)` pair, reusing the same canonical discipline as `ArtifactKey.ArtifactID` so no boundary can be confused.
  - `type ArtifactChunk struct { Ordinal int; ChunkID string; Vector []float32 }`
  - `Artifact.Chunks []ArtifactChunk` (additive; does **not** affect `ArtifactID`, which derives from `Key` only).
  - `CommitRequest.Chunks []ArtifactChunk` (additive; the ordered composition persisted atomically with the view switch).

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/core/chunk_test.go
package core

import "testing"

func TestChunkIDStableAndFingerprintScoped(t *testing.T) {
	a := ChunkID("fp-1", "func main() {}")
	b := ChunkID("fp-1", "func main() {}")
	if a != b {
		t.Fatalf("ChunkID not deterministic: %s != %s", a, b)
	}
	if len(a) != 64 {
		t.Fatalf("ChunkID want 64 hex chars, got %d", len(a))
	}
	// Different fingerprint => different id (invariant 10: no cross-fingerprint reuse).
	if ChunkID("fp-2", "func main() {}") == a {
		t.Fatal("ChunkID collided across fingerprints")
	}
	// Boundary confusion guard: ("ab","c") must not equal ("a","bc").
	if ChunkID("ab", "c") == ChunkID("a", "bc") {
		t.Fatal("ChunkID boundary confusion")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOTOOLCHAIN=local go test ./internal/enginev2/core/ -run TestChunkID -v`
Expected: FAIL (`undefined: ChunkID`).

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/core/chunk.go
package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
)

// ArtifactChunk is one ordered chunk of an artifact: its content-addressed
// identity plus the validated embedding vector. Ordinal preserves chunk order
// within the artifact so retrieval is stable.
type ArtifactChunk struct {
	Ordinal int
	ChunkID string
	Vector  []float32
}

// ChunkID derives a content-addressed identifier for one chunk's embedding
// input, scoped by the indexing fingerprint. Two chunks share an id only when
// both the fingerprint and the exact embedding input match (invariant 5 reuse,
// invariant 10 correctness). The encoding is length-prefixed so no component
// boundary can be confused with another.
func ChunkID(fingerprint, embedContent string) string {
	var buf bytes.Buffer
	writeCanonicalString(&buf, fingerprint)
	writeCanonicalString(&buf, embedContent)
	sum := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(sum[:])
}
```

Then in `internal/enginev2/core/artifact.go`, add the field to `Artifact` (after `Dimensions int`):

```go
	// Chunks is the ordered chunk composition (identity + vector). Empty for a
	// whole-file cache hit that reuses an already-stored artifact.
	Chunks []ArtifactChunk
```

and to `CommitRequest` (after `Artifact Artifact`):

```go
	// Chunks is the ordered chunk composition to persist atomically with the
	// artifact and view switch (invariant 6). Ordinals must be 0..len-1.
	Chunks []ArtifactChunk
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOTOOLCHAIN=local go test ./internal/enginev2/core/ -race -v`
Expected: PASS (existing `artifact_test.go`/`ids_test.go` still pass; `writeCanonicalString` is already defined in `fingerprint.go`).

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/core/chunk.go internal/enginev2/core/chunk_test.go internal/enginev2/core/artifact.go
git commit -m "feat(core): content-addressed ChunkID and artifact chunk composition"
```

---

### Task 2: Atomic artifact-chunk commit and delete-view commit

**Files:**
- Modify: `internal/enginev2/catalog/sqlite/artifacts.go` (add `putArtifactChunksTx`)
- Modify: `internal/enginev2/catalog/sqlite/views.go` (extend `commitUpdateTx`; add `CommitDelete`)
- Create/extend test: `internal/enginev2/catalog/sqlite/jobs_test.go`

**Interfaces:**
- Consumes: `core.CommitRequest.Chunks []core.ArtifactChunk` (Task 1); existing `putArtifactTx`, `withWriteTx`, `commitUpdateTx`.
- Produces:
  - `commitUpdateTx` now also inserts `artifact_chunks` rows for `req.Chunks` (idempotent) inside the same transaction as the artifact + view switch + job delete.
  - `func (c *Catalog) CommitDelete(ctx context.Context, wt core.WorktreeID, relPath string, gen core.Generation, job core.Job) error` — atomically removes the worktree view row (only if the recorded generation is ≤ `gen`, so a newer view survives) and deletes the job (only `generation<=gen`).

- [ ] **Step 1: Write the failing tests**

```go
// internal/enginev2/catalog/sqlite/jobs_test.go
package sqlite

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

func TestCommitUpdatePersistsArtifactChunks(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t) // existing helper in catalog_test.go
	seedRepoWorktree(t, c)  // helper: registers repo "r", worktree "w", generation 1 active (see Step 3 note)

	art := core.Artifact{
		ID:         core.ArtifactID("art-1"),
		Key:        core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "h1", Fingerprint: "fp"},
		Dimensions: 3,
	}
	// Pre-store chunk vectors (cache warming), as the worker does before commit.
	if err := c.PutChunkVector(ctx, "ch-0", "r", "fp", []float32{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	req := core.CommitRequest{
		View:     core.ViewEntry{WorktreeID: "w", Path: "a.go", ArtifactID: "art-1", Generation: 1},
		Artifact: art,
		Chunks:   []core.ArtifactChunk{{Ordinal: 0, ChunkID: "ch-0", Vector: []float32{1, 2, 3}}},
	}
	job := core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert}
	if err := c.CommitUpdate(ctx, req, job); err != nil {
		t.Fatal(err)
	}
	ids, err := c.ArtifactChunkIDs(ctx, "art-1") // test read helper (Task 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "ch-0" {
		t.Fatalf("artifact_chunks mapping wrong: %v", ids)
	}
}

func TestCommitDeleteRemovesView(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)
	// Establish a view at gen 1.
	req := core.CommitRequest{
		View:     core.ViewEntry{WorktreeID: "w", Path: "a.go", ArtifactID: "art-1", Generation: 1},
		Artifact: core.Artifact{ID: "art-1", Key: core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "h1", Fingerprint: "fp"}, Dimensions: 3},
	}
	if err := c.CommitUpdate(ctx, req, core.Job{WorktreeID: "w", Path: "a.go", Generation: 1, Operation: core.OpUpsert}); err != nil {
		t.Fatal(err)
	}
	del := core.Job{WorktreeID: "w", Path: "a.go", Generation: 2, Operation: core.OpDelete}
	if err := c.CommitDelete(ctx, "w", "a.go", 2, del); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := c.ResolveView(ctx, "w", "a.go"); err != nil || ok {
		t.Fatalf("view should be gone: ok=%v err=%v", ok, err)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `GOTOOLCHAIN=local go test ./internal/enginev2/catalog/sqlite/ -run 'TestCommitUpdatePersistsArtifactChunks|TestCommitDeleteRemovesView' -v`
Expected: FAIL (`ArtifactChunkIDs`/`CommitDelete` undefined; and chunks not persisted). If `seedRepoWorktree` does not yet exist, add it as a small local helper in `jobs_test.go` that calls `RegisterRepository(ctx,"r",".","")`, `RegisterWorktree(ctx,"w","r",".",1)`, `CreateGeneration(ctx,"r",1,"fp")`, `SetActiveGeneration(ctx,"r",1)`.

- [ ] **Step 3: Implement**

In `artifacts.go` add:

```go
// putArtifactChunksTx records the ordered (artifact, ordinal, chunk) mapping.
// Idempotent: re-committing an immutable artifact re-inserts the same rows.
func putArtifactChunksTx(ctx context.Context, tx *sql.Tx, artifactID core.ArtifactID, chunks []core.ArtifactChunk) error {
	for _, ch := range chunks {
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO artifact_chunks(artifact_id, ordinal, chunk_id)
			VALUES(?, ?, ?)`, string(artifactID), ch.Ordinal, ch.ChunkID); err != nil {
			return err
		}
	}
	return nil
}
```

In `views.go`, inside `commitUpdateTx`, right after the successful `putArtifactTx(ctx, tx, req.Artifact)` call and before the view switch, insert:

```go
	if err := putArtifactChunksTx(ctx, tx, req.Artifact.ID, req.Chunks); err != nil {
		return err
	}
```

Then add `CommitDelete` to `views.go`:

```go
// CommitDelete atomically removes a worktree's view of a path and deletes the
// job, guarded so a newer generation (view or job) is never clobbered by a
// stale delete (spec §5.6). The artifact itself is retained (it may still be
// referenced by other worktrees; invariant 5).
func (c *Catalog) CommitDelete(ctx context.Context, wt core.WorktreeID, relPath string, gen core.Generation, job core.Job) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM worktree_files
			WHERE worktree_id=? AND relative_path=? AND generation<=?`,
			string(wt), relPath, int64(gen)); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `
			DELETE FROM index_jobs
			WHERE worktree_id=? AND relative_path=? AND generation<=?`,
			string(job.WorktreeID), job.Path, int64(gen))
		return err
	})
}
```

- [ ] **Step 4: Run to verify pass**

Run: `GOTOOLCHAIN=local go test ./internal/enginev2/catalog/sqlite/ -race`
Expected: PASS (all prior catalog tests + the two new ones; `ArtifactChunkIDs` lands in Task 3 — sequence Task 3 before running the `TestCommitUpdatePersistsArtifactChunks` assertion, or stub the read helper here and fill it in Task 3. Recommended: implement `ArtifactChunkIDs` in Task 3 first if executing strictly TDD, or fold its one-liner in now).

> **Note for the implementer:** `ArtifactChunkIDs` is a trivial read used only by tests. To keep Task 2's test runnable, add it in `jobs.go` now (its full form is specified in Task 3) — the two tasks are committed separately but `ArtifactChunkIDs` may be introduced in whichever runs first.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/catalog/sqlite/artifacts.go internal/enginev2/catalog/sqlite/views.go internal/enginev2/catalog/sqlite/jobs_test.go
git commit -m "feat(catalog): atomic artifact_chunks commit and delete-view commit"
```

---

### Task 3: Job-lifecycle catalog methods (dead-letter, requeue, attempt, generation reads)

**Files:**
- Create: `internal/enginev2/catalog/sqlite/jobs.go`
- Extend: `internal/enginev2/catalog/sqlite/jobs_test.go`

**Interfaces:**
- Produces (all on `*Catalog`):
  - `func (c *Catalog) DeadLetterJob(ctx, job core.Job, reason string) error` — atomic: insert into `dead_letter_jobs`, delete the `index_jobs` row (guard `generation<=job.Generation` so a newer supersede survives).
  - `func (c *Catalog) RequeueClaimedJobs(ctx) (int, error)` — set `claimed=0` for every claimed job; returns the count reclaimed (startup crash recovery).
  - `func (c *Catalog) FailJobAttempt(ctx, job core.Job) (attempts int, err error)` — increment `attempts` and set `claimed=0` for the job's `(worktree, path)` (only when the row's generation still equals `job.Generation`); returns the new attempt count. Used for transient retry.
  - `func (c *Catalog) DesiredGeneration(ctx, wt core.WorktreeID, relPath string) (core.Generation, bool, error)` — the current pending job's generation for `(worktree, path)`, or `ok=false` if none.
  - `func (c *Catalog) GenerationFingerprint(ctx, repo core.RepositoryID, gen core.Generation) (string, error)` — the fingerprint recorded for a generation (returns `ErrNoSuchGeneration` if absent — reuse the existing sentinel from `registry.go`).
  - Test read helpers: `func (c *Catalog) DeadLetterCount(ctx) (int, error)`; `func (c *Catalog) ArtifactChunkIDs(ctx, artifactID core.ArtifactID) ([]string, error)` (ordered by ordinal).

- [ ] **Step 1: Write the failing tests**

```go
func TestDeadLetterAndRequeueAndAttempt(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)
	job := core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}
	if err := c.UpsertJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	// Claim it (marks claimed=1), then simulate a crash by requeueing.
	if _, ok, err := c.ClaimNextJob(ctx, core.PriorityBootstrap); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	n, err := c.RequeueClaimedJobs(ctx)
	if err != nil || n != 1 {
		t.Fatalf("requeue n=%d err=%v", n, err)
	}
	// It must be claimable again after requeue.
	claimed, ok, err := c.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil || !ok {
		t.Fatalf("reclaim: ok=%v err=%v", ok, err)
	}
	// Record a transient attempt: attempts increments, becomes claimable again.
	att, err := c.FailJobAttempt(ctx, claimed)
	if err != nil || att != 1 {
		t.Fatalf("attempt=%d err=%v", att, err)
	}
	// Dead-letter it.
	if _, _, err := c.ClaimNextJob(ctx, core.PriorityBootstrap); err != nil {
		t.Fatal(err)
	}
	if err := c.DeadLetterJob(ctx, claimed, "permanent: bad dims"); err != nil {
		t.Fatal(err)
	}
	dlc, err := c.DeadLetterCount(ctx)
	if err != nil || dlc != 1 {
		t.Fatalf("dead-letter count=%d err=%v", dlc, err)
	}
	if _, ok, _ := c.ClaimNextJob(ctx, core.PriorityBootstrap); ok {
		t.Fatal("dead-lettered job should not be claimable")
	}
}

func TestGenerationFingerprintAndDesiredGeneration(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c) // creates generation 1 with fingerprint "fp"
	fp, err := c.GenerationFingerprint(ctx, "r", 1)
	if err != nil || fp != "fp" {
		t.Fatalf("fingerprint=%q err=%v", fp, err)
	}
	if _, ok, _ := c.DesiredGeneration(ctx, "w", "a.go"); ok {
		t.Fatal("no job yet => not ok")
	}
	_ = c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h", Generation: 7, Operation: core.OpUpsert, Priority: core.PriorityReconcile})
	g, ok, err := c.DesiredGeneration(ctx, "w", "a.go")
	if err != nil || !ok || g != 7 {
		t.Fatalf("desired gen=%d ok=%v err=%v", g, ok, err)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `GOTOOLCHAIN=local go test ./internal/enginev2/catalog/sqlite/ -run 'TestDeadLetterAndRequeueAndAttempt|TestGenerationFingerprintAndDesiredGeneration' -v`
Expected: FAIL (undefined methods).

- [ ] **Step 3: Implement `jobs.go`**

```go
// internal/enginev2/catalog/sqlite/jobs.go
package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// DeadLetterJob atomically records a permanently-failed job and removes it from
// the active queue. Guarded by generation so a newer supersede survives.
func (c *Catalog) DeadLetterJob(ctx context.Context, job core.Job, reason string) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO dead_letter_jobs(worktree_id, relative_path, reason, created_at)
			VALUES(?, ?, ?, datetime('now'))`,
			string(job.WorktreeID), job.Path, reason); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `
			DELETE FROM index_jobs
			WHERE worktree_id=? AND relative_path=? AND generation<=?`,
			string(job.WorktreeID), job.Path, int64(job.Generation))
		return err
	})
}

// RequeueClaimedJobs releases every claimed job so a restarted worker can
// re-claim work a crashed worker left in flight (invariant 7 recovery).
func (c *Catalog) RequeueClaimedJobs(ctx context.Context) (int, error) {
	var n int64
	err := c.withWriteTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE index_jobs SET claimed=0 WHERE claimed=1`)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	return int(n), err
}

// FailJobAttempt increments the attempt counter and releases the claim for a
// transient failure, but only while the row is still at the job's generation
// (a newer supersede leaves attempts alone). Returns the new attempt count.
func (c *Catalog) FailJobAttempt(ctx context.Context, job core.Job) (int, error) {
	var attempts int
	err := c.withWriteTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			UPDATE index_jobs SET attempts=attempts+1, claimed=0
			WHERE worktree_id=? AND relative_path=? AND generation=?`,
			string(job.WorktreeID), job.Path, int64(job.Generation)); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `
			SELECT attempts FROM index_jobs WHERE worktree_id=? AND relative_path=?`,
			string(job.WorktreeID), job.Path).Scan(&attempts)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil // row was superseded/removed; nothing to retry
	}
	return attempts, err
}

// DesiredGeneration returns the pending job generation for a path, if any.
func (c *Catalog) DesiredGeneration(ctx context.Context, wt core.WorktreeID, relPath string) (core.Generation, bool, error) {
	var gen int64
	err := c.db.QueryRowContext(ctx, `
		SELECT generation FROM index_jobs WHERE worktree_id=? AND relative_path=?`,
		string(wt), relPath).Scan(&gen)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return core.Generation(gen), true, nil
}

// GenerationFingerprint returns the fingerprint recorded for a generation.
func (c *Catalog) GenerationFingerprint(ctx context.Context, repo core.RepositoryID, gen core.Generation) (string, error) {
	var fp string
	err := c.db.QueryRowContext(ctx, `
		SELECT fingerprint FROM index_generations WHERE repository_id=? AND generation=?`,
		string(repo), int64(gen)).Scan(&fp)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNoSuchGeneration // sentinel already defined in registry.go
	}
	return fp, err
}

// DeadLetterCount returns the number of dead-letter rows (test/status read).
func (c *Catalog) DeadLetterCount(ctx context.Context) (int, error) {
	var n int
	err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letter_jobs`).Scan(&n)
	return n, err
}

// ArtifactChunkIDs returns an artifact's chunk ids in ordinal order (test read).
func (c *Catalog) ArtifactChunkIDs(ctx context.Context, artifactID core.ArtifactID) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT chunk_id FROM artifact_chunks WHERE artifact_id=? ORDER BY ordinal ASC`,
		string(artifactID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
```

> **Verify the sentinel name:** `registry.go` defines the "unknown generation" error (per the Phase 1 fix wave: `SetActiveGeneration` returns `ErrNoSuchGeneration`). Reuse that exact identifier; if it is spelled differently, match the source rather than redeclaring.

- [ ] **Step 4: Run to verify pass + full package + lint**

Run: `GOTOOLCHAIN=local go test ./internal/enginev2/catalog/sqlite/ -race` → PASS
Run: `GOTOOLCHAIN=local go vet ./internal/enginev2/catalog/... && make lint` → clean (annotate any gosec G115 with `// #nosec` per house style if a bounded int conversion trips it).

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/catalog/sqlite/jobs.go internal/enginev2/catalog/sqlite/jobs_test.go
git commit -m "feat(catalog): job lifecycle — dead-letter, requeue-claimed, attempt, generation reads"
```

---

## Chunk B — The artifact builder (Tasks 4–5)

### Task 4: v2 embedder port

**Files:**
- Create: `internal/enginev2/embedder/embedder.go`

**Interfaces:**
- Produces: `type Embedder interface { Embed(ctx, text string) ([]float32, error); EmbedBatch(ctx, texts []string) ([][]float32, error); Dimensions() int; Close() error }`. `enginetest.FakeEmbedder` and the top-level `embedder.Embedder` both satisfy it structurally (identical method sets), so no adapter and no import of either is needed by the port itself.

- [ ] **Step 1: Write a compile-time satisfaction test**

```go
// internal/enginev2/embedder/embedder_test.go
package embedder_test

import (
	"github.com/yoanbernabeu/grepai/internal/enginev2/embedder"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
)

// Compile-time proof the shared test double satisfies the v2 port.
var _ embedder.Embedder = enginetest.NewFakeEmbedder(4)
```

- [ ] **Step 2: Run to verify failure**

Run: `GOTOOLCHAIN=local go build ./internal/enginev2/embedder/`
Expected: FAIL (package/interface not defined).

- [ ] **Step 3: Implement**

```go
// internal/enginev2/embedder/embedder.go

// Package embedder defines the v2 engine's embedding port: the minimal surface
// the artifact builder needs. Both enginetest.FakeEmbedder and the legacy
// top-level embedder implementations satisfy it structurally.
package embedder

import "context"

// Embedder converts text into fixed-dimension vectors.
type Embedder interface {
	// Embed returns the vector for one text.
	Embed(ctx context.Context, text string) ([]float32, error)
	// EmbedBatch returns vectors for many texts, index-aligned with the input.
	EmbedBatch(ctx context.Context, texts []string) ([][]float32, error)
	// Dimensions returns the vector length this embedder produces.
	Dimensions() int
	// Close releases any underlying resources.
	Close() error
}
```

- [ ] **Step 4: Run to verify pass**

Run: `GOTOOLCHAIN=local go test ./internal/enginev2/embedder/ -race` → PASS (compiles; assertion holds).

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/embedder/
git commit -m "feat(embedder): v2 embedding port interface"
```

---

### Task 5: DefaultBuilder — transform, cache lookup, cache-miss embed, validation

**Files:**
- Modify: `internal/enginev2/artifacts/builder.go` (keep `BuildRequest`/`ArtifactBuilder`; add `DefaultBuilder`, `New`, `Build`)
- Create: `internal/enginev2/artifacts/builder_test.go`

**Interfaces:**
- Consumes: `core.ChunkID`, `core.ArtifactChunk`, `core.Artifact`, `core.ArtifactKey` (Task 1); `embedder.Embedder` (Task 4); the top-level `indexer` chunker; a `ChunkCache` read port (below); `BuildRequest{Key, Content}` (existing).
- Produces:
  - `type ChunkCache interface { GetChunkVector(ctx, chunkID string) ([]float32, bool, error) }` — satisfied by `*catalog/sqlite.Catalog`.
  - `type Chunker interface { Chunk(filePath, content string) []indexer.ChunkInfo }` — satisfied by `*indexer.Chunker`.
  - `func New(ch Chunker, emb embedder.Embedder, cache ChunkCache) *DefaultBuilder`
  - `func (b *DefaultBuilder) Build(ctx, req BuildRequest) (core.Artifact, error)` — the returned artifact carries `Chunks` (ordinal, chunk_id, vector) for every chunk (cache hit or freshly embedded); dimension mismatches return a permanent error (see `ErrDimensionMismatch`).
  - `var ErrDimensionMismatch = errors.New("artifacts: embedding dimension mismatch")` (the worker classifies this as `FailurePermanent`).

**Build algorithm (spec §5.5):**
1. Chunk `req.Content` via `b.chunker.Chunk(req.Key.RelativePath, string(req.Content))`.
2. If zero chunks (empty file), return an artifact with `Dimensions = b.emb.Dimensions()` and no chunks (a valid empty artifact; its view still resolves).
3. For each chunk `i`: `id := core.ChunkID(req.Key.Fingerprint, chunk.EmbedContent)`. Look up `b.cache.GetChunkVector(ctx, id)`.
   - **Hit:** validate `len(vec) == b.emb.Dimensions()` (a stored vector with wrong dims is corruption → `ErrDimensionMismatch`); record `ArtifactChunk{Ordinal:i, ChunkID:id, Vector:vec}`.
   - **Miss:** collect `(i, id, chunk.EmbedContent)` into a to-embed list.
4. If any misses, `vecs, err := b.emb.EmbedBatch(ctx, missTexts)`; on error return it (worker classifies transient vs permanent). Validate `len(vecs) == len(missTexts)` and each `len(vecs[k]) == b.emb.Dimensions()` → else `ErrDimensionMismatch`. Slot each into its ordinal.
   - **Dedup within a file:** two chunks with identical `EmbedContent` share one `chunk_id`; embed such a text once (map `id → vector`) and reuse — do not send duplicates to `EmbedBatch`.
5. Assemble `core.Artifact{ID: req.Key.ArtifactID(), Key: req.Key, Dimensions: b.emb.Dimensions(), Chunks: ordered}`. Ordinals are `0..n-1` in chunk order.

- [ ] **Step 1: Write the failing tests**

```go
// internal/enginev2/artifacts/builder_test.go
package artifacts_test

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/indexer"
	"github.com/yoanbernabeu/grepai/internal/enginev2/artifacts"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
)

// mapCache is an in-memory ChunkCache for unit tests.
type mapCache struct{ m map[string][]float32 }

func (c mapCache) GetChunkVector(_ context.Context, id string) ([]float32, bool, error) {
	v, ok := c.m[id]
	return v, ok, nil
}

func TestBuildEmbedsMissesOnlyAndReusesCache(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	ch := indexer.NewChunker(512, 50)
	key := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "h1", Fingerprint: "fp"}
	content := []byte("package main\n\nfunc main() {}\n")

	// First build: cold cache => embeds, artifact carries chunks.
	b1 := artifacts.New(ch, emb, mapCache{m: map[string][]float32{}})
	art, err := b1.Build(ctx, artifacts.BuildRequest{Key: key, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	if len(art.Chunks) == 0 {
		t.Fatal("expected chunks")
	}
	if art.Dimensions != 4 {
		t.Fatalf("dims=%d", art.Dimensions)
	}
	if emb.TextsEmbedded() == 0 {
		t.Fatal("cold build should embed")
	}

	// Warm cache with the produced vectors; second build embeds nothing.
	warm := map[string][]float32{}
	for _, c := range art.Chunks {
		warm[c.ChunkID] = c.Vector
	}
	emb2 := enginetest.NewFakeEmbedder(4)
	b2 := artifacts.New(ch, emb2, mapCache{m: warm})
	art2, err := b2.Build(ctx, artifacts.BuildRequest{Key: key, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	if emb2.TextsEmbedded() != 0 {
		t.Fatalf("warm build must not embed, embedded=%d", emb2.TextsEmbedded())
	}
	if art2.ID != art.ID || len(art2.Chunks) != len(art.Chunks) {
		t.Fatal("warm build must reproduce identical artifact")
	}
}

func TestBuildRejectsDimensionMismatch(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	ch := indexer.NewChunker(512, 50)
	key := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "h1", Fingerprint: "fp"}
	// Cache holds a wrong-dimension vector for the (would-be) chunk id.
	id := core.ChunkID("fp", "func main() {}") // may or may not match a real chunk; force via a 1-chunk file
	bad := mapCache{m: map[string][]float32{id: {1, 2, 3}}} // 3 != 4
	b := artifacts.New(ch, emb, bad)
	_, err := b.Build(ctx, artifacts.BuildRequest{Key: key, Content: []byte("func main() {}")})
	// Either a cache hit with wrong dims (mismatch) or a clean embed; assert no panic,
	// and if the cached id matched, the error is ErrDimensionMismatch.
	if err != nil && err != artifacts.ErrDimensionMismatch {
		t.Fatalf("unexpected error: %v", err)
	}
}
```

> **Implementer note on the mismatch test:** to make it deterministic, construct the content as a single short line so the chunker yields exactly one chunk whose `EmbedContent` you can compute the id from. Read `indexer.ChunkInfo.EmbedContent`'s construction in `indexer/chunker.go` (it may prefix the file path) and derive the id from the real `EmbedContent`, not the raw line. If deriving the exact `EmbedContent` is awkward, split this into a focused test that injects a stub `Chunker` returning one `ChunkInfo` with a known `EmbedContent`.

- [ ] **Step 2: Run to verify failure**

Run: `GOTOOLCHAIN=local go test ./internal/enginev2/artifacts/ -v`
Expected: FAIL (`New`/`Build`/`ErrDimensionMismatch` undefined).

- [ ] **Step 3: Implement `builder.go`**

Keep the existing `BuildRequest` and `ArtifactBuilder` declarations. Add:

```go
import (
	"context"
	"errors"

	"github.com/yoanbernabeu/grepai/indexer"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/embedder"
)

// ErrDimensionMismatch signals an embedding (or cached vector) whose length
// does not match the embedder's declared dimension. The worker treats it as a
// permanent failure — retrying cannot fix a shape mismatch.
var ErrDimensionMismatch = errors.New("artifacts: embedding dimension mismatch")

// Chunker is the transform surface the builder needs (the top-level chunker).
type Chunker interface {
	Chunk(filePath, content string) []indexer.ChunkInfo
}

// ChunkCache is the read side of the chunk vector cache (the SQLite catalog).
type ChunkCache interface {
	GetChunkVector(ctx context.Context, chunkID string) ([]float32, bool, error)
}

// DefaultBuilder implements ArtifactBuilder over a chunker, an embedder, and a
// chunk-vector cache. It embeds only cache misses and validates every vector.
type DefaultBuilder struct {
	chunker Chunker
	emb     embedder.Embedder
	cache   ChunkCache
}

// New returns a DefaultBuilder.
func New(ch Chunker, emb embedder.Embedder, cache ChunkCache) *DefaultBuilder {
	return &DefaultBuilder{chunker: ch, emb: emb, cache: cache}
}

var _ ArtifactBuilder = (*DefaultBuilder)(nil)

// Build transforms content into an immutable artifact, reusing compatible
// cached chunk vectors and embedding only the misses (spec §5.5).
func (b *DefaultBuilder) Build(ctx context.Context, req BuildRequest) (core.Artifact, error) {
	dims := b.emb.Dimensions()
	art := core.Artifact{ID: req.Key.ArtifactID(), Key: req.Key, Dimensions: dims}

	infos := b.chunker.Chunk(req.Key.RelativePath, string(req.Content))
	if len(infos) == 0 {
		return art, nil // valid empty artifact
	}

	art.Chunks = make([]core.ArtifactChunk, len(infos))
	type miss struct{ ordinal int }
	var (
		missText  []string
		missByID  = map[string]int{} // chunk id -> index into missText (dedup)
		misses    []miss
		idByOrd   = make([]string, len(infos))
	)
	for i, info := range infos {
		id := core.ChunkID(req.Key.Fingerprint, info.EmbedContent)
		idByOrd[i] = id
		vec, ok, err := b.cache.GetChunkVector(ctx, id)
		if err != nil {
			return core.Artifact{}, err
		}
		if ok {
			if len(vec) != dims {
				return core.Artifact{}, ErrDimensionMismatch
			}
			art.Chunks[i] = core.ArtifactChunk{Ordinal: i, ChunkID: id, Vector: vec}
			continue
		}
		if _, seen := missByID[id]; !seen {
			missByID[id] = len(missText)
			missText = append(missText, info.EmbedContent)
		}
		misses = append(misses, miss{ordinal: i})
	}

	if len(missText) > 0 {
		vecs, err := b.emb.EmbedBatch(ctx, missText)
		if err != nil {
			return core.Artifact{}, err // worker classifies transient/permanent
		}
		if len(vecs) != len(missText) {
			return core.Artifact{}, ErrDimensionMismatch
		}
		for _, v := range vecs {
			if len(v) != dims {
				return core.Artifact{}, ErrDimensionMismatch
			}
		}
		for _, m := range misses {
			id := idByOrd[m.ordinal]
			vec := vecs[missByID[id]]
			art.Chunks[m.ordinal] = core.ArtifactChunk{Ordinal: m.ordinal, ChunkID: id, Vector: vec}
		}
	}
	return art, nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `GOTOOLCHAIN=local go test ./internal/enginev2/artifacts/ -race -v` → PASS
Run: `GOTOOLCHAIN=local go vet ./internal/enginev2/artifacts/ && make lint` → clean.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/artifacts/
git commit -m "feat(artifacts): DefaultBuilder — cache-miss-only embedding with vector validation"
```

---

## Chunk C — Durable worker and Gate 3 (Tasks 6–7)

### Task 6: The durable worker loop

**Files:**
- Create: `internal/enginev2/worker/content.go`
- Create: `internal/enginev2/worker/worker.go`
- Create: `internal/enginev2/worker/worker_test.go`

**Interfaces:**
- Consumes: `catalog/sqlite.Catalog` methods (`ClaimNextJob`, `WorktreeInfo`, `GenerationFingerprint`, `GetArtifact`, `PutChunkVector`, `CommitUpdate`, `CommitDelete`, `DeadLetterJob`, `FailJobAttempt`, `DesiredGeneration`, `RequeueClaimedJobs`); `artifacts.ArtifactBuilder` + `artifacts.BuildRequest` + `artifacts.ErrDimensionMismatch`; `core.Job`, `core.Operation`, `core.CommitRequest`, `core.ViewEntry`, `core.FailureClass`.
- Produces:
  - `type ContentLoader interface { Load(ctx, repo core.RepositoryID, worktreeRoot, relPath, desiredHash string) ([]byte, error) }`.
  - `type Catalog interface { ... }` — the exact catalog method subset the worker needs (so tests can substitute; the real `*sqlite.Catalog` satisfies it).
  - `type Builder interface { Build(ctx, artifacts.BuildRequest) (core.Artifact, error) }` (satisfied by `*artifacts.DefaultBuilder`).
  - `type CrashHook func(name string) error` (satisfied by `enginetest.CrashRegistry.Check`; production uses `NoCrash`).
  - `func NoCrash(string) error { return nil }`
  - `func New(cat Catalog, build Builder, load ContentLoader, crash CrashHook, maxAttempts int) *Worker`
  - `func (w *Worker) Recover(ctx) (int, error)` — calls `RequeueClaimedJobs`; run once at startup.
  - `func (w *Worker) ProcessOne(ctx) (bool, error)` — claims and fully processes the next job at any priority; returns `(true, nil)` if a job was handled (committed, dead-lettered, retried, or dropped as superseded), `(false, nil)` if the queue was empty, and a non-nil error only for infrastructure failures (which the caller/test treats as a crash). Injected crashes surface as `enginetest.ErrInjectedCrash`.
  - `func (w *Worker) Run(ctx) error` — loop `ProcessOne` until the queue drains and ctx is live; returns on ctx cancel.

**Crash-point names (call `w.crash(name)` at each boundary):**
- `"after-claim"` — job claimed, before content load.
- `"after-build"` — vectors computed, before persisting chunk cache.
- `"after-chunks"` — chunk vectors persisted, before the atomic commit.

**ProcessOne algorithm:**
1. `job, ok, err := cat.ClaimNextJob(ctx, core.PriorityBootstrap)` (lowest priority bound = drains all). If `!ok`, return `(false, nil)`.
2. `w.crash("after-claim")` → if err, return `(false, err)` (the job stays claimed; recovery requeues it).
3. **Supersession pre-check:** `desired, ok, _ := cat.DesiredGeneration(ctx, job.WorktreeID, job.Path)`. If `ok && desired > job.Generation`, this claim is stale → return `(true, nil)` without committing (the newer job is unclaimed and will be processed; the stale claim is abandoned — the newer `UpsertJob` already reset `claimed=0`). *(Rationale: the row now belongs to the newer generation; leaving it is correct. A belt-and-suspenders requeue is unnecessary because `UpsertJob` set claimed=0 on supersede.)*
4. **Delete op:** if `job.Operation == core.OpDelete`, `cat.CommitDelete(ctx, job.WorktreeID, job.Path, job.Generation, job)`; return `(true, err)`.
5. Resolve namespace + fingerprint: `root, repo, err := cat.WorktreeInfo(ctx, job.WorktreeID)`; `fp, err := cat.GenerationFingerprint(ctx, repo, job.Generation)`.
6. Load content: `content, err := w.load.Load(ctx, repo, root, job.Path, job.DesiredHash)`. A load error is **transient** (file may be mid-write) → `w.retryOrDeadLetter(ctx, job, FailureTransient, err)`; return `(true, nil)`.
7. Build key `core.ArtifactKey{RepositoryID: repo, RelativePath: job.Path, SourceHash: job.DesiredHash, Fingerprint: fp}`.
8. **Whole-file cache hit:** `if art, ok, _ := cat.GetArtifact(ctx, key); ok` → reuse it, but its `Chunks` are not loaded; for the commit we need the `artifact_chunks` mapping to already exist. Since the artifact exists, its mapping already exists (it was committed before) → set `req.Chunks = nil` and commit only the view switch + job delete. *(CommitUpdate with empty `Chunks` inserts no new mapping; `putArtifactTx` is `INSERT OR IGNORE`.)* Skip embedding entirely. Go to step 11 with this artifact.
9. Otherwise `art, err := w.build.Build(ctx, artifacts.BuildRequest{Key: key, Content: content})`. Classify a build error: `errors.Is(err, artifacts.ErrDimensionMismatch)` → **permanent**; any other non-nil → **transient** (endpoint/timeouts). On error call `w.retryOrDeadLetter`; return `(true, nil)`.
10. `w.crash("after-build")` → if err return `(false, err)`.
11. **Persist chunk vectors (cache warming, idempotent):** for each `ch := range art.Chunks`, `cat.PutChunkVector(ctx, ch.ChunkID, repo, fp, ch.Vector)`. (Content-addressed + `INSERT OR IGNORE` → re-running after a crash is a no-op; a crash here leaves harmless orphan cache rows.) For a whole-file cache hit (step 8) this loop is empty.
12. `w.crash("after-chunks")` → if err return `(false, err)`.
13. **Second supersession guard (cheap):** re-check `DesiredGeneration`; if a newer generation arrived, abandon (return `(true, nil)`). The atomic commit's generation guard is the ultimate safety net, but this avoids a wasted commit.
14. **Atomic commit:** `req := core.CommitRequest{View: core.ViewEntry{WorktreeID: job.WorktreeID, Path: job.Path, ArtifactID: art.ID, Generation: job.Generation}, Artifact: art, Chunks: art.Chunks}`; `cat.CommitUpdate(ctx, req, job)`. A commit error is **transient** → `retryOrDeadLetter`. On success return `(true, nil)`.

**`retryOrDeadLetter(ctx, job, class, cause)`:**
- `FailurePermanent` → `cat.DeadLetterJob(ctx, job, "permanent: "+cause.Error())`.
- `FailureSuperseded` → no-op (row already newer).
- `FailureTransient` → `attempts, _ := cat.FailJobAttempt(ctx, job)`; if `attempts >= w.maxAttempts` → `cat.DeadLetterJob(ctx, job, "attempts exhausted: "+cause.Error())`. Otherwise the job is released (claimed=0) and re-eligible immediately (Phase 4 adds timed backoff).

- [ ] **Step 1: Write the failing tests (`worker_test.go`)** — representative cases:

```go
// internal/enginev2/worker/worker_test.go
package worker_test

import (
	"context"
	"errors"
	"testing"

	"github.com/yoanbernabeu/grepai/indexer"
	"github.com/yoanbernabeu/grepai/internal/enginev2/artifacts"
	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog/sqlite"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
	"github.com/yoanbernabeu/grepai/internal/enginev2/worker"
)

// staticLoader returns fixed content regardless of hash (unit tests).
type staticLoader struct{ content []byte }

func (l staticLoader) Load(_ context.Context, _ core.RepositoryID, _, _, _ string) ([]byte, error) {
	return l.content, nil
}

func newWorkerFixture(t *testing.T, emb *enginetest.FakeEmbedder, content []byte, crash worker.CrashHook) (*sqlite.Catalog, *worker.Worker) {
	t.Helper()
	c := newTestCatalog(t) // shared helper (mirror catalog_test.go's constructor)
	ctx := context.Background()
	must(t, c.RegisterRepository(ctx, "r", ".", ""))
	must(t, c.RegisterWorktree(ctx, "w", "r", ".", 1))
	must(t, c.CreateGeneration(ctx, "r", 1, "fp"))
	must(t, c.SetActiveGeneration(ctx, "r", 1))
	b := artifacts.New(indexer.NewChunker(512, 50), emb, c)
	w := worker.New(c, b, staticLoader{content: content}, crash, 5)
	return c, w
}

func TestProcessOneCommitsUpsert(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	c, w := newWorkerFixture(t, emb, []byte("func main() {}"), worker.NoCrash)
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	did, err := w.ProcessOne(ctx)
	if err != nil || !did {
		t.Fatalf("did=%v err=%v", did, err)
	}
	if id, ok, _ := c.ResolveView(ctx, "w", "a.go"); !ok || id == "" {
		t.Fatal("view not committed")
	}
	if dlc, _ := c.DeadLetterCount(ctx); dlc != 0 {
		t.Fatalf("unexpected dead-letters: %d", dlc)
	}
}

func TestTransientRetryThenSucceeds(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	emb.FailNext(2, errors.New("503 upstream")) // transient; recovers on 3rd
	c, w := newWorkerFixture(t, emb, []byte("func main() {}"), worker.NoCrash)
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	for i := 0; i < 3; i++ {
		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
	}
	if _, ok, _ := c.ResolveView(ctx, "w", "a.go"); !ok {
		t.Fatal("expected eventual commit after transient retries")
	}
	if dlc, _ := c.DeadLetterCount(ctx); dlc != 0 {
		t.Fatalf("should not dead-letter a recoverable transient: %d", dlc)
	}
}

func TestPermanentFailureDeadLetters(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	c, w := newWorkerFixture(t, emb, []byte("func main() {}"), worker.NoCrash)
	// Poison the cache with a wrong-dimension vector so Build returns ErrDimensionMismatch.
	// (Simplest: use a builder whose embedder returns bad dims — construct a 3-dim embedder
	// against a worker/catalog seeded for 4 dims via a dedicated sub-fixture, OR assert the
	// permanent path by injecting a stub builder. Prefer a stub builder here.)
	_ = c
	_ = w
	t.Skip("covered by gate3_test permanent path with stub builder; keep unit focus on transient")
}
```

> **Implementer note:** define the shared `newTestCatalog`, `must`, and (if needed) a `stubBuilder` in a small `worker_test.go` helper block. For the permanent-failure assertion prefer a `stubBuilder{err: artifacts.ErrDimensionMismatch}` over contorting the embedder — it makes the classification path explicit. Keep the transient/commit cases on the real builder.

- [ ] **Step 2: Run to verify failure**

Run: `GOTOOLCHAIN=local go test ./internal/enginev2/worker/ -v`
Expected: FAIL (package `worker` undefined).

- [ ] **Step 3: Implement `content.go` then `worker.go`** per the algorithm above.

```go
// internal/enginev2/worker/content.go
package worker

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// ContentLoader fetches the exact bytes for a job's desired file version.
// Production reads the git blob (clean tracked) or the on-disk file (dirty/
// untracked); tests supply fakes. A returned error is treated as transient.
type ContentLoader interface {
	Load(ctx context.Context, repo core.RepositoryID, worktreeRoot, relPath, desiredHash string) ([]byte, error)
}
```

```go
// internal/enginev2/worker/worker.go
package worker

import (
	"context"
	"errors"

	"github.com/yoanbernabeu/grepai/internal/enginev2/artifacts"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// Catalog is the durable-state surface the worker needs.
type Catalog interface {
	ClaimNextJob(ctx context.Context, minPriority core.Priority) (core.Job, bool, error)
	WorktreeInfo(ctx context.Context, wt core.WorktreeID) (string, core.RepositoryID, error)
	GenerationFingerprint(ctx context.Context, repo core.RepositoryID, gen core.Generation) (string, error)
	GetArtifact(ctx context.Context, key core.ArtifactKey) (core.Artifact, bool, error)
	PutChunkVector(ctx context.Context, chunkID string, repo core.RepositoryID, fingerprint string, vec []float32) error
	CommitUpdate(ctx context.Context, req core.CommitRequest, job core.Job) error
	CommitDelete(ctx context.Context, wt core.WorktreeID, relPath string, gen core.Generation, job core.Job) error
	DeadLetterJob(ctx context.Context, job core.Job, reason string) error
	FailJobAttempt(ctx context.Context, job core.Job) (int, error)
	DesiredGeneration(ctx context.Context, wt core.WorktreeID, relPath string) (core.Generation, bool, error)
	RequeueClaimedJobs(ctx context.Context) (int, error)
}

// Builder builds an artifact from content (satisfied by *artifacts.DefaultBuilder).
type Builder interface {
	Build(ctx context.Context, req artifacts.BuildRequest) (core.Artifact, error)
}

// CrashHook is called at durable-state boundaries; a non-nil return simulates
// a process crash at that point. Production uses NoCrash.
type CrashHook func(name string) error

// NoCrash is the production crash hook: it never crashes.
func NoCrash(string) error { return nil }

// Worker drains the durable job queue: claim → build (cache-miss-only) →
// persist chunk cache → atomic commit, classifying failures.
type Worker struct {
	cat         Catalog
	build       Builder
	load        ContentLoader
	crash       CrashHook
	maxAttempts int
}

// New constructs a Worker.
func New(cat Catalog, build Builder, load ContentLoader, crash CrashHook, maxAttempts int) *Worker {
	if crash == nil {
		crash = NoCrash
	}
	if maxAttempts < 1 {
		maxAttempts = 5
	}
	return &Worker{cat: cat, build: build, load: load, crash: crash, maxAttempts: maxAttempts}
}

// Recover requeues jobs a crashed worker left claimed. Run once at startup.
func (w *Worker) Recover(ctx context.Context) (int, error) {
	return w.cat.RequeueClaimedJobs(ctx)
}

// ProcessOne claims and fully processes the next eligible job. See plan §Task 6.
func (w *Worker) ProcessOne(ctx context.Context) (bool, error) {
	job, ok, err := w.cat.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if err := w.crash("after-claim"); err != nil {
		return false, err
	}
	if desired, ok, _ := w.cat.DesiredGeneration(ctx, job.WorktreeID, job.Path); ok && desired > job.Generation {
		return true, nil // stale claim; newer job is unclaimed and will run
	}
	if job.Operation == core.OpDelete {
		return true, w.cat.CommitDelete(ctx, job.WorktreeID, job.Path, job.Generation, job)
	}
	root, repo, err := w.cat.WorktreeInfo(ctx, job.WorktreeID)
	if err != nil {
		return true, w.retryOrDeadLetter(ctx, job, core.FailureTransient, err)
	}
	fp, err := w.cat.GenerationFingerprint(ctx, repo, job.Generation)
	if err != nil {
		return true, w.retryOrDeadLetter(ctx, job, core.FailureTransient, err)
	}
	content, err := w.load.Load(ctx, repo, root, job.Path, job.DesiredHash)
	if err != nil {
		return true, w.retryOrDeadLetter(ctx, job, core.FailureTransient, err)
	}
	key := core.ArtifactKey{RepositoryID: repo, RelativePath: job.Path, SourceHash: job.DesiredHash, Fingerprint: fp}

	var art core.Artifact
	wholeHit := false
	if existing, ok, gerr := w.cat.GetArtifact(ctx, key); gerr == nil && ok {
		art, wholeHit = existing, true
	} else {
		art, err = w.build.Build(ctx, artifacts.BuildRequest{Key: key, Content: content})
		if err != nil {
			class := core.FailureTransient
			if errors.Is(err, artifacts.ErrDimensionMismatch) {
				class = core.FailurePermanent
			}
			return true, w.retryOrDeadLetter(ctx, job, class, err)
		}
	}
	if err := w.crash("after-build"); err != nil {
		return false, err
	}
	if !wholeHit {
		for _, ch := range art.Chunks {
			if err := w.cat.PutChunkVector(ctx, ch.ChunkID, repo, fp, ch.Vector); err != nil {
				return true, w.retryOrDeadLetter(ctx, job, core.FailureTransient, err)
			}
		}
	}
	if err := w.crash("after-chunks"); err != nil {
		return false, err
	}
	if desired, ok, _ := w.cat.DesiredGeneration(ctx, job.WorktreeID, job.Path); ok && desired > job.Generation {
		return true, nil // superseded during build
	}
	req := core.CommitRequest{
		View:     core.ViewEntry{WorktreeID: job.WorktreeID, Path: job.Path, ArtifactID: art.ID, Generation: job.Generation},
		Artifact: art,
		Chunks:   art.Chunks, // nil for a whole-file cache hit (mapping already present)
	}
	if err := w.cat.CommitUpdate(ctx, req, job); err != nil {
		return true, w.retryOrDeadLetter(ctx, job, core.FailureTransient, err)
	}
	return true, nil
}

// Run drains the queue until empty, then returns when ctx is canceled.
func (w *Worker) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		did, err := w.ProcessOne(ctx)
		if err != nil {
			return err
		}
		if !did {
			return nil // queue drained
		}
	}
}

func (w *Worker) retryOrDeadLetter(ctx context.Context, job core.Job, class core.FailureClass, cause error) error {
	switch class {
	case core.FailurePermanent:
		return w.cat.DeadLetterJob(ctx, job, "permanent: "+cause.Error())
	case core.FailureSuperseded:
		return nil
	default: // transient
		attempts, err := w.cat.FailJobAttempt(ctx, job)
		if err != nil {
			return err
		}
		if attempts >= w.maxAttempts {
			return w.cat.DeadLetterJob(ctx, job, "attempts exhausted: "+cause.Error())
		}
		return nil
	}
}
```

- [ ] **Step 4: Run to verify pass**

Run: `GOTOOLCHAIN=local go test ./internal/enginev2/worker/ -race -v` → PASS
Run: `GOTOOLCHAIN=local go vet ./internal/enginev2/worker/ && make lint` → clean.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/worker/content.go internal/enginev2/worker/worker.go internal/enginev2/worker/worker_test.go
git commit -m "feat(worker): durable claim→build→commit loop with failure classification"
```

---

### Task 7: Gate 3 integration tests

**Files:**
- Create: `internal/enginev2/worker/gate3_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1–6 plus `enginetest.CrashRegistry`, real `*sqlite.Catalog`, real `artifacts.DefaultBuilder`, `enginetest.FakeEmbedder`.

**The three Gate 3 criteria, each an explicit test:**

**(a) A failed request preserves the old searchable file.** Commit gen 1 for `a.go`. Then queue gen 2 (new content/hash), make the embedder fail permanently (`SetError`), process → the job dead-letters (or, if you use `FailNext` beyond maxAttempts, exhausts) and `ResolveView("w","a.go")` still returns the **gen-1** artifact. Assert the gen-1 chunks remain present (`ArtifactChunkIDs` unchanged).

**(b) A crash at every injection point recovers to a valid state.** For each `name ∈ {"after-claim","after-build","after-chunks"}`: fresh catalog, queue one upsert, arm the point (`reg.ArmAt(name)`), `ProcessOne` returns the injected error (view still empty or unchanged). Then **recover**: `w.Recover(ctx)` (requeue) + a fresh `Worker` with `NoCrash`, drive `ProcessOne` to a commit. Assert: final view resolves to the correct artifact, all its chunks present, `DeadLetterCount==0`, and — the strong cache assertion — after an `"after-chunks"` crash the retry performs **zero new embeddings** (`emb.TextsEmbedded()` did not increase post-recovery, because the chunks were content-addressed and already persisted).

**(c) Rapid saves commit only the final desired generation.** Queue gen 1 (`h1`) then, before processing, `UpsertJob` gen 2 (`h2`) for the same path (supersede). Drive `Run` to drain. Assert `ResolveView` resolves the **gen-2** artifact (`SourceHash h2`), and the queue is empty. Additionally: claim gen 1 first (`ClaimNextJob`), then `UpsertJob` gen 2, then finish processing the gen-1 claim — assert the pre-commit supersession check abandons gen 1 and only gen 2 becomes the view.

- [ ] **Step 1: Write the tests**

```go
// internal/enginev2/worker/gate3_test.go
package worker_test

import (
	"context"
	"errors"
	"testing"

	"github.com/yoanbernabeu/grepai/indexer"
	"github.com/yoanbernabeu/grepai/internal/enginev2/artifacts"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
	"github.com/yoanbernabeu/grepai/internal/enginev2/worker"
)

// hashLoader returns content chosen by desiredHash, so gen1/gen2 differ.
type hashLoader struct{ byHash map[string][]byte }

func (l hashLoader) Load(_ context.Context, _ core.RepositoryID, _, _, desiredHash string) ([]byte, error) {
	if b, ok := l.byHash[desiredHash]; ok {
		return b, nil
	}
	return nil, errors.New("no content for hash")
}

func TestGate3_FailedRequestPreservesOldFile(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)
	emb := enginetest.NewFakeEmbedder(4)
	b := artifacts.New(indexer.NewChunker(512, 50), emb, c)
	load := hashLoader{byHash: map[string][]byte{"h1": []byte("func v1() {}"), "h2": []byte("func v2() {}")}}
	w := worker.New(c, b, load, worker.NoCrash, 5)

	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	_, _ = w.ProcessOne(ctx)
	v1, ok, _ := c.ResolveView(ctx, "w", "a.go")
	if !ok {
		t.Fatal("gen1 not committed")
	}
	// Now gen2 fails permanently.
	emb.SetError(errors.New("permanent-ish"))
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h2", Generation: 2, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	for i := 0; i < 10; i++ { // exhaust attempts
		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatal(err)
		}
		if done, _ := c.DeadLetterCount(ctx); done > 0 {
			break
		}
	}
	v2, ok, _ := c.ResolveView(ctx, "w", "a.go")
	if !ok || v2 != v1 {
		t.Fatalf("old searchable file not preserved: v1=%s v2=%s ok=%v", v1, v2, ok)
	}
}

func TestGate3_CrashAtEveryInjectionPointRecovers(t *testing.T) {
	for _, name := range []string{"after-claim", "after-build", "after-chunks"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			c := newTestCatalog(t)
			seedRepoWorktree(t, c)
			emb := enginetest.NewFakeEmbedder(4)
			b := artifacts.New(indexer.NewChunker(512, 50), emb, c)
			load := hashLoader{byHash: map[string][]byte{"h1": []byte("func v1() {}")}}
			reg := enginetest.NewCrashRegistry()
			w := worker.New(c, b, load, reg.Check, 5)
			must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))

			reg.ArmAt(name)
			if _, err := w.ProcessOne(ctx); !errors.Is(err, enginetest.ErrInjectedCrash) {
				t.Fatalf("expected injected crash at %s, got %v", name, err)
			}
			if _, ok, _ := c.ResolveView(ctx, "w", "a.go"); ok {
				t.Fatal("view must not be visible after a pre-commit crash")
			}
			embeddedBefore := emb.TextsEmbedded()

			// Restart: recover + fresh worker, no crash.
			w2 := worker.New(c, b, load, worker.NoCrash, 5)
			if _, err := w2.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			for {
				did, err := w2.ProcessOne(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if !did {
					break
				}
			}
			if _, ok, _ := c.ResolveView(ctx, "w", "a.go"); !ok {
				t.Fatalf("recovery did not commit a valid view (point=%s)", name)
			}
			if dlc, _ := c.DeadLetterCount(ctx); dlc != 0 {
				t.Fatalf("recovery dead-lettered (point=%s): %d", name, dlc)
			}
			if name == "after-chunks" && emb.TextsEmbedded() != embeddedBefore {
				t.Fatalf("after-chunks recovery re-embedded: before=%d after=%d", embeddedBefore, emb.TextsEmbedded())
			}
		})
	}
}

func TestGate3_RapidSavesCommitFinalGenerationOnly(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)
	emb := enginetest.NewFakeEmbedder(4)
	b := artifacts.New(indexer.NewChunker(512, 50), emb, c)
	load := hashLoader{byHash: map[string][]byte{"h1": []byte("func v1() {}"), "h2": []byte("func v2() {}")}}
	w := worker.New(c, b, load, worker.NoCrash, 5)

	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	// Supersede before any processing.
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h2", Generation: 2, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	id, ok, _ := c.ResolveView(ctx, "w", "a.go")
	if !ok {
		t.Fatal("no view committed")
	}
	// The committed artifact must be gen2's (SourceHash h2).
	want := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "h2", Fingerprint: "fp"}.ArtifactID()
	if id != want {
		t.Fatalf("final view is not gen2: got %s want %s", id, want)
	}
}
```

> `seedRepoWorktree`, `newTestCatalog`, `must` are the shared helpers introduced in Chunk A / Task 6; consolidate them so both `worker_test.go` and `gate3_test.go` reuse one copy (same package `worker_test`).

- [ ] **Step 2: Run to verify (red → green as Tasks 1–6 land)**

Run: `GOTOOLCHAIN=local go test ./internal/enginev2/worker/ -race -run Gate3 -v`
Expected: PASS.

- [ ] **Step 3: Full-suite + lint gate**

Run: `GOTOOLCHAIN=local go build ./... && CGO_ENABLED=0 GOTOOLCHAIN=local go build ./... && GOTOOLCHAIN=local go vet ./... && GOTOOLCHAIN=local go test -race ./internal/enginev2/... && gofmt -l internal/enginev2 && make lint`
Expected: all green; `gofmt -l` prints nothing.

- [ ] **Step 4: Commit**

```bash
git add internal/enginev2/worker/gate3_test.go
git commit -m "test(worker): Gate 3 — failed-request preservation, crash recovery, final-generation-only"
```

---

## Chunk D — Legacy GOB inspection spike (Task 8)

### Task 8: Read-only legacy GOB inspection

**Files:**
- Create: `internal/enginev2/legacyimport/gobspike.go`
- Create: `internal/enginev2/legacyimport/gobspike_test.go`

**Rationale:** de-risk Phase 6 migration compatibility now by proving v2 can decode a legacy GOB store's on-disk format (`gobData{Chunks map[string]Chunk, Documents map[string]Document}`) without importing the legacy `store` package — GOB is decoded structurally by field name/type into a v2-local mirror. **Strictly read-only:** no catalog writes, no store mutation.

**Interfaces:**
- Produces:
  - `type Summary struct { ChunkCount int; DocumentCount int; Dimensions int; SampleContentHash string }`
  - `func InspectGOB(path string) (Summary, error)` — opens the file, `gob.Decode`s into the mirror, returns counts + the vector length of the first chunk (0 if none) + one sample `ContentHash`.

- [ ] **Step 1: Write the failing test** (builds a fixture with the real legacy store, then inspects it)

```go
// internal/enginev2/legacyimport/gobspike_test.go
package legacyimport_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/legacyimport"
	"github.com/yoanbernabeu/grepai/store"
)

func TestInspectGOBReadsLegacyStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	idx := filepath.Join(dir, "index.gob")
	s := store.NewGOBStore(idx)
	if err := s.SaveChunks(ctx, []store.Chunk{
		{ID: "c1", FilePath: "a.go", Vector: []float32{1, 2, 3, 4}, ContentHash: "ch1"},
		{ID: "c2", FilePath: "b.go", Vector: []float32{5, 6, 7, 8}, ContentHash: "ch2"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveDocument(ctx, store.Document{Path: "a.go", Hash: "h", ChunkIDs: []string{"c1"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Persist(ctx); err != nil {
		t.Fatal(err)
	}
	sum, err := legacyimport.InspectGOB(idx)
	if err != nil {
		t.Fatal(err)
	}
	if sum.ChunkCount != 2 || sum.DocumentCount != 1 || sum.Dimensions != 4 {
		t.Fatalf("summary wrong: %+v", sum)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `GOTOOLCHAIN=local go test ./internal/enginev2/legacyimport/ -v`
Expected: FAIL (`InspectGOB` undefined).

- [ ] **Step 3: Implement**

```go
// internal/enginev2/legacyimport/gobspike.go

// Package legacyimport is a READ-ONLY Phase 3 spike proving v2 can decode the
// legacy GOB index format ahead of the Phase 6 migration. It never writes to
// the v2 catalog and never mutates a legacy store.
package legacyimport

import (
	"encoding/gob"
	"fmt"
	"os"
)

// mirror mirrors store.gobData's shape so gob can decode it structurally
// without importing the legacy store package.
type mirror struct {
	Chunks map[string]struct {
		ID          string
		FilePath    string
		StartLine   int
		EndLine     int
		Content     string
		Vector      []float32
		Hash        string
		ContentHash string
		UpdatedAt   [15]byte // time.Time gob-encodes via GobEncode; decoded opaquely — see note
	}
	Documents map[string]struct {
		Path     string
		Hash     string
		ModTime  [15]byte
		ChunkIDs []string
	}
}

// Summary is a read-only description of a legacy GOB index.
type Summary struct {
	ChunkCount        int
	DocumentCount     int
	Dimensions        int
	SampleContentHash string
}

// InspectGOB decodes a legacy GOB index read-only and returns a summary.
func InspectGOB(path string) (Summary, error) {
	f, err := os.Open(path) // #nosec G304 - operator-supplied legacy index path (read-only spike)
	if err != nil {
		return Summary{}, err
	}
	defer f.Close()

	var m mirror
	if err := gob.NewDecoder(f).Decode(&m); err != nil {
		return Summary{}, fmt.Errorf("decode legacy gob: %w", err)
	}
	s := Summary{ChunkCount: len(m.Chunks), DocumentCount: len(m.Documents)}
	for _, ch := range m.Chunks {
		s.Dimensions = len(ch.Vector)
		s.SampleContentHash = ch.ContentHash
		break
	}
	return s, nil
}
```

> **Implementer note on `time.Time`:** `time.Time` implements `GobEncode`/`GobDecode`, so a mirror field typed as a raw `[15]byte` will **not** decode correctly — gob matches by the encoded representation, not raw bytes. Simpler and correct: type the mirror's time fields as `time.Time` (import `time`). If a stray field-shape mismatch surfaces, gob **ignores unknown fields and tolerates missing ones**, so the mirror only needs `Vector`, `ContentHash`, and the map shapes to satisfy this spike — trim the mirror to the minimum that decodes cleanly and note in the test what was proven (counts + dimensions decode from the real on-disk format). Verify by running the test; adjust the mirror until green. This iteration **is** the spike's value.

- [ ] **Step 4: Run to verify pass**

Run: `GOTOOLCHAIN=local go test ./internal/enginev2/legacyimport/ -race -v` → PASS
Run: `make lint` → clean (the `#nosec G304` annotation covers the read-only open).

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/legacyimport/
git commit -m "spike(legacyimport): read-only legacy GOB decode to de-risk Phase 6 migration"
```

---

## Gate 3 Exit Criteria (spec §9, Phase 3)

All must hold on the full repository, not just `internal/enginev2`:

1. **Failed request preserves the old file** — `TestGate3_FailedRequestPreservesOldFile` green: a permanently failing gen-2 dead-letters while `ResolveView` still returns the gen-1 artifact with its chunks intact.
2. **Crash at every injection point recovers** — `TestGate3_CrashAtEveryInjectionPointRecovers` green for `after-claim`, `after-build`, `after-chunks`: recovery commits a valid view, zero dead-letters, and the `after-chunks` retry re-embeds nothing (content-addressed idempotency).
3. **Rapid saves commit only the final generation** — `TestGate3_RapidSavesCommitFinalGenerationOnly` green: a superseded gen-1 never becomes the visible view; the committed artifact is gen-2's.
4. **No data races** — `GOTOOLCHAIN=local go test -race ./...` clean.
5. **Build discipline** — `go build ./...`, `CGO_ENABLED=0 go build ./...`, `go vet ./...`, `gofmt -l internal/enginev2` (empty), `make lint` all green; go.mod unchanged (`go 1.24.2`, `modernc.org/sqlite v1.45.0`), no new module dependency.
6. **Cache-miss-only embedding** — a warm-cache rebuild embeds zero texts (`TestBuildEmbedsMissesOnlyAndReusesCache`).
7. **Atomic composition** — `TestCommitUpdatePersistsArtifactChunks`: the `artifact_chunks` mapping is present exactly when the view is switched (one transaction).

## Self-Review Notes

- **Spec coverage:** exact-input chunk cache lookup (Task 5 §3) ✓; cache-miss-only embedding (Task 5 §4, Gate crit 6) ✓; vector validation (Task 5, `ErrDimensionMismatch`) ✓; superseded-generation protection (Task 6 steps 3 & 13, existing commit guards) ✓; atomic artifact/view/job commit (Task 2 + existing `commitUpdateTx`, Gate crit 7) ✓; dead-letter classification (Task 3 `DeadLetterJob`, Task 6 `retryOrDeadLetter`) ✓; durable worker loop (Task 6) ✓. **Deferred by scope decision:** artifact-scoped symbol extraction and scheduled RPG refresh → Phase 4 (documented in Scope/Non-goals; `symbols`/`symbol_edges` left empty). GOB spike (Task 8) is an added de-risking deliverable, not a spec Gate-3 item.
- **Forward-dependency check:** the worker's `Run` is a plain drain loop, not the host scheduler; timed backoff/jitter/circuit-breaker are Phase 4. Phase 3 proves *correctness* of failure classification and recovery, not *pacing*. This mirrors the Phase 2 precedent (correctness now, scheduling later).
- **Type consistency:** `ContentLoader.Load(ctx, repo, worktreeRoot, relPath, desiredHash)` is used identically in `content.go`, `worker.go`, and both test loaders. `Catalog` interface method set in `worker.go` matches the concrete `*sqlite.Catalog` signatures (verified against `catalog.go`, `views.go`, `reader.go`, `jobs.go`). `core.ArtifactKey.ArtifactID()` is the single artifact-identity source used by both the builder and the Gate-3 `want` assertion.
- **Known implementer iteration points (flagged inline, not placeholders):** (1) the exact `EmbedContent` construction in `indexer/chunker.go` for the dimension-mismatch unit test — prefer a stub `Chunker`/`Builder` to avoid coupling to chunker internals; (2) the GOB mirror's `time.Time` fields — iterate the mirror to a clean decode (that iteration is the spike). Both are real investigations with a defined success signal (test green), not vague hand-waves.
- **Idempotency invariant to preserve during review:** every durable write on the recovery path (`PutChunkVector`, `putArtifactChunksTx`, `putArtifactTx`) is `INSERT OR IGNORE` or content-addressed, so re-running a partially completed job is a no-op — this is what makes "crash at every injection point" recover without duplication or corruption.
