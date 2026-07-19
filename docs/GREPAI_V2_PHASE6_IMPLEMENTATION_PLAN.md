# GrepAI v2 — Phase 6: Migration & Shadow Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Import a legacy v1 GOB index into the v2 catalog so v2 can search it without re-embedding, and provide a live v1-vs-v2 search-parity harness that proves the migrated index ranks equivalently.

**Architecture:** A read-only `legacyimport` package decodes the v1 GOB (`store.Chunk`/`store.Document`), derives a stable fingerprint from the v1 config, and writes the artifacts/chunk-vectors/views into a v2 SQLite catalog by reusing the already-tested `PutChunkVector` + `CommitUpdate` seams (no new core write path). Two CLI verbs — `grepai v2 migrate` and `grepai v2 parity` — drive the import and the shadow comparison. Migration is **import-for-search**: v1 built its vectors with `framework_processing` that the v2 builder does not replicate, so a v2 *native* re-index is a distinct generation (documented, not gated on idle-reconcile). Both the stored chunk vectors and freshly-embedded query vectors live in the same `qwen3-embedding-8b` space, so search over the imported index is meaningful and parity is high.

**Tech Stack:** Go 1.24.2, `modernc.org/sqlite` v1.45.0 (pure Go, `CGO_ENABLED=0`), `encoding/gob`, cobra. No new module dependencies.

## Global Constraints

- Module path `github.com/yoanbernabeu/grepai`; fork of record `ibreakshit/grepai` (`origin`); upstream `yoanbernabeu/grepai`. Always `gh <cmd> --repo ibreakshit/grepai`.
- No new go.mod dependencies. Pure-Go SQLite only; `CGO_ENABLED=0` must build.
- Toolchain pinned local: `export PATH=$HOME/.local/go/bin:$PATH GOTOOLCHAIN=local`.
- Gates per task: `go build ./...`, `CGO_ENABLED=0 go build ./...`, `go vet ./...`, `gofmt -l` clean, `golangci-lint run` 0 issues, `go test -race ./...` green.
- The `legacyimport` package is **read-only** toward v1 data — it never writes, moves, or deletes a legacy index file.
- Content-addressing: v2 `chunk_id = core.ChunkID(fingerprint, content)`. Identical-content chunks dedupe across files, so raw unique-vector counts are ≤ v1 chunk count; reconciliation therefore compares **document count** and **chunk-composition count** (`artifact_chunks` rows), not unique vectors.
- `SourceHash = v1 Document.Hash` (self-contained; import needs no git checkout and no network).
- Independent review of the finished branch goes to `codex-bg` FIRST (independent model), before any same-model pass.

---

### Task 1: Typed v1 GOB loader

Extend the Phase-3 spike (`gobspike.go`, currently summary-only) with a full typed loader that decodes every field the importer and parity harness need. The GOB is a struct-shaped `map`; we mirror it locally so we don't import the legacy `store` package's private layout, but we DO reuse `store` in tests to author fixtures.

**Files:**
- Modify: `internal/enginev2/legacyimport/gobspike.go` (add `Load`, `LegacyIndex`, `LegacyChunk`, `LegacyDocument`)
- Test: `internal/enginev2/legacyimport/load_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `type LegacyChunk struct { ID, FilePath string; StartLine, EndLine int; Content string; Vector []float32; ContentHash string }`
  - `type LegacyDocument struct { Path, Hash string; ChunkIDs []string }`
  - `type LegacyIndex struct { Chunks map[string]LegacyChunk; Documents map[string]LegacyDocument; Dimensions int }`
  - `func Load(path string) (LegacyIndex, error)` — decodes the GOB; `Dimensions` = len of the first chunk vector (0 if empty); returns an error if the file is unreadable or decodes to zero chunks AND zero documents.

- [ ] **Step 1: Write the failing test**

```go
package legacyimport_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/legacyimport"
	"github.com/yoanbernabeu/grepai/store"
)

func writeFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "index.gob")
	s := store.NewGOBStore(path)
	ctx := context.Background()
	if err := s.SaveChunks(ctx, []store.Chunk{
		{ID: "c1", FilePath: "a.go", StartLine: 1, EndLine: 3, Content: "func A() {}", Vector: []float32{1, 0, 0, 0}, ContentHash: "ha"},
		{ID: "c2", FilePath: "a.go", StartLine: 4, EndLine: 6, Content: "func B() {}", Vector: []float32{0, 1, 0, 0}, ContentHash: "hb"},
		{ID: "c3", FilePath: "b.go", StartLine: 1, EndLine: 2, Content: "package x", Vector: []float32{0, 0, 1, 0}, ContentHash: "hc"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveDocument(ctx, store.Document{Path: "a.go", Hash: "doc-a", ChunkIDs: []string{"c1", "c2"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveDocument(ctx, store.Document{Path: "b.go", Hash: "doc-b", ChunkIDs: []string{"c3"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadDecodesChunksAndDocuments(t *testing.T) {
	idx, err := legacyimport.Load(writeFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Chunks) != 3 || len(idx.Documents) != 2 {
		t.Fatalf("counts: chunks=%d documents=%d", len(idx.Chunks), len(idx.Documents))
	}
	if idx.Dimensions != 4 {
		t.Fatalf("dims=%d want 4", idx.Dimensions)
	}
	c1 := idx.Chunks["c1"]
	if c1.FilePath != "a.go" || c1.StartLine != 1 || c1.EndLine != 3 || c1.Content != "func A() {}" || len(c1.Vector) != 4 {
		t.Fatalf("c1 not fully decoded: %+v", c1)
	}
	if got := idx.Documents["a.go"].ChunkIDs; len(got) != 2 || got[0] != "c1" {
		t.Fatalf("doc a chunk ids: %v", got)
	}
}

func TestLoadRejectsEmpty(t *testing.T) {
	dir := t.TempDir()
	if _, err := legacyimport.Load(filepath.Join(dir, "missing.gob")); err == nil {
		t.Fatal("expected error for missing index")
	}
}
```

- [ ] **Step 2: Run the test to verify it fails** — `go test ./internal/enginev2/legacyimport/ -run TestLoad -v` → FAIL (`Load` undefined).

- [ ] **Step 3: Implement `Load` + types** in `gobspike.go`. Reuse the existing `mirror` struct pattern but with the full field set; the GOB was written by `store.GOBStore` whose on-disk shape is `struct{ Chunks map[string]store.Chunk; Documents map[string]store.Document }` — mirror those fields by name/type. Decode with `gob.NewDecoder(bufio.NewReader(f))`. Populate `Dimensions` from any one chunk. Return an error when the file can't be opened/decoded, or when both maps are empty.

- [ ] **Step 4: Run the test to verify it passes** — same command → PASS.

- [ ] **Step 5: Gates + commit**

```bash
export PATH=$HOME/.local/go/bin:$PATH GOTOOLCHAIN=local
gofmt -l internal/enginev2/legacyimport/ && go vet ./internal/enginev2/legacyimport/ && go test -race ./internal/enginev2/legacyimport/
git add internal/enginev2/legacyimport/
git commit -m "feat(enginev2): typed v1 GOB loader for migration"
```

---

### Task 2: Deterministic fingerprint from the v1 config

The imported generation must carry a stable fingerprint so v2 records what produced it and search treats it as one coherent generation. It is derived from the v1 `config.Config` embedder + chunking, plus a `framework:v1` marker so it can never collide with a v2 native fingerprint (`runtime.Fingerprint`).

**Files:**
- Create: `internal/enginev2/legacyimport/fingerprint.go`
- Test: `internal/enginev2/legacyimport/fingerprint_test.go`

**Interfaces:**
- Consumes: `config.Config` (the repo's existing `config` package).
- Produces: `func DeriveFingerprint(cfg *config.Config) string` — a hex SHA-256 over `provider|model|dimensions|chunkSize|chunkOverlap|framework:v1`; deterministic, stable across calls, differs whenever any field differs.

- [ ] **Step 1: Write the failing test**

```go
package legacyimport_test

import (
	"testing"

	"github.com/yoanbernabeu/grepai/config"
	"github.com/yoanbernabeu/grepai/internal/enginev2/legacyimport"
)

func TestDeriveFingerprintStableAndSensitive(t *testing.T) {
	base := &config.Config{}
	base.Embedder.Provider = "openai"
	base.Embedder.Model = "qwen3-embedding-8b"
	base.Chunking.Size = 512
	base.Chunking.Overlap = 50

	a := legacyimport.DeriveFingerprint(base)
	b := legacyimport.DeriveFingerprint(base)
	if a == "" || a != b {
		t.Fatalf("not stable: %q %q", a, b)
	}
	other := *base
	other.Chunking.Size = 256
	if legacyimport.DeriveFingerprint(&other) == a {
		t.Fatal("fingerprint must change when chunk size changes")
	}
}
```

- [ ] **Step 2: Run it → FAIL** (`DeriveFingerprint` undefined). Confirm the real `config.Config` field paths first: `grep -nE "Provider|Model|Size|Overlap|Dimensions" config/config.go`. Use whatever the actual field names are (adjust the test + impl together if they differ).

- [ ] **Step 3: Implement** — `sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%d|%d|%d|framework:v1", provider, model, dims, size, overlap)))`, hex-encoded. If the config exposes no explicit dimensions, omit that field (keep the format string in sync with what you feed it).

- [ ] **Step 4: Run it → PASS.**

- [ ] **Step 5: Gates + commit** (same gate block; `git commit -m "feat(enginev2): derive migration fingerprint from v1 config"`).

---

### Task 3: Importer — v1 index → v2 catalog

Write the artifacts, chunk vectors, and worktree views into a v2 catalog, reusing `PutChunkVector` + `CommitUpdate`. Deterministic ordering (sorted document paths, then the document's own `ChunkIDs` order) so the import is reproducible and idempotent.

**Files:**
- Create: `internal/enginev2/legacyimport/import.go`
- Test: `internal/enginev2/legacyimport/import_test.go`

**Interfaces:**
- Consumes: `LegacyIndex` (Task 1); the catalog methods `RegisterRepository`, `RegisterWorktree`, `EnsureActiveGeneration`, `PutChunkVector`, `CommitUpdate`; `core.ChunkID`, `core.ArtifactKey`, `core.CommitRequest`, `core.ViewEntry`, `core.ArtifactChunk`.
- Produces:
  - `type CatalogWriter interface { RegisterRepository(...); RegisterWorktree(...); EnsureActiveGeneration(...); PutChunkVector(...); CommitUpdate(ctx, core.CommitRequest, core.Job) error }` (exact signatures copied from `sqlite.Catalog`).
  - `type Stats struct { Documents, Chunks, UniqueVectors, SkippedMissingChunk int }`
  - `func Import(ctx context.Context, cat CatalogWriter, repo core.RepositoryID, wt core.WorktreeID, root string, idx LegacyIndex, fingerprint string) (Stats, error)`

- [ ] **Step 1: Write the failing test** — import the Task-1 fixture into a real `sqlite.Catalog`, assert (a) `Stats.Documents==2`, `Stats.Chunks==3`; (b) each view resolves (`ResolveView`); (c) `SearchWorktree` with `c1`'s exact vector returns `a.go` first with a non-empty snippet; (d) re-running `Import` is idempotent (counts identical, no error). Use the real `sqlite.Open` catalog (as `worker_test` does).

```go
func TestImportWritesSearchableViews(t *testing.T) {
	ctx := context.Background()
	idx, err := legacyimport.Load(writeFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	c, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "cat.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fp := "fp-v1"
	st, err := legacyimport.Import(ctx, c, "repo", "wt", "/root", idx, fp)
	if err != nil {
		t.Fatal(err)
	}
	if st.Documents != 2 || st.Chunks != 3 {
		t.Fatalf("stats: %+v", st)
	}
	if id, ok, _ := c.ResolveView(ctx, "wt", "a.go"); !ok || id == "" {
		t.Fatal("a.go view not committed")
	}
	hits, err := c.SearchWorktree(ctx, "wt", []float32{1, 0, 0, 0}, 5)
	if err != nil || len(hits) == 0 {
		t.Fatalf("search: hits=%d err=%v", len(hits), err)
	}
	if hits[0].Path != "a.go" || hits[0].Content == "" {
		t.Fatalf("top hit: %+v", hits[0])
	}
	// Idempotent re-import.
	st2, err := legacyimport.Import(ctx, c, "repo", "wt", "/root", idx, fp)
	if err != nil || st2.Documents != st.Documents || st2.Chunks != st.Chunks {
		t.Fatalf("re-import not idempotent: %+v err=%v", st2, err)
	}
}
```

- [ ] **Step 2: Run it → FAIL** (`Import` undefined).

- [ ] **Step 3: Implement `Import`:**
  1. `RegisterRepository(ctx, repo, root, "")`; `RegisterWorktree(ctx, wt, repo, root, 1)`; `EnsureActiveGeneration(ctx, repo, 1, fingerprint)` — all idempotent, so re-import is safe.
  2. Sort document paths for determinism. For each document:
     - Build `[]core.ArtifactChunk` in `Document.ChunkIDs` order. For each id, look up the `LegacyChunk`; if missing, increment `SkippedMissingChunk` and skip it (a dangling id in a legacy index must not abort the whole migration). `chunkID := core.ChunkID(fingerprint, ch.Content)`.
     - `PutChunkVector(ctx, chunkID, repo, fingerprint, ch.Vector, ch.Content)` (idempotent; content-addressed).
     - Append `core.ArtifactChunk{ChunkID: chunkID, Ordinal: i, Content: ch.Content, StartLine: ch.StartLine, EndLine: ch.EndLine}`.
     - If a document ends up with zero usable chunks, skip committing a view for it (still counts as a Document seen but commits nothing searchable).
     - `key := core.ArtifactKey{RepositoryID: repo, RelativePath: doc.Path, SourceHash: doc.Hash, Fingerprint: fingerprint}`; `art := core.Artifact{ID: key.ArtifactID(), Key: key, Dimensions: idx.Dimensions, Chunks: artChunks}`.
     - `view := core.ViewEntry{WorktreeID: wt, Path: doc.Path, ArtifactID: art.ID, Generation: 1}`.
     - `CommitUpdate(ctx, core.CommitRequest{View: view, Artifact: art, Chunks: artChunks}, core.Job{})` — the empty job's `WorktreeID/Path` match no `index_jobs` row, so the internal job-delete is a harmless no-op; the view switches because gen 1 is active.
  3. Accumulate `Stats`: `Documents = len(idx.Documents)`, `Chunks = Σ len(committed artChunks)`, `UniqueVectors` counts distinct `chunkID`s written.
  - **Confirm `core.ArtifactChunk` field names first** (`grep -n "ArtifactChunk struct" -A8 internal/enginev2/core/chunk.go`) — use the real field names for Content/StartLine/EndLine/Ordinal/ChunkID.

- [ ] **Step 4: Run it → PASS.** Also add `TestImportSkipsDanglingChunkID` (a document referencing a missing chunk id imports the rest and sets `SkippedMissingChunk`).

- [ ] **Step 5: Gates + commit** (`git commit -m "feat(enginev2): import v1 GOB index into v2 catalog"`).

---

### Task 4: `grepai v2 migrate` CLI + reconciliation report

Wire the loader + importer behind a CLI verb that opens (or creates) a v2 catalog next to the v1 index, imports, and prints a reconciliation summary proving document + composition counts match the source.

**Files:**
- Modify: `cli/v2.go` (add the `migrate` subcommand; reuse `openV2Runtime`/catalog-open helpers already there)
- Test: `internal/enginev2/legacyimport/reconcile_test.go` (unit-test the reconciliation helper, not cobra wiring)

**Interfaces:**
- Consumes: `Load`, `DeriveFingerprint`, `Import`, `config` loader.
- Produces: `func Reconcile(idx LegacyIndex, st Stats) (ok bool, detail string)` — compares `st.Documents == len(idx.Documents)` and `st.Chunks == Σ len(doc.ChunkIDs) − st.SkippedMissingChunk`; returns a human-readable line.

- [ ] **Step 1: Write the failing test** for `Reconcile` — a matching `Stats` returns `ok=true`; a `Stats` with fewer documents returns `ok=false` and a detail mentioning the mismatch.

```go
func TestReconcileMatches(t *testing.T) {
	idx := legacyimport.LegacyIndex{
		Documents: map[string]legacyimport.LegacyDocument{
			"a.go": {ChunkIDs: []string{"c1", "c2"}},
			"b.go": {ChunkIDs: []string{"c3"}},
		},
	}
	ok, _ := legacyimport.Reconcile(idx, legacyimport.Stats{Documents: 2, Chunks: 3})
	if !ok {
		t.Fatal("expected reconcile ok")
	}
	if ok, _ := legacyimport.Reconcile(idx, legacyimport.Stats{Documents: 1, Chunks: 3}); ok {
		t.Fatal("document mismatch must not reconcile")
	}
}
```

- [ ] **Step 2: Run it → FAIL.**
- [ ] **Step 3: Implement `Reconcile`** in `import.go`; add the `migrate` cobra command in `cli/v2.go`: resolve the v1 index path (arg, or `<root>/.grepai/index.gob`), load config from the same `.grepai`, `DeriveFingerprint`, open a v2 catalog at `<root>/.grepai/catalog.db` (or a `--catalog` flag), `Import`, then print `Reconcile`'s detail and exit non-zero if `!ok`.
- [ ] **Step 4: Run it → PASS**; manually smoke `grepai v2 migrate` against a temp fixture repo.
- [ ] **Step 5: Gates + commit** (`git commit -m "feat(cli): grepai v2 migrate with reconciliation report"`).

---

### Task 5: Search-parity harness + `grepai v2 parity`

Compare v1 and v2 ranked results over the SAME imported index for a query set, embedding each query once with the configured embedder (live `qwen3-embedding-8b` via the v1 config's endpoint). Report per-query top-K path overlap and the mean.

**Files:**
- Create: `internal/enginev2/legacyimport/parity.go`
- Modify: `cli/v2.go` (add the `parity` subcommand)
- Test: `internal/enginev2/legacyimport/parity_test.go`

**Interfaces:**
- Consumes: a minimal `type v1Searcher interface { Search(ctx, []float32, int) ([]string, error) }` and `type v2Searcher interface { SearchWorktree(ctx, core.WorktreeID, []float32, int) ([]core.SearchHit, error) }`, plus `embedder.Embedder`.
- Produces:
  - `type QueryParity struct { Query string; Overlap float64; V1Paths, V2Paths []string }`
  - `type ParityReport struct { PerQuery []QueryParity; Mean float64 }`
  - `func TopKOverlap(a, b []string) float64` — |intersection| / K over the top-K path sets (deterministic; used by both the harness and its test).
  - `func RunParity(ctx, emb embedder.Embedder, v1 v1Searcher, v2 v2Searcher, wt core.WorktreeID, queries []string, k int) (ParityReport, error)`

- [ ] **Step 1: Write the failing test** — `TopKOverlap` with `{a,b,c}` vs `{a,b,d}` at K=3 → `2/3`; identical sets → `1.0`; disjoint → `0.0`. Plus `RunParity` with a fake embedder + two fake searchers returning known path lists → mean overlap matches hand calculation. (No network in the unit test.)
- [ ] **Step 2: Run it → FAIL.**
- [ ] **Step 3: Implement** `TopKOverlap` (set intersection over the first `k` of each, guarding `k>len`), and `RunParity` (embed each query once, call both searchers, extract v2 paths from `SearchHit.Path`, compute overlap, accumulate mean). Add the `parity` cobra command: load the v1 GOB via `store.NewGOBStore` (for real v1 search including its ranking) and the v2 catalog, build the configured embedder via `embedder.NewFromConfig`, read queries from `--query` (repeatable) or a `--queries-file`, print a per-query table + mean, and exit non-zero if `mean < --threshold` (default e.g. 0.6).
- [ ] **Step 4: Run it → PASS** (unit). Then a live smoke against longwave once (documented, not a CI gate).
- [ ] **Step 5: Gates + commit** (`git commit -m "feat(cli): grepai v2 parity shadow-validation harness"`).

---

### Task 6: Docs, Gate 6, and memory

**Files:**
- Modify: `docs/GREPAI_V2_ARCHITECTURE_PLAN.md` (mark Gate 6 criteria + the framework-transform caveat)
- Modify: `.superpowers/sdd/progress.md`

- [ ] **Step 1:** Document in the architecture plan: Gate 6 = (1) `Reconcile` ok against a real index; (2) parity mean ≥ threshold on a live run; (3) migration is import-for-search — a v2 native re-index is a distinct generation because `framework_processing` is not replicated (symbol/RPG import + generation-scoped views remain deferred).
- [ ] **Step 2:** Full-suite gate: `go build ./... && CGO_ENABLED=0 go build ./... && go vet ./... && gofmt -l . && golangci-lint run && go test -race ./...` all green.
- [ ] **Step 3:** Live validation run captured: `grepai v2 migrate ~/longwave` then `grepai v2 parity ~/longwave --query "..."` → record reconcile result + parity mean in the ledger.
- [ ] **Step 4: Independent review** — hand the whole Phase-6 diff to `codex-bg` FIRST; address findings in fix waves; only then an optional same-model pass.
- [ ] **Step 5: Commit + push** the branch and open/update the fork PR (`--repo ibreakshit/grepai`). Verify lint is green in the SAME step before (never chained after) the push.

---

## Self-Review

- **Spec coverage:** import (T1–T3), reconciliation/Gate-6 crit-1 (T4), parity/Gate-6 crit-2 (T5), fingerprint assertion (T2, recorded on the generation), docs+gate+review (T6). Deferred-by-scope (symbol/RPG import, generation-scoped views, backups/rollback automation) is explicitly documented, not silently dropped.
- **Placeholder scan:** every code step carries real code or an exact command; field-name confirmations are called out where the real struct must be checked before coding (`config.Config`, `core.ArtifactChunk`).
- **Type consistency:** `LegacyIndex/LegacyChunk/LegacyDocument`, `Stats`, `Import`, `Reconcile`, `TopKOverlap`, `RunParity`, `ParityReport` are used consistently across tasks; `CatalogWriter` mirrors `sqlite.Catalog`'s real signatures (confirm at implementation time).
