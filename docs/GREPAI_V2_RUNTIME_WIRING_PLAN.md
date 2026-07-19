# GrepAI v2 — Runtime Wiring & Useful Search (implementation plan)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make v2 actually runnable and useful: `grepai v2 index` indexes a real repo through the full v2 stack with the config-driven embedder, and `grepai v2 search "…"` returns ranked results **with code snippets and line numbers** (like v1). This is the first production wiring of `internal/enginev2`.

**Architecture:** Persist chunk **display content** (in `chunks`, content-addressed) and **line ranges** (in `artifact_chunks`, per-artifact) so search can show snippets. A new `internal/enginev2/runtime` package assembles the catalog + reconciler + worker + service + real embedder behind two operations — `Index` (register → reconcile → enqueue → drain via `worker.Run`) and `Search` (via `service.Server`). New `grepai v2 index|search` cobra subcommands wire it to config + `embedder.NewFromConfig`, isolated from the v1 engine (zero risk to existing commands). The host-wide scheduler stays a daemon concern (deferred); a one-shot CLI index drains serially with bounded per-job retries.

**Tech Stack:** Go 1.24.2, the shipped `internal/enginev2/*`, the legacy `embedder`/`config`/`indexer` packages, `cobra`.

## Global Constraints

- Go 1.24.2 floor; `modernc.org/sqlite` v1.45.0; CGO_ENABLED=0 buildable; **no new module dependency** (cobra/embedder/config already vendored).
- `go test -race ./...`, `gofmt`, `make lint` green. Conventional commits (scope `catalog`, `core`, `artifacts`, `worker`, `runtime`, `cli`). Never push to `main`.
- **v1 isolation:** the new `v2` subcommands must not change any existing command's behavior. The legacy `embedder.Embedder` already satisfies the v2 `embedder.Embedder` port (identical methods) — no adapter.
- **Fingerprint consistency:** `index` and `search` derive the SAME `core.IndexingFingerprint` from config, so ChunkIDs match and query vectors are comparable.

## Scope / Non-goals

- **In:** chunk content + line-range persistence (schema migration0002 + thread-through core→catalog→builder→worker→search); the `runtime` assembly package; `grepai v2 index` and `grepai v2 search` (text + `--json`); an end-to-end integration test.
- **Out (deferred):** the `grepaid` daemon + Unix-RPC + systemd + the host-wide scheduler in production (one-shot CLI uses `worker.Run`); fsnotify watch; Trace/symbols; incremental re-index UX polish; non-git `.gitignore` honoring (git repos use git truth which respects ignores; a non-git fs walk indexes all regular files — documented); reranking/hybrid search.

## Consumed surfaces

- `core.ArtifactChunk`, `core.SearchHit` (extend, additive); `catalog/sqlite` (`PutChunkVector` gains a content arg; `putArtifactChunksTx`, `SearchWorktree`, schema extend); `artifacts.DefaultBuilder`; `worker.Worker`; `service.Server`; `reconcile.Engine`.
- Legacy `embedder.NewFromConfig(cfg) (embedder.Embedder, error)`, `config.Load(root)`, `indexer.NewChunker`.

---

## Chunk A — Chunk content & line persistence (Tasks 1–3)

### Task 1: Schema + core fields

**Files:** `internal/enginev2/catalog/sqlite/schema.go` (add migration0002), `internal/enginev2/core/chunk.go` + `core/search.go` (fields).

- migration0002 (append to `migrations`):
  ```sql
  ALTER TABLE chunks ADD COLUMN content TEXT NOT NULL DEFAULT '';
  ALTER TABLE artifact_chunks ADD COLUMN start_line INTEGER NOT NULL DEFAULT 0;
  ALTER TABLE artifact_chunks ADD COLUMN end_line INTEGER NOT NULL DEFAULT 0;
  ```
  Bump `schemaVersion` to 2; `migrations = []string{migration0001, migration0002}`.
- `core.ArtifactChunk`: add `Content string`, `StartLine int`, `EndLine int`.
- `core.SearchHit`: add `Content string`, `StartLine int`, `EndLine int`.

- [ ] Test: open a fresh catalog, assert `schemaVersion(ctx) == 2` and the new columns exist (a `PRAGMA table_info` check or an insert using them). Run → fail → implement → pass. Commit `feat(catalog,core): persist chunk content and line ranges (schema v2)`.

### Task 2: Catalog store + search of content/lines

**Files:** `catalog/sqlite/artifacts.go` (`PutChunkVector` + `putArtifactChunksTx`), `catalog/sqlite/query.go` (`SearchWorktree`).

- `PutChunkVector(ctx, chunkID string, repo core.RepositoryID, fingerprint string, vec []float32, content string) error` — store `content` (INSERT OR IGNORE keeps the first writer; content is stable per chunk_id).
- `putArtifactChunksTx(ctx, tx, artifactID, chunks []core.ArtifactChunk)` — insert `start_line, end_line` from each chunk.
- `SearchWorktree` — `SELECT wf.relative_path, ch.dimensions, ch.vector, ch.content, ac.start_line, ac.end_line`; populate `SearchHit{Path, Score, Content, StartLine, EndLine}` for the best chunk per path (carry the best chunk's content/lines, not just its score).

- [ ] Update all `PutChunkVector` call sites (worker.go + tests) to pass content. Tests: extend `TestSearchWorktree*` to assert returned `Content`/`StartLine`/`EndLine`. Commit `feat(catalog): store and return chunk content + line ranges in search`.

> Note: "best chunk per path" now needs to remember which chunk won — track `best[path] = {score, content, startLine, endLine}` (a small struct), not just the float score.

### Task 3: Builder populates content/lines

**Files:** `artifacts/builder.go`.

- In `Build`, set each `core.ArtifactChunk{Ordinal:i, ChunkID:id, Vector:vec, Content: info.Content, StartLine: info.StartLine, EndLine: info.EndLine}` (from `indexer.ChunkInfo`). Applies on both the cache-hit and freshly-embedded paths.
- Worker persist loop: `PutChunkVector(ctx, ch.ChunkID, repo, fp, ch.Vector, ch.Content)`.

- [ ] Test: extend the builder test to assert a built artifact's chunks carry non-empty `Content` and plausible line ranges. Commit `feat(artifacts): carry chunk content and line ranges from the chunker`.

---

## Chunk B — Runtime assembly (Task 4)

### Task 4: `internal/enginev2/runtime` package

**Files:** create `internal/enginev2/runtime/runtime.go`, `runtime_test.go`.

- `func Fingerprint(cfg *config.Config) string` — build a `core.IndexingFingerprint{EmbedderProvider, EmbedderModel, Dimensions, ChunkerImplementation:"indexer.Chunker", ChunkerSettings:{size,overlap}, ...}` from config and return `.Hash()`.
- `type Engine struct { cat *sqlite.Catalog; wk *worker.Worker; rec *reconcile.Engine; svc *service.Server; wt core.WorktreeID; fp string }`
- `func Open(ctx, catalogPath, root string, emb embedder.Embedder, fp string) (*Engine, error)` — open the catalog; construct `artifacts.New(indexer.NewChunker(size,overlap), emb, cat)`, `worker.New(cat, builder, diskLoader{}, worker.NoCrash, 5)`, `reconcile.New(cat)`, `service.New(cat, rec, emb, fp, searchLimit)`; derive `repo`/`wt` from `root` (canonical path). Register + bootstrap gen 1 via `svc.Register`.
- `func (e *Engine) Index(ctx) (indexed int, deadLettered int, err error)` — `rec.Reconcile(wt)` → `cat.UpsertJob` each → `wk.Recover(ctx)` → `wk.Run(ctx)` (drains: commits or dead-letters each job). Return counts (jobs queued, dead-letter count).
- `func (e *Engine) Search(ctx, query string, limit int) ([]core.SearchHit, core.Generation, bool, error)` — `svc.Search` → results + active generation + fresh.
- `func (e *Engine) Close() error`.
- `diskLoader` (internal): `Load(...) = os.ReadFile(filepath.Join(root, rel))` — the working-tree bytes match the git blob OID for clean files and the reconciler's content hash for dirty/untracked.

- [ ] Test (`runtime_test.go`): with a `GitFixture` (write 2 files, commit) + `enginetest.FakeEmbedder`, `Open` → `Index` (assert indexed>0, deadLettered==0) → `Search` for content matching one file (assert the right path is returned with non-empty `Content` and line numbers), and a second `Index` on the unchanged repo indexes 0 (idle). Commit `feat(runtime): assemble the v2 index+search runtime`.

---

## Chunk C — CLI subcommands (Task 5)

### Task 5: `grepai v2 index` / `grepai v2 search`

**Files:** create `cli/v2.go`; register `v2Cmd` in `cli/root.go`.

- `v2Cmd` = parent `&cobra.Command{Use:"v2", Short:"GrepAI v2 engine (experimental)"}`; add `v2IndexCmd` and `v2SearchCmd`.
- Shared: resolve project root (cwd or arg); `cfg, _ := config.Load(root)`; `emb, _ := embedder.NewFromConfig(cfg)`; `fp := runtime.Fingerprint(cfg)`; `eng, _ := runtime.Open(ctx, filepath.Join(root, ".grepai", "catalog_v2.db"), root, emb, fp)`; `defer eng.Close()`.
- `v2 index [dir]`: `n, dl, _ := eng.Index(ctx)`; print `indexed N files (M dead-lettered)`.
- `v2 search <query> [--json] [--limit N]`: `hits, gen, fresh, _ := eng.Search(ctx, query, limit)`; print each `path:startLine-endLine  (score)` + an indented content snippet; `--json` emits `{results:[{path,score,startLine,endLine,content}], activeGeneration, fresh}`. If `!fresh`, print a one-line "index may be stale; run `grepai v2 index`" hint to stderr.
- Register `rootCmd.AddCommand(v2Cmd)`.

- [ ] Test (`cli/v2_test.go`): a smoke test constructing the commands and running `v2 index` then `v2 search` in a temp git repo with the **synthetic** embedder (`config` provider `synthetic`, deterministic, no network) — assert the search output contains the expected file path and a snippet. (If wiring a real embedder in tests is impractical, drive `runtime` directly with `FakeEmbedder` in `runtime_test` and keep the CLI test to argument/flag parsing + a synthetic-embedder happy path.) Commit `feat(cli): grepai v2 index and v2 search subcommands`.

> **v1 isolation check (review focus):** `v2.go` must not import or mutate any v1 command state; `root.go` only gains one `AddCommand`. The v2 catalog lives at `.grepai/catalog_v2.db` (separate from v1's `.grepai/index.gob`).

---

## Chunk D — End-to-end gate (Task 6)

### Task 6: Integration test + full gate

- [ ] A `runtime` (or `cli`) integration test that indexes a small multi-file git fixture with the **synthetic** or fake embedder and asserts: (a) search returns the correct file for a distinctive query, (b) the result carries a non-empty content snippet and start/end lines, (c) re-indexing the unchanged repo is idle (0 jobs), (d) a query for content only in file B never returns file A (isolation holds through the real runtime).
- [ ] Full gate: `go build ./... && CGO_ENABLED=0 go build ./... && go vet ./... && go test -race ./... && gofmt -l . (enginev2+cli+runtime clean) && make lint`.
- [ ] Manual smoke (documented, not automated — needs a real embedder): `cd <repo> && grepai v2 index && grepai v2 search "…"` with an `ollama`/`openai` config. Note in the commit that this path is manually verified, since CI has no embedding backend.
- [ ] Commit `test(runtime): end-to-end index+search gate`.

## Exit Criteria

1. `grepai v2 index` indexes a real repo through catalog+reconciler+worker with the config embedder; `grepai v2 search` returns ranked results **with snippets + line numbers**.
2. Re-indexing an unchanged repo is idle (invariant 1 holds through the real runtime).
3. Worktree isolation holds through the runtime (a query never returns another file's exclusive content).
4. v1 commands are byte-for-byte unaffected (only one `AddCommand` added; separate catalog file).
5. `go test -race ./...` clean; build/CGO0/vet/gofmt/lint green; go.mod unchanged.

## Self-Review Notes

- **This is the first production import of `internal/enginev2`** — after this, `grep -rl internal/enginev2 | grep -v internal/enginev2/` is non-empty (cli/ + runtime/).
- **Usefulness bar:** the deliverable is snippets-in-search (Option B), not just file-level hits — that's what makes `v2 search` worth using vs. a demo.
- **Honest limitations to document:** one-shot serial indexing (no host-wide scheduler/daemon yet); no fsnotify (re-run `v2 index`); non-git repos index all regular files (no `.gitignore`); no rerank/hybrid; Trace absent. All deferred, tracked.
- **Content-addressing check (review):** chunk content lives in `chunks` (stable per chunk_id — verify `EmbedContent`/`Content` are path-independent so the same code yields the same chunk_id + content); line ranges live in `artifact_chunks` (per-artifact, since identical content can sit at different lines in different files).
