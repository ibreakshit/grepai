# v2 Symbol Extraction + Trace — Design Spec (issue #9)

**Date:** 2026-07-20 · **Status:** Approved (design)
**Goal:** Restore call-graph tracing under engine:v2. Symbols become artifact-
scoped derived data in the catalog (architecture plan §5.3: "symbol extraction
is stored per artifact and inherits the artifact's identity"), served by the
daemon with worktree/generation filtering, with a CPU-only backfill for the
already-indexed fleet.

## 1. Data model (migration 0003 → schema v3)

Phase 1 stubbed `symbols(artifact_id,name,kind)` / `symbol_edges(artifact_id,
caller,callee)`. Migration 0003 adds what useful trace output needs:

- `symbols`: `+ line, end_line INTEGER NOT NULL DEFAULT 0`, `+ signature TEXT NOT NULL DEFAULT ''`
- `symbol_edges`: `+ line INTEGER NOT NULL DEFAULT 0` (call site)
- `file_artifacts`: `+ symbols_version INTEGER NOT NULL DEFAULT 0` — 0 = never
  extracted, 1 = extracted with extractor v1. This is the idempotence marker:
  a markdown file with zero symbols is NOT re-scanned forever, and a future
  extractor upgrade bumps the constant to re-backfill.

`LatestSchemaVersion` → 3; the existing guard keeps older binaries off newer
catalogs. Symbols inherit artifact identity: content change ⇒ new artifact ⇒
fresh extraction; whole-file cache hit ⇒ symbols already exist ⇒ no work.

## 2. Extraction in the build path

`worker` gains a `SymbolExtractor` port (`Extract(ctx, relPath, content) →
([]core.SymbolDef, []core.SymbolEdge, error)`), set via
`(*Worker).SetSymbolExtractor` (nil ⇒ skip — existing tests unchanged). The
production adapter (`internal/enginev2/symbols`) wraps v1's
`trace.NewTreeSitterExtractor()` (regex fallback if init fails) — extraction
LOGIC is reused, per the architecture plan; only persistence moves. The worker
extracts from the already-loaded content and puts defs/edges on
`core.CommitRequest`; `commitUpdateTx` persists them + `symbols_version=1`
ATOMICALLY with the artifact/view/job (no partially-symboled artifacts).
Extraction errors are non-fatal (log, commit without symbols, version stays 0 —
the backfill retries later); binary/empty artifacts extract nothing but still
mark version 1.

## 3. Backfill for the existing fleet

Fleet artifacts predate extraction (symbols_version=0). Backfill is CPU-only —
no embedder, no scheduler involvement:

- Catalog: `ArtifactsMissingSymbols(ctx, wt) → []{Path, ArtifactID, SourceHash}`
  (active view JOIN file_artifacts WHERE symbols_version=0);
  `PutArtifactSymbols(ctx, artifactID, defs, edges)` — one tx: insert symbols +
  edges + set symbols_version=1.
- Daemon: after Register (rehydrate or live), if the worktree has missing-symbol
  artifacts, a background backfill goroutine (per-repo, ONE at a time host-wide
  via a semaphore) loads each file via the hash-verified diskLoader path
  (content must match the artifact's SourceHash — a changed file is SKIPPED:
  the watcher/reconcile path will produce a new artifact that extracts
  normally), extracts, persists. Loud progress logs; failures skip (version
  stays 0, retried on next daemon start).

## 4. Trace service (replaces the inert stub)

- `service.TraceRequest{WorktreeID, Symbol, Direction ("callers"|"callees"|
  "graph"), Depth}` (depth default 2, cap 5, graph only).
- `service.TraceResponse{Definitions []TraceSymbol{Path,Name,Kind,Line,EndLine,
  Signature}, Edges []TraceEdge{Caller,Callee,Path,Line}, BackfillPending int}`
  — BackfillPending>0 tells the caller symbol coverage is still building.
- Catalog reads (all ACTIVE-VIEW joins ⇒ worktree isolation + generation
  scoping for free): `SymbolDefinitions(wt, name)`, `SymbolEdges(wt, name,
  callersOf bool)`. Graph = BFS in the service over per-level edge queries.
- Query-only: never enqueues (invariant 3). rpc `MethodTrace` already exists —
  request/response shapes extend in place (fork ships both ends together).

## 5. CLI

`grepai trace callers|callees|graph <symbol>` gains the same engine gate as
search/watch/status: engine:v2 → daemon RPC (loud failures, no fallback), v1
body byte-identical otherwise. `--json` for agents; text shows
`path:line kind name (via caller→callee edges)`. A BackfillPending note prints
to stderr. `grepai refs` (readers/writers) stays v1-only: the v2 model stores
call EDGES, not general references — noted on issue #9 as an explicit
follow-up decision.

## 6. Testing & gates

- sqlite: migration 0003 upgrade-in-place test (v2 catalog opens, gains
  columns, data intact); symbol persist/read round-trip; view-scoped queries
  isolate worktrees; ArtifactsMissingSymbols honors symbols_version.
- worker: build commits symbols atomically; nil extractor = old behavior;
  extraction error commits artifact w/ version 0; cache-hit skips extraction.
- symbols adapter: Go + TS fixtures extract expected defs/edges (reuse v1
  extractor test expectations, thin).
- service: callers/callees/graph vs a seeded two-file call chain; depth cap;
  worktree isolation (two worktrees, same symbol names, no cross-talk).
- daemon integration: fixture repo with A→B→C calls; register; wait fresh;
  trace callers/callees/graph over RPC; backfill test: artifacts committed
  WITHOUT symbols (extractor unset), restart daemon WITH extractor, backfill
  populates, trace works.
- Gates: full `-race`, vet, gofmt, lint, windows/darwin cross-builds; codex-bg
  merge gate; deploy + live verify (`grepai trace callers <fn>` on nanoclaw).

## 7. Non-goals

- `grepai refs` readers/writers under v2 (Reference-level data; separate
  decision), RPG under the daemon, MCP trace tools (#10), cross-repo trace,
  precise/fast extractor config (tree-sitter with regex fallback, fixed),
  docstring/Signature-based ranking.
