# Trace v1-parity output from the daemon (issue #20)

Follow-up to #9. Goal: `grepai trace callers|callees|graph` under engine:v2
produces the same `trace.TraceResult` output surface as v1 — byte-identical
JSON/TOON/text/UI rendering — by persisting the fields the shared v1 extractor
already produces and assembling the v1 structs in the CLI.

## Principle

The daemon embeds v1's `trace.SymbolExtractor`. Parity is achieved by NOT
dropping fields, not by new extraction logic. Traversal semantics are NOT the
parity target: the v2 server BFS (strict caps, active-view joins) stays; the
CLI maps its results into v1's output structs.

## Layers

1. **core**: `SymbolDef` += Receiver, Package, Exported, Language, Docstring;
   `SymbolEdge` += Context (call-site source line). Read-side `SymbolAt` +=
   the same five; `EdgeAt` += Context. `TraceResponse` += `Related
   map[string][]SymbolAt` — definitions for every distinct edge endpoint name,
   resolved server-side within the same worktree view (invariant 3 holds:
   reads only).
2. **catalog**: migration 0004 (schema v4) ALTERs the v3 tables (no PK change —
   v3 keys already include line). `SymbolsVersionCurrent` 1→2: the #9 backfill
   machinery re-extracts every artifact on daemon restart (replace semantics,
   `symbols_version < current` selection, zero embedding).
3. **adapter**: pass the v1 `Symbol`/`Reference` fields through.
4. **service**: after building edges, resolve `Related` for endpoint names
   (bounded by the edge caps from #9).
5. **CLI**: `runTraceDaemon` builds `trace.TraceResult` mirroring v1 assembly
   (`pickBestTargetSymbol`, `pickBestSymbolForFile`, `CallerInfo{Symbol,
   CallSite{File, Line, Context}}`, `CallGraph{Root, Nodes, Edges, Depth}`)
   and renders via `outputAndRecord` — JSON/TOON/UI/text and gstats recording
   become v1's own code paths. `--toon`/`--ui` rejections removed.

## Non-goals

- `feature_path` (RPG enrichment) — RPG is not daemon-served; field stays
  absent (omitempty).
- `--mode precise` — tree-sitter is a daemon build-tag property; still
  rejected loudly.
- `--workspace`/`--project` — cross-repo trace is a separate feature; still
  rejected loudly.
- v1 BFS heuristics (name-collision skip, self-edge pruning) — not replicated.

## Compatibility

Old daemon + new CLI: `Served` marker (#9) already forces a loud restart
error. New daemon + old CLI: additive JSON fields are ignored harmlessly.
Existing v4-less catalogs migrate on open; the version bump re-backfills the
fleet on the first post-deploy restart.
