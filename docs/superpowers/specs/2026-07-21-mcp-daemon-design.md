# MCP served from the daemon (issue #10)

Goal: `grepai mcp-serve` works for engine:v2 repos by routing query tools to
the grepaid daemon, so agent MCP integrations regain grepai access on the v2
fleet. Query-only per invariant 3 (MCP never enqueues indexing work beyond the
idempotent Register), explicit worktree context (the served project root).

## Design

1. **Shared daemon client** — the socket-resolution + dial-or-spawn logic
   moves from `cli/daemonclient.go` to `daemonctl.Connect(ctx, timeout)`
   (host-scoped socket precedence GREPAID_SOCKET > daemon.json > XDG; loud
   failures). cli keeps thin wrappers; mcp uses it directly.
2. **Shared trace assembly** — `cli.buildTraceResult` core moves to
   `internal/enginev2/traceview.Assemble(symbol, direction, depth, mode,
   resp) trace.TraceResult` (the #20 v1-parity assembly); cli wraps it for
   view/gstats, mcp formats it for tool output (compact variant strips
   context, as v1 MCP does).
3. **mcp daemon mode** — `Server` gains an `engineV2` flag and a
   `daemonBackend` seam (Search/Trace/Status bound to the served root;
   real impl dials per call via daemonctl.Connect + Register — robust to
   daemon restarts; fake impl for handler tests).
   - `grepai_search`: daemon Search; limit/compact/format/path honored
     (path normalized exactly as the CLI does); `workspace`/`projects`
     params → error result pointing at `grepai search-all`.
   - `grepai_trace_*`: daemon Trace → traceview.Assemble → v1 shapes
     (full or compact). No RPG enrichment (feature_path absent).
   - `grepai_index_status`: daemon Status → v2 status shape (engine,
     generation, fresh, pending, dead_letters, symbols_backfill_pending).
     File/chunk counts arrive with #11.
   - `grepai_refs_*`, `grepai_rpg_*`: error results — v1-only features,
     retired under engine:v2 (refs stays v1 per #9 non-goal).
   - `grepai_stats`, `grepai_list_workspaces`, `grepai_list_projects`:
     unchanged — they read the gstats ledger / workspace config, not the
     retired v1 index.
4. **Startup gating (cli/mcp_serve.go)** — an engine:v2 LOCAL repo now
   starts the daemon-mode server instead of refusing; the
   workspace-with-v2-members startup rejection stays (workspace serving is
   v1-store-based).
5. **Interim unscoped-mode fix (v1 mode)** — tool-call-time workspace
   selection (the #8 review's gap): every v1-mode workspace path
   (search/trace/refs/rpg/status) rejects a dynamically selected workspace
   containing engine:v2 members with the same loud message as startup.

## Non-goals

- Workspace/cross-repo MCP under v2 (search-all as an MCP tool is future
  work; the CLI serves it today).
- File/chunk counts in v2 index_status (#11 stats).
- RPG/refs under v2.

## Compatibility

v1 repos: byte-identical behavior (daemon mode is engine:v2-gated at
construction). Old daemon + new mcp: trace tools inherit the #20 Protocol
gate semantics via the shared response check; search/status work against any
daemon generation.
