# grepaid Daemon — Design Spec

**Date:** 2026-07-19
**Status:** Approved (design)
**Scope:** Build the `grepaid` host-level daemon, the Unix-socket JSON-RPC transport, a per-repo catalog substrate (`catalogset` + registry), engine-gated CLI thin clients, and a systemd user unit. This lands the daemon slice deferred across Phases 4/5 of `docs/GREPAI_V2_ARCHITECTURE_PLAN.md` (the "TIGHT scope" decision).

## 1. Objective

Make GrepAI v2 run as a single long-lived host service. Every dispatch target already exists and is tested — the catalog (`internal/enginev2/catalog/sqlite`), reconciler (`reconcile.Engine`), durable worker (`worker.Worker` + `artifacts`), host-wide scheduler (`scheduler.Engine`), and the transport-independent service surface (`service.Server` implementing `service.Service`). What is missing is the *process* that wires them together, the *transport* that exposes them, and a *multi-repo substrate* so one daemon serves many repositories. This spec builds exactly that, plus the client side needed to use it.

## 2. Catalog model: one daemon, one DB file per repo

A single `grepaid` process serves every registered repository, but each repository keeps its **own** SQLite catalog at `<repo>/.grepai/catalog_v2.db` (the existing v2 convention). There is no host-wide merged catalog. This is possible without touching the scheduler or service because both consume **interfaces**:

- `scheduler.Engine` takes an injected `scheduler.Queue`.
- `service.Server` takes an injected `service.Catalog`.
- `worker.Worker` / `artifacts` builder take an injected catalog (the durable commit surface).

We add one adapter, `catalogset`, that owns `map[RepositoryID]*sqlite.Catalog` and implements the **union** of those catalog-facing interfaces by routing per-repo calls to the right catalog and fanning out the host-wide aggregate calls. The scheduler still sees a single logical queue and enforces a single global in-flight budget (invariant 2: one host, one scheduler) — the budget lives in the scheduler's semaphores, not in any DB.

Rationale (vs. a single merged catalog):

- **Reuses existing per-repo indexes** — the repos already indexed at `.grepai/catalog_v2.db` are opened as-is; no forced re-embed (subject to the fingerprint check in §4.3).
- **Write-lock isolation** — SQLite WAL is single-writer *per file*; per-repo DBs let different repos commit index updates in parallel if `MaxIndexInflight` is ever raised above 1.
- **Corruption blast radius** — one bad file cannot sink every repo.
- **Natural lifecycle/portability** — the index lives in the repo's `.grepai/`, travels with it, and is removed with it.
- A merged catalog would buy nothing on cross-repo embedding dedup: chunk-vector cache identity already includes the `repository_id` namespace (§5.3), so identical text in two repos re-embeds either way.

## 3. What already exists (do not rebuild)

- `internal/enginev2/service`: `Service` interface + `Server` implementing all 8 methods (Register, Reconcile, Search, Trace, Status, WaitFresh, Rebuild, DeadLetters). Query methods (Search/Status/WaitFresh/Trace) only read + embed; they never enqueue (invariant 3). Takes a `Catalog` interface.
- `internal/enginev2/scheduler`: `Engine` with `New(cfg, Queue, Processor, Clock, seed)`, `Run(ctx)` (continuous round-robin drain with admission control, circuit breaker, jittered backoff, dead-lettering), `Submit`, `Stats`. `Queue` and `Processor` are injected interfaces. `DefaultConfig()` provides safe local defaults.
- `internal/enginev2/rpc`: JSON-RPC 2.0 envelope (`Request`/`Response`/`Error`) + the 8 method-name constants. **Framing and dispatch are not implemented** — this spec adds them.
- `internal/enginev2/runtime`: the one-shot, single-worktree wiring (`Open` → reconcile → drain via `worker.Run`). The daemon reuses its component-assembly patterns (`Fingerprint`, `diskLoader`, `ensureSelfIgnore`, chunk params); the per-repo daemon substrate is its long-running, multi-repo counterpart.
- `daemon/` (v1): reusable `GetDefaultLogDir` (XDG state dir) and the flock-based `WritePIDFile` singleton pattern.

## 4. Architecture

```
grepai CLI (thin client, engine:v2)          MCP (later)
            |  JSON-RPC 2.0 over Unix socket, Content-Length framed
            v
      grepaid (singleton, one host)
      ┌──────────────────────────────────────────────┐
      │ rpc.Server  ── goroutine per connection       │
      │    dispatch method → service.Service          │
      │ service.Server (existing)                     │
      │ scheduler.Engine.Run(ctx)  ── background loop │
      │    Queue=catalogset  Processor=worker.Worker  │
      │ catalogset  ── map[repo]->*sqlite.Catalog     │
      └──────────────────────────────────────────────┘
        |            |            |
        v            v            v
   repoA/.grepai   repoB/.grepai   repoC/.grepai
    catalog_v2.db   catalog_v2.db   catalog_v2.db
        |
        v   host registry: <state>/grepai/registry.json
            (registered repos + roots, re-opened on restart)
            embedding endpoint (config-derived embedder)
```

### 4.1 Filesystem locations

Resolved once at startup, Linux/XDG conventions (mirror `GetDefaultLogDir`):

- **State dir:** `$XDG_STATE_HOME/grepai` else `~/.local/state/grepai`.
- **Per-repo catalog:** `<repo>/.grepai/catalog_v2.db` (unchanged from today's v2 convention; `ensureSelfIgnore` already keeps it out of reconciliation).
- **Host registry:** `<state>/grepai/registry.json` — the set of registered repositories and their canonical roots, so the daemon re-opens their catalogs on restart.
- **Socket:** `$XDG_RUNTIME_DIR/grepai/grepaid.sock` else `<state>/grepaid.sock`.
- **Lock:** `<state>/grepaid.lock` (flock, held for process lifetime).
- **Log:** existing `GetDefaultLogDir()` → `<state>/logs/grepaid.log`.

`daemon.socket` in config overrides the socket path for both server and clients.

### 4.2 Daemon lifecycle (`cmd/grepaid/main.go`)

1. Resolve paths; `MkdirAll` state + runtime dirs (0700).
2. Acquire the singleton flock. If held, exit non-zero: "grepaid already running (pid N)".
3. Unlink a stale socket file if present (only after the lock is ours), then `net.Listen("unix", …)` with socket mode 0600.
4. Build the config-derived host-global embedder + fingerprint (§4.3).
5. Load the registry; for each registered repo, open its `.grepai/catalog_v2.db` into the `catalogset` (skipping/logging repos whose dir is gone or whose fingerprint is incompatible — §4.3).
6. Assemble the single `worker.Worker` (Processor, whose builder + committer route through the catalogset) + `scheduler.Engine` (`scheduler.DefaultConfig()`, overridable via config) + `service.Server` (Catalog = catalogset).
7. `go scheduler.Run(ctx)`; `go rpc.Server.Serve(listener)`.
8. Block on SIGINT/SIGTERM. On signal: stop accepting new connections, cancel the scheduler ctx (drains in-flight dispatch via its WaitGroup), close the listener, close every catalog in the set, persist the registry, remove the socket, release the lock. Exit 0.

### 4.3 Embedder & fingerprint (host-global this slice)

The daemon uses **one host-global embedder + indexing fingerprint** derived from a daemon config block (defaulting to the current 4B embedder). Every repo is registered/indexed under that fingerprint. Because the cache and artifact identity are namespaced per repo (§5.3), the per-repo DBs stay isolated; the daemon's single embedder just means all repos are indexed the same way.

On opening a repo's existing catalog whose active generation carries a *different* fingerprint (it was built with an incompatible embedder/chunker), the daemon does **not** hard-error the way the interactive `runtime.Open` does — it logs the mismatch and starts a fresh generation under the daemon's fingerprint on next reconcile (a background service must not wedge on one stale repo). Matching-fingerprint catalogs are reused as-is (the reuse win).

**Deferred (natural next refinement):** honoring each repo's *own* config embedder/fingerprint. Per-repo DBs already make this natural for storage — it additionally requires routing the embedder per repo in the Processor (a `processorset`) and in query embedding (per-repo `service.Server` or an embedder router). Out of this slice; noted so the substrate here does not preclude it.

### 4.4 catalogset + registry

- `catalogset` (new, `internal/enginev2/…`): owns `map[RepositoryID]*sqlite.Catalog` under a mutex. Implements the union of the catalog-facing interfaces — `scheduler.Queue`, `service.Catalog`, and the `worker`/`artifacts` commit surface (verify these method sets in Phase 0 and route each per-repo method by its repo/worktree/job/artifact id).
  - **Routing methods** (`WorktreeInfo`, `ActiveGeneration`, `SearchWorktree`, `ClaimNextJobInRepo(repo,…)`, `UpsertJob`, `DeadLetterJob`, commit paths, …): resolve the target repo (explicit `repo` arg, or `job`/`artifact`/worktree→repo) → delegate to that catalog. An op for an unregistered repo is an error, never a silent cross-repo write.
  - **Host-wide aggregate methods** (`RepositoriesWithPendingJobs`, `QueueDepthByPriority`, `DeadLetterCount`): fan out over the open catalogs and combine (union of repos, summed depths/counts).
  - `Register(repo, root)` opens/creates that repo's catalog and adds it; `Close()` closes all.
- `registry` (new): a small JSON file (`registry.json`) of `{repositoryID, root}` entries with atomic write (temp + rename). Loaded at startup, appended on Register, used to re-open catalogs. Worktree→repo resolution derives the repo id from the canonical root/git-common-dir exactly as `service.Server.Register` does today.

## 5. RPC transport (`internal/enginev2/rpc/`)

### 5.1 Server (`server.go`)

- Dispatch target is `service.Service`.
- `Serve(l net.Listener, h service.Service)` accepts connections; one goroutine per connection.
- Per connection: read `Content-Length: N\r\n\r\n<N bytes>` frames (LSP-style), decode `rpc.Request`, dispatch by `Method` to the matching `service.Service` call, encode `rpc.Response`, write framed. Sequential per connection (simplest correct model; per-repo WAL catalogs + read-only query methods make concurrent connections safe).
- Dispatch table maps each `Method*` constant to (a) a params-decode into the method's `*Request` struct and (b) the service call. `id` is echoed verbatim for correlation. A request with no `id` (notification) gets no response.
- Errors → JSON-RPC codes: `-32700` parse (malformed frame/JSON), `-32600` invalid request (missing jsonrpc/method), `-32601` method not found, `-32602` invalid params (params decode fails), `-32603` internal (service returned an error — wrapped; `Data` = error string). A per-request panic is recovered into `-32603`, never killing the connection or process.

### 5.2 Client (`client.go`)

- `Dial(socketPath) (*Client, error)` — connects; distinguishes "daemon not running" (ENOENT/ECONNREFUSED) from other errors so callers can fall back to v1.
- `Call(ctx, method, params, *result) error` — monotonic request ids, framed write, framed read, matches response id, maps `rpc.Error` back to a typed Go error (code preserved). Respects `ctx` deadline.
- Thin typed wrappers per method (`Register`, `Reconcile`, `Search`, `Status`, `WaitFresh`, …) returning the `service` response structs, so CLI code is transport-agnostic.

## 6. CLI clients (`cli/`, gated)

- **Config:** add `Engine string` (`"v1"` default | `"v2"`) and a `Daemon` block (`Socket string`, optional scheduler overrides) to `config.Config`. Absent = `v1`. Loading an old config is unchanged.
- **`grepai daemon` (new):** `start` (spawn/exec `grepaid`, or instruct via systemd), `stop` (SIGTERM the pid), `status` (dial socket → `Status`/health, report running + socket + registered-repo count).
- **`grepai v2 search|status|watch`:** talk to the daemon over the socket when reachable. `v2 watch` = ensure-registered (adds the repo to the registry + opens its catalog) + reconcile + tail status. `v2 search`/`v2 status` = RPC calls resolving worktree id from cwd (canonical root). If the daemon is unreachable these report the daemon-down error (explicit v2 surface — no silent v1 fallback).
- **Top-level `grepai watch|search|status`:** when `engine: v2`, use the daemon path (watch degrades to ensure-registered + reconcile + status-tail **with a deprecation notice**; search/status → RPC); if the daemon is unreachable, **fall back to v1** behavior. When `engine: v1` (default), behavior is exactly as today. `grepai init` gains, under `engine: v2`, a best-effort register-with-daemon step.
- Worktree identity resolves from cwd's canonical root; ambiguous/missing identity is an error, never a fallback to another worktree's index.

## 7. systemd (`packaging/`)

- `packaging/grepaid.service` — a **user** unit (`systemctl --user`): `ExecStart=%h/.local/bin/grepaid`, `Restart=on-failure`, `RestartSec`, journald logging, `Type=simple`. No hardcoded socket (daemon resolves XDG). Documented install steps in `docs/`.

## 8. Error handling summary

| Layer | Failure | Behavior |
|-------|---------|----------|
| Daemon start | lock held | exit non-zero, "already running (pid N)" |
| Daemon start | stale socket | unlink (lock owned) then listen |
| Daemon start | registered repo dir gone / fingerprint mismatch | log + skip (or fresh generation), never wedge |
| catalogset | op for unregistered repo | error, never silent cross-repo write |
| RPC server | bad frame/JSON | `-32700`, connection continues |
| RPC server | unknown method | `-32601` |
| RPC server | bad params | `-32602` |
| RPC server | service error | `-32603`, `Data`=error string |
| RPC server | handler panic | recovered → `-32603`, connection + process survive |
| Client | socket absent/refused | typed daemon-down error → v1 fallback (top-level, engine:v2) |
| Service | unknown/ambiguous worktree | error, never cross-worktree fallback |
| Shutdown | SIGINT/SIGTERM | stop-accept → scheduler cancel+drain → close catalogs → persist registry → unlink → exit 0 |

## 9. Testing

- **catalogset unit:** routing to the right catalog per repo; aggregate methods union/sum across repos; op for an unregistered repo errors; concurrent access is race-clean.
- **registry unit:** round-trip load/save; atomic write (temp+rename); append; corrupt/missing file handling.
- **rpc unit:** frame round-trip; reader handling partial reads and multiple frames in one buffer; each error code path; id-correlation across interleaved ids; notification (no-id) produces no response; panic-in-handler recovered.
- **daemon integration:** real Unix socket + two tmp git fixture repos. Register both; reconcile; `waitFresh(deadline)`; `search` in each returns that repo's hit and not the other's (isolation); `status` reports fresh; `DeadLetterCount` aggregates. Second `grepaid` instance fails the singleton lock. SIGTERM drains and exits 0 with no leaked socket/lock, registry persisted.
- **cli:** `engine:v1` path unchanged (table test asserts no daemon dial); `engine:v2` with daemon-down falls back to v1; `grepai daemon status` against a running fixture daemon.
- **gates per phase:** `make build`, `make lint`, `go test ./... -race`, `go vet`, `gofmt` all green. Independent `codex-bg` review before advancing a phase.

## 10. Sequenced phases (one spec)

- **Phase 0 — Multi-repo substrate:** `catalogset` (implements `scheduler.Queue` + `service.Catalog`) + `registry` + tests. Pure, isolated, no daemon/transport yet. Gate 0.
- **Phase A — RPC transport:** `rpc/server.go` + `rpc/client.go` + tests. Isolated. Gate A.
- **Phase B — Daemon process:** `cmd/grepaid/main.go` (paths, singleton, listen, load registry, open catalogs into catalogset, wire embedder+scheduler+service, background scheduler, graceful shutdown) + integration test. Gate B.
- **Phase C — CLI clients:** config `Engine`/`Daemon` fields + `grepai daemon` command + `v2 search/status/watch` daemon path + gated top-level fallback + `init` register. Gate C.
- **Phase D — Packaging:** `packaging/grepaid.service` + install/ops docs + `Makefile` target for the `grepaid` binary. Gate D.

## 11. Non-goals (explicitly out of this slice)

- Per-repo heterogeneous embedders/fingerprints (host-global embedder this slice; substrate does not preclude it — §4.3).
- Repointing top-level `grepai search`/`watch`/`status` *unconditionally* (they are gated behind `engine: v2` with v1 fallback; full cutover with default `engine: v2` is a later phase).
- Symbol/Trace population, RPG refresh, fsnotify watcher wiring, generation-scoped controlled rebuild (remain as current deferred stubs).
- Windows daemon service packaging (systemd unit is Linux-only; the binary still builds cross-platform).
- Lazy-open / idle-close of catalog handles (open-all-on-start this slice; add an LRU later if registered-repo count grows large).

## 12. Definition of done

`grepaid` builds and runs; `grepai daemon start` (or `systemctl --user start grepaid`) brings it up; from a repo with `engine: v2`, `grepai init` registers it (adding it to the registry + opening its `.grepai/catalog_v2.db`), indexing proceeds under the background scheduler, and `grepai search`/`v2 search` returns that repo's daemon-served results with freshness metadata while a second registered repo's results stay isolated; default (`engine: v1`) installs are byte-for-byte unchanged; all gates green; each phase independently reviewed.
