# grepaid Daemon — Design Spec

**Date:** 2026-07-19
**Status:** Approved (design)
**Code-verified:** 2026-07-19 — every §3 anchor and the §4.4 interface union checked against the tree; no signature collisions. Findings folded into §2, §4.2–§4.6, §7, §8.
**Scope:** Build the `grepaid` host daemon, the Unix-socket JSON-RPC transport, a per-repo catalog substrate (`catalogset` + registry), a solid **lazy-start** lifecycle (no systemd), and engine-gated CLI thin clients. This lands the daemon slice deferred across Phases 4/5 of `docs/GREPAI_V2_ARCHITECTURE_PLAN.md` (the "TIGHT scope" decision).

## 1. Objective

Make GrepAI v2 run as a single long-lived host process. Every dispatch target already exists and is tested — the catalog (`internal/enginev2/catalog/sqlite`), reconciler (`reconcile.Engine`), durable worker (`worker.Worker` + `artifacts`), host-wide scheduler (`scheduler.Engine`), and the transport-independent service surface (`service.Server` implementing `service.Service`). What is missing is the *process* that wires them together, the *transport* that exposes them, and a *multi-repo substrate* so one daemon serves many repositories. This spec builds exactly that, plus the client side needed to use it.

**Operational model:** grepai is still *installed per repo* (`grepai init` writes `<repo>/.grepai/config.yaml`), but repos with `engine: v2` **share one host daemon**, started lazily on first use. The index for each repo stays in that repo.

## 2. Catalog model: one daemon, one DB file per repo

A single `grepaid` process serves every registered repository, but each repository keeps its **own** SQLite catalog at `<repo>/.grepai/catalog_v2.db` (the existing v2 convention). There is no host-wide merged catalog. This is possible without touching the scheduler or service because both consume **interfaces**:

- `scheduler.Engine` takes an injected `scheduler.Queue`.
- `service.Server` takes an injected `service.Catalog`.
- `worker.Worker` / `artifacts` builder take an injected catalog (the durable commit surface).
- `reconcile.Engine` takes an injected `reconcile.CatalogReader`, and `Reconcile(ctx, wt)` is worktree-parameterized — one reconciler instance serves every repo through the catalogset.

We add one adapter, `catalogset`, that owns `map[RepositoryID]*sqlite.Catalog` and implements the **union** of those catalog-facing interfaces by routing per-repo calls to the right catalog and fanning out the host-wide aggregate calls. The scheduler still sees a single logical queue and enforces a single global in-flight budget (invariant 2: one host, one scheduler) — the budget lives in the scheduler's semaphores, not in any DB.

Rationale (vs. a single merged catalog):

- **Isolation guardrail against cross-repo hallucination.** A query resolves against exactly one repo's catalog; it is *physically impossible* for a search in repo A to surface repo B's code. With a merged catalog, that isolation would be a `WHERE project = …` filter — one missed predicate away from bleeding unrelated code into an agent's context. Per-repo files make the boundary structural, not a query convention.
- **Reuses existing per-repo indexes** — repos already indexed at `.grepai/catalog_v2.db` are opened as-is; no forced re-embed (subject to the fingerprint/schema checks in §4.3/§4.6).
- **Write-lock isolation** — SQLite WAL is single-writer *per file*; per-repo DBs let different repos commit index updates in parallel if `MaxIndexInflight` is ever raised above 1.
- **Corruption blast radius** — one bad file cannot sink every repo.
- **Natural lifecycle/portability** — the index lives in the repo's `.grepai/`, travels with it, and is removed with it.
- A merged catalog would buy nothing on cross-repo embedding dedup: chunk-vector cache identity already includes the `repository_id` namespace (§5.3), so identical text in two repos re-embeds either way.

**Cost we accept:** cross-repo / workspace search is a query-time fan-out (query each member repo's catalog, merge-rank) rather than one SQL query. The `catalogset` already fans out for aggregates, so this is a merge step, not new plumbing — but it is deferred (§9).

## 3. What already exists (do not rebuild)

- `internal/enginev2/service`: `Service` interface + `Server` implementing all 8 methods (Register, Reconcile, Search, Trace, Status, WaitFresh, Rebuild, DeadLetters). Query methods only read + embed; they never enqueue (invariant 3). Takes a `Catalog` interface.
- `internal/enginev2/scheduler`: `Engine` with `New(cfg, Queue, Processor, Clock, seed)`, `Run(ctx)` (continuous round-robin drain with admission control, circuit breaker, jittered backoff, dead-lettering), `Submit`, `Stats`. `Queue`/`Processor` are injected interfaces. `DefaultConfig()` provides safe local defaults.
- `internal/enginev2/rpc`: JSON-RPC 2.0 envelope (`Request`/`Response`/`Error`) + the 8 method-name constants. **Framing and dispatch are not implemented** — this spec adds them.
- `internal/enginev2/runtime`: the one-shot, single-worktree wiring (`Open` → reconcile → drain via `worker.Run`). The daemon reuses its assembly patterns (`Fingerprint`, `diskLoader`, `ensureSelfIgnore`, chunk params).
- `daemon/` (v1): reusable `GetDefaultLogDir` (XDG state dir) and the flock-based `WritePIDFile` singleton pattern.

## 4. Architecture

```
grepai CLI (thin client, engine:v2)          MCP (later)
    |  dial socket; if down, lazily spawn grepaid, wait, retry
    |  JSON-RPC 2.0 over Unix socket, Content-Length framed
    v
grepaid (singleton via flock, one host, lazily started)
┌──────────────────────────────────────────────┐
│ rpc.Server  ── goroutine per connection        │
│    dispatch method → service.Service           │
│ service.Server (existing)                      │
│ scheduler.Engine.Run(ctx)  ── background loop  │
│    Queue=catalogset  Processor=worker.Worker   │
│ catalogset  ── map[repo]->*sqlite.Catalog      │
└──────────────────────────────────────────────┘
   |            |            |
   v            v            v
repoA/.grepai  repoB/.grepai  repoC/.grepai
 catalog_v2.db  catalog_v2.db  catalog_v2.db

host state: <state>/grepai/
  registry.json   registered repos + roots + cursors (re-opened on restart)
  daemon.json     host-global daemon settings (embedder, scheduler, socket)
  grepaid.sock    Unix socket        grepaid.lock   singleton flock
  logs/grepaid.log
```

### 4.1 Filesystem locations

Resolved once at startup, Linux/XDG conventions (mirror `GetDefaultLogDir`):

- **State dir:** `$XDG_STATE_HOME/grepai` else `~/.local/state/grepai`.
- **Per-repo catalog:** `<repo>/.grepai/catalog_v2.db` (unchanged; `ensureSelfIgnore` keeps it out of reconciliation).
- **Host registry:** `<state>/grepai/registry.json` (§4.4).
- **Host daemon config:** `<state>/grepai/daemon.json` (§4.3) — global settings, `GREPAID_*` env overrides.
- **Socket:** `$XDG_RUNTIME_DIR/grepai/grepaid.sock` else `<state>/grepaid.sock`.
- **Lock:** `<state>/grepaid.lock` (flock, held for process lifetime — the authoritative liveness signal).
- **Log:** `GetDefaultLogDir()` → `<state>/logs/grepaid.log`.

### 4.2 Lazy-start lifecycle (no systemd)

There is no service manager. The daemon is **spawned on demand** by the first client that needs it and stays resident, kept correct by the flock. `grepai daemon start|stop|status` exists for explicit control/debugging, but is not required for normal use.

**Client side — `ensureDaemon(socketPath)` (in the CLI/RPC client):**
1. `Dial(socket)`. On success, use it.
2. On daemon-down (ENOENT/ECONNREFUSED): spawn `grepaid` **detached** (`SysProcAttr{Setsid:true}`, stdio → log file, not `Wait`ed), so it outlives the client.
3. Poll `Dial` with a bounded deadline (e.g. 5 s, short backoff). Connect → proceed. Timeout → clear error ("grepaid failed to start; see `<log>`").

**Daemon side — `cmd/grepaid/main.go` startup:**
1. Resolve paths; `MkdirAll` state + runtime dirs (0700).
2. **Acquire the singleton flock.** If already held, another daemon is live → exit 0 quietly (a lazy-start race loser; the winner owns the socket the client is polling).
3. Unlink a stale socket if present (safe: we hold the lock), then `net.Listen("unix", …)`, socket mode 0600.
4. Load `daemon.json` (+ `GREPAID_*` env); build the host-global embedder + fingerprint (§4.3).
5. Load `registry.json`; open each registered repo's catalog into the `catalogset` (skip + log a repo whose dir is gone or whose catalog fails the fingerprint/schema guard — §4.3/§4.6).
6. Assemble the single `worker.Worker` (Processor; commits route through the catalogset, builds through the per-repo builder router — §4.4) + `scheduler.Engine` (`daemon.json`, defaults from `scheduler.DefaultConfig()`) + `service.Server` (Catalog = catalogset, Reconciler = one `reconcile.Engine` over the catalogset).
7. `go scheduler.Run(ctx)`; `go rpc.Server.Serve(listener)`.
8. Block on SIGINT/SIGTERM. On signal: stop accepting, cancel the scheduler ctx (drains in-flight via its WaitGroup), close the listener, close all catalogs, persist the registry, remove the socket, release the lock. Exit 0.

**Why this is solid (the race analysis):** the flock — not the socket file — is the single source of truth. Two clients that both observe "down" and both spawn `grepaid` produce two processes; exactly one wins the flock and listens, the other exits 0 at step 2. Both clients are merely polling the socket, which the winner creates, so both connect. A crashed daemon releases its flock (OS-guaranteed) and leaves a stale socket; the next client's spawn wins the freed lock, unlinks the stale socket, and relistens. No orphaned lock, no double-bind, no lost writes (jobs are durable in the catalogs).

The daemon stays resident (indexing keeps draining in the background). If it crashes between commands, the next `grepai v2 …` transparently respawns it. Idle auto-exit is out of scope (§9).

### 4.3 Embedder, fingerprint & host config (`daemon.json`)

Global daemon settings live host-wide in `<state>/grepai/daemon.json`, **not** in any repo's `.grepai/config.yaml` (a host daemon has no single owning repo). Fields: embedder (provider/endpoint/model/dimensions), scheduler tuning (overrides `scheduler.DefaultConfig()`), and an optional socket override. Every field is overridable by a `GREPAID_*` env var (e.g. `GREPAID_SOCKET`, `GREPAID_EMBEDDER_ENDPOINT`). Defaults target the current 4B embedder. A first run with no `daemon.json` writes one with defaults.

The daemon uses **one host-global embedder + indexing fingerprint** derived from `daemon.json`. Every repo is registered/indexed under that fingerprint; per-repo DBs keep the data isolated regardless. On opening a repo catalog whose active generation carries a *different* fingerprint, the daemon does **not** hard-error like interactive `runtime.Open` — it logs and rolls the repo forward (a background service must not wedge on one stale repo). Pinned sequence: `CreateGeneration(active+1, daemonFP)` → `SetActiveGeneration` (both exist in the sqlite catalog) → the next reconcile enqueues under the new generation. That repo's results are transiently empty until reindexed — correct, not a regression: the old vectors are unrankable against queries embedded by the daemon's embedder, and per-generation views keep the mismatched dimensions out of ranking. (The daemon's reconcile retires old-generation jobs per path via `UpsertJob` supersession, rather than `runtime.Index`'s `DeleteJobsForWorktree` pre-clear.) Matching-fingerprint catalogs are reused as-is.

**Deferred (natural next refinement):** honoring each repo's *own* config embedder/fingerprint — per-repo DBs already make this natural for storage; it additionally needs a per-repo embedder router in the Processor and in query embedding (§9).

### 4.4 catalogset + registry

- `catalogset` (new, `internal/enginev2/…`): owns `map[RepositoryID]*sqlite.Catalog` under a mutex. Implements the union of the **four** catalog-facing interfaces — `scheduler.Queue`, `service.Catalog`, `worker.Catalog`, and `reconcile.CatalogReader` (~25 distinct methods; pre-verified 2026-07-19: the five names shared across them — `UpsertJob`, `DeadLetterJob`, `WorktreeInfo`, `GenerationFingerprint`, `ActiveGeneration` — carry identical signatures, so one type satisfies all four).
  - **Routing methods** delegate to the target repo's catalog, keyed by an explicit repo id or — since `core.Job` and most service calls carry only a `WorktreeID` — via an internal worktree→repo map (learned at `RegisterWorktree`, rehydrated from `registry.json` at startup; today wt == repo == canonical root, but keep the map explicit). An op for an unregistered repo/worktree is an error, never a silent cross-repo write.
  - **Aggregate methods** (`RepositoriesWithPendingJobs`, `QueueDepthByPriority`, `DeadLetterCount`, `RequeueClaimedJobs`) fan out over the open catalogs and combine.
  - `worker.Catalog.ClaimNextJob` (host-wide, no repo arg) is implemented as a fan-out claim for interface completeness only — the daemon path never calls it (the scheduler claims per-repo via `ClaimNextJobInRepo` and calls `ProcessClaimed` directly).
  - **Builder router** (companion type): `artifacts.ChunkCache.GetChunkVector(chunkID)` carries no repo id, so chunk-cache reads *cannot* route through the catalogset. A routing `worker.Builder` dispatches by `BuildRequest.Key.RepositoryID` to a per-repo `artifacts.DefaultBuilder`, each holding that repo's catalog as its `ChunkCache`.
  - `Register(repo, root)` opens/creates that repo's catalog (and its builder) and adds both; `Close()` closes all.
- `registry` (new): `registry.json`, atomic write (temp + rename, fsync temp + parent dir). Each entry (borrowing claude-mem's `chroma-sync-state.json`) carries `{repositoryID, root, catalogPath, activeGeneration, lastReconciledAt, pendingCount}` so `grepai daemon status` and restart re-open are cheap and don't require opening every catalog first. Loaded at startup, updated on register/reconcile. Worktree→repo id derives from the canonical root/git-common-dir exactly as `service.Server.Register` does today.

**Phase B obligations surfaced by the Phase 0 independent review (do not lose):**
- **Catalog quarantine / aggregate isolation.** `catalogset`'s fan-out aggregates (`RepositoriesWithPendingJobs`, `QueueDepthByPriority`, `DeadLetterCount`, `RequeueClaimedJobs`) fail fast on the first catalog error. Because the scheduler calls `RepositoriesWithPendingJobs` before every claim, one persistently-erroring catalog would stall **all** repos — contradicting the per-repo blast-radius promise. Phase B must quarantine a repeatedly-failing catalog (remove it from the live set, surface it in `status`/logs, keep serving the rest) rather than letting its error propagate through the aggregate.
- **Registry write-serialization.** `Registry.Upsert`+`Save` are not internally synchronized; the daemon must serialize registry mutations (single owner goroutine or a mutex) since register/reconcile updates can race.
- **Per-repo builder assembly.** Phase B builds one `artifacts.DefaultBuilder` per repo via `Set.ChunkCache(repo)` (shares the open handle) and registers each in the `BuilderRouter` — never a second `sqlite.Open` of the same file.

### 4.5 RPC transport (`internal/enginev2/rpc/`)

**Server (`server.go`):** dispatch target is `service.Service`. `Serve(l net.Listener, h service.Service)` accepts connections, one goroutine per connection. Per connection: read `Content-Length: N\r\n\r\n<N bytes>` frames (LSP-style), decode `rpc.Request`, dispatch by `Method` to the matching call, encode + write framed `rpc.Response`. Sequential per connection (per-repo WAL catalogs + read-only query methods make concurrent connections safe). A dispatch table maps each `Method*` constant to a params-decode + service call; `id` is echoed verbatim; a no-`id` notification gets no response. Errors → JSON-RPC codes: `-32700` parse, `-32600` invalid request, `-32601` method not found, `-32602` invalid params, `-32603` internal (service error wrapped; `Data` = error string). A per-request panic is recovered into `-32603`, never killing the connection/process.

**Client (`client.go`):** `Dial(socketPath)` distinguishes daemon-down (ENOENT/ECONNREFUSED) from other errors (drives lazy-start). `Call(ctx, method, params, *result)` uses monotonic ids, framed write/read, id-matching, maps `rpc.Error` back to a typed Go error (code preserved), respects `ctx` deadline. Thin typed per-method wrappers return the `service` response structs so CLI code is transport-agnostic. `ensureDaemon` (§4.2) wraps `Dial`.

### 4.6 Catalog schema guard (light)

Borrowing claude-mem's explicit `schema_versions` discipline, but minimal for this slice: the daemon checks each opened catalog's schema version and **refuses (skips + logs) a catalog newer than the binary understands**, rather than risking corruption. The stamp already exists — the sqlite catalog records applied migrations in `schema_migrations` — but its readers are unexported; Phase 0 adds a small exported accessor (e.g. `sqlite.Catalog.SchemaVersion(ctx)`) for the guard to compare against the binary's supported version. Older-but-current schemas are used as-is; a full migration framework + pre-migration `backups/` snapshots are deferred (§9) — noted here so the long-lived multi-version daemon has a guardrail from day one.

## 5. CLI clients (`cli/`, gated)

- **Config:** add `Engine string` (`"v1"` default | `"v2"`) and an optional per-repo `Daemon.Socket` override to `config.Config`. Absent = `v1`; loading an old config is unchanged. (Global daemon settings live in `daemon.json`, not here.) *[Superseded during the merge-gate review: the per-repo `Daemon.Socket` override was removed as incoherent with the single host daemon + singleton lock; the socket is strictly host-scoped — `GREPAID_SOCKET` > `daemon.json` > XDG.]*
- **`grepai daemon` (new):** `start` (spawn detached `grepaid`, wait for socket), `stop` (SIGTERM the pid from the lock/pidfile), `status` (dial → health + registered-repo count from the registry).
- **Command taxonomy (resolved during Phase C):** the daemon-backed *user* surface is the **top-level** `grepai search|watch|status` under `engine:v2` (below) — not a separate `grepai v2 search|status|watch`, which would clobber the existing one-shot `grepai v2 search`/`index` (the standalone-runtime tools used by migrate/parity, kept unchanged) and duplicate the top-level gating. Explicit daemon control is `grepai daemon start|stop|status`.
- **Top-level `grepai watch|search|status`:** the `engine` field selects the active engine. `engine: v1` (default/unset) keeps today's behavior exactly. `engine: v2` makes **v1 inert** for that repo and routes the command through `ensureDaemon` + the daemon (watch degrades to ensure-registered + reconcile + status-tail **with a deprecation notice**). There is **no silent v1 fallback**: if the daemon cannot be started, or the v2 path errors, the command **fails loudly** — a broken v2 must complain (on v1 or v2), never silently serve stale/inert v1 results. `grepai init` gains, under `engine: v2`, a best-effort ensure-registered step.
- **Coexistence is allowed, not prevented.** A repo may have both a v1 index and a v2 catalog, or run a v1 `grepai watch` while registered with the v2 daemon; that is redundant, the operator's concern, and never corrupting (separate files, separate processes). The `engine` field is what keeps a repo in one lane — dropping `engine: v2` on a v1 repo makes v1 inert.
- Worktree identity resolves from cwd's canonical root; ambiguous/missing identity is an error, never a fallback to another worktree's index.

## 6. Error handling summary

| Layer | Failure | Behavior |
|-------|---------|----------|
| Client ensureDaemon | socket down | spawn detached grepaid, poll socket to deadline |
| Client ensureDaemon | spawn didn't come up | loud error pointing at the log; **no v1 fallback** (v1 is inert under engine:v2) |
| v2 path errors (embedder/backend down) | surface loudly to the caller; never silently serve v1 |
| Daemon start | flock held (lazy-start race loser) | exit 0 quietly; winner owns the socket |
| Daemon start | stale socket (crash) | unlink (lock owned) then listen |
| Daemon start | registered repo dir gone / fingerprint mismatch / schema too new | log + skip (or fresh generation), never wedge |
| catalogset | op for unregistered repo | error, never silent cross-repo write |
| RPC server | bad frame/JSON · unknown method · bad params · service error · panic | `-32700` · `-32601` · `-32602` · `-32603` · recovered `-32603` (conn+proc survive) |
| Service | unknown/ambiguous worktree | error, never cross-worktree fallback |
| Shutdown | SIGINT/SIGTERM | stop-accept → scheduler cancel+drain → close catalogs → persist registry → unlink → exit 0 |

## 7. Testing

- **catalogset unit:** routes per repo (incl. the worktree→repo map rehydrated from a registry snapshot); aggregates union/sum; builder router dispatches to the right repo's cache; unregistered-repo/worktree op errors; race-clean.
- **registry unit:** round-trip load/save; atomic temp+rename; append/update; corrupt/missing handling.
- **rpc unit:** frame round-trip; partial reads and multiple frames in one buffer; each error code; id-correlation across interleaved ids; no-id notification → no response; panic-in-handler recovered.
- **daemon integration:** real socket + two tmp git fixtures. Register both; reconcile; `waitFresh(deadline)`; `search` in each returns that repo's hit and **not** the other's (isolation guardrail); `status` fresh; `DeadLetterCount` aggregates. **Lazy-start:** with no daemon, an `ensureDaemon` call spawns one and connects; two concurrent `ensureDaemon` calls yield exactly one live daemon (flock race); killing the daemon and calling again respawns it with no orphaned lock/socket. SIGTERM drains, exits 0, registry persisted. A repo whose catalog carries a mismatched fingerprint rolls to a fresh generation and reindexes (transiently empty, then fresh hits — §4.3); a catalog stamped with a newer schema version is skipped with a log while the daemon serves the rest.
- **cli:** `engine:v1` unchanged (asserts no daemon dial); `engine:v2` with spawn-failure returns a loud error (no v1 fallback); `grepai daemon status` against a running fixture.
- **gates per phase:** `make build`, `make lint`, `go test ./... -race`, `go vet`, `gofmt` green. Independent `codex-bg` review before advancing.

## 8. Sequenced phases (one spec)

- **Phase 0 — Multi-repo substrate:** `catalogset` (four-interface union — §4.4 — incl. the worktree→repo routing map) + per-repo builder router + `registry` + exported schema-version accessor + guard + tests. Isolated. Gate 0.
- **Phase A — RPC transport:** `rpc/server.go` + `rpc/client.go` (+ `Dial` daemon-down detection) + tests. Isolated. Gate A.
- **Phase B — Daemon process + lazy-start:** `cmd/grepaid/main.go` (paths, `daemon.json`, flock singleton + race handling, listen, load registry + open catalogs, wire embedder/scheduler/service, background scheduler, graceful shutdown) + `ensureDaemon` spawn helper + integration test. Gate B.
- **Phase C — CLI clients:** config `Engine`/`Daemon.Socket` + `grepai daemon start|stop|status` + `v2 search/status/watch` via `ensureDaemon` + `engine:v2`-gated top-level (v1 inert, loud on failure, no silent fallback) + `init` ensure-registered. Gate C.
- **Phase D — Binary + docs:** `Makefile` target for the `grepaid` binary + operational docs (lazy-start model, `daemon.json`, state-dir layout, today-vs-daemon migration, the interim watcher gap). **No systemd unit.** Gate D.

## 9. Non-goals (explicitly out of this slice)

- **systemd/service-manager packaging** — replaced by lazy-start. (Could be added later for headless/boot-time indexing, but not needed for the interactive model.)
- Per-repo heterogeneous embedders/fingerprints (host-global embedder this slice; substrate does not preclude it — §4.3).
- Cross-repo / workspace search (query-time fan-out over member catalogs) — the `catalogset` makes it a merge step, but it is deferred.
- Repointing top-level `grepai search`/`watch`/`status` *unconditionally* (gated behind `engine: v2`, which makes v1 inert for that repo; a machine-wide default-`engine: v2` cutover is later).
- Symbol/Trace population, RPG refresh, fsnotify watcher wiring, generation-scoped controlled rebuild (remain deferred stubs). **Consequence:** freshness is reconcile-on-command, not live fs-event-driven, in this slice.
- Full catalog migration framework + pre-migration backups (only the refuse-on-newer schema guard lands now — §4.6).
- Idle auto-exit of the daemon; lazy-open/idle-close of catalog handles (open-all-on-start; add an LRU later if repo count grows large).
- Windows daemon (the binary builds cross-platform; the detached-spawn/flock path is validated on Linux only this slice).

## 10. Definition of done

`grepaid` builds; the first `grepai v2 …` command in an `engine: v2` repo **lazily starts** the shared daemon (no manual step, no systemd), registers the repo (adding it to `registry.json` + opening its `.grepai/catalog_v2.db`), indexing proceeds under the background scheduler, and `grepai search`/`v2 search` returns that repo's results with freshness metadata while a second registered repo's results stay isolated (the guardrail). Concurrent first-uses never produce two daemons; a crashed daemon transparently respawns on next use with no orphaned lock/socket. Default (`engine: v1`) installs are byte-for-byte unchanged; all gates green; each phase independently reviewed.
