# grepaid Daemon — Design Spec

**Date:** 2026-07-19
**Status:** Approved (design)
**Scope:** Build the `grepaid` host-level daemon, the Unix-socket JSON-RPC transport, engine-gated CLI thin clients, and a systemd user unit. This lands the daemon slice deferred across Phases 4/5 of `docs/GREPAI_V2_ARCHITECTURE_PLAN.md` (the "TIGHT scope" decision).

## 1. Objective

Make GrepAI v2 run as a single long-lived host service. Every dispatch target already exists and is tested — the catalog (`internal/enginev2/catalog/sqlite`), reconciler (`reconcile.Engine`), durable worker (`worker.Worker` + `artifacts`), host-wide scheduler (`scheduler.Engine`), and the transport-independent service surface (`service.Server` implementing `service.Service`). What is missing is the *process* that wires them together and the *transport* that exposes them. This spec builds exactly that, plus the client side needed to use it.

## 2. What already exists (do not rebuild)

- `internal/enginev2/service`: `Service` interface + `Server` implementing all 8 methods (Register, Reconcile, Search, Trace, Status, WaitFresh, Rebuild, DeadLetters). Query methods (Search/Status/WaitFresh/Trace) only read + embed; they never enqueue (invariant 3).
- `internal/enginev2/scheduler`: `Engine` with `New(cfg, Queue, Processor, Clock, seed)`, `Run(ctx)` (continuous round-robin drain with admission control, circuit breaker, jittered backoff, dead-lettering), `Submit`, `Stats`. `Queue` is satisfied by the catalog; `Processor` by `*worker.Worker`. `DefaultConfig()` provides safe local defaults.
- `internal/enginev2/rpc`: JSON-RPC 2.0 envelope (`Request`/`Response`/`Error`) + the 8 method-name constants. **Framing and dispatch are not implemented** — this spec adds them.
- `internal/enginev2/runtime`: the one-shot, single-worktree wiring (`Open` → reconcile → drain via `worker.Run`). The daemon is its long-running, multi-repo counterpart; reuse its component-assembly patterns (`Fingerprint`, `diskLoader`, `ensureSelfIgnore`, chunk params).
- `daemon/` (v1): reusable `GetDefaultLogDir` (XDG state dir) and the flock-based `WritePIDFile` singleton pattern.

## 3. Non-goals (explicitly out of this slice)

- Importing existing per-repo `.grepai/catalog_v2.db` indexes into the host-wide catalog (daemon reconciles from git truth; content-addressed vector cache limits re-embed cost).
- Repointing top-level `grepai search`/`watch`/`status` to the daemon *unconditionally* — they are gated behind `engine: v2` with automatic v1 fallback (see §6). Full production cutover (default `engine: v2`) is a later phase.
- Symbol/Trace population, RPG refresh, fsnotify watcher wiring, generation-scoped controlled rebuild (all remain as their current deferred stubs).
- Windows daemon service packaging (systemd unit is Linux-only; the binary still builds cross-platform).

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
      │    Queue=catalog  Processor=worker.Worker     │
      └──────────────────────────────────────────────┘
            |
            v
   <state>/grepai/catalog.db  (SQLite WAL, all repos)
            |
            v
   embedding endpoint (config-derived embedder)
```

### 4.1 Filesystem locations

Resolved once at startup, Linux/XDG conventions (mirror `GetDefaultLogDir`):

- **State dir:** `$XDG_STATE_HOME/grepai` else `~/.local/state/grepai`.
- **Catalog:** `<state>/catalog.db` (host-wide; holds every registered repo).
- **Socket:** `$XDG_RUNTIME_DIR/grepai/grepaid.sock` else `<state>/grepaid.sock`.
- **Lock:** `<state>/grepaid.lock` (flock, held for process lifetime).
- **Log:** existing `GetDefaultLogDir()` → `<state>/logs/grepaid.log`.

`daemon.socket` in config overrides the socket path for both server and clients.

### 4.2 Daemon lifecycle (`cmd/grepaid/main.go`)

1. Resolve paths; `MkdirAll` state + runtime dirs (0700).
2. Acquire the singleton flock. If held, exit non-zero with a clear "grepaid already running (pid N)" message.
3. Unlink a stale socket file if present (only after the lock is ours), then `net.Listen("unix", …)` with socket mode 0600.
4. Open the shared catalog (`sqlite.Open`). Build the config-derived embedder (reuse `embedder.NewFromConfig` semantics; the daemon's embedder is host-global — see §4.3).
5. Assemble `worker.Worker` (Processor) + `artifacts` builder + `scheduler.Engine` (from `scheduler.DefaultConfig()`, overridable via config) + `service.Server`.
6. `go scheduler.Run(ctx)`; `go rpc.Server.Serve(listener)`.
7. Block on SIGINT/SIGTERM. On signal: stop accepting new connections, cancel the scheduler ctx (drains in-flight dispatch via its WaitGroup), close the listener, `catalog.Close()`, remove the socket, release the lock. Exit 0.

### 4.3 Embedder & fingerprint (resolution of the multi-repo question)

The host-wide catalog namespaces artifacts and chunk-vector cache by `repository_id` **and** `indexing_fingerprint`. Different repos may carry different embedder configs. For this slice the daemon uses **one host-global embedder + fingerprint** derived from a daemon config block (defaulting to the current 4B embedder), and registration bootstraps each repo's generation with that fingerprint. A repo whose on-disk `.grepai/config.yaml` specifies an incompatible embedder is reconciled under the daemon's fingerprint (its vectors are the daemon's, not the repo's stale per-repo index). Per-repo heterogeneous embedders are a later refinement (the fingerprint already isolates them in-catalog; only the daemon's *selection* logic is deferred). This keeps the singleton-scheduler/single-budget invariant intact.

## 5. RPC transport (`internal/enginev2/rpc/`)

### 5.1 Server (`server.go`)

- `type Handler` = `service.Service` (the dispatch target).
- `Serve(l net.Listener, h service.Service)` accepts connections; one goroutine per connection.
- Per connection: read `Content-Length: N\r\n\r\n<N bytes>` frames (LSP-style), decode `rpc.Request`, dispatch by `Method` to the matching `service.Service` call, encode `rpc.Response`, write framed. Sequential per connection (simplest correct model; the WAL catalog + read-only query methods make concurrent connections safe).
- Dispatch table maps each `Method*` constant to (a) a params-decode into the method's `*Request` struct and (b) the service call. `id` is echoed verbatim for correlation. A request with no `id` (notification) gets no response.
- Errors → JSON-RPC codes: `-32700` parse (malformed frame/JSON), `-32600` invalid request (missing jsonrpc/method), `-32601` method not found, `-32602` invalid params (params decode fails), `-32603` internal (service returned an error — wrapped; `Data` = error string). A per-request panic is recovered into `-32603`, never killing the connection or process.

### 5.2 Client (`client.go`)

- `Dial(socketPath) (*Client, error)` — connects; distinguishes "daemon not running" (ENOENT/ECONNREFUSED) from other errors so callers can fall back to v1.
- `Call(ctx, method, params, *result) error` — monotonic request ids, framed write, framed read, matches response id, maps `rpc.Error` back to a typed Go error (code preserved). Respects `ctx` deadline.
- Thin typed wrappers per method (`Register`, `Reconcile`, `Search`, `Status`, `WaitFresh`, …) returning the `service` response structs, so CLI code is transport-agnostic.

## 6. CLI clients (`cli/`, gated)

- **Config:** add `Engine string` (`"v1"` default | `"v2"`) and a `Daemon` block (`Socket string`, optional scheduler overrides) to `config.Config`. Absent = `v1`. Loading an old config is unchanged.
- **`grepai daemon` (new):** `start` (spawn/exec `grepaid`, or instruct via systemd), `stop` (SIGTERM the pid), `status` (dial socket → `Status`/health, report running + socket + catalog path). `start` is a thin supervisor; systemd is the recommended production path.
- **`grepai v2 search|status|watch`:** talk to the daemon over the socket when reachable. `v2 watch` = ensure-registered + reconcile + tail status. `v2 search`/`v2 status` = RPC calls resolving worktree id from cwd (canonical root). If the daemon is unreachable these report the daemon-down error (they are the explicit v2 surface — no silent v1 fallback).
- **Top-level `grepai watch|search|status`:** when `engine: v2`, use the daemon path (watch degrades to ensure-registered + reconcile + status-tail **with a deprecation notice**; search/status → RPC); if the daemon is unreachable, **fall back to v1** behavior. When `engine: v1` (default), behavior is exactly as today. `grepai init` gains, under `engine: v2`, a best-effort register-with-daemon step.
- Worktree identity resolves from cwd's canonical root; ambiguous/missing identity is an error, never a fallback to another worktree's index.

## 7. systemd (`packaging/`)

- `packaging/grepaid.service` — a **user** unit (`systemctl --user`): `ExecStart=%h/.local/bin/grepaid`, `Restart=on-failure`, `RestartSec`, journald logging, `Type=simple`. No hardcoded socket (daemon resolves XDG). Documented install steps in `docs/`. Honors the existing 5-container/host memory constraints only indirectly (single process, not a container).

## 8. Error handling summary

| Layer | Failure | Behavior |
|-------|---------|----------|
| Daemon start | lock held | exit non-zero, "already running (pid N)" |
| Daemon start | stale socket | unlink (lock owned) then listen |
| RPC server | bad frame/JSON | `-32700`, connection continues |
| RPC server | unknown method | `-32601` |
| RPC server | bad params | `-32602` |
| RPC server | service error | `-32603`, `Data`=error string |
| RPC server | handler panic | recovered → `-32603`, connection + process survive |
| Client | socket absent/refused | typed daemon-down error → v1 fallback (top-level, engine:v2) |
| Service | unknown/ambiguous worktree | error, never cross-worktree fallback |
| Shutdown | SIGINT/SIGTERM | stop-accept → scheduler cancel+drain → close → unlink → exit 0 |

## 9. Testing

- **rpc unit:** frame round-trip; reader handling partial reads and multiple frames in one buffer; each error code path; id-correlation across interleaved ids; notification (no-id) produces no response; panic-in-handler recovered.
- **daemon integration:** real Unix socket + tmp git fixture repo. `register → reconcile → waitFresh(deadline) → search` returns the expected hit; `status` reports fresh. Second `grepaid` instance fails the singleton lock. SIGTERM drains and exits 0 with no leaked socket/lock.
- **cli:** `engine:v1` path unchanged (table test asserts no daemon dial); `engine:v2` with daemon-down falls back to v1; `grepai daemon status` against a running fixture daemon.
- **gates per phase:** `make build`, `make lint`, `go test ./... -race`, `go vet`, `gofmt` all green. Independent `codex-bg` review before advancing a phase.

## 10. Sequenced phases (one spec)

- **A — RPC transport:** `rpc/server.go` + `rpc/client.go` + tests. Isolated, no daemon yet. Gate A.
- **B — Daemon process:** `cmd/grepaid/main.go` (paths, singleton, listen, wire catalog+embedder+scheduler+service, background scheduler, graceful shutdown) + integration test. Gate B.
- **C — CLI clients:** config `Engine`/`Daemon` fields + `grepai daemon` command + `v2 search/status/watch` daemon path + gated top-level fallback + `init` register. Gate C.
- **D — Packaging:** `packaging/grepaid.service` + install/ops docs + `Makefile` target for the `grepaid` binary. Gate D.

## 11. Definition of done

`grepaid` builds and runs; `grepai daemon start` (or `systemctl --user start grepaid`) brings it up; from a repo with `engine: v2`, `grepai init` registers it, indexing proceeds under the background scheduler, and `grepai search`/`v2 search` returns daemon-served results with freshness metadata; default (`engine: v1`) installs are byte-for-byte unchanged; all gates green; each phase independently reviewed.
