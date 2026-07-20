# grepaid Phase C — CLI Thin Clients Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Wire the `grepai` CLI to the daemon: an `engine` config field, a `grepai daemon start|stop|status` control command, `grepai v2 search|status|watch` daemon subcommands, and `engine:v2`-gated top-level `search|watch|status` — with **v1 inert and loud failures, no silent v1 fallback**.

**Architecture:** `engine: v1` (default/unset) leaves every existing command byte-identical (the daemon code is never dialed). `engine: v2` makes v1 inert for that repo and routes the top-level commands through `daemonctl.EnsureDaemon` + `rpc.Client`. A broken v2 path fails loudly. Coexistence of v1 and v2 is allowed (operator's concern), never corrupting. Phase C of `docs/superpowers/specs/2026-07-19-grepaid-daemon-design.md` §5.

**Tech Stack:** Go 1.24.2, cobra (existing CLI), `internal/enginev2/{daemoncfg,daemonctl,rpc,service}`.

## Global Constraints

- Module `github.com/yoanbernabeu/grepai`; Go floor 1.24.2.
- **No silent v1 fallback.** Under `engine:v2`, a daemon/embedder failure surfaces loudly to the user; never quietly serve v1.
- **v1 default is untouched.** With `engine` unset/`v1`, no code path dials the daemon; existing behavior and output are byte-identical. A CLI test asserts no daemon dial on v1.
- The daemon binary is `grepaid`; the CLI finds it via `exec.LookPath("grepaid")`, else a sibling of the running `grepai` executable (`os.Executable()` dir), else `grepaid` on PATH — error loudly if not found.
- Gates (Gate C): `make build`, `make lint`, `go test ./... -race`, `go vet`, `gofmt` green; independent `codex-bg` review before Phase D.
- Commit per task, conventional commits.

## Scouted structure (verified 2026-07-19)

- `config.Config` (config/config.go:63) top-level struct with `yaml` tags; `config.Load(projectRoot)` (line 496); `config.FindProjectRoot()` (line 678) walks up to `.grepai`.
- `cli/root.go` registers commands in `init()`; `cli/v2.go` has `v2Cmd` with `v2IndexCmd`/`v2SearchCmd` and `runV2Index`/`runV2Search`.
- Top-level `searchCmd` (cli/search.go, `RunE: runSearch`), `watchCmd` (cli/watch.go), `statusCmd` (cli/status.go), `initCmd` (cli/init.go).
- `daemoncfg.ResolvePaths()` → `Paths{Socket,Lock,...}`; `daemonctl.EnsureDaemon(ctx, socket, binPath, timeout)`, `StopDaemon(lock, timeout)`, `ReadPID(lock)`; `rpc.Client` typed wrappers (`Search`/`Status`/`Register`/`Reconcile`), `rpc.Dial`, `rpc.ErrDaemonDown`.
- `service.SearchRequest{WorktreeID, Query}`, `SearchResponse{Results []core.SearchHit, ActiveGeneration, Fresh}`, `core.SearchHit{Path,Score,Content,StartLine,EndLine}`, `StatusRequest/Response`, `RegisterRequest{Root}`/`RegisterResponse{RepositoryID,WorktreeID}`, `ReconcileRequest{WorktreeID}`.

## File Structure

- `config/config.go` — **modify**: add `Engine string` + `Daemon DaemonConfig{Socket string}`.
- `internal/enginev2/daemonctl/locate.go` — **create**: `LocateBinary()` (grepaid path).
- `cli/daemonclient.go` — **create**: shared helper `dialDaemon(cmd) (*rpc.Client, worktreeID, error)` (resolve socket from daemoncfg + optional config override, EnsureDaemon, canonical cwd → WorktreeID via Register-or-resolve).
- `cli/daemon.go` — **create**: `grepai daemon start|stop|status`.
- `cli/v2_daemon.go` — **create**: `grepai v2 search|status|watch` subcommands.
- `cli/search.go`, `cli/watch.go`, `cli/status.go`, `cli/init.go` — **modify**: add the `engine:v2` fork at the top of each RunE.
- `*_test.go` for config + a cli engine-gate test.

## Task 1: `engine` + `daemon.socket` config fields

**Files:** Modify `config/config.go`; Test `config/config_test.go` (append).

**Interfaces:** `Config.Engine string yaml:"engine,omitempty"` (""/"v1" = v1, "v2" = v2); `Config.Daemon DaemonConfig`; `type DaemonConfig struct { Socket string yaml:"socket,omitempty" }`; helper `func (c *Config) EngineV2() bool { return c.Engine == "v2" }`.

- [ ] **Step 1: Failing test** — append to `config/config_test.go`:

```go
func TestEngineV2Detection(t *testing.T) {
	if (&Config{}).EngineV2() {
		t.Fatal("empty engine must be v1")
	}
	if (&Config{Engine: "v1"}).EngineV2() {
		t.Fatal("v1 must not be v2")
	}
	if !(&Config{Engine: "v2"}).EngineV2() {
		t.Fatal("v2 must be detected")
	}
}
```

- [ ] **Step 2: Run — FAIL** (`EngineV2` undefined).
- [ ] **Step 3: Implement** — add the fields to the `Config` struct (near `Version`) and the `DaemonConfig` type + `EngineV2()`. Confirm the loader tolerates the new field (it will — additive struct field; old configs default to "").
- [ ] **Step 4: Run — PASS.** Also run the full config package tests to confirm no regression.
- [ ] **Step 5: Commit** — `git commit -m "feat(config): add engine (v1|v2) + daemon.socket fields"`

## Task 2: `LocateBinary` for grepaid

**Files:** Create `internal/enginev2/daemonctl/locate.go` + `locate_test.go`.

**Interfaces:** `func LocateBinary() (string, error)` — `exec.LookPath("grepaid")`; else `filepath.Join(dir(os.Executable()), "grepaid")` if it exists; else error "grepaid not found (build it with `make build-daemon` or put it on PATH)".

- [ ] Steps: test that when a fake `grepaid` is on PATH it is found; implement; commit `feat(daemonctl): locate the grepaid binary`.

## Task 3: shared `dialDaemon` CLI helper

**Files:** Create `cli/daemonclient.go` + test.

**Interfaces:** `func daemonSocket(cfg *config.Config) (string, error)` (cfg.Daemon.Socket if set, else `daemoncfg.ResolvePaths().Socket`); `func ensureDaemonClient(ctx, cfg) (*rpc.Client, error)` (LocateBinary + EnsureDaemon, 8s; a failure is returned loudly). Worktree id resolution: the CLI resolves the project root (`config.FindProjectRoot`), canonicalizes it, and uses `core.WorktreeID(canonicalRoot)`; ensuring registration calls `client.Register({Root: root})` which is idempotent and returns the canonical `WorktreeID`.

- [ ] Steps: helper + a unit test that `daemonSocket` honors the config override; commit `feat(cli): daemon client helper (socket resolve + ensure + worktree id)`.

## Task 4: `grepai daemon start|stop|status`

**Files:** Create `cli/daemon.go`; register in `cli/root.go`.

- `start`: `LocateBinary` → `daemonctl.EnsureDaemon` → print "grepaid running (pid N)" from `daemonctl.ReadPID`.
- `stop`: `daemonctl.StopDaemon(paths.Lock, 5s)` → print stopped / "not running".
- `status`: `rpc.Dial`; if `ErrDaemonDown` print "not running"; else `client.Status` on the cwd worktree if registered, plus registered-repo count from `registry.Load(paths.Registry)`; print socket + pid + repo count.

- [ ] Steps: implement the three subcommands; a light test drives `status` against no-daemon (prints not-running, exit 0) and `start`+`status`+`stop` against the built binary in a tmp state dir (mirror the Phase B lazy-start test env). Commit `feat(cli): grepai daemon start|stop|status`.

## Task 5: `grepai v2 search|status|watch` (daemon subcommands)

**Files:** Create `cli/v2_daemon.go`; register under `v2Cmd` in its `init()`.

- `v2 search <query>`: `ensureDaemonClient` → `Register({Root})` (idempotent) → `client.Search({WorktreeID, Query})` → render results (reuse the JSON/plain rendering shape of `runV2Search`).
- `v2 status`: `Register` → `client.Status` → print generation + fresh + pending + dead-letters.
- `v2 watch`: `Register` + `client.Reconcile` + poll `client.Status` printing freshness until fresh (then exit) — a foreground "ensure indexed" with a deprecation-style note that continuous fs-watching is a later slice.

- [ ] Steps: implement; a light integration test (build grepaid, tmp state dir, a git fixture) that `v2 search` returns the file's own hit. Commit `feat(cli): grepai v2 search|status|watch via the daemon`.

## Task 6: `engine:v2`-gated top-level commands + `init`

**Files:** Modify `cli/search.go`, `cli/watch.go`, `cli/status.go`, `cli/init.go`.

Each top-level RunE gains, at the very top (after loading config), a fork:

```go
cfg, err := config.Load(root)
if err != nil { return err }
if cfg.EngineV2() {
    return runSearchV2Daemon(cmd, args, cfg) // the v2 path; loud on error, no v1 fallback
}
// ... existing v1 body unchanged ...
```

- `search` → delegate to the `v2 search` daemon path.
- `status` → the `v2 status` daemon path.
- `watch` → the `v2 watch` daemon path (ensure-registered + reconcile + status-tail) with a deprecation notice that per-repo watching is now daemon-managed.
- `init` → after writing config, if `--engine v2` was passed (add the flag; default v1), best-effort `ensureDaemonClient` + `Register` (a failure warns but does not fail `init`).

- [ ] **Critical test:** `engine:v1` (or unset) asserts the daemon is never dialed — the v1 body runs unchanged. Do this by a table test invoking `runSearch` with a v1 config in a repo with a v1 index and asserting normal results with no socket activity (e.g. point the socket env at a path that would error if dialed, and assert success).
- [ ] Steps: implement each fork; keep the v1 body byte-identical (only prepend the fork). Commit `feat(cli): route top-level search/watch/status to the daemon under engine:v2`.

## Gate C

- [ ] `gofmt -l`, `go vet`, `make build`, `make lint`, `go test ./... -race` green.
- [ ] Manual smoke: in a v1 repo, `grepai search` behaves exactly as before; set `engine: v2`, `grepai search` uses the daemon (or fails loudly if it can't start).
- [ ] Independent `codex-bg` review; address findings before Phase D.

## Self-Review notes (author)

- **No-fallback invariant** is the load-bearing correctness property: every v2 path returns its error to the user; grep the diff for any `if err != nil { ...v1... }` pattern and remove it.
- **v1 untouched:** the only edit to existing RunE handlers is prepending the `if cfg.EngineV2()` fork; the v1 body is unchanged. The engine-gate test enforces no-dial on v1.
- **Grep-confirm at implementation:** the exact `config.Config` field block + `config.Load` strictness; the v1 `runSearch`/`runWatch`/`runStatus` signatures to delegate cleanly; `runV2Search`'s rendering to reuse.
