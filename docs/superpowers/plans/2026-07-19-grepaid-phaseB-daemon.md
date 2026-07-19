# grepaid Phase B — Daemon Process + Lazy-Start Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Ship the `grepaid` binary: a singleton host daemon that opens every registered repo's catalog into a `catalogset`, runs the scheduler continuously, serves the RPC surface over a Unix socket, and is started lazily by clients — with graceful shutdown, catalog quarantine, and serialized registry writes.

**Architecture:** `cmd/grepaid/main.go` resolves host paths, takes a flock singleton, loads `daemon.json` (host embedder + fingerprint + scheduler config), rehydrates repos from `registry.json` into a `catalogset.Set` (+ per-repo `BuilderRouter` builders via `Set.ChunkCache`), assembles `worker.Worker`→`scheduler.Engine`→`service.Server`, then runs the scheduler and `rpc.Serve` until SIGINT/SIGTERM. A library `daemonctl.EnsureDaemon` dials the socket and, if down, spawns `grepaid` detached and polls. This is Phase B of `docs/superpowers/specs/2026-07-19-grepaid-daemon-design.md` §4.2–§4.4.

**Tech Stack:** Go 1.24.2, stdlib + existing `internal/enginev2/*`, `config`, `embedder`, `indexer`. Unix-only for the process paths (`//go:build` guards where needed).

## Global Constraints

- Module `github.com/yoanbernabeu/grepai`; Go floor 1.24.2; CGO_ENABLED=0 preserved.
- New: `cmd/grepaid/`, `internal/enginev2/daemoncfg/`, `internal/enginev2/daemonctl/`. Small additions to `scheduler` (SystemClock) and `runtime` (exported DiskLoader).
- The flock — not the socket file — is the authoritative liveness signal.
- One host-global embedder + fingerprint (from `daemon.json`); per-repo fingerprint mismatch → roll a fresh generation, never wedge.
- Gates (Gate B): `make build`, `make lint`, `go test ./... -race`, `go vet`, `gofmt -l` empty; independent `codex-bg` review before Phase C.
- Commit per task, conventional commits.

## Scouted APIs (verified against the tree 2026-07-19)

- `embedder.NewFromConfig(cfg *config.Config) (embedder.Embedder, error)` — reads `cfg.Embedder`.
- `runtime.Fingerprint(cfg *config.Config) string` — the v2 indexing fingerprint (must match the embedder).
- `worker.New(cat worker.Catalog, build worker.Builder, load worker.ContentLoader, crash worker.CrashHook, maxAttempts int) *worker.Worker`; `worker.NoCrash`; `worker.ContentLoader.Load(ctx, repo, worktreeRoot, relPath, desiredHash) ([]byte, error)`.
- `runtime.diskLoader{}` is **unexported** → Task 1 exports `runtime.NewDiskLoader()`.
- `scheduler.New(cfg Config, q Queue, p Processor, clock Clock, seed int64) (*Engine, error)`; `scheduler.DefaultConfig()`; `scheduler.Clock` has `Now()`/`After(d)`. **No production clock exists** → Task 2 adds `scheduler.SystemClock`.
- `artifacts.New(ch Chunker, emb embedder.Embedder, cache artifacts.ChunkCache) *artifacts.DefaultBuilder`; `indexer.NewChunker(size, overlap)`.
- `service.New(cat service.Catalog, rec service.Reconciler, emb embedder.Embedder, fingerprint string, searchLimit int) *service.Server`; `reconcile.New(cat reconcile.CatalogReader) *reconcile.Engine`.
- `catalogset.New()`, `Set.Add(ctx, repo, catalogPath) error`, `Set.ChunkCache(repo) (artifacts.ChunkCache, error)`, `Set.Close()`, `Set.SetActiveGeneration`, `Set.CreateGeneration`, `Set.GenerationFingerprint`, `Set.ActiveGeneration`, `catalogset.NewBuilderRouter()`, `BuilderRouter.Add(repo, RepoBuilder)`.
- `registry.Load(path)`, `(*Registry).Upsert`, `(*Registry).Save(path)`, `Entry{RepositoryID, Root, CatalogPath, ActiveGeneration, ...}`.
- `rpc.Serve(l net.Listener, h service.Service) error`; `rpc.Dial(socket)`, `rpc.ErrDaemonDown`.
- `daemon.GetDefaultLogDir()` (XDG state dir helper to mirror for the state root).

## File Structure

- `internal/enginev2/runtime/runtime.go` — **modify**: export `NewDiskLoader() worker.ContentLoader`.
- `internal/enginev2/scheduler/clock.go` — **modify**: add `SystemClock`.
- `internal/enginev2/daemoncfg/paths.go` — **create**: host path resolution (state dir, socket, lock, registry, daemon.json, log).
- `internal/enginev2/daemoncfg/config.go` — **create**: `daemon.json` schema, `Load`, defaults, `ToConfig()` (→ `*config.Config`).
- `internal/enginev2/daemoncfg/*_test.go` — **create**.
- `internal/enginev2/daemonctl/ensure.go` — **create**: `EnsureDaemon(socket, binPath)` (dial-or-spawn-detached-and-poll) + `Singleton` flock helper.
- `internal/enginev2/daemonctl/*_test.go` — **create**.
- `cmd/grepaid/main.go` — **create**: the daemon assembly + run loop + signal shutdown.
- `cmd/grepaid/daemon.go` — **create**: `run(ctx, paths, cfg)` (testable assembly, separate from `main`).
- `cmd/grepaid/integration_test.go` — **create**: end-to-end over a real socket + two git fixtures.

---

## Task 1: Export a reusable disk `ContentLoader` from `runtime`

**Files:** Modify `internal/enginev2/runtime/runtime.go`; Test `internal/enginev2/runtime/runtime_test.go` (append).

**Interfaces:** Produces `func NewDiskLoader() worker.ContentLoader` returning the existing `diskLoader{}`.

- [ ] **Step 1: Failing test** — append:

```go
func TestNewDiskLoaderRejectsChangedContent(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f.txt"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	l := NewDiskLoader()
	// A wrong desiredHash must be rejected (content changed since reconciliation).
	if _, err := l.Load(context.Background(), core.RepositoryID(dir), dir, "f.txt", "deadbeef"); err == nil {
		t.Fatal("expected rejection for mismatched desiredHash")
	}
}
```

- [ ] **Step 2: Run — FAIL** (`NewDiskLoader` undefined). `go test ./internal/enginev2/runtime/ -run TestNewDiskLoader -v`
- [ ] **Step 3: Implement** — add to `runtime.go` (near `diskLoader`):

```go
// NewDiskLoader returns the worktree-file ContentLoader used by the one-shot
// runtime and the daemon. It is stateless (the worktree root arrives per call),
// so one instance serves every repository.
func NewDiskLoader() worker.ContentLoader { return diskLoader{} }
```

(Add the `worker` import if not already present — it is, via `worker.New`/`worker.NoCrash`.)

- [ ] **Step 4: Run — PASS.**
- [ ] **Step 5: Commit** — `git commit -m "feat(runtime): export NewDiskLoader for the daemon"`

---

## Task 2: Production `SystemClock` for the scheduler

**Files:** Modify `internal/enginev2/scheduler/clock.go`; Test `internal/enginev2/scheduler/clock_test.go` (create).

**Interfaces:** Produces `type SystemClock struct{}` implementing `Clock` (`Now()=time.Now()`, `After(d)=time.After(d)`), with `var _ Clock = SystemClock{}`.

- [ ] **Step 1: Failing test** — create `clock_test.go`:

```go
package scheduler

import (
	"testing"
	"time"
)

func TestSystemClockImplementsClock(t *testing.T) {
	var c Clock = SystemClock{}
	before := time.Now()
	if c.Now().Before(before) {
		t.Fatal("SystemClock.Now went backwards")
	}
	select {
	case <-c.After(1 * time.Millisecond):
	case <-time.After(time.Second):
		t.Fatal("After did not fire")
	}
}
```

- [ ] **Step 2: Run — FAIL** (`SystemClock` undefined).
- [ ] **Step 3: Implement** — add to `clock.go`:

```go
// SystemClock is the production Clock backed by the real monotonic clock.
type SystemClock struct{}

var _ Clock = SystemClock{}

// Now returns the current wall-clock time.
func (SystemClock) Now() time.Time { return time.Now() }

// After delegates to time.After.
func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
```

- [ ] **Step 4: Run — PASS.**
- [ ] **Step 5: Commit** — `git commit -m "feat(scheduler): add production SystemClock"`

---

## Task 3: Host path resolution (`daemoncfg`)

**Files:** Create `internal/enginev2/daemoncfg/paths.go` + `paths_test.go`.

**Interfaces:** Produces:
- `type Paths struct { StateDir, Socket, Lock, Registry, Config, Log string }`
- `func ResolvePaths() (Paths, error)` — StateDir = `$XDG_STATE_HOME/grepai` else `~/.local/state/grepai`; Socket = `$XDG_RUNTIME_DIR/grepai/grepaid.sock` else `<state>/grepaid.sock`; Lock=`<state>/grepaid.lock`; Registry=`<state>/registry.json`; Config=`<state>/daemon.json`; Log=`<state>/logs/grepaid.log`. A `GREPAID_SOCKET` env overrides Socket.
- `func (p Paths) EnsureDirs() error` — MkdirAll (0700) the state dir, socket dir, log dir.

- [ ] **Step 1: Failing test** — create `paths_test.go`:

```go
package daemoncfg

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestResolvePathsHonorsXDGStateHome(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "/xdg/state")
	t.Setenv("XDG_RUNTIME_DIR", "")
	t.Setenv("GREPAID_SOCKET", "")
	p, err := ResolvePaths()
	if err != nil {
		t.Fatalf("ResolvePaths: %v", err)
	}
	if p.StateDir != "/xdg/state/grepai" {
		t.Fatalf("StateDir = %q", p.StateDir)
	}
	if p.Registry != "/xdg/state/grepai/registry.json" || p.Config != "/xdg/state/grepai/daemon.json" {
		t.Fatalf("bad derived paths: %+v", p)
	}
	if !strings.HasSuffix(p.Socket, "grepaid.sock") {
		t.Fatalf("Socket = %q", p.Socket)
	}
	_ = filepath.Dir(p.Log)
}

func TestSocketEnvOverride(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "/xdg/state")
	t.Setenv("GREPAID_SOCKET", "/tmp/custom.sock")
	p, err := ResolvePaths()
	if err != nil {
		t.Fatalf("ResolvePaths: %v", err)
	}
	if p.Socket != "/tmp/custom.sock" {
		t.Fatalf("env override ignored: Socket = %q", p.Socket)
	}
}
```

- [ ] **Step 2: Run — FAIL.**
- [ ] **Step 3: Implement** `paths.go`:

```go
// Package daemoncfg resolves the grepaid host paths and loads daemon.json.
package daemoncfg

import (
	"os"
	"path/filepath"
)

// Paths holds the resolved host locations for the daemon.
type Paths struct {
	StateDir string
	Socket   string
	Lock     string
	Registry string
	Config   string
	Log      string
}

// ResolvePaths derives the host paths from XDG env (Linux conventions).
func ResolvePaths() (Paths, error) {
	state, err := stateDir()
	if err != nil {
		return Paths{}, err
	}
	p := Paths{
		StateDir: state,
		Lock:     filepath.Join(state, "grepaid.lock"),
		Registry: filepath.Join(state, "registry.json"),
		Config:   filepath.Join(state, "daemon.json"),
		Log:      filepath.Join(state, "logs", "grepaid.log"),
	}
	p.Socket = socketPath(state)
	return p, nil
}

func stateDir() (string, error) {
	if base := os.Getenv("XDG_STATE_HOME"); base != "" {
		return filepath.Join(base, "grepai"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".local", "state", "grepai"), nil
}

func socketPath(state string) string {
	if s := os.Getenv("GREPAID_SOCKET"); s != "" {
		return s
	}
	if rt := os.Getenv("XDG_RUNTIME_DIR"); rt != "" {
		return filepath.Join(rt, "grepai", "grepaid.sock")
	}
	return filepath.Join(state, "grepaid.sock")
}

// EnsureDirs creates the state, socket, and log directories (0700).
func (p Paths) EnsureDirs() error {
	for _, d := range []string{p.StateDir, filepath.Dir(p.Socket), filepath.Dir(p.Log)} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
	}
	return nil
}
```

- [ ] **Step 4: Run — PASS.**
- [ ] **Step 5: Commit** — `git commit -m "feat(daemoncfg): resolve host paths (state/socket/lock/registry/config/log)"`

---

## Task 4: `daemon.json` config + `ToConfig`

**Files:** Create `internal/enginev2/daemoncfg/config.go` + `config_test.go`.

**Interfaces:** Produces:
- `type Config struct { Socket string; Embedder EmbedderConfig; Chunking ChunkingConfig; SearchLimit int; Scheduler *SchedulerConfig }` with json tags; `EmbedderConfig{Provider, Endpoint, Model, APIKey string; Dimensions *int; Parallelism int}`; `ChunkingConfig{Size, Overlap int}`; `SchedulerConfig{MaxIndexInflight, ReservedQueryInflight, MaxJobAttempts int}`.
- `func Load(path string) (*Config, bool, error)` — returns `(defaults, false, nil)` when the file is missing (so the daemon can write defaults); `(parsed, true, nil)` otherwise.
- `func Default() *Config` — provider `openai`, endpoint `http://127.0.0.1:4000/v1`, model `qwen3-embedding-4b`, Dimensions=2560, chunk 512/64, SearchLimit 10 (mirrors the current 4B deployment; overridable).
- `func (c *Config) ToConfig() *config.Config` — maps into a `config.Config` so `embedder.NewFromConfig` + `runtime.Fingerprint` consume it.
- `func (c *Config) SchedulerConfigOrDefault() scheduler.Config`.

- [ ] **Step 1: Failing test** — create `config_test.go`:

```go
package daemoncfg

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadMissingReturnsDefaults(t *testing.T) {
	c, existed, err := Load(filepath.Join(t.TempDir(), "daemon.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if existed {
		t.Fatal("missing file should report existed=false")
	}
	if c.Embedder.Provider == "" || c.ToConfig().Embedder.Provider != c.Embedder.Provider {
		t.Fatalf("defaults/ToConfig wrong: %+v", c)
	}
}

func TestLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.json")
	if err := os.WriteFile(path, []byte(`{"embedder":{"provider":"openai","model":"m","endpoint":"http://x"},"chunking":{"size":100,"overlap":10}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	c, existed, err := Load(path)
	if err != nil || !existed {
		t.Fatalf("Load: existed=%v err=%v", existed, err)
	}
	cc := c.ToConfig()
	if cc.Embedder.Model != "m" || cc.Chunking.Size != 100 {
		t.Fatalf("ToConfig mapping wrong: %+v", cc)
	}
}
```

- [ ] **Step 2: Run — FAIL.**
- [ ] **Step 3: Implement** `config.go` — define the structs with json tags, `Default()`, `Load` (json.Unmarshal; missing→Default,false), `ToConfig()` copying Embedder/Chunking fields into a `config.Config` (set `Dimensions` pointer through), and `SchedulerConfigOrDefault()` (start from `scheduler.DefaultConfig()`, apply non-zero overrides). Verify `config.EmbedderConfig`/`config.ChunkingConfig` field names by grepping `config/config.go` before writing (Provider/Endpoint/Model/APIKey/Dimensions *int/Parallelism; Size/Overlap).

- [ ] **Step 4: Run — PASS.**
- [ ] **Step 5: Commit** — `git commit -m "feat(daemoncfg): daemon.json config, defaults, ToConfig mapping"`

---

## Task 5: Singleton flock + lazy-start `EnsureDaemon`

**Files:** Create `internal/enginev2/daemonctl/singleton.go`, `ensure.go`, `*_test.go`.

**Interfaces:** Produces:
- `type Lock struct { … }`; `func Acquire(lockPath string) (*Lock, error)` — `flock(LOCK_EX|LOCK_NB)`; returns `ErrAlreadyRunning` if held; `(*Lock).Release() error`. (`//go:build unix`.)
- `var ErrAlreadyRunning = errors.New("grepaid: already running")`
- `func EnsureDaemon(ctx context.Context, socket, binPath string, timeout time.Duration) (*rpc.Client, error)` — `rpc.Dial`; if `ErrDaemonDown`, spawn `binPath` detached (`Setsid`, stdio→/dev/null or log), poll `rpc.Dial` until success or timeout.

- [ ] **Step 1: Failing tests** — `singleton_test.go`:

```go
package daemonctl

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestSingletonSecondAcquireFails(t *testing.T) {
	lp := filepath.Join(t.TempDir(), "d.lock")
	l1, err := Acquire(lp)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer l1.Release()
	if _, err := Acquire(lp); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second acquire = %v; want ErrAlreadyRunning", err)
	}
	if err := l1.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	l2, err := Acquire(lp)
	if err != nil {
		t.Fatalf("re-acquire after release: %v", err)
	}
	l2.Release()
}
```

> Note: `flock` is advisory and per-open-file-description; two `Acquire` calls in the same process open the file separately, so the second `LOCK_NB` fails — this test is valid in-process.

- [ ] **Step 2: Run — FAIL.**
- [ ] **Step 3: Implement** `singleton.go` (`//go:build unix`):

```go
//go:build unix

package daemonctl

import (
	"errors"
	"os"
	"syscall"
)

// ErrAlreadyRunning means another process holds the singleton lock.
var ErrAlreadyRunning = errors.New("grepaid: already running")

// Lock is a held advisory flock, released on Release or process exit.
type Lock struct{ f *os.File }

// Acquire takes an exclusive non-blocking flock on lockPath.
func Acquire(lockPath string) (*Lock, error) {
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600) // #nosec G304 - operator's own state file
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrAlreadyRunning
		}
		return nil, err
	}
	return &Lock{f: f}, nil
}

// Release drops the lock.
func (l *Lock) Release() error {
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	return l.f.Close()
}
```

`ensure.go`:

```go
package daemonctl

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/rpc"
)

// EnsureDaemon returns a connected client, lazily spawning grepaid (detached) if
// the socket is down. binPath is the grepaid executable.
func EnsureDaemon(ctx context.Context, socket, binPath string, timeout time.Duration) (*rpc.Client, error) {
	if c, err := rpc.Dial(socket); err == nil {
		return c, nil
	} else if !errors.Is(err, rpc.ErrDaemonDown) {
		return nil, err
	}
	if err := spawnDetached(binPath); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(timeout)
	for {
		if c, err := rpc.Dial(socket); err == nil {
			return c, nil
		}
		if time.Now().After(deadline) {
			return nil, errors.New("grepaid: did not become reachable before timeout")
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func spawnDetached(binPath string) error {
	cmd := exec.Command(binPath) // #nosec G204 - fixed daemon binary path
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = nil, nil, nil
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Process.Release()
}
```

- [ ] **Step 4: Run singleton test — PASS.** (EnsureDaemon is covered by Task 7 integration.)
- [ ] **Step 5: Commit** — `git commit -m "feat(daemonctl): singleton flock + lazy-start EnsureDaemon spawn helper"`

---

## Task 6: Daemon assembly + run loop (`cmd/grepaid`)

**Files:** Create `cmd/grepaid/daemon.go` (testable `run`), `cmd/grepaid/main.go` (thin).

**Interfaces:** Produces `func run(ctx context.Context, p daemoncfg.Paths, cfg *daemoncfg.Config, logw io.Writer) error` — assembles and serves until ctx cancel. `main` resolves paths, takes the lock, opens the log, installs signal handling, calls `run`.

Assembly steps inside `run` (each documented in code):
1. Build embedder: `emb, err := embedder.NewFromConfig(cfg.ToConfig())`; `fp := runtime.Fingerprint(cfg.ToConfig())`.
2. `set := catalogset.New()`; `br := catalogset.NewBuilderRouter()`.
3. Load registry; for each entry: `set.Add(ctx, repo, entry.CatalogPath)` — on `catalogset.ErrSchemaTooNew` or any open error, **log + skip** (quarantine at open). Then `cache,_ := set.ChunkCache(repo); br.Add(repo, artifacts.New(indexer.NewChunker(size,overlap), emb, cache))`.
4. `rec := reconcile.New(set)`; `svc := service.New(set, rec, emb, fp, cfg.SearchLimit)`.
5. For each registered repo, `svc.Register(ctx, service.RegisterRequest{Root: entry.Root})` to rehydrate the worktree→repo map + bootstrap/roll generation. On fingerprint mismatch (existing active gen fp != fp): `set.CreateGeneration(repo, active+1, fp)` + `set.SetActiveGeneration(repo, active+1)` then log; continue.
6. `wk := worker.New(set, br, runtime.NewDiskLoader(), worker.NoCrash, cfg.MaxJobAttempts())`; `_ , _ = wk.Recover(ctx)` (requeue claimed).
7. `sch, err := scheduler.New(cfg.SchedulerConfigOrDefault(), set, wk, scheduler.SystemClock{}, 1)`.
8. Listen: `os.Remove(p.Socket)` (stale-socket unlink; safe — lock held by main), `l, err := net.Listen("unix", p.Socket)`, `os.Chmod(p.Socket, 0o600)`.
9. Run: `go sch.Run(ctx)`; `go rpc.Serve(l, svc)`. On `ctx.Done()`: close `l` (stops Serve), cancel already propagates to `sch`, `set.Close()`, persist registry, `os.Remove(p.Socket)`. Return nil.

Worker.Processor note: `scheduler.New` takes the worker as the `Processor` (it calls `ProcessClaimed`); the same `wk` is fine.

> This task has no unit test of its own beyond compiling; it is exercised end-to-end by Task 7. Keep `run` free of `os.Exit`/signal handling (those live in `main`) so the integration test can drive it with a cancelable ctx.

- [ ] **Step 1:** Write `cmd/grepaid/daemon.go` with `run` per the steps above (full code — no stub). Grep-confirm `service.RegisterRequest`, `service.Reconciler`, chunk-size accessors before writing.
- [ ] **Step 2:** Write `cmd/grepaid/main.go`: `ResolvePaths` → `EnsureDirs` → `daemoncfg.Load` (write defaults if missing) → `daemonctl.Acquire(lock)` (exit 0 on `ErrAlreadyRunning`) → open log (append) → `signal.NotifyContext(SIGINT,SIGTERM)` → `run(ctx, …)` → exit.
- [ ] **Step 3:** `go build ./cmd/grepaid` — compiles.
- [ ] **Step 4:** `make build` (adds the second binary target in Task’s Makefile edit if needed — the default `make build` builds only `grepai`; add a `grepaid` target in Phase D).
- [ ] **Step 5: Commit** — `git commit -m "feat(grepaid): daemon assembly, multi-repo rehydrate, scheduler+rpc run loop"`

---

## Task 7: End-to-end integration test

**Files:** Create `cmd/grepaid/integration_test.go`.

**Behavior under test** (build the `grepaid` binary via `go test` helper `os/exec` of `go build`, or drive `run` in-process with a cancelable ctx — **prefer in-process `run`** for speed and determinism; spawn-based lazy-start is covered by a separate subtest using the built binary):

- [ ] **Step 1: Write tests:**
  1. `TestDaemonRegisterReconcileSearchIsolation`: create two tmp git repos (reuse `enginetest.gitfixture` if importable, else `git init` + a file). Write a `daemon.json` with the `synthetic` embedder (deterministic, no network — confirm `embedder` supports `synthetic`; it does per factory.go). Start `run` in a goroutine with a cancelable ctx pointed at a tmp state dir (set `XDG_STATE_HOME`, `XDG_RUNTIME_DIR`, `GREPAID_SOCKET` to tmp). Register both repos via an `rpc.Client`; `Reconcile`; `WaitFresh`; `Search` in repo A returns a hit from A and **not** B (isolation). Cancel ctx; assert clean shutdown (socket removed).
  2. `TestSingletonRejectsSecondDaemon`: acquire the lock, then a second `run`/`Acquire` fails fast.
  3. `TestLazyStartSpawnsDaemon` (uses the built binary): `go build -o <tmp>/grepaid ./cmd/grepaid`; `daemonctl.EnsureDaemon` with no daemon running spawns it and connects; a second concurrent `EnsureDaemon` yields one daemon; kill it and `EnsureDaemon` respawns; no orphaned lock/socket.

> Registration of a repo requires the daemon to have `Add`ed its catalog first. Decide the registration flow: `Register` (service) opens/bootstraps via the catalog, but the catalog must already be in the `set`. So the integration test (and Phase C `v2 watch`) must call a daemon method that **adds** the repo to the set before/within Register. Add a thin `set.Add` call inside the daemon's `Register` path, OR extend the flow: the RPC `Register` handler in the daemon first `set.Add(repo, <repo>/.grepai/catalog_v2.db)` then delegates to `svc.Register`. **Implement this in Task 6’s `run` by wrapping the service with a small registering-service that Adds to the set + builder router on Register, then updates the registry.** (This is the missing link the plan surfaces — see note.)

- [ ] **Step 2–4:** Run the integration tests `-race`; fix until green.
- [ ] **Step 5: Commit** — `git commit -m "test(grepaid): end-to-end register/reconcile/search isolation + lazy-start + singleton"`

---

## Design note surfaced while planning: dynamic Register must Add to the set

`catalogset.Set.Add` (open the catalog) and `service.Server.Register` (bootstrap generation) are separate. On **restart**, Task 6 step 3 Adds every registry repo before Register. But a **new** repo registered at runtime (via `grepai v2 watch`/`init` in Phase C) is not yet in the set. So the daemon must wrap `service.Service.Register` with a decorator that:
1. resolves the repo's catalog path (`<root>/.grepai/catalog_v2.db`),
2. `set.Add(ctx, repo, catalogPath)` (+ `br.Add` with a fresh builder),
3. delegates to `svc.Register`,
4. `registry.Upsert` + serialized `Save`.

Implement this decorator (`registeringService`) in Task 6; the RPC server dispatches to it (it satisfies `service.Service`). Registry writes go through a mutex-guarded manager (Phase 0 review obligation).

## Deferred to a later slice (documented, from the Phase 0 review)

- **Catalog quarantine beyond open-time:** Task 6 quarantines at open (skip a broken/too-new catalog). Runtime quarantine — removing a catalog that starts erroring *after* open so its failures don't stall the scheduler aggregates — is deferred; the daemon logs aggregate errors. (Full fix: aggregate error-observer + `Set.Remove` + threshold.) Note this in Phase D docs.

## Gate B

- [ ] `gofmt -l`, `go vet`, `make build`, `make lint`, `go test ./... -race` green.
- [ ] Independent `codex-bg` review of the Phase B diff; address findings before Phase C.

## Self-Review notes (author)

- **Spec coverage:** §4.2 lifecycle (Tasks 5–6), §4.1 paths (Task 3), §4.3 daemon.json + embedder/fingerprint + mismatch-roll (Tasks 4, 6), §4.4 registry rehydrate + serialized writes + builder-per-repo (Task 6), lazy-start (Task 5), integration (Task 7).
- **Open sub-decision flagged for implementation:** the `registeringService` decorator + mutex registry manager (design note) — this is the one non-mechanical piece; it is specified but its exact code is written during Task 6 against the real `service.Service` signature.
- **Grep-confirm at implementation:** `config.EmbedderConfig`/`ChunkingConfig` field names (Task 4); `service.RegisterRequest`/`Reconciler` + chunk-size config accessors (Task 6); `embedder` `synthetic` provider options (Task 7). All exist per earlier scouting; confirm exact field spellings when writing.
