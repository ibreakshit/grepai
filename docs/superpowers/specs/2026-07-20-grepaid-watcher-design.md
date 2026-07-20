# grepaid Continuous Watcher — Design Spec

**Date:** 2026-07-20
**Status:** Approved (design agreed in conversation; this records it)
**Scope:** Continuous file-change detection inside `grepaid` — inotify hints (via fsnotify) that trigger debounced reconciles per worktree. Removes the "freshness is reconcile-on-command" limitation. Fork-only; builds on the merged daemon (PR #4).

## 1. The principle: events are hints, never truth

An fsnotify event never creates an index job. It does one thing: marks a
worktree dirty, which schedules a **reconcile** — the same idempotent,
atomic-plan operation everything else uses. Content hashing inside reconcile
decides *whether* anything changed; inotify only decides *when to look*.
Consequences (these are the review anchors):

- A dropped, duplicated, reordered, or mid-write event can at worst DELAY
  freshness, never corrupt it. Torn reads are already rejected by the
  diskLoader hash check and replaced via job supersession.
- Spurious events (touch, identical checkout) hash to no-ops: idle=idle holds.
- Anything inotify never reports is repaired by a periodic safety-net
  reconcile. Correctness never depends on the watcher; it adds promptness only.

## 2. Components (`internal/enginev2/watch`)

### 2.1 `Manager`
Owns one watch per registered repo. Lifecycle mirrors the catalogset: started
for every rehydrated repo at daemon boot and for each new `Register`; stopped
when a repo's root vanishes or at shutdown. API:

```go
type ReconcileFunc func(ctx context.Context, wt core.WorktreeID) error
New(cfg Config, clock scheduler.Clock, rec ReconcileFunc, newBackend BackendFactory, log Logger) *Manager
(m *Manager) Watch(root string, wt core.WorktreeID) error   // idempotent per wt
(m *Manager) Close()                                        // stops all, waits
```

### 2.2 `Backend` (seam for tests)
```go
type Backend interface {
    Start() error            // recursive add over root; returns ErrExhausted on ENOSPC
    Hints() <-chan struct{}  // coalesced "something under root changed"
    Errors() <-chan error    // ErrOverflow (queue overflow), ErrRootGone, other
    Close() error
}
type BackendFactory func(root string) (Backend, error)
```
Unit tests drive a fake Backend + `enginetest.FakeClock`; the real one is
`fsBackend` (§2.4). Integration tests use the real one on tmp git repos.

### 2.3 Per-worktree debounce state machine (Clock-driven)
- Any hint sets dirty and (re)arms a **quiet-window** timer (default 1000 ms).
- A **max-latency** bound (default 10 s) fires the reconcile even under
  continuous churn (a branch switch coalesces to ONE reconcile).
- Reconciles are serialized per worktree: if hints arrive while one runs, run
  exactly one trailing reconcile after it (no queue growth).
- A failed reconcile leaves the worktree dirty and retries after the quiet
  window (bounded by max-latency pacing) — loud in the log, never wedged.
- **Safety net:** every repo is marked dirty every `SafetyNetInterval`
  (default 1 h) regardless of events. With idle=idle this costs one
  `git ls-files` on an unchanged repo, zero embedder calls.
- **Poll fallback:** a repo whose backend reports `ErrExhausted`
  (inotify watch-descriptor limit) degrades to dirty-every-`PollInterval`
  (default 5 m) with a loud log; other repos stay evented.
- **Overflow:** `ErrOverflow` = hints were lost → mark dirty (every reconcile
  is already a full re-derivation; there is no partial reconcile to upgrade).
- **Root gone** (`ErrRootGone`, e.g. a deleted Codex worktree): stop that
  repo's watch, log; registry rehydrate handles the rest at next boot.

### 2.4 `fsBackend` (the real mechanism)
fsnotify v1.9.0 (already a dependency) over inotify:

- **Recursive add:** walk the root, `Add` every non-ignored directory
  (inotify watches directories, not trees). Ignore decisions come from
  `indexer.NewIgnoreMatcher(root, nil, "")` (gitignore hierarchy — keeps
  node_modules out of the watch table, which is what keeps descriptor counts
  sane), PLUS two hard guards independent of any matcher: **`.grepai/`**
  (the catalog's own WAL churns on every commit — watching it would
  self-trigger reconcile loops) and **`.git/`** (internal churn; a branch
  switch fires events on working files anyway).
- **Dirty signal:** any `Create|Write|Rename|Remove` event on a non-ignored
  path → one coalesced hint (Chmod excluded). No per-file semantics — the
  editor atomic-rename-save problem (vim/VS Code/gofmt write tmp + rename)
  disappears because MOVED_TO is just another hint. Unlike the v1 watcher,
  there is **no extension filter and no dotfile skip**: v2 indexes every
  git-tracked file (`.github/workflows/…` included), so every event under a
  watched dir counts.
- **New directories:** a Create event naming a directory → recursive add of
  that subtree AND a hint (files may have landed inside before watches
  attached).
- **Failure surfaces:** fsnotify's overflow error → `ErrOverflow`; `Add`
  failing with ENOSPC → `ErrExhausted`; root stat failing → `ErrRootGone`.

## 3. Priority: live edits jump the queue

The scheduler already defines `PriorityLiveChange` above `PriorityReconcile`
(currently unused). `service.ReconcileRequest` gains `Live bool`; the server
rewrites the plan's job priorities to `PriorityLiveChange` before the atomic
`UpsertJobs` when set. The watcher reconciles with `Live: true`; CLI/register
reconciles keep `PriorityReconcile`. (UpsertJob's conflict-update already
carries priority, so a live re-save of a queued file upgrades its priority.)

## 4. Daemon wiring

- `daemoncfg.Config` gains `Watch`: `{Enabled *bool (default true),
  QuietMS (1000), MaxLatencyMS (10000), PollMinutes (5), SafetyNetMinutes (60)}`.
- `runWithEmbedder`: after rehydrate, construct the Manager (clock =
  `scheduler.SystemClock{}`, rec = `regsvc.Reconcile(…, Live: true)` under
  `runCtx`) and `Watch` every registered root. `registeringService` gets an
  `onRegistered(root, wt)` hook so a live `Register` starts watching
  immediately. Shutdown: `manager.Close()` (waits for its goroutines) happens
  with the scheduler drain, before catalogs close; in-flight watcher
  reconciles abort via `runCtx` like every other handler.
- `grepai watch` UX: unchanged tail-until-fresh; its note becomes "the daemon
  watches continuously". The stale-index search note stays (now rare).

## 5. Testing

- **Debounce unit (FakeClock):** quiet window; max-latency under continuous
  hints; storm→1 reconcile; serialize + exactly-one trailing; failure→retry;
  safety-net tick; poll-mode tick; overflow→dirty; Close waits.
- **fsBackend (real fs, tmpdir):** write/create/rename/delete each produce a
  hint; new nested dir picked up; `.grepai` and `.git` churn produce NO hint;
  gitignored dir not watched; chmod produces no hint.
- **Integration (real daemon + git fixture):** edit a file → index goes
  stale→fresh with NO command issued; branch-switch storm → exactly one
  reconcile (count via reconcile log lines); `.grepai` WAL churn during
  indexing does not self-trigger; live-priority jobs observed ahead of
  bootstrap jobs when both queued.
- **Gates:** build (+windows/darwin cross), full `-race`, vet, gofmt, lint;
  one whole-slice codex-bg review before merge (merge-gate rule), findings
  fixed, verification pass, then PR + merge + deploy (rebuild installed
  binaries, restart daemon, live-verify on a fleet repo).

## 6. Non-goals

- Hint-scoped partial reconciles (reconcile stays whole-worktree; it is cheap
  and the atomicity story depends on whole-plan enqueue).
- Watching unregistered repos; per-repo watch config beyond the global block.
- fanotify/eBPF/Watchman alternatives (privilege/dependency-inappropriate).
- PostToolUse agent-hook integration (complementary, separate follow-up).
- macOS/Windows watch validation this slice (fsnotify abstracts; Linux is the
  deployment target — same stance as the daemon).
