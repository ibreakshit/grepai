# GrepAI v2 — Phase 4 Implementation Plan (Global scheduler engine)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Build the host-wide scheduler engine so Gate 4 passes: multiple repositories cannot exceed the configured global indexing budget, interactive queries keep reserved capacity while indexing is saturated, and an unavailable embedding endpoint produces only bounded calls (circuit breaker + bounded retries) without the scheduler loop crashing or restarting.

**Architecture:** A concrete `scheduler.Engine` (implements the Phase 0 `scheduler.Scheduler` interface) is the single admission-control and pacing layer over the durable catalog job queue and the Phase 3 `worker.Worker`. It owns two independent counting semaphores — an **index** pool of size `max_index_inflight` and a **reserved query** pool of size `reserved_query_inflight` — so query work always has capacity indexing cannot consume. `Run` drives a single claim loop: while the circuit is closed and an index slot is free, it claims the next eligible job **round-robin across repositories** (so no repo starves) and dispatches it to a worker goroutine. Each dispatch calls a new claim-free `worker.ProcessClaimed`, which returns a classified `Outcome`; the Engine — not the worker — owns retry timing (exponential **full-jitter backoff** via the injected `scheduler.Clock`, bounded by `max_job_attempts` before dead-lettering) and circuit accounting (consecutive transient failures open a global breaker that pauses indexing and half-open-probes after `circuit_probe_interval`). All timing flows through `Clock`, so `enginetest.FakeClock` makes backoff and probe behavior deterministic under `-race`.

**Tech Stack:** Go 1.24.2, the Phase 1 `catalog/sqlite`, the Phase 3 `worker`/`artifacts`, `scheduler.Clock` + `enginetest.FakeClock`, `math/rand` (seeded, injected — deterministic jitter), standard `sync`/channels for the semaphores and breaker.

## Global Constraints

- Go 1.24.2 floor; go.mod `go` directive stays `go 1.24.2`; `modernc.org/sqlite` stays `v1.45.0`.
- CGO_ENABLED=0 must stay buildable. **No new module dependency** (stdlib + already-vendored only).
- Module `github.com/yoanbernabeu/grepai`. New scheduler code under `internal/enginev2/scheduler/`; small additive catalog reads under `internal/enginev2/catalog/sqlite/`; a worker seam under `internal/enginev2/worker/`.
- `go test -race ./...` must pass (the scheduler is concurrent — race-freedom is a hard requirement); `gofmt`-clean; `make lint` (golangci-lint v1.64.2) green (annotate justified gosec with `// #nosec GXXX - reason`; `_test.go` excluded from gosec/errcheck).
- Conventional commits (scope `scheduler`, `worker`, or `catalog`). Never push to `main`.
- **One host, one scheduler (invariant 2):** a single `Engine` governs all repositories under one shared budget for index work; there is no per-repository scheduler.
- **Determinism:** every delay and timeout is derived from the injected `scheduler.Clock` and a seeded `*rand.Rand` — never `time.Now`, `time.After`, or an unseeded `rand`. Tests advance `FakeClock` explicitly.
- **Config defaults (spec §5.4), exact values:** `max_index_inflight: 1`, `reserved_query_inflight: 1`, `max_job_attempts: 5`, `base_retry_delay: 1s`, `max_retry_delay: 5m`, `circuit_open_after: 5`, `circuit_probe_interval: 60s`. These are configuration, not hard-coded constants.

## Scope / Non-goals (this phase)

- **In:** `scheduler.Config` + defaults; the circuit breaker; full-jitter exponential backoff; the `worker.ProcessClaimed` seam + `Outcome`; the `Engine` (index/query semaphores, `Submit`, `Run` claim/dispatch loop, retry-with-backoff, dead-lettering after bounded attempts, circuit pausing/probing, `Stats`, `AcquireQuerySlot`/`AcquireIndexSlot`); round-robin repository fairness; the three small catalog reads that support fairness and stats; Gate 4 tests.
- **Out (deferred, per the TIGHT scope decision):** the `grepaid` process (`main`), Unix-socket JSON-RPC framing/dispatch, and systemd packaging — they pair with the Phase 5 `service.Service` they dispatch to; the fsnotify watcher (event aggregation, overflow recovery, watch-descriptor exhaustion fallback); artifact-scoped symbol extraction; scheduled RPG refresh with LLM extraction. Also out: durable (cross-restart) backoff/circuit state — Phase 4 keeps retry-attempt counts and breaker state in memory; a crash re-derives them (a claimed job is requeued by `worker.Recover`, then retried immediately — safe, just not backoff-preserving). Token-aware batch sizing beyond a fixed batch limit is out (the builder already batches per file).

## Consumed surfaces (do not modify their existing behavior)

- Phase 0 `scheduler.Scheduler` interface (`Submit`, `Run`, `Stats`) and `scheduler.Stats{InFlight, ReservedQuery, QueueDepthByPriority}`; `scheduler.Clock` (`Now`, `After`). `*Engine` MUST satisfy `scheduler.Scheduler` (`var _ Scheduler = (*Engine)(nil)`).
- `core.Job`, `core.Priority` (1 `PriorityInteractiveQuery` … 4 `PriorityBootstrap`), `core.RepositoryID`/`WorktreeID`, `core.FailureClass`.
- Phase 3 `worker.Worker`: existing `New`, `ProcessOne`, `Run`, `Recover`, `NoCrash`, `CrashHook`. Task 4 **adds** `Outcome` + `ProcessClaimed` and refactors `ProcessOne` to call it — `ProcessOne`'s external behavior and all Phase 3 worker tests stay green.
- Phase 1/3 `catalog/sqlite.Catalog`: existing `ClaimNextJob`, `UpsertJob`, `DeadLetterJob`, `RequeueClaimedJobs`, `WorktreeInfo`, etc. Task 5 **adds** three read/claim methods (new file `queue.go`); no existing method changes.
- `enginetest.FakeClock` (`NewFakeClock`, `Now`, `After`, `Advance`), `enginetest.FakeEmbedder`, `enginetest.CrashRegistry`.

---

## File Structure

```
internal/enginev2/scheduler/
  scheduler.go          # (existing) Scheduler interface + Stats — unchanged
  clock.go              # (existing) Clock — unchanged
  config.go             # Config, DefaultConfig, (Config).validate
  config_test.go
  backoff.go            # fullJitterDelay(attempt, cfg, rng) time.Duration
  backoff_test.go
  circuit.go            # circuitBreaker (Allow/RecordSuccess/RecordFailure/State) driven by Clock
  circuit_test.go
  engine.go             # Engine: New, Submit, Run, Stats, AcquireQuerySlot, AcquireIndexSlot, dispatch, retry
  engine_test.go        # unit tests (budget, reserved, submit, stats)
  gate4_test.go         # Gate 4 integration: budget cap across repos, reserved query capacity, endpoint-down bounded + circuit
internal/enginev2/worker/
  worker.go             # (modify) add Outcome + ProcessClaimed; ProcessOne delegates to it
internal/enginev2/catalog/sqlite/
  queue.go              # ClaimNextJobInRepo, RepositoriesWithPendingJobs, QueueDepthByPriority
  queue_test.go
```

---

## Chunk A — Scheduler primitives (Tasks 1–3)

### Task 1: Config and defaults

**Files:** Create `internal/enginev2/scheduler/config.go`, `internal/enginev2/scheduler/config_test.go`.

**Interfaces — Produces:**
- `type Config struct { MaxIndexInflight int; ReservedQueryInflight int; MaxJobAttempts int; BaseRetryDelay time.Duration; MaxRetryDelay time.Duration; CircuitOpenAfter int; CircuitProbeInterval time.Duration }`
- `func DefaultConfig() Config` — the exact spec §5.4 defaults.
- `func (c Config) Validate() error` — every count ≥ 1, every duration > 0, `BaseRetryDelay <= MaxRetryDelay`.

- [ ] **Step 1: failing test**

```go
// config_test.go
package scheduler

import "testing"

func TestDefaultConfigMatchesSpec(t *testing.T) {
	c := DefaultConfig()
	if c.MaxIndexInflight != 1 || c.ReservedQueryInflight != 1 || c.MaxJobAttempts != 5 {
		t.Fatalf("counts: %+v", c)
	}
	if c.BaseRetryDelay.String() != "1s" || c.MaxRetryDelay.String() != "5m0s" {
		t.Fatalf("delays: base=%s max=%s", c.BaseRetryDelay, c.MaxRetryDelay)
	}
	if c.CircuitOpenAfter != 5 || c.CircuitProbeInterval.String() != "1m0s" {
		t.Fatalf("circuit: %+v", c)
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("default must validate: %v", err)
	}
}

func TestValidateRejectsBadConfig(t *testing.T) {
	bad := DefaultConfig()
	bad.MaxIndexInflight = 0
	if bad.Validate() == nil {
		t.Fatal("zero inflight must fail")
	}
	bad = DefaultConfig()
	bad.BaseRetryDelay = bad.MaxRetryDelay + 1
	if bad.Validate() == nil {
		t.Fatal("base>max must fail")
	}
}
```

- [ ] **Step 2:** Run `GOTOOLCHAIN=local go test ./internal/enginev2/scheduler/ -run Config` → FAIL (undefined).
- [ ] **Step 3: implement**

```go
// config.go
package scheduler

import (
	"errors"
	"time"
)

// Config is the host-wide scheduler configuration (spec §5.4). Values are
// configuration, not hard-coded assumptions.
type Config struct {
	MaxIndexInflight      int
	ReservedQueryInflight int
	MaxJobAttempts        int
	BaseRetryDelay        time.Duration
	MaxRetryDelay         time.Duration
	CircuitOpenAfter      int
	CircuitProbeInterval  time.Duration
}

// DefaultConfig returns the safe initial defaults for the local deployment.
func DefaultConfig() Config {
	return Config{
		MaxIndexInflight:      1,
		ReservedQueryInflight: 1,
		MaxJobAttempts:        5,
		BaseRetryDelay:        1 * time.Second,
		MaxRetryDelay:         5 * time.Minute,
		CircuitOpenAfter:      5,
		CircuitProbeInterval:  60 * time.Second,
	}
}

// Validate rejects nonsensical configuration.
func (c Config) Validate() error {
	if c.MaxIndexInflight < 1 || c.ReservedQueryInflight < 1 || c.MaxJobAttempts < 1 || c.CircuitOpenAfter < 1 {
		return errors.New("scheduler: counts must be >= 1")
	}
	if c.BaseRetryDelay <= 0 || c.MaxRetryDelay <= 0 || c.CircuitProbeInterval <= 0 {
		return errors.New("scheduler: durations must be > 0")
	}
	if c.BaseRetryDelay > c.MaxRetryDelay {
		return errors.New("scheduler: base_retry_delay must be <= max_retry_delay")
	}
	return nil
}
```

- [ ] **Step 4:** `GOTOOLCHAIN=local go test ./internal/enginev2/scheduler/ -race` → PASS. **Step 5:** commit `feat(scheduler): config and spec defaults`.

---

### Task 2: Full-jitter exponential backoff

**Files:** Create `internal/enginev2/scheduler/backoff.go`, `backoff_test.go`.

**Interfaces — Produces:**
- `func fullJitterDelay(attempt int, cfg Config, unit float64) time.Duration` — AWS full-jitter: `cap = min(MaxRetryDelay, BaseRetryDelay << (attempt-1))` (guard the shift against overflow), delay = `unit * cap` where `unit ∈ [0,1)` is supplied by the caller's seeded rand. `attempt` is 1-based (first retry = attempt 1). For `attempt <= 0`, treat as 1.

- [ ] **Step 1: failing test**

```go
// backoff_test.go
package scheduler

import (
	"testing"
	"time"
)

func TestFullJitterBoundsAndCap(t *testing.T) {
	cfg := DefaultConfig() // base 1s, max 5m
	// unit=1.0 (max end): grows exponentially but never exceeds the cap.
	if d := fullJitterDelay(1, cfg, 0.999999); d > 1*time.Second {
		t.Fatalf("attempt1 exceeded base cap: %s", d)
	}
	if d := fullJitterDelay(3, cfg, 0.999999); d > 4*time.Second+1 {
		t.Fatalf("attempt3 cap wrong: %s", d)
	}
	// Very large attempt must clamp to MaxRetryDelay, not overflow.
	if d := fullJitterDelay(100, cfg, 0.999999); d > cfg.MaxRetryDelay {
		t.Fatalf("attempt100 exceeded max: %s", d)
	}
	// unit=0 => zero delay (full jitter lower bound).
	if d := fullJitterDelay(5, cfg, 0.0); d != 0 {
		t.Fatalf("unit 0 must be zero, got %s", d)
	}
}
```

- [ ] **Step 2:** run → FAIL. **Step 3: implement**

```go
// backoff.go
package scheduler

import "time"

// fullJitterDelay returns an AWS "full jitter" backoff delay for a 1-based
// attempt: a uniform sample in [0, cap) where cap = min(MaxRetryDelay,
// BaseRetryDelay * 2^(attempt-1)). unit is a caller-supplied sample in [0,1)
// (from a seeded rand) so the delay is deterministic in tests.
func fullJitterDelay(attempt int, cfg Config, unit float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	cap := cfg.MaxRetryDelay
	// Shift safely: stop doubling once we reach/exceed the max.
	d := cfg.BaseRetryDelay
	for i := 1; i < attempt; i++ {
		if d >= cfg.MaxRetryDelay {
			d = cfg.MaxRetryDelay
			break
		}
		d *= 2
	}
	if d < cap {
		cap = d
	}
	if unit < 0 {
		unit = 0
	}
	if unit >= 1 {
		unit = 0.999999
	}
	return time.Duration(unit * float64(cap))
}
```

- [ ] **Step 4:** `go test ./internal/enginev2/scheduler/ -race` → PASS. **Step 5:** commit `feat(scheduler): full-jitter exponential backoff`.

---

### Task 3: Circuit breaker

**Files:** Create `internal/enginev2/scheduler/circuit.go`, `circuit_test.go`.

**Semantics:** `closed` normally. Each `RecordFailure` increments a consecutive-failure counter; at `CircuitOpenAfter` it transitions to `open` and stamps the open time. `RecordSuccess` resets the counter and closes. While `open`, `Allow()` returns `false` until `CircuitProbeInterval` has elapsed on the clock, at which point one call transitions to `half-open` and `Allow()` returns `true` exactly once (the probe); further `Allow()` calls stay `false` until the probe resolves. A `RecordSuccess` in `half-open` closes; a `RecordFailure` in `half-open` re-opens and re-stamps.

**Interfaces — Produces:**
- `type circuitBreaker struct { … }`
- `func newCircuitBreaker(clock Clock, openAfter int, probeInterval time.Duration) *circuitBreaker`
- `func (b *circuitBreaker) Allow() bool`
- `func (b *circuitBreaker) RecordSuccess()`
- `func (b *circuitBreaker) RecordFailure()`
- `func (b *circuitBreaker) State() string` — `"closed" | "open" | "half-open"` (for Stats/tests). All methods are mutex-guarded (called from multiple goroutines).

- [ ] **Step 1: failing test**

```go
// circuit_test.go
package scheduler

import (
	"testing"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
)

func TestCircuitOpensAfterThresholdAndProbes(t *testing.T) {
	clk := enginetest.NewFakeClock(time.Unix(0, 0))
	b := newCircuitBreaker(clk, 3, 60*time.Second)
	if !b.Allow() {
		t.Fatal("starts closed")
	}
	b.RecordFailure()
	b.RecordFailure()
	if b.State() != "closed" {
		t.Fatalf("still closed under threshold: %s", b.State())
	}
	b.RecordFailure() // 3rd => open
	if b.State() != "open" || b.Allow() {
		t.Fatalf("must be open and disallow: state=%s", b.State())
	}
	// Before probe interval: still disallowed.
	clk.Advance(59 * time.Second)
	if b.Allow() {
		t.Fatal("probe too early")
	}
	// At probe interval: one probe allowed, then half-open blocks further.
	clk.Advance(1 * time.Second)
	if !b.Allow() {
		t.Fatal("probe should be allowed at interval")
	}
	if b.State() != "half-open" || b.Allow() {
		t.Fatalf("half-open must block a second probe: state=%s", b.State())
	}
	// Probe success closes.
	b.RecordSuccess()
	if b.State() != "closed" || !b.Allow() {
		t.Fatalf("success must close: %s", b.State())
	}
}

func TestCircuitHalfOpenFailureReopens(t *testing.T) {
	clk := enginetest.NewFakeClock(time.Unix(0, 0))
	b := newCircuitBreaker(clk, 1, 10*time.Second)
	b.RecordFailure() // open
	clk.Advance(10 * time.Second)
	if !b.Allow() {
		t.Fatal("probe allowed")
	}
	b.RecordFailure() // half-open failure re-opens
	if b.State() != "open" || b.Allow() {
		t.Fatalf("reopen expected: %s", b.State())
	}
	clk.Advance(10 * time.Second)
	if !b.Allow() {
		t.Fatal("re-probe after another interval")
	}
}
```

- [ ] **Step 2:** run → FAIL. **Step 3: implement**

```go
// circuit.go
package scheduler

import (
	"sync"
	"time"
)

type circuitState int

const (
	circuitClosed circuitState = iota
	circuitOpen
	circuitHalfOpen
)

// circuitBreaker trips after openAfter consecutive failures and, once open,
// allows a single half-open probe every probeInterval. All time comes from the
// injected Clock so behavior is deterministic under FakeClock.
type circuitBreaker struct {
	clock         Clock
	openAfter     int
	probeInterval time.Duration

	mu           sync.Mutex
	state        circuitState
	failures     int
	openedAt     time.Time
}

func newCircuitBreaker(clock Clock, openAfter int, probeInterval time.Duration) *circuitBreaker {
	return &circuitBreaker{clock: clock, openAfter: openAfter, probeInterval: probeInterval, state: circuitClosed}
}

// Allow reports whether a call may proceed. When open and the probe interval
// has elapsed it transitions to half-open and permits exactly one probe.
func (b *circuitBreaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case circuitClosed:
		return true
	case circuitOpen:
		if !b.clock.Now().Before(b.openedAt.Add(b.probeInterval)) {
			b.state = circuitHalfOpen
			return true
		}
		return false
	default: // half-open: a probe is already in flight
		return false
	}
}

// RecordSuccess resets the breaker to closed.
func (b *circuitBreaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.state = circuitClosed
}

// RecordFailure advances toward / re-enters the open state.
func (b *circuitBreaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == circuitHalfOpen {
		b.state = circuitOpen
		b.openedAt = b.clock.Now()
		return
	}
	b.failures++
	if b.failures >= b.openAfter {
		b.state = circuitOpen
		b.openedAt = b.clock.Now()
	}
}

// State returns a human-readable state for Stats/tests.
func (b *circuitBreaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case circuitOpen:
		return "open"
	case circuitHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}
```

- [ ] **Step 4:** `go test ./internal/enginev2/scheduler/ -race` → PASS. **Step 5:** commit `feat(scheduler): clock-driven circuit breaker`.

---

## Chunk B — Worker seam (Task 4)

### Task 4: Extract `ProcessClaimed` + `Outcome`

**Files:** Modify `internal/enginev2/worker/worker.go`. (No behavior change to `ProcessOne`; its Phase 3 tests must stay green.)

**Interfaces — Produces:**
- `type Outcome uint8` with `OutcomeCommitted`, `OutcomeSuperseded`, `OutcomeTransient`, `OutcomePermanent` (iota+1).
- `func (w *Worker) ProcessClaimed(ctx context.Context, job core.Job) (Outcome, error)` — processes an **already-claimed** job and returns its classified outcome. It does NOT retry, dead-letter, or unclaim (the scheduler owns those). For `OutcomeTransient`/`OutcomePermanent` the returned error is the cause (informational). An injected crash returns `(0, ErrInjectedCrash-wrapped err)` so the caller can treat it as a crash (job stays claimed). Committed/Superseded return a nil error.
- `ProcessOne` is refactored to: `ClaimNextJob` → `ProcessClaimed` → map the outcome onto the existing Phase 3 durable retry (`OutcomeTransient`→`FailJobAttempt`/exhaustion→`DeadLetterJob`; `OutcomePermanent`→`DeadLetterJob`; else nil). **`ProcessOne`'s signature and behavior are unchanged.**

**Refactor recipe:** move the body of `ProcessOne` from just after the successful claim (the `after-claim` crash point) through the final commit into `ProcessClaimed`, returning an `Outcome`+cause at each classification point instead of calling `retryOrDeadLetter`/committing-then-returning. Concretely:
- supersession precheck (either site) → `return OutcomeSuperseded, nil`.
- `OpDelete` → `CommitDelete`; on success `return OutcomeCommitted, nil`, on error `return OutcomeTransient, err`.
- `WorktreeInfo`/`GenerationFingerprint`/`Load`/`PutChunkVector`/`CommitUpdate` errors → `return OutcomeTransient, err`.
- `Build` error → `OutcomePermanent` if `errors.Is(err, artifacts.ErrDimensionMismatch)`, else `OutcomeTransient`.
- successful `CommitUpdate` (or whole-file hit commit) → `return OutcomeCommitted, nil`.
- a crash hook returning non-nil → `return 0, err` (outcome zero signals "not classified — job still claimed").

Then:

```go
// ProcessOne claims the next job and processes it, applying durable retry and
// dead-lettering itself (the standalone Phase 3 driver). The scheduler instead
// claims jobs and calls ProcessClaimed directly, owning retry/backoff.
func (w *Worker) ProcessOne(ctx context.Context) (bool, error) {
	job, ok, err := w.cat.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	oc, cause := w.ProcessClaimed(ctx, job)
	if oc == 0 { // crash / unclassified: leave claimed for Recover
		return false, cause
	}
	switch oc {
	case OutcomePermanent:
		return true, w.cat.DeadLetterJob(ctx, job, "permanent: "+cause.Error())
	case OutcomeTransient:
		attempts, aerr := w.cat.FailJobAttempt(ctx, job)
		if aerr != nil {
			return true, aerr
		}
		if attempts >= w.maxAttempts {
			return true, w.cat.DeadLetterJob(ctx, job, "attempts exhausted: "+cause.Error())
		}
		return true, nil
	default: // Committed, Superseded
		return true, nil
	}
}
```

- [ ] **Step 1: failing test** — add to `worker_test.go`:

```go
func TestProcessClaimedClassifies(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)
	w := worker.New(c, realBuilder(emb, c), staticLoader{content: []byte("func main() {}")}, worker.NoCrash, 5)
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	job, ok, err := c.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil || !ok {
		t.Fatalf("claim: %v", err)
	}
	oc, cause := w.ProcessClaimed(ctx, job)
	if oc != worker.OutcomeCommitted || cause != nil {
		t.Fatalf("want committed, got oc=%d cause=%v", oc, cause)
	}
	if id, ok, _ := c.ResolveView(ctx, "w", "a.go"); !ok || id == "" {
		t.Fatal("view not committed by ProcessClaimed")
	}
}
```

- [ ] **Step 2:** run `go test ./internal/enginev2/worker/ -run ProcessClaimed` → FAIL (undefined). **Step 3:** implement the refactor above. **Step 4:** `go test ./internal/enginev2/worker/ -race` → **all** Phase 3 worker tests + the new one PASS (behavior preserved). **Step 5:** commit `refactor(worker): extract ProcessClaimed seam for scheduler integration`.

---

## Chunk C — Catalog fairness/stats reads + the Engine (Tasks 5–6)

### Task 5: Catalog reads for fairness and stats

**Files:** Create `internal/enginev2/catalog/sqlite/queue.go`, `queue_test.go`.

**Interfaces — Produces (all read/claim, no schema change):**
- `func (c *Catalog) RepositoriesWithPendingJobs(ctx) ([]core.RepositoryID, error)` — distinct `repository_id` (join `index_jobs`→`worktrees`) of **unclaimed** jobs, ordered by id for determinism.
- `func (c *Catalog) ClaimNextJobInRepo(ctx, repo core.RepositoryID, minPriority core.Priority) (core.Job, bool, error)` — exactly `ClaimNextJob` but restricted to jobs whose worktree belongs to `repo` (enables round-robin fairness). Same claim semantics (`claimed=0 AND priority<=?`, order `priority ASC, job_id ASC`, mark `claimed=1`).
- `func (c *Catalog) QueueDepthByPriority(ctx) (map[core.Priority]int, error)` — count of unclaimed jobs grouped by priority (for `Stats`).

- [ ] **Step 1: failing test**

```go
// queue_test.go
package sqlite

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

func TestClaimNextJobInRepoAndPending(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "rA", "wA")
	seedRepoWorktree(t, c, "rB", "wB")
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "wA", Path: "a.go", DesiredHash: "h", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "wB", Path: "b.go", DesiredHash: "h", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))

	repos, err := c.RepositoriesWithPendingJobs(ctx)
	if err != nil || len(repos) != 2 {
		t.Fatalf("repos=%v err=%v", repos, err)
	}
	// Claiming in repo rA yields only rA's job.
	job, ok, err := c.ClaimNextJobInRepo(ctx, "rA", core.PriorityBootstrap)
	if err != nil || !ok || job.WorktreeID != "wA" {
		t.Fatalf("claim rA: job=%+v ok=%v err=%v", job, ok, err)
	}
	// rA now has no unclaimed jobs; rB still does.
	if _, ok, _ := c.ClaimNextJobInRepo(ctx, "rA", core.PriorityBootstrap); ok {
		t.Fatal("rA should be drained")
	}
	depth, err := c.QueueDepthByPriority(ctx)
	if err != nil || depth[core.PriorityReconcile] != 1 {
		t.Fatalf("depth=%v err=%v", depth, err)
	}
}
```

- [ ] **Step 2:** run → FAIL. **Step 3: implement `queue.go`** (mirror `ClaimNextJob` from `views.go`, adding the `repository_id` join; reuse the same `//nolint:gosec // #nosec G115` annotations on the `operation`/`priority` enum scans).

```go
// queue.go
package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// RepositoriesWithPendingJobs returns the repositories that currently have at
// least one unclaimed job, for round-robin fair scheduling.
func (c *Catalog) RepositoriesWithPendingJobs(ctx context.Context) ([]core.RepositoryID, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT DISTINCT w.repository_id
		FROM index_jobs j JOIN worktrees w ON w.worktree_id = j.worktree_id
		WHERE j.claimed=0
		ORDER BY w.repository_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var repos []core.RepositoryID
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			return nil, err
		}
		repos = append(repos, core.RepositoryID(r))
	}
	return repos, rows.Err()
}

// ClaimNextJobInRepo claims the highest-priority unclaimed job (at or above
// minPriority) whose worktree belongs to repo. Same claim discipline as
// ClaimNextJob, scoped to one repository for fair round-robin dispatch.
func (c *Catalog) ClaimNextJobInRepo(ctx context.Context, repo core.RepositoryID, minPriority core.Priority) (core.Job, bool, error) {
	var job core.Job
	var found bool
	err := c.withWriteTx(ctx, func(tx *sql.Tx) error {
		var (
			jobID            int64
			wt, path, hash   string
			gen              int64
			op, prio, att    int
		)
		row := tx.QueryRowContext(ctx, `
			SELECT j.job_id, j.worktree_id, j.relative_path, j.desired_hash, j.generation, j.operation, j.priority, j.attempts
			FROM index_jobs j JOIN worktrees w ON w.worktree_id = j.worktree_id
			WHERE j.claimed=0 AND j.priority<=? AND w.repository_id=?
			ORDER BY j.priority ASC, j.job_id ASC
			LIMIT 1`, int(minPriority), string(repo))
		if err := row.Scan(&jobID, &wt, &path, &hash, &gen, &op, &prio, &att); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE index_jobs SET claimed=1 WHERE job_id=?`, jobID); err != nil {
			return err
		}
		job = core.Job{
			WorktreeID:  core.WorktreeID(wt),
			Path:        path,
			DesiredHash: hash,
			Generation:  core.Generation(gen),
			Operation:   core.Operation(op),  //nolint:gosec // #nosec G115 - small enum persisted by this package
			Priority:    core.Priority(prio), //nolint:gosec // #nosec G115 - small enum persisted by this package
			Attempts:    att,
		}
		found = true
		return nil
	})
	if err != nil {
		return core.Job{}, false, err
	}
	return job, found, nil
}

// QueueDepthByPriority returns the count of unclaimed jobs per priority.
func (c *Catalog) QueueDepthByPriority(ctx context.Context) (map[core.Priority]int, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT priority, COUNT(*) FROM index_jobs WHERE claimed=0 GROUP BY priority`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[core.Priority]int{}
	for rows.Next() {
		var prio, n int
		if err := rows.Scan(&prio, &n); err != nil {
			return nil, err
		}
		out[core.Priority(prio)] = n //nolint:gosec // #nosec G115 - small enum persisted by this package
	}
	return out, rows.Err()
}
```

- [ ] **Step 4:** `go test ./internal/enginev2/catalog/sqlite/ -race && make lint` → PASS/clean. **Step 5:** commit `feat(catalog): per-repo claim, pending repos, and queue depth reads`.

---

### Task 6: The scheduler Engine

**Files:** Create `internal/enginev2/scheduler/engine.go`, `engine_test.go`.

**Design:**
- Semaphores are buffered channels: `indexSlots chan struct{}` cap `MaxIndexInflight`, `querySlots chan struct{}` cap `ReservedQueryInflight`. Acquire = send; release = receive. Independent pools ⇒ query capacity is never consumable by indexing (Gate 4 crit 2), and concurrent index dispatches never exceed `MaxIndexInflight` (crit 1).
- `Engine` depends on narrow interfaces so it is unit-testable: a `Queue` (the catalog reads/claim + `DeadLetterJob` + `UpsertJob` + `Recover`) and a `Processor` (`ProcessClaimed(ctx, job) (worker.Outcome, error)`, satisfied by `*worker.Worker`).
- `Run` is one goroutine: loop while `ctx` live — if circuit disallows, wait `min(probeInterval, poll)` on the clock; else round-robin the repos from `RepositoriesWithPendingJobs`, `ClaimNextJobInRepo` from the next repo, acquire an index slot, and `go e.dispatch(job)`. A `signal chan struct{}` (buffered 1) woken by `Submit` avoids busy-polling; also wake on a bounded `clock.After(pollInterval)` fallback.
- `dispatch`: run `ProcessClaimed`; on return, account the outcome (below), release the index slot, and `signal` the loop. Track `inFlight` with an atomic/mutex for `Stats`.
- **Outcome accounting:**
  - `OutcomeCommitted`/`OutcomeSuperseded` → `circuit.RecordSuccess()`; drop any retry state for the job key.
  - `OutcomePermanent` → `DeadLetterJob(job, "permanent: …")`; `circuit` unaffected (a shape error is not an endpoint-availability signal).
  - `OutcomeTransient` → `circuit.RecordFailure()`; increment the in-memory attempt count for the job key; if `>= MaxJobAttempts` → `DeadLetterJob(job, "attempts exhausted: …")` and drop state; else schedule a retry: `go func(){ <-e.clock.After(fullJitterDelay(attempt, cfg, e.unit())); if ctx live: re-acquire an index slot (respecting circuit) and dispatch(job) again }()`. The job is still `claimed=1`, so no re-claim is needed.
  - unclassified crash (`oc==0`) → leave claimed; `circuit.RecordFailure()` is NOT called (it's not a backend signal); log via `Stats`-visible counter is out of scope — just release the slot. (Crashes don't occur in production dispatch — the Engine constructs the worker with `NoCrash`.)
- `Submit(ctx, job)` → `queue.UpsertJob(job)` then non-blocking `signal`. (Durable enqueue; the daemon/reconciler call this instead of `UpsertJob` directly.)
- `AcquireIndexSlot(ctx)`/`AcquireQuerySlot(ctx)` → block on the respective channel honoring `ctx`; return a `release func()`. `AcquireQuerySlot` is what the future search path calls to hold reserved capacity; Gate 4 crit 2 tests it directly.
- `Stats()` → `{InFlight, ReservedQuery: len(querySlots-in-use), QueueDepthByPriority}` from the atomic counters + `QueueDepthByPriority(ctx)`. (Use a short `context.Background()` for the depth read, or cache the last depth on each loop turn to avoid a blocking DB read inside `Stats` — cache is simpler and race-safe under a mutex.)

**Interfaces — Produces:**
```go
type Queue interface {
	RepositoriesWithPendingJobs(ctx context.Context) ([]core.RepositoryID, error)
	ClaimNextJobInRepo(ctx context.Context, repo core.RepositoryID, minPriority core.Priority) (core.Job, bool, error)
	QueueDepthByPriority(ctx context.Context) (map[core.Priority]int, error)
	UpsertJob(ctx context.Context, job core.Job) error
	DeadLetterJob(ctx context.Context, job core.Job, reason string) error
	RequeueClaimedJobs(ctx context.Context) (int, error)
}
type Processor interface {
	ProcessClaimed(ctx context.Context, job core.Job) (worker.Outcome, error)
}
func New(cfg Config, q Queue, p Processor, clock Clock, seed int64) (*Engine, error) // validates cfg
func (e *Engine) Submit(ctx context.Context, job core.Job) error
func (e *Engine) Run(ctx context.Context) error
func (e *Engine) Stats() Stats
func (e *Engine) AcquireIndexSlot(ctx context.Context) (release func(), err error)
func (e *Engine) AcquireQuerySlot(ctx context.Context) (release func(), err error)
var _ Scheduler = (*Engine)(nil)
```

> **Concurrency discipline (required for `-race`):** the retry-attempt map, the cached queue depth, the round-robin cursor, and the in-flight counter are each guarded by one `sync.Mutex` (or `sync/atomic` for the counter). The circuit breaker is already internally locked. Slot channels need no lock. `e.unit()` wraps the seeded `*rand.Rand` in a mutex (rand.Rand is not concurrency-safe). Never hold a lock across a channel send/receive or a `ProcessClaimed` call.

- [ ] **Step 1: failing unit tests** (`engine_test.go`) — Submit enqueues + Stats reflects depth; AcquireIndexSlot blocks at capacity; AcquireQuerySlot independent of index saturation. Example:

```go
func TestAcquireQueryIndependentOfIndexSaturation(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfig() // index 1, query 1
	e := mustEngine(t, cfg) // helper: New with a fake queue + fake processor + FakeClock
	rel, err := e.AcquireIndexSlot(ctx) // saturate the single index slot
	if err != nil {
		t.Fatal(err)
	}
	defer rel()
	// A query slot must still be immediately available.
	done := make(chan struct{})
	go func() { r, _ := e.AcquireQuerySlot(ctx); r(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("query starved by index saturation")
	}
}
```

- [ ] **Step 2:** run → FAIL. **Step 3:** implement `engine.go` per the design. **Step 4:** `go test ./internal/enginev2/scheduler/ -race` → PASS. **Step 5:** commit `feat(scheduler): admission-control Engine with budget, fairness, retry, and circuit`.

---

## Chunk D — Gate 4 (Task 7)

### Task 7: Gate 4 integration tests

**Files:** Create `internal/enginev2/scheduler/gate4_test.go` (package `scheduler_test`, using a real `sqlite.Catalog` + real `worker.Worker` + `FakeEmbedder` + `FakeClock`).

**The three Gate 4 criteria, each an explicit test:**

**(a) Multiple repositories cannot exceed the global indexing budget.** `MaxIndexInflight=2`. Register 3 repos, flood each with several upsert jobs (distinct content). Use a `FakeEmbedder` variant (or a wrapping `Processor`) that records the **max concurrent** `ProcessClaimed` in flight (increment on entry, decrement on exit, track peak with a mutex; add a small blocking gate so overlap is observable). Run the Engine; drain; assert peak concurrency ≤ 2 and every job eventually committed (views resolve). Also assert work came from all 3 repos (fair round-robin, not one repo monopolizing).

**(b) Interactive queries retain reserved capacity during bootstrap.** `MaxIndexInflight=1`, `ReservedQueryInflight=1`. Flood the queue with `PriorityBootstrap` jobs whose `ProcessClaimed` blocks on a gate (so the single index slot stays occupied). While indexing is saturated, `AcquireQuerySlot(ctx)` must succeed within a deadline. Release the gate; the bootstrap work then drains.

**(c) An unavailable endpoint produces bounded calls without restart.** `CircuitOpenAfter=3`. `FakeEmbedder.SetError(5xx)` (all embeds fail → every job `OutcomeTransient`). Submit several jobs. Run the Engine on a goroutine. After the failures reach the threshold the circuit opens and the loop stops dispatching new claims — assert: (1) the total number of `ProcessClaimed` invocations is **bounded** (≤ a small function of `CircuitOpenAfter` + in-flight, not unbounded), (2) `Run` has **not returned** (the loop survives — the daemon does not restart), (3) `Stats()`/breaker `State()` reports `"open"`, and (4) after `clk.Advance(CircuitProbeInterval)` a single probe dispatch occurs; with the embedder still failing it re-opens (assert one additional attempt, not a storm). Then cancel `ctx` and assert `Run` returns.

> **Determinism:** all backoff and probe timing is driven by `FakeClock.Advance`; use a seeded Engine (`seed` fixed). Where a test needs a dispatch to be observably "in flight," gate `ProcessClaimed` on a channel the test controls (via a wrapping `Processor` around the real worker, or a fake `Processor`) rather than sleeping.

- [ ] **Step 1:** write the three tests (real catalog + worker; a small gating `Processor` wrapper for concurrency observation). **Step 2:** run `GOTOOLCHAIN=local go test ./internal/enginev2/scheduler/ -race -run Gate4 -v` → red until Task 6 lands, then PASS. **Step 3:** full gate — `go build ./... && CGO_ENABLED=0 go build ./... && go vet ./... && go test -race ./... && gofmt -l internal/enginev2 && make lint`. **Step 4:** commit `test(scheduler): Gate 4 — global budget, reserved query capacity, endpoint-down bounded`.

---

## Gate 4 Exit Criteria (spec §9, Phase 4)

1. **Global budget** — `TestGate4_GlobalIndexBudgetAcrossRepos`: peak concurrent index dispatches ≤ `MaxIndexInflight` with multiple repos flooding; all work completes; every repo makes progress (fairness).
2. **Reserved query capacity** — `TestGate4_QueryReservedDuringBootstrap`: `AcquireQuerySlot` succeeds while the index pool is saturated by bootstrap work.
3. **Bounded calls on endpoint failure** — `TestGate4_EndpointDownCircuitBounds`: a persistently failing endpoint trips the breaker after `CircuitOpenAfter`, `Run` keeps running (no restart), calls are bounded, and a single probe fires per `CircuitProbeInterval`.
4. **No data races** — `go test -race ./...` clean (the scheduler is concurrent; this is the headline gate).
5. **Build discipline** — `go build ./...`, `CGO_ENABLED=0 go build ./...`, `go vet ./...`, `gofmt -l internal/enginev2` empty, `make lint` 0; go.mod unchanged; no new module dependency.
6. **Determinism** — no `time.Now`/`time.After`/unseeded `rand` in `internal/enginev2/scheduler` non-test code (grep clean); all timing via `Clock` + seeded rand.

## Self-Review Notes

- **Spec coverage:** host-wide priority queue + fair scheduling (Engine round-robin over `RepositoriesWithPendingJobs` + priority-ordered `ClaimNextJobInRepo`) ✓; global in-flight budget + reserved query capacity (two semaphores) ✓; request/batch limits (`MaxIndexInflight`; per-file batching already in the builder) ✓; circuit breaker + bounded retries (circuit.go + full-jitter backoff + `MaxJobAttempts`→dead-letter) ✓; structured status (`Stats`) ✓. **Deferred by scope decision:** grepaid process/Unix-RPC/systemd, watcher, symbol/RPG, durable cross-restart backoff — documented in Scope/Non-goals.
- **Forward-dependency check:** the Engine depends only on the Phase 1/3 catalog + the Phase 3 worker seam; nothing here needs Phase 5's `Service` or the RPC transport. `Submit` durably enqueues via `UpsertJob`, so a future daemon wires `reconcile.Plan.Jobs` → `Submit` with no Engine change.
- **Type consistency:** `worker.Outcome`/`ProcessClaimed` are used identically by `ProcessOne`, the `Processor` interface, and the Engine dispatch. `Queue`/`Processor` interfaces match the concrete `*sqlite.Catalog` / `*worker.Worker` method sets. `Clock` is the Phase 0 interface satisfied by `FakeClock`.
- **Concurrency hazards to verify in review:** (1) no lock held across a channel op or `ProcessClaimed`; (2) the seeded `rand.Rand` is mutex-wrapped; (3) retry goroutines observe `ctx` cancellation and never dispatch after `Run` returns (use a `WaitGroup` so `Run` drains in-flight dispatch+retry goroutines before returning); (4) slot release is deferred/guaranteed on every dispatch exit path incl. panics; (5) the circuit's half-open single-probe cannot admit two concurrent probes (breaker lock covers the state transition).
- **Known deferred correctness note (documented, not a bug):** in-memory retry attempts and breaker state reset on process restart; a claimed job whose retry was pending is requeued by `worker.Recover` and retried immediately (safe). Durable backoff is a Phase-4-followup, not required by Gate 4.
