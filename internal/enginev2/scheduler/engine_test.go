package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
	"github.com/yoanbernabeu/grepai/internal/enginev2/worker"
)

// pollUntil polls pred until true or the deadline (internal-package helper).
func pollUntil(t *testing.T, d time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

// fakeQueue is a minimal in-memory Queue for Engine unit tests.
type fakeQueue struct {
	mu       sync.Mutex
	upserted []core.Job
}

func (q *fakeQueue) RepositoriesWithPendingJobs(context.Context) ([]core.RepositoryID, error) {
	return nil, nil
}
func (q *fakeQueue) ClaimNextJobInRepo(context.Context, core.RepositoryID, core.Priority) (core.Job, bool, error) {
	return core.Job{}, false, nil
}
func (q *fakeQueue) QueueDepthByPriority(context.Context) (map[core.Priority]int, error) {
	return map[core.Priority]int{}, nil
}
func (q *fakeQueue) UpsertJob(_ context.Context, job core.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.upserted = append(q.upserted, job)
	return nil
}
func (q *fakeQueue) DeadLetterJob(context.Context, core.Job, string) error { return nil }

// fakeProcessor always commits (with backend contact).
type fakeProcessor struct{}

func (fakeProcessor) ProcessClaimed(context.Context, core.Job) (worker.Outcome, bool, error) {
	return worker.OutcomeCommitted, true, nil
}

// countProc counts ProcessClaimed invocations (to prove terminal retry does not
// reprocess the job).
type countProc struct {
	mu    sync.Mutex
	calls int
}

func (c *countProc) ProcessClaimed(context.Context, core.Job) (worker.Outcome, bool, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return worker.OutcomePermanent, true, errors.New("bad dims")
}
func (c *countProc) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func mustEngine(t *testing.T, cfg Config) *Engine {
	t.Helper()
	e, err := New(cfg, &fakeQueue{}, fakeProcessor{}, enginetest.NewFakeClock(time.Unix(0, 0)), 1)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return e
}

func TestNewValidatesConfig(t *testing.T) {
	bad := DefaultConfig()
	bad.MaxIndexInflight = 0
	if _, err := New(bad, &fakeQueue{}, fakeProcessor{}, enginetest.NewFakeClock(time.Unix(0, 0)), 1); err == nil {
		t.Fatal("New must reject invalid config")
	}
}

func TestAcquireQueryIndependentOfIndexSaturation(t *testing.T) {
	ctx := context.Background()
	e := mustEngine(t, DefaultConfig()) // index 1, query 1
	rel, err := e.AcquireIndexSlot(ctx) // saturate the single index slot
	if err != nil {
		t.Fatal(err)
	}
	defer rel()
	done := make(chan struct{})
	go func() {
		r, err := e.AcquireQuerySlot(ctx)
		if err == nil {
			r()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("query starved by index saturation")
	}
}

func TestSubmitEnqueues(t *testing.T) {
	ctx := context.Background()
	q := &fakeQueue{}
	e, err := New(DefaultConfig(), q, fakeProcessor{}, enginetest.NewFakeClock(time.Unix(0, 0)), 1)
	if err != nil {
		t.Fatal(err)
	}
	job := core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityLiveChange}
	if err := e.Submit(ctx, job); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.upserted) != 1 || q.upserted[0].Path != "a.go" {
		t.Fatalf("submit did not enqueue: %+v", q.upserted)
	}
}

func TestStatsInitiallyZero(t *testing.T) {
	e := mustEngine(t, DefaultConfig())
	s := e.Stats()
	if s.InFlight != 0 || s.ReservedQuery != 0 {
		t.Fatalf("expected zeroed stats, got %+v", s)
	}
	if s.Circuit != "closed" {
		t.Fatalf("circuit should start closed, got %q", s.Circuit)
	}
}

// rrFakeQueue serves a scripted repository set on each claim and always has
// work, so claimRoundRobin's repo selection can be asserted directly.
type rrFakeQueue struct {
	repos [][]core.RepositoryID
	call  int
}

func (q *rrFakeQueue) RepositoriesWithPendingJobs(context.Context) ([]core.RepositoryID, error) {
	r := q.repos[q.call]
	q.call++
	return r, nil
}
func (q *rrFakeQueue) ClaimNextJobInRepo(_ context.Context, repo core.RepositoryID, _ core.Priority) (core.Job, bool, error) {
	return core.Job{WorktreeID: core.WorktreeID(repo), Path: "x"}, true, nil
}
func (q *rrFakeQueue) QueueDepthByPriority(context.Context) (map[core.Priority]int, error) {
	return map[core.Priority]int{}, nil
}
func (q *rrFakeQueue) UpsertJob(context.Context, core.Job) error             { return nil }
func (q *rrFakeQueue) DeadLetterJob(context.Context, core.Job, string) error { return nil }

// Retry/attempt state must be keyed by the full intent, so a superseding
// re-save of the same path neither inherits nor erases another intent's
// attempt count (Codex review Important #1).
func TestJobKeyDistinguishesIntent(t *testing.T) {
	base := core.Job{WorktreeID: "w", Path: "a.go", Generation: 1, DesiredHash: "h1", Operation: core.OpUpsert}
	twin := core.Job{WorktreeID: "w", Path: "a.go", Generation: 1, DesiredHash: "h1", Operation: core.OpUpsert}
	if jobKey(base) != jobKey(twin) {
		t.Fatal("identical intents must share a key")
	}
	reSave := base
	reSave.DesiredHash = "h2"
	if jobKey(base) == jobKey(reSave) {
		t.Fatal("a re-save (different desired hash) must not share retry state")
	}
	newGen := base
	newGen.Generation = 2
	if jobKey(base) == jobKey(newGen) {
		t.Fatal("a different generation must not share retry state")
	}
	del := base
	del.Operation = core.OpDelete
	del.DesiredHash = ""
	if jobKey(base) == jobKey(del) {
		t.Fatal("delete vs upsert must not share retry state")
	}
}

// faultQueue fails the first dlErrs DeadLetterJob calls, then succeeds, and
// records the reasons it was given.
type faultQueue struct {
	fakeQueue
	mu      sync.Mutex
	dlErrs  int
	dlCalls int
	reasons []string
}

func (q *faultQueue) ClaimNextJobInRepo(_ context.Context, repo core.RepositoryID, _ core.Priority) (core.Job, bool, error) {
	return core.Job{}, false, nil
}
func (q *faultQueue) DeadLetterJob(_ context.Context, _ core.Job, reason string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.dlCalls++
	q.reasons = append(q.reasons, reason)
	if q.dlCalls <= q.dlErrs {
		return errors.New("durable-store i/o")
	}
	return nil
}
func (q *faultQueue) calls() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dlCalls
}
func (q *faultQueue) allReasons(want string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, r := range q.reasons {
		if r != want {
			return false
		}
	}
	return len(q.reasons) > 0
}

// A terminal DeadLetterJob write failure must not silently abandon the job: the
// terminal transition is re-attempted (Codex Important #4), retrying ONLY the
// write (no full reprocess) and preserving the original reason (Codex re-review).
func TestAccountRetriesTerminalWriteFailure(t *testing.T) {
	ctx := context.Background()
	q := &faultQueue{dlErrs: 1}
	cp := &countProc{}
	clk := enginetest.NewFakeClock(time.Unix(0, 0))
	e, err := New(DefaultConfig(), q, cp, clk, 1)
	if err != nil {
		t.Fatal(err)
	}
	job := core.Job{WorktreeID: "w", Path: "a.go", Generation: 1, DesiredHash: "h1", Operation: core.OpUpsert}
	const reason = "permanent: bad dims"
	e.account(ctx, job, worker.OutcomePermanent, true, errors.New("bad dims"), admission{ok: true})
	if q.calls() != 1 {
		t.Fatalf("expected one failed dead-letter attempt, got %d", q.calls())
	}
	pollUntil(t, 3*time.Second, func() bool {
		clk.Advance(DefaultConfig().MaxRetryDelay)
		return q.calls() >= 2
	})
	if cp.count() != 0 {
		t.Fatalf("terminal retry must not reprocess the job (%d ProcessClaimed calls)", cp.count())
	}
	if !q.allReasons(reason) {
		t.Fatal("terminal retry must preserve the original dead-letter reason")
	}
}

// A half-open probe that made no endpoint call (superseded/delete/cache-served)
// must NOT close the breaker (Codex re-review Important #1).
func TestAccountProbeWithoutContactDoesNotClose(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfig()
	cfg.CircuitOpenAfter = 2
	clk := enginetest.NewFakeClock(time.Unix(0, 0))
	e, err := New(cfg, &fakeQueue{}, fakeProcessor{}, clk, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Open the breaker directly (avoid spawning retry goroutines).
	for i := 0; i < cfg.CircuitOpenAfter; i++ {
		e.breaker.record(e.breaker.Allow(), resultFailure)
	}
	if e.breaker.State() != "open" {
		t.Fatalf("expected open, got %s", e.breaker.State())
	}
	clk.Advance(cfg.CircuitProbeInterval)
	probe := e.breaker.Allow()
	if !probe.probe {
		t.Fatal("expected a half-open probe token")
	}
	job := core.Job{WorktreeID: "w", Path: "a.go", Generation: 1, DesiredHash: "h1", Operation: core.OpUpsert}
	e.account(ctx, job, worker.OutcomeSuperseded, false /*contacted*/, nil, probe)
	if e.breaker.State() == "closed" {
		t.Fatal("a probe with no endpoint contact must not close the breaker")
	}
}

// A half-open probe that really reached the endpoint successfully closes it.
func TestAccountProbeWithContactCloses(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfig()
	cfg.CircuitOpenAfter = 2
	clk := enginetest.NewFakeClock(time.Unix(0, 0))
	e, err := New(cfg, &fakeQueue{}, fakeProcessor{}, clk, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < cfg.CircuitOpenAfter; i++ {
		e.breaker.record(e.breaker.Allow(), resultFailure)
	}
	clk.Advance(cfg.CircuitProbeInterval)
	probe := e.breaker.Allow()
	job := core.Job{WorktreeID: "w", Path: "a.go", Generation: 1, DesiredHash: "h1", Operation: core.OpUpsert}
	e.account(ctx, job, worker.OutcomeCommitted, true /*contacted*/, nil, probe)
	if e.breaker.State() != "closed" {
		t.Fatalf("a real endpoint success on the probe must close: %s", e.breaker.State())
	}
}

// A repository must not be starved by others being inserted/removed from the
// sorted pending set between claims (Codex review Important #5).
func TestClaimRoundRobinResumesAfterLastRepo(t *testing.T) {
	ctx := context.Background()
	q := &rrFakeQueue{repos: [][]core.RepositoryID{
		{"a", "b"},        // -> a (resume after "")
		{"a", "aa1", "b"}, // -> aa1 (first > a)
		{"a", "b"},        // -> b   (first > aa1) : b is NOT starved
		{"a", "b"},        // -> a   (none > b, wrap)
	}}
	e, err := New(DefaultConfig(), q, fakeProcessor{}, enginetest.NewFakeClock(time.Unix(0, 0)), 1)
	if err != nil {
		t.Fatal(err)
	}
	want := []core.WorktreeID{"a", "aa1", "b", "a"}
	for i, w := range want {
		job, ok, err := e.claimRoundRobin(ctx)
		if err != nil || !ok {
			t.Fatalf("step %d: ok=%v err=%v", i, ok, err)
		}
		if job.WorktreeID != w {
			t.Fatalf("step %d: served %q, want %q (starvation)", i, job.WorktreeID, w)
		}
	}
}
