package watch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
)

// fakeBackend delivers scripted hints/errors.
type fakeBackend struct {
	hints    chan struct{}
	errs     chan error
	startErr error
	mu       sync.Mutex
	closed   bool
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{hints: make(chan struct{}, 16), errs: make(chan error, 4)}
}
func (b *fakeBackend) Start() error           { return b.startErr }
func (b *fakeBackend) Hints() <-chan struct{} { return b.hints }
func (b *fakeBackend) Errors() <-chan error   { return b.errs }
func (b *fakeBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

// harness wires a Manager to a fake backend + FakeClock + counting reconciler.
type harness struct {
	m       *Manager
	clock   *enginetest.FakeClock
	backend *fakeBackend
	recN    atomic.Int64
	recErr  error         // guarded by blockMu; returned from reconcile
	blockCh chan struct{} // if non-nil, reconcile blocks until it receives
	blockMu sync.Mutex
}

func (h *harness) setErr(err error) {
	h.blockMu.Lock()
	h.recErr = err
	h.blockMu.Unlock()
}

func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	h := &harness{clock: enginetest.NewFakeClock(time.Unix(1_700_000_000, 0)), backend: newFakeBackend()}
	rec := func(ctx context.Context, wt core.WorktreeID) error {
		h.blockMu.Lock()
		bc := h.blockCh
		e := h.recErr
		h.blockMu.Unlock()
		if bc != nil {
			<-bc
		}
		h.recN.Add(1)
		return e
	}
	factory := func(root string) (Backend, error) { return h.backend, nil }
	h.m = New(cfg, h.clock, rec, factory, testLogger(t))
	t.Cleanup(h.m.Close)
	if err := h.m.Watch("/repo", core.WorktreeID("/repo")); err != nil {
		t.Fatalf("Watch: %v", err)
	}
	return h
}

// advanceUntil advances the fake clock in steps until cond holds (bounded).
func (h *harness) advanceUntil(t *testing.T, step time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not reached (reconciles=%d)", h.recN.Load())
		}
		h.clock.Advance(step)
		time.Sleep(time.Millisecond)
	}
}

// settle gives loop goroutines a moment and asserts the count is stable.
func (h *harness) settle(t *testing.T, want int64) {
	t.Helper()
	time.Sleep(30 * time.Millisecond)
	if got := h.recN.Load(); got != want {
		t.Fatalf("reconciles = %d, want %d", got, want)
	}
}

func testCfg() Config {
	return Config{Quiet: time.Second, MaxLatency: 10 * time.Second, PollInterval: 5 * time.Minute, SafetyNet: time.Hour}
}

func TestQuietWindowCoalescesStorm(t *testing.T) {
	h := newHarness(t, testCfg())
	for i := 0; i < 50; i++ { // a branch-switch storm
		h.backend.hints <- struct{}{}
	}
	h.settle(t, 0) // nothing fires before the quiet window
	h.advanceUntil(t, time.Second, func() bool { return h.recN.Load() >= 1 })
	h.settle(t, 1) // exactly one reconcile for the whole storm
}

func TestMaxLatencyBoundsContinuousChurn(t *testing.T) {
	h := newHarness(t, testCfg())
	// Keep hinting so the quiet window never elapses; advance in sub-quiet
	// steps. Max-latency (10s) must force a reconcile anyway.
	deadline := time.Now().Add(3 * time.Second)
	for h.recN.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("max-latency never fired under continuous churn")
		}
		h.backend.hints <- struct{}{}
		h.clock.Advance(500 * time.Millisecond) // < Quiet, so quiet keeps re-arming
		time.Sleep(time.Millisecond)
	}
}

func TestSerializedWithOneTrailingReconcile(t *testing.T) {
	h := newHarness(t, testCfg())
	h.blockMu.Lock()
	h.blockCh = make(chan struct{})
	h.blockMu.Unlock()

	h.backend.hints <- struct{}{}
	h.advanceUntil(t, time.Second, func() bool { return h.m.running(core.WorktreeID("/repo")) })
	// While the reconcile is blocked, more hints arrive.
	for i := 0; i < 5; i++ {
		h.backend.hints <- struct{}{}
	}
	h.blockMu.Lock()
	close(h.blockCh)
	h.blockCh = nil
	h.blockMu.Unlock()
	// The trailing hints coalesce into exactly ONE more reconcile.
	h.advanceUntil(t, time.Second, func() bool { return h.recN.Load() >= 2 })
	h.settle(t, 2)
}

func TestFailedReconcileRetries(t *testing.T) {
	h := newHarness(t, testCfg())
	h.setErr(errors.New("embedder down"))
	h.backend.hints <- struct{}{}
	h.advanceUntil(t, time.Second, func() bool { return h.recN.Load() >= 1 })
	// Still dirty after failure: it retries without new hints.
	h.advanceUntil(t, time.Second, func() bool { return h.recN.Load() >= 2 })
	// Recovery: clear the error, next retry succeeds and stops retrying.
	h.setErr(nil)
	h.advanceUntil(t, time.Second, func() bool { return h.recN.Load() >= 3 })
	n := h.recN.Load()
	h.clock.Advance(30 * time.Second)
	h.settle(t, n) // no further reconciles until safety net
}

func TestSafetyNetTicks(t *testing.T) {
	h := newHarness(t, testCfg())
	h.advanceUntil(t, time.Hour, func() bool { return h.recN.Load() >= 1 })
	h.advanceUntil(t, time.Hour, func() bool { return h.recN.Load() >= 2 })
}

func TestOverflowMarksDirty(t *testing.T) {
	h := newHarness(t, testCfg())
	h.backend.errs <- ErrOverflow
	h.advanceUntil(t, time.Second, func() bool { return h.recN.Load() >= 1 })
}

func TestExhaustedDegradesToPolling(t *testing.T) {
	cfg := testCfg()
	h := newHarness(t, cfg)
	h.backend.errs <- ErrExhausted
	// Poll mode: no hints needed; every PollInterval a reconcile fires.
	h.advanceUntil(t, time.Minute, func() bool { return h.recN.Load() >= 1 })
	h.advanceUntil(t, time.Minute, func() bool { return h.recN.Load() >= 2 })
}

func TestRootGoneStopsWatch(t *testing.T) {
	h := newHarness(t, testCfg())
	h.backend.errs <- ErrRootGone
	h.advanceUntil(t, time.Second, func() bool { return !h.m.watching(core.WorktreeID("/repo")) })
	// No reconciles after stop, even across safety-net horizons.
	h.clock.Advance(3 * time.Hour)
	h.settle(t, 0)
}

func TestWatchIdempotentAndCloseWaits(t *testing.T) {
	h := newHarness(t, testCfg())
	if err := h.m.Watch("/repo", core.WorktreeID("/repo")); err != nil {
		t.Fatalf("second Watch should be a no-op, got %v", err)
	}
	h.m.Close() // must not deadlock or panic; Cleanup will Close again (idempotent)
}
