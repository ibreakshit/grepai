package scheduler_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yoanbernabeu/grepai/indexer"
	"github.com/yoanbernabeu/grepai/internal/enginev2/artifacts"
	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog/sqlite"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
	"github.com/yoanbernabeu/grepai/internal/enginev2/scheduler"
	"github.com/yoanbernabeu/grepai/internal/enginev2/worker"
)

// --- test helpers ---

func newCatalog(t *testing.T) *sqlite.Catalog {
	t.Helper()
	c, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func seedRepo(t *testing.T, c *sqlite.Catalog, repo core.RepositoryID, wt core.WorktreeID) {
	t.Helper()
	ctx := context.Background()
	if err := c.RegisterRepository(ctx, repo, "/"+string(repo), ""); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterWorktree(ctx, wt, repo, "/"+string(wt), 1); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateGeneration(ctx, repo, 1, "fp"); err != nil {
		t.Fatal(err)
	}
	if err := c.SetActiveGeneration(ctx, repo, 1); err != nil {
		t.Fatal(err)
	}
}

type staticLoader struct{}

func (staticLoader) Load(_ context.Context, _ core.RepositoryID, _, _, _ string) ([]byte, error) {
	return []byte("func main() {}"), nil
}

func realWorker(emb *enginetest.FakeEmbedder, c *sqlite.Catalog) *worker.Worker {
	return worker.New(c, artifacts.New(indexer.NewChunker(512, 50), emb, c), staticLoader{}, worker.NoCrash, 5)
}

// gatingProcessor wraps a Processor to observe max concurrency and, when gated,
// hold each call on a release channel so the test controls overlap.
type gatingProcessor struct {
	inner   scheduler.Processor
	gate    bool
	entered chan struct{}
	release chan struct{}

	mu               sync.Mutex
	cur, peak, calls int
}

func (g *gatingProcessor) ProcessClaimed(ctx context.Context, job core.Job) (worker.Outcome, error) {
	g.mu.Lock()
	g.cur++
	g.calls++
	if g.cur > g.peak {
		g.peak = g.cur
	}
	g.mu.Unlock()
	if g.gate {
		g.entered <- struct{}{}
		<-g.release
	}
	oc, err := g.inner.ProcessClaimed(ctx, job)
	g.mu.Lock()
	g.cur--
	g.mu.Unlock()
	return oc, err
}

func (g *gatingProcessor) peakConcurrency() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.peak
}

// countingProcessor just counts ProcessClaimed calls.
type countingProcessor struct {
	inner scheduler.Processor
	mu    sync.Mutex
	calls int
}

func (c *countingProcessor) ProcessClaimed(ctx context.Context, job core.Job) (worker.Outcome, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return c.inner.ProcessClaimed(ctx, job)
}

func (c *countingProcessor) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func waitUntil(t *testing.T, d time.Duration, pred func() bool) {
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

// --- Gate 4 (a): global index budget across repositories ---

func TestGate4_GlobalIndexBudgetAcrossRepos(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := scheduler.DefaultConfig()
	cfg.MaxIndexInflight = 2
	c := newCatalog(t)
	repos := []struct {
		repo core.RepositoryID
		wt   core.WorktreeID
	}{{"r1", "w1"}, {"r2", "w2"}, {"r3", "w3"}}
	for _, r := range repos {
		seedRepo(t, c, r.repo, r.wt)
		for _, p := range []string{"a.go", "b.go"} {
			must(t, c.UpsertJob(ctx, core.Job{WorktreeID: r.wt, Path: p, DesiredHash: string(r.wt) + p, Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
		}
	}
	emb := enginetest.NewFakeEmbedder(4)
	gp := &gatingProcessor{inner: realWorker(emb, c), gate: true, entered: make(chan struct{}, 64), release: make(chan struct{})}
	e, err := scheduler.New(cfg, c, gp, enginetest.NewFakeClock(time.Unix(0, 0)), 1)
	must(t, err)
	go func() { _ = e.Run(ctx) }()

	// Exactly MaxIndexInflight dispatches may start; a further one must not.
	<-gp.entered
	<-gp.entered
	select {
	case <-gp.entered:
		t.Fatal("index concurrency exceeded MaxIndexInflight")
	case <-time.After(200 * time.Millisecond):
	}
	if p := gp.peakConcurrency(); p != 2 {
		t.Fatalf("peak concurrency = %d, want 2", p)
	}

	// Drain: keep releasing so all jobs complete.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case gp.release <- struct{}{}:
			}
		}
	}()
	waitUntil(t, 5*time.Second, func() bool {
		for _, r := range repos {
			for _, p := range []string{"a.go", "b.go"} {
				if _, ok, _ := c.ResolveView(ctx, r.wt, p); !ok {
					return false
				}
			}
		}
		return true
	})
	if p := gp.peakConcurrency(); p > cfg.MaxIndexInflight {
		t.Fatalf("peak concurrency %d exceeded budget %d", p, cfg.MaxIndexInflight)
	}
	// Fairness: every repo made progress.
	for _, r := range repos {
		if _, ok, _ := c.ResolveView(ctx, r.wt, "a.go"); !ok {
			t.Fatalf("repo %s made no progress (starved)", r.repo)
		}
	}
}

// --- Gate 4 (b): interactive queries keep reserved capacity during bootstrap ---

func TestGate4_QueryReservedDuringBootstrap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := scheduler.DefaultConfig() // index 1, query 1
	c := newCatalog(t)
	seedRepo(t, c, "r1", "w1")
	for _, p := range []string{"a.go", "b.go", "c.go"} {
		must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w1", Path: p, DesiredHash: p, Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityBootstrap}))
	}
	emb := enginetest.NewFakeEmbedder(4)
	gp := &gatingProcessor{inner: realWorker(emb, c), gate: true, entered: make(chan struct{}, 64), release: make(chan struct{})}
	e, err := scheduler.New(cfg, c, gp, enginetest.NewFakeClock(time.Unix(0, 0)), 1)
	must(t, err)
	go func() { _ = e.Run(ctx) }()

	<-gp.entered // the single index slot is now occupied by a parked bootstrap job

	// A query slot must remain available despite index saturation.
	acquired := make(chan struct{})
	go func() {
		r, err := e.AcquireQuerySlot(ctx)
		if err == nil {
			r()
		}
		close(acquired)
	}()
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("interactive query starved during bootstrap")
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case gp.release <- struct{}{}:
			}
		}
	}()
}

// --- Gate 4 (c): unavailable endpoint => bounded calls, no restart ---

func TestGate4_EndpointDownCircuitBounds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := scheduler.DefaultConfig()
	cfg.MaxIndexInflight = 1
	cfg.CircuitOpenAfter = 3
	c := newCatalog(t)
	seedRepo(t, c, "r1", "w1")
	for _, p := range []string{"a.go", "b.go", "c.go", "d.go", "e.go"} {
		must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w1", Path: p, DesiredHash: p, Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	}
	emb := enginetest.NewFakeEmbedder(4)
	emb.SetError(errors.New("503 service unavailable")) // every embed fails => transient
	clk := enginetest.NewFakeClock(time.Unix(0, 0))
	cp := &countingProcessor{inner: realWorker(emb, c)}
	e, err := scheduler.New(cfg, c, cp, clk, 1)
	must(t, err)
	runDone := make(chan struct{})
	go func() { _ = e.Run(ctx); close(runDone) }()

	// The breaker opens after CircuitOpenAfter consecutive transient failures.
	waitUntil(t, 3*time.Second, func() bool { return e.Stats().Circuit == "open" })
	callsAtOpen := cp.count()

	// The scheduler loop must NOT have exited (no daemon restart).
	select {
	case <-runDone:
		t.Fatal("Run exited on endpoint failure")
	default:
	}
	// Calls are bounded, not an unbounded retry storm.
	if callsAtOpen > cfg.CircuitOpenAfter+cfg.MaxIndexInflight {
		t.Fatalf("unbounded calls before open: %d", callsAtOpen)
	}

	// A single probe fires per probe interval; the still-down endpoint re-opens.
	clk.Advance(cfg.CircuitProbeInterval)
	waitUntil(t, 3*time.Second, func() bool { return cp.count() > callsAtOpen })
	if cp.count() > callsAtOpen+2 {
		t.Fatalf("probe storm: %d calls after one interval", cp.count())
	}
	waitUntil(t, 3*time.Second, func() bool { return e.Stats().Circuit == "open" })

	cancel()
	select {
	case <-runDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// must fails the test on a non-nil error.
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
