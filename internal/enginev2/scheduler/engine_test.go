package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
	"github.com/yoanbernabeu/grepai/internal/enginev2/worker"
)

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

// fakeProcessor always commits.
type fakeProcessor struct{}

func (fakeProcessor) ProcessClaimed(context.Context, core.Job) (worker.Outcome, error) {
	return worker.OutcomeCommitted, nil
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
}
