package scheduler

import (
	"context"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/worker"
)

// pollInterval is the fallback wake used when no Submit signal arrives, so an
// idle loop still re-checks the queue. Signals (from Submit and dispatch
// completion) are the primary wake; this only bounds worst-case latency.
const pollInterval = 1 * time.Second

// Queue is the durable job-queue surface the Engine paces over (the catalog).
type Queue interface {
	RepositoriesWithPendingJobs(ctx context.Context) ([]core.RepositoryID, error)
	ClaimNextJobInRepo(ctx context.Context, repo core.RepositoryID, minPriority core.Priority) (core.Job, bool, error)
	QueueDepthByPriority(ctx context.Context) (map[core.Priority]int, error)
	UpsertJob(ctx context.Context, job core.Job) error
	DeadLetterJob(ctx context.Context, job core.Job, reason string) error
}

// Processor processes one already-claimed job (satisfied by *worker.Worker).
// The bool reports whether the embedding backend was contacted, so the circuit
// breaker reacts only to real endpoint outcomes.
type Processor interface {
	ProcessClaimed(ctx context.Context, job core.Job) (worker.Outcome, bool, error)
}

// Engine is the single host-wide admission-control and pacing layer over the
// durable catalog queue and the worker (invariant 2: one host, one scheduler).
type Engine struct {
	cfg     Config
	q       Queue
	p       Processor
	clock   Clock
	breaker *circuitBreaker

	indexSlots chan struct{} // buffered cap MaxIndexInflight; send=acquire, recv=release
	querySlots chan struct{} // buffered cap ReservedQueryInflight; independent reserved pool
	signal     chan struct{} // buffered 1; wakes the Run loop

	wg sync.WaitGroup // tracks dispatch + retry goroutines so Run drains before returning

	mu       sync.Mutex
	rng      *rand.Rand
	attempts map[string]int
	depth    map[core.Priority]int
	inFlight int
	lastRepo core.RepositoryID // round-robin resume point (by identity, not index)
}

var _ Scheduler = (*Engine)(nil)

// New constructs an Engine. The seed makes jitter deterministic in tests.
func New(cfg Config, q Queue, p Processor, clock Clock, seed int64) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Engine{
		cfg:        cfg,
		q:          q,
		p:          p,
		clock:      clock,
		breaker:    newCircuitBreaker(clock, cfg.CircuitOpenAfter, cfg.CircuitProbeInterval),
		indexSlots: make(chan struct{}, cfg.MaxIndexInflight),
		querySlots: make(chan struct{}, cfg.ReservedQueryInflight),
		signal:     make(chan struct{}, 1),
		rng:        rand.New(rand.NewSource(seed)), // #nosec G404 - jitter only, not security-sensitive
		attempts:   map[string]int{},
		depth:      map[core.Priority]int{},
	}, nil
}

// Submit durably enqueues a job and wakes the loop.
func (e *Engine) Submit(ctx context.Context, job core.Job) error {
	if err := e.q.UpsertJob(ctx, job); err != nil {
		return err
	}
	e.wake()
	return nil
}

// Run drives the claim/dispatch loop until ctx is canceled, then waits for all
// in-flight dispatch and retry goroutines to finish before returning.
func (e *Engine) Run(ctx context.Context) error {
	defer e.wg.Wait()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		e.refreshDepth(ctx)

		// Acquire an index slot (blocks on a free slot or ctx). This is the
		// backpressure: at most MaxIndexInflight dispatches run at once.
		release, err := e.acquireIndex(ctx)
		if err != nil {
			return ctx.Err()
		}

		// Circuit gate. When open+elapsed, Allow grants a single half-open probe.
		// When not admitted, release the slot and wait for the probe interval.
		adm := e.breaker.Allow()
		if !adm.ok {
			release()
			if !e.waitBackoff(ctx) {
				return ctx.Err()
			}
			continue
		}

		job, ok, cerr := e.claimRoundRobin(ctx)
		if cerr != nil || !ok {
			// Admission granted but no work to dispatch: resolve the token so a
			// probe never wedges the breaker half-open.
			e.breaker.record(adm, resultAbort)
			release()
			if cerr != nil {
				// Transient read error: back off briefly, then retry the loop.
				if !e.waitBackoff(ctx) {
					return ctx.Err()
				}
				continue
			}
			if !e.waitForWork(ctx) {
				return ctx.Err()
			}
			continue
		}

		e.wg.Add(1)
		go e.dispatch(ctx, job, release, adm)
	}
}

// dispatch processes one claimed job, accounts the outcome (resolving its
// breaker admission), releases the slot, and wakes the loop. Runs in its own
// goroutine.
func (e *Engine) dispatch(ctx context.Context, job core.Job, release func(), adm admission) {
	defer e.wg.Done()
	defer e.wake()
	defer release()
	e.addInFlight(1)
	defer e.addInFlight(-1)

	oc, contacted, cause := e.p.ProcessClaimed(ctx, job)
	e.account(ctx, job, oc, contacted, cause, adm)
}

// account resolves the breaker admission exactly once and disposes of the job.
// The breaker reacts ONLY to a real endpoint signal: a call that never reached
// the backend (superseded, delete, fully cache-served, or a pre-build catalog
// error) resolves as an abort so it can neither trip nor falsely recover the
// circuit — in particular a half-open probe that made no endpoint call cannot
// close the breaker. Retry state is keyed by full intent identity (jobKey) so a
// superseding re-save neither inherits nor erases another intent's attempts.
func (e *Engine) account(ctx context.Context, job core.Job, oc worker.Outcome, contacted bool, cause error, adm admission) {
	switch {
	case !contacted:
		e.breaker.record(adm, resultAbort)
	case oc == worker.OutcomeTransient:
		e.breaker.record(adm, resultFailure)
	default: // committed / superseded / permanent that actually reached the endpoint
		e.breaker.record(adm, resultSuccess)
	}

	key := jobKey(job)
	switch oc {
	case worker.OutcomeCommitted, worker.OutcomeSuperseded:
		e.clearAttempts(key)
	case worker.OutcomePermanent:
		e.terminate(ctx, job, key, "permanent: "+errMsg(cause))
	case worker.OutcomeTransient:
		n := e.incAttempts(key)
		if n >= e.cfg.MaxJobAttempts {
			e.terminate(ctx, job, key, "attempts exhausted: "+errMsg(cause))
			return
		}
		e.scheduleRetry(ctx, job, n)
	default:
		// Unclassified (injected crash): the admission was already aborted
		// above; leave the job claimed for a restart's worker.Recover.
	}
}

// terminate dead-letters a job, retrying ONLY the terminal write (not the whole
// job) with growing capped backoff if the durable store errors — so a job is
// never silently left claimed, the original reason is preserved, and a broken
// store cannot spin. clearAttempts happens only after the write succeeds.
func (e *Engine) terminate(ctx context.Context, job core.Job, key, reason string) {
	if err := e.q.DeadLetterJob(ctx, job, reason); err != nil {
		e.scheduleTerminate(ctx, job, reason, 1)
		return
	}
	e.clearAttempts(key)
}

func (e *Engine) scheduleTerminate(ctx context.Context, job core.Job, reason string, attempt int) {
	delay := fullJitterDelay(attempt, e.cfg, e.unit())
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		select {
		case <-ctx.Done():
			return
		case <-e.clock.After(delay):
		}
		if ctx.Err() != nil {
			return
		}
		if err := e.q.DeadLetterJob(ctx, job, reason); err != nil {
			e.scheduleTerminate(ctx, job, reason, attempt+1) // grow backoff (rate bounded)
			return
		}
		e.clearAttempts(jobKey(job))
	}()
}

// scheduleRetry re-dispatches a transiently-failed job after a full-jitter
// backoff. The job is still claimed, so it needs no re-claim; the retry
// goroutine holds it until it can dispatch (respecting the circuit).
func (e *Engine) scheduleRetry(ctx context.Context, job core.Job, attempt int) {
	delay := fullJitterDelay(attempt, e.cfg, e.unit())
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		select {
		case <-ctx.Done():
			return
		case <-e.clock.After(delay):
		}
		e.dispatchWhenReady(ctx, job)
	}()
}

// dispatchWhenReady blocks until an index slot is free and the circuit admits
// a call, then dispatches the (still-claimed) job. It self-drives because a
// claimed job is invisible to the Run loop's claim.
func (e *Engine) dispatchWhenReady(ctx context.Context, job core.Job) {
	for {
		if ctx.Err() != nil {
			return
		}
		release, err := e.acquireIndex(ctx)
		if err != nil {
			return
		}
		adm := e.breaker.Allow()
		if adm.ok {
			e.wg.Add(1)
			e.dispatch(ctx, job, release, adm)
			return
		}
		release()
		if !e.waitBackoff(ctx) {
			return
		}
	}
}

// Stats returns a point-in-time snapshot.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	depth := make(map[core.Priority]int, len(e.depth))
	for k, v := range e.depth {
		depth[k] = v
	}
	return Stats{
		InFlight:             e.inFlight,
		ReservedQuery:        len(e.querySlots),
		QueueDepthByPriority: depth,
		Circuit:              e.breaker.State(),
	}
}

// AcquireIndexSlot blocks for an index slot (or ctx), returning a release func.
func (e *Engine) AcquireIndexSlot(ctx context.Context) (func(), error) {
	return e.acquireIndex(ctx)
}

// AcquireQuerySlot blocks for a reserved query slot (or ctx). Query capacity is
// an independent pool, so it is never consumed by indexing.
func (e *Engine) AcquireQuerySlot(ctx context.Context) (func(), error) {
	select {
	case e.querySlots <- struct{}{}:
		return func() { <-e.querySlots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// --- internals ---

func (e *Engine) acquireIndex(ctx context.Context) (func(), error) {
	select {
	case e.indexSlots <- struct{}{}:
		return func() { <-e.indexSlots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// claimRoundRobin claims one job, resuming after the last repository served (by
// identity, not index) so a repo cannot be starved by others being inserted or
// removed from the sorted pending set between claims. RepositoriesWithPendingJobs
// returns repos sorted ascending.
func (e *Engine) claimRoundRobin(ctx context.Context) (core.Job, bool, error) {
	repos, err := e.q.RepositoriesWithPendingJobs(ctx)
	if err != nil {
		return core.Job{}, false, err
	}
	if len(repos) == 0 {
		return core.Job{}, false, nil
	}
	e.mu.Lock()
	last := e.lastRepo
	e.mu.Unlock()
	// Start at the first repo strictly greater than the last one served; if none
	// is greater (last was the max, or was removed past the end), wrap to 0.
	start := 0
	for i, r := range repos {
		if r > last {
			start = i
			break
		}
	}
	for i := 0; i < len(repos); i++ {
		idx := (start + i) % len(repos)
		job, ok, err := e.q.ClaimNextJobInRepo(ctx, repos[idx], core.PriorityBootstrap)
		if err != nil {
			return core.Job{}, false, err
		}
		if ok {
			e.mu.Lock()
			e.lastRepo = repos[idx]
			e.mu.Unlock()
			return job, true, nil
		}
	}
	return core.Job{}, false, nil
}

func (e *Engine) waitBackoff(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-e.clock.After(e.cfg.CircuitProbeInterval):
		return true
	case <-e.signal:
		return true
	}
}

func (e *Engine) waitForWork(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-e.signal:
		return true
	case <-e.clock.After(pollInterval):
		return true
	}
}

func (e *Engine) wake() {
	select {
	case e.signal <- struct{}{}:
	default:
	}
}

func (e *Engine) refreshDepth(ctx context.Context) {
	d, err := e.q.QueueDepthByPriority(ctx)
	if err != nil {
		return
	}
	e.mu.Lock()
	e.depth = d
	e.mu.Unlock()
}

func (e *Engine) unit() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rng.Float64()
}

func (e *Engine) addInFlight(d int) {
	e.mu.Lock()
	e.inFlight += d
	e.mu.Unlock()
}

func (e *Engine) incAttempts(key string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.attempts[key]++
	return e.attempts[key]
}

func (e *Engine) clearAttempts(key string) {
	e.mu.Lock()
	delete(e.attempts, key)
	e.mu.Unlock()
}

// jobKey identifies a job by its full desired-intent (worktree, path,
// generation, desired hash, operation), so retry/attempt state for one intent
// is never inherited or erased by a superseding re-save of the same path.
func jobKey(j core.Job) string {
	return strings.Join([]string{
		string(j.WorktreeID),
		j.Path,
		strconv.FormatInt(int64(j.Generation), 10),
		j.DesiredHash,
		strconv.Itoa(int(j.Operation)),
	}, "\x00")
}

func errMsg(err error) string {
	if err == nil {
		return "unknown"
	}
	return err.Error()
}
