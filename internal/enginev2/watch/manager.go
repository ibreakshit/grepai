// Package watch is grepaid's continuous file-change detector: filesystem
// events are HINTS that mark a worktree dirty and schedule a debounced
// reconcile — they never create index jobs directly. Content hashing inside
// reconcile decides whether anything actually changed, so a dropped,
// duplicated, or mid-write event can at worst delay freshness, never corrupt
// the index (spec: docs/superpowers/specs/2026-07-20-grepaid-watcher-design.md).
package watch

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/scheduler"
)

// Sentinel errors a Backend reports on its Errors channel (or from Start).
var (
	// ErrOverflow means the kernel event queue overflowed: hints were lost and
	// the worktree must be treated as dirty.
	ErrOverflow = errors.New("watch: event queue overflow")
	// ErrExhausted means watch descriptors ran out (inotify max_user_watches):
	// the repo degrades to periodic polling.
	ErrExhausted = errors.New("watch: watch descriptors exhausted")
	// ErrRootGone means the watched root disappeared: the watch stops.
	ErrRootGone = errors.New("watch: root directory gone")
)

// Config tunes the debounce state machine. Zero values take defaults.
type Config struct {
	Quiet        time.Duration // quiet window after the last hint (default 1s)
	MaxLatency   time.Duration // reconcile at least this often under churn (default 10s)
	PollInterval time.Duration // poll cadence for an exhausted repo (default 5m)
	SafetyNet    time.Duration // unconditional reconcile cadence (default 1h)
}

func (c Config) withDefaults() Config {
	if c.Quiet <= 0 {
		c.Quiet = time.Second
	}
	if c.MaxLatency <= 0 {
		c.MaxLatency = 10 * time.Second
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 5 * time.Minute
	}
	if c.SafetyNet <= 0 {
		c.SafetyNet = time.Hour
	}
	return c
}

// Backend is the raw event source for one worktree root (real: fsnotify).
type Backend interface {
	// Start begins watching. It may return ErrExhausted to request poll mode.
	Start() error
	// Hints delivers coalesced "something changed under root" signals.
	Hints() <-chan struct{}
	// Errors delivers ErrOverflow / ErrExhausted / ErrRootGone / other.
	Errors() <-chan error
	Close() error
}

// BackendFactory builds a Backend for a root.
type BackendFactory func(root string) (Backend, error)

// ReconcileFunc triggers one reconcile for a worktree (the daemon wires this
// to service.Reconcile with live priority).
type ReconcileFunc func(ctx context.Context, wt core.WorktreeID) error

// Manager owns one debounced watch per registered worktree.
type Manager struct {
	cfg     Config
	clock   scheduler.Clock
	rec     ReconcileFunc
	factory BackendFactory
	lg      *log.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	repos  map[core.WorktreeID]*wtWatch
	closed bool
}

type wtWatch struct {
	root     string
	backend  Backend // nil in poll mode
	inFlight atomic.Bool
}

// New constructs a Manager. Close must be called to release it.
func New(cfg Config, clock scheduler.Clock, rec ReconcileFunc, factory BackendFactory, lg *log.Logger) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		cfg: cfg.withDefaults(), clock: clock, rec: rec, factory: factory, lg: lg,
		ctx: ctx, cancel: cancel, repos: make(map[core.WorktreeID]*wtWatch),
	}
}

// Watch starts (idempotently) watching root for the given worktree.
func (m *Manager) Watch(root string, wt core.WorktreeID) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errors.New("watch: manager closed")
	}
	if _, ok := m.repos[wt]; ok {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock() // factory/Start may do filesystem work; don't hold the lock

	b, err := m.factory(root)
	if err != nil {
		return err
	}
	pollMode := false
	if err := b.Start(); err != nil {
		_ = b.Close()
		if !errors.Is(err, ErrExhausted) {
			return err
		}
		// Descriptor exhaustion at startup: degrade this repo to polling.
		m.lg.Printf("watch: %s: %v; degrading to %s polling", wt, err, m.cfg.PollInterval)
		b = nil
		pollMode = true
	}

	st := &wtWatch{root: root, backend: b}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		if b != nil {
			_ = b.Close()
		}
		return errors.New("watch: manager closed")
	}
	if _, ok := m.repos[wt]; ok { // lost a Watch race; keep the first
		m.mu.Unlock()
		if b != nil {
			_ = b.Close()
		}
		return nil
	}
	m.repos[wt] = st
	m.wg.Add(1)
	m.mu.Unlock()

	go m.loop(st, wt, pollMode)
	return nil
}

// Close stops every watch and waits for the loops to exit. Idempotent.
func (m *Manager) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	m.mu.Unlock()
	m.cancel()
	m.wg.Wait()
}

// loop is the per-worktree debounce state machine. Reconciles run inline
// (serialization for free); hints arriving during a reconcile buffer in the
// backend channel and coalesce into exactly one trailing reconcile.
func (m *Manager) loop(st *wtWatch, wt core.WorktreeID, pollMode bool) {
	defer m.wg.Done()
	defer func() {
		if st.backend != nil {
			_ = st.backend.Close()
		}
	}()

	var (
		dirty        bool
		quiet, maxLt <-chan time.Time
		poll         <-chan time.Time
	)
	safety := m.clock.After(m.cfg.SafetyNet)
	if pollMode {
		poll = m.clock.After(m.cfg.PollInterval)
	}
	hints, errsCh := m.hintChans(st)

	fire := func() {
		if !dirty {
			return
		}
		dirty = false
		quiet, maxLt = nil, nil
		st.inFlight.Store(true)
		err := m.rec(m.ctx, wt)
		st.inFlight.Store(false)
		if err != nil {
			if m.ctx.Err() != nil {
				return // shutting down; not a retryable failure
			}
			m.lg.Printf("watch: %s: reconcile failed (will retry): %v", wt, err)
			dirty = true
			quiet = m.clock.After(m.cfg.Quiet)
		}
	}

	for {
		select {
		case <-m.ctx.Done():
			return

		case _, ok := <-hints:
			if !ok {
				hints = nil // backend closed its channel; errors/polling continue
				continue
			}
			dirty = true
			quiet = m.clock.After(m.cfg.Quiet)
			if maxLt == nil {
				maxLt = m.clock.After(m.cfg.MaxLatency)
			}

		case <-quiet:
			fire()

		case <-maxLt:
			fire()

		case <-safety:
			safety = m.clock.After(m.cfg.SafetyNet)
			dirty = true
			fire()

		case <-poll:
			poll = m.clock.After(m.cfg.PollInterval)
			dirty = true
			fire()

		case err, ok := <-errsCh:
			if !ok {
				errsCh = nil
				continue
			}
			switch {
			case errors.Is(err, ErrOverflow):
				// Hints were lost; a reconcile re-derives everything anyway.
				m.lg.Printf("watch: %s: %v; scheduling reconcile", wt, err)
				dirty = true
				quiet = m.clock.After(m.cfg.Quiet)
			case errors.Is(err, ErrExhausted):
				m.lg.Printf("watch: %s: %v; degrading to %s polling", wt, err, m.cfg.PollInterval)
				if st.backend != nil {
					_ = st.backend.Close()
					st.backend = nil
				}
				hints, errsCh = nil, nil
				poll = m.clock.After(m.cfg.PollInterval)
			case errors.Is(err, ErrRootGone):
				m.lg.Printf("watch: %s: %v; stopping watch", wt, err)
				m.mu.Lock()
				delete(m.repos, wt)
				m.mu.Unlock()
				return
			default:
				m.lg.Printf("watch: %s: backend error: %v", wt, err)
			}
		}
	}
}

// hintChans returns the backend channels, or nil channels in poll mode.
func (m *Manager) hintChans(st *wtWatch) (<-chan struct{}, <-chan error) {
	if st.backend == nil {
		return nil, nil
	}
	return st.backend.Hints(), st.backend.Errors()
}

// watching reports whether wt currently has an active watch (tests/status).
func (m *Manager) watching(wt core.WorktreeID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.repos[wt]
	return ok
}

// running reports whether wt has a reconcile in flight (tests).
func (m *Manager) running(wt core.WorktreeID) bool {
	m.mu.Lock()
	st, ok := m.repos[wt]
	m.mu.Unlock()
	return ok && st.inFlight.Load()
}
