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
// injected Clock so behavior is deterministic under FakeClock. Every method is
// mutex-guarded — the breaker is shared across dispatch goroutines.
type circuitBreaker struct {
	clock         Clock
	openAfter     int
	probeInterval time.Duration

	mu       sync.Mutex
	state    circuitState
	failures int
	openedAt time.Time
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

// RecordFailure advances toward, or re-enters, the open state.
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

// abortProbe reverts a half-open probe back to open (re-stamping the timer)
// when the scheduler consumed a probe token via Allow but found no work to
// dispatch — so the breaker is not wedged half-open waiting for a probe result
// that never comes.
func (b *circuitBreaker) abortProbe() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == circuitHalfOpen {
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
