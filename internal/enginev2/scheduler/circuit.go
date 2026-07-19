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

// breakerResult is how a dispatched call resolves its admission.
type breakerResult int

const (
	resultSuccess breakerResult = iota // the backend answered (reachable)
	resultFailure                      // a transient/availability failure
	resultAbort                        // no real call happened (no work, or unclassified crash)
)

// admission is the token returned by Allow. It ties a dispatched call to the
// breaker generation (epoch) it was admitted under, and marks whether it was
// the single half-open probe. record must be called exactly once per granted
// admission so a probe can never wedge the breaker half-open.
type admission struct {
	epoch int
	probe bool
	ok    bool // false = admission denied (do not dispatch, do not record)
}

// circuitBreaker trips after openAfter consecutive failures and, once open,
// admits a single half-open probe every probeInterval. epoch increments on
// every open so a stale completion admitted under an earlier generation cannot
// reset a breaker that has since opened. All time comes from the injected Clock.
type circuitBreaker struct {
	clock         Clock
	openAfter     int
	probeInterval time.Duration

	mu       sync.Mutex
	state    circuitState
	failures int
	epoch    int
	openedAt time.Time
}

func newCircuitBreaker(clock Clock, openAfter int, probeInterval time.Duration) *circuitBreaker {
	return &circuitBreaker{clock: clock, openAfter: openAfter, probeInterval: probeInterval, state: circuitClosed}
}

// Allow returns an admission. A granted admission (ok=true) MUST be resolved by
// exactly one record call. When open and the probe interval has elapsed it
// transitions to half-open and grants exactly one probe.
func (b *circuitBreaker) Allow() admission {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case circuitClosed:
		return admission{epoch: b.epoch, ok: true}
	case circuitOpen:
		if !b.clock.Now().Before(b.openedAt.Add(b.probeInterval)) {
			b.state = circuitHalfOpen
			return admission{epoch: b.epoch, probe: true, ok: true}
		}
		return admission{}
	default: // half-open: a probe is already in flight
		return admission{}
	}
}

// record resolves a granted admission. Stale non-probe completions (whose epoch
// no longer matches, because the breaker opened since they were admitted) are
// ignored so they cannot reset a newer state. The half-open probe is resolved
// by its token regardless of outcome, so it never wedges.
func (b *circuitBreaker) record(a admission, r breakerResult) {
	if !a.ok {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if a.probe {
		if b.state != circuitHalfOpen {
			return // already resolved
		}
		if r == resultSuccess {
			b.state = circuitClosed
			b.failures = 0
			return
		}
		// failure or abort: reopen and wait another interval.
		b.state = circuitOpen
		b.epoch++
		b.openedAt = b.clock.Now()
		return
	}
	// Non-probe (admitted while closed): ignore if the breaker has since opened.
	if a.epoch != b.epoch {
		return
	}
	switch r {
	case resultSuccess:
		b.failures = 0
	case resultFailure:
		b.failures++
		if b.failures >= b.openAfter {
			b.state = circuitOpen
			b.epoch++
			b.openedAt = b.clock.Now()
		}
	case resultAbort:
		// No real call happened; leave the closed state untouched.
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
