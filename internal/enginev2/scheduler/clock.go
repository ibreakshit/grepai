package scheduler

import "time"

// Clock abstracts time so retry/backoff scheduling is deterministically
// testable. Production uses a real-time clock; tests use enginetest.FakeClock.
type Clock interface {
	// Now returns the current time on this clock.
	Now() time.Time
	// After returns a channel that receives once d has elapsed on this clock.
	After(d time.Duration) <-chan time.Time
}

// SystemClock is the production Clock backed by the real monotonic clock.
type SystemClock struct{}

var _ Clock = SystemClock{}

// Now returns the current wall-clock time.
func (SystemClock) Now() time.Time { return time.Now() }

// After delegates to time.After.
func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
