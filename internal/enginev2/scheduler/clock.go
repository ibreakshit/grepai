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
