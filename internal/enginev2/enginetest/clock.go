// internal/enginev2/enginetest/clock.go
// Package enginetest provides deterministic fakes and fixtures for exercising
// the v2 engine contracts before production implementations exist.
package enginetest

import (
	"sync"
	"time"
)

// FakeClock is a deterministic, manually advanced clock implementing
// scheduler.Clock. Timers created via After fire when Advance crosses their
// deadline.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []fakeTimer
}

type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
}

// NewFakeClock returns a FakeClock set to start.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now returns the current fake time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After returns a channel that fires when the clock advances to now+d.
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.timers = append(c.timers, fakeTimer{deadline: c.now.Add(d), ch: ch})
	return ch
}

// Advance moves the clock forward by d, firing any timers whose deadline is
// now reached.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	remaining := c.timers[:0]
	for _, tm := range c.timers {
		if !c.now.Before(tm.deadline) {
			tm.ch <- c.now
		} else {
			remaining = append(remaining, tm)
		}
	}
	c.timers = remaining
}
