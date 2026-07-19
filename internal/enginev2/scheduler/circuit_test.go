package scheduler

import (
	"testing"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
)

// failN admits and fails n calls, opening the breaker.
func failN(b *circuitBreaker, n int) {
	for i := 0; i < n; i++ {
		a := b.Allow()
		b.record(a, resultFailure)
	}
}

func TestCircuitOpensAfterThresholdAndProbes(t *testing.T) {
	clk := enginetest.NewFakeClock(time.Unix(0, 0))
	b := newCircuitBreaker(clk, 3, 60*time.Second)
	if a := b.Allow(); !a.ok {
		t.Fatal("starts closed")
	} else {
		b.record(a, resultSuccess)
	}
	failN(b, 2)
	if b.State() != "closed" {
		t.Fatalf("still closed under threshold: %s", b.State())
	}
	failN(b, 1) // 3rd => open
	if b.State() != "open" || b.Allow().ok {
		t.Fatalf("must be open and disallow: state=%s", b.State())
	}
	clk.Advance(59 * time.Second)
	if b.Allow().ok {
		t.Fatal("probe too early")
	}
	clk.Advance(1 * time.Second)
	probe := b.Allow()
	if !probe.ok || !probe.probe {
		t.Fatal("probe should be allowed at interval")
	}
	if b.State() != "half-open" || b.Allow().ok {
		t.Fatalf("half-open must block a second probe: state=%s", b.State())
	}
	b.record(probe, resultSuccess) // probe success closes
	if b.State() != "closed" || !b.Allow().ok {
		t.Fatalf("success must close: %s", b.State())
	}
}

func TestCircuitHalfOpenFailureReopens(t *testing.T) {
	clk := enginetest.NewFakeClock(time.Unix(0, 0))
	b := newCircuitBreaker(clk, 1, 10*time.Second)
	failN(b, 1) // open
	clk.Advance(10 * time.Second)
	probe := b.Allow()
	if !probe.ok {
		t.Fatal("probe allowed")
	}
	b.record(probe, resultFailure) // half-open failure re-opens
	if b.State() != "open" || b.Allow().ok {
		t.Fatalf("reopen expected: %s", b.State())
	}
	clk.Advance(10 * time.Second)
	if !b.Allow().ok {
		t.Fatal("re-probe after another interval")
	}
}

// A stale completion admitted while closed must not reset a breaker that opened
// after it was admitted (the MaxIndexInflight>1 race).
func TestCircuitStaleClosedSuccessIgnored(t *testing.T) {
	clk := enginetest.NewFakeClock(time.Unix(0, 0))
	b := newCircuitBreaker(clk, 2, 60*time.Second)
	slow := b.Allow() // admitted while closed (epoch 0)
	if !slow.ok {
		t.Fatal("first admission")
	}
	failN(b, 2) // two other calls fail => breaker opens (epoch advances)
	if b.State() != "open" {
		t.Fatalf("should be open, got %s", b.State())
	}
	b.record(slow, resultSuccess) // stale success must be ignored
	if b.State() != "open" {
		t.Fatalf("stale success wrongly reset breaker: %s", b.State())
	}
}

// A half-open probe that returns a non-failure, non-success result (abort) must
// still resolve the probe (reopen), never wedge half-open forever.
func TestCircuitHalfOpenAbortDoesNotWedge(t *testing.T) {
	clk := enginetest.NewFakeClock(time.Unix(0, 0))
	b := newCircuitBreaker(clk, 1, 10*time.Second)
	failN(b, 1) // open
	clk.Advance(10 * time.Second)
	probe := b.Allow()
	if !probe.ok || !probe.probe {
		t.Fatal("probe expected")
	}
	b.record(probe, resultAbort) // e.g. an unclassified crash on the probe
	if b.State() != "open" {
		t.Fatalf("abort must reopen, not wedge: %s", b.State())
	}
	clk.Advance(10 * time.Second)
	if !b.Allow().ok {
		t.Fatal("breaker must be probeable again after an aborted probe")
	}
}

// A permanent outcome on the probe proves reachability and closes the breaker.
func TestCircuitHalfOpenPermanentCloses(t *testing.T) {
	clk := enginetest.NewFakeClock(time.Unix(0, 0))
	b := newCircuitBreaker(clk, 1, 10*time.Second)
	failN(b, 1)
	clk.Advance(10 * time.Second)
	probe := b.Allow()
	b.record(probe, resultSuccess) // permanent is accounted as success (reachable)
	if b.State() != "closed" {
		t.Fatalf("permanent probe should close (reachable): %s", b.State())
	}
}
