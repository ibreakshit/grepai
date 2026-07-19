package scheduler

import (
	"testing"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
)

func TestCircuitOpensAfterThresholdAndProbes(t *testing.T) {
	clk := enginetest.NewFakeClock(time.Unix(0, 0))
	b := newCircuitBreaker(clk, 3, 60*time.Second)
	if !b.Allow() {
		t.Fatal("starts closed")
	}
	b.RecordFailure()
	b.RecordFailure()
	if b.State() != "closed" {
		t.Fatalf("still closed under threshold: %s", b.State())
	}
	b.RecordFailure() // 3rd => open
	if b.State() != "open" || b.Allow() {
		t.Fatalf("must be open and disallow: state=%s", b.State())
	}
	// Before probe interval: still disallowed.
	clk.Advance(59 * time.Second)
	if b.Allow() {
		t.Fatal("probe too early")
	}
	// At probe interval: one probe allowed, then half-open blocks further.
	clk.Advance(1 * time.Second)
	if !b.Allow() {
		t.Fatal("probe should be allowed at interval")
	}
	if b.State() != "half-open" || b.Allow() {
		t.Fatalf("half-open must block a second probe: state=%s", b.State())
	}
	// Probe success closes.
	b.RecordSuccess()
	if b.State() != "closed" || !b.Allow() {
		t.Fatalf("success must close: %s", b.State())
	}
}

func TestCircuitHalfOpenFailureReopens(t *testing.T) {
	clk := enginetest.NewFakeClock(time.Unix(0, 0))
	b := newCircuitBreaker(clk, 1, 10*time.Second)
	b.RecordFailure() // open
	clk.Advance(10 * time.Second)
	if !b.Allow() {
		t.Fatal("probe allowed")
	}
	b.RecordFailure() // half-open failure re-opens
	if b.State() != "open" || b.Allow() {
		t.Fatalf("reopen expected: %s", b.State())
	}
	clk.Advance(10 * time.Second)
	if !b.Allow() {
		t.Fatal("re-probe after another interval")
	}
}
