package scheduler

import (
	"testing"
	"time"
)

func TestFullJitterBoundsAndCap(t *testing.T) {
	cfg := DefaultConfig() // base 1s, max 5m
	// unit near 1 (max end): grows exponentially but never exceeds the cap.
	if d := fullJitterDelay(1, cfg, 0.999999); d > 1*time.Second {
		t.Fatalf("attempt1 exceeded base cap: %s", d)
	}
	if d := fullJitterDelay(3, cfg, 0.999999); d > 4*time.Second+1 {
		t.Fatalf("attempt3 cap wrong: %s", d)
	}
	// Very large attempt must clamp to MaxRetryDelay, not overflow.
	if d := fullJitterDelay(100, cfg, 0.999999); d > cfg.MaxRetryDelay {
		t.Fatalf("attempt100 exceeded max: %s", d)
	}
	// unit=0 => zero delay (full jitter lower bound).
	if d := fullJitterDelay(5, cfg, 0.0); d != 0 {
		t.Fatalf("unit 0 must be zero, got %s", d)
	}
}
