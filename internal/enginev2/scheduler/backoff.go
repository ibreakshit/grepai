package scheduler

import "time"

// fullJitterDelay returns an AWS "full jitter" backoff delay for a 1-based
// attempt: a uniform sample in [0, limit) where limit = min(MaxRetryDelay,
// BaseRetryDelay * 2^(attempt-1)). unit is a caller-supplied sample in [0,1)
// (from a seeded rand) so the delay is deterministic in tests.
func fullJitterDelay(attempt int, cfg Config, unit float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	// Double from the base, stopping at the max so the shift can never overflow.
	d := cfg.BaseRetryDelay
	for i := 1; i < attempt; i++ {
		if d >= cfg.MaxRetryDelay {
			d = cfg.MaxRetryDelay
			break
		}
		d *= 2
	}
	limit := cfg.MaxRetryDelay
	if d < limit {
		limit = d
	}
	if unit < 0 {
		unit = 0
	}
	if unit >= 1 {
		unit = 0.999999
	}
	return time.Duration(unit * float64(limit))
}
