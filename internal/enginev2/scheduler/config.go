package scheduler

import (
	"errors"
	"time"
)

// Config is the host-wide scheduler configuration (spec §5.4). Values are
// configuration, not hard-coded assumptions.
type Config struct {
	MaxIndexInflight      int
	ReservedQueryInflight int
	MaxJobAttempts        int
	BaseRetryDelay        time.Duration
	MaxRetryDelay         time.Duration
	CircuitOpenAfter      int
	CircuitProbeInterval  time.Duration
}

// DefaultConfig returns the safe initial defaults for the local deployment.
func DefaultConfig() Config {
	return Config{
		MaxIndexInflight:      1,
		ReservedQueryInflight: 1,
		MaxJobAttempts:        5,
		BaseRetryDelay:        1 * time.Second,
		MaxRetryDelay:         5 * time.Minute,
		CircuitOpenAfter:      5,
		CircuitProbeInterval:  60 * time.Second,
	}
}

// Validate rejects nonsensical configuration.
func (c Config) Validate() error {
	if c.MaxIndexInflight < 1 || c.ReservedQueryInflight < 1 || c.MaxJobAttempts < 1 || c.CircuitOpenAfter < 1 {
		return errors.New("scheduler: counts must be >= 1")
	}
	if c.BaseRetryDelay <= 0 || c.MaxRetryDelay <= 0 || c.CircuitProbeInterval <= 0 {
		return errors.New("scheduler: durations must be > 0")
	}
	if c.BaseRetryDelay > c.MaxRetryDelay {
		return errors.New("scheduler: base_retry_delay must be <= max_retry_delay")
	}
	return nil
}
