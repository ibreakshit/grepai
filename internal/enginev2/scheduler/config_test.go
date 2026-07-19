package scheduler

import "testing"

func TestDefaultConfigMatchesSpec(t *testing.T) {
	c := DefaultConfig()
	if c.MaxIndexInflight != 1 || c.ReservedQueryInflight != 1 || c.MaxJobAttempts != 5 {
		t.Fatalf("counts: %+v", c)
	}
	if c.BaseRetryDelay.String() != "1s" || c.MaxRetryDelay.String() != "5m0s" {
		t.Fatalf("delays: base=%s max=%s", c.BaseRetryDelay, c.MaxRetryDelay)
	}
	if c.CircuitOpenAfter != 5 || c.CircuitProbeInterval.String() != "1m0s" {
		t.Fatalf("circuit: %+v", c)
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("default must validate: %v", err)
	}
}

func TestValidateRejectsBadConfig(t *testing.T) {
	bad := DefaultConfig()
	bad.MaxIndexInflight = 0
	if bad.Validate() == nil {
		t.Fatal("zero inflight must fail")
	}
	bad = DefaultConfig()
	bad.BaseRetryDelay = bad.MaxRetryDelay + 1
	if bad.Validate() == nil {
		t.Fatal("base>max must fail")
	}
}
