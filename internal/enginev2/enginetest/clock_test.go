// internal/enginev2/enginetest/clock_test.go
package enginetest

import (
	"testing"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/scheduler"
)

var _ scheduler.Clock = (*FakeClock)(nil)

func TestFakeClockAdvance(t *testing.T) {
	start := time.Unix(1000, 0)
	c := NewFakeClock(start)
	if !c.Now().Equal(start) {
		t.Fatalf("Now = %v, want %v", c.Now(), start)
	}
	c.Advance(5 * time.Second)
	if !c.Now().Equal(start.Add(5 * time.Second)) {
		t.Fatalf("Now after advance = %v", c.Now())
	}
}

func TestFakeClockAfterFiresOnAdvance(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	ch := c.After(10 * time.Second)
	select {
	case <-ch:
		t.Fatal("timer fired before advancing")
	default:
	}
	c.Advance(10 * time.Second)
	select {
	case <-ch:
	default:
		t.Fatal("timer did not fire after advancing past its deadline")
	}
}

func TestFakeClockAfterDoesNotFireEarly(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	ch := c.After(10 * time.Second)
	c.Advance(9 * time.Second)
	select {
	case <-ch:
		t.Fatal("timer fired before its deadline")
	default:
	}
}
