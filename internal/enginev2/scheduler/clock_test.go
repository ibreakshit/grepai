package scheduler

import (
	"testing"
	"time"
)

func TestSystemClockImplementsClock(t *testing.T) {
	var c Clock = SystemClock{}
	before := time.Now()
	if c.Now().Before(before) {
		t.Fatal("SystemClock.Now went backwards")
	}
	select {
	case <-c.After(1 * time.Millisecond):
	case <-time.After(time.Second):
		t.Fatal("After did not fire")
	}
}
