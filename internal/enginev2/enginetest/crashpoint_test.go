// internal/enginev2/enginetest/crashpoint_test.go
package enginetest

import (
	"errors"
	"testing"
)

func TestCrashPointArmedFiresOnce(t *testing.T) {
	r := NewCrashRegistry()
	if err := r.Check("before-commit"); err != nil {
		t.Fatalf("unarmed point must not fire: %v", err)
	}
	r.ArmAt("before-commit")
	if err := r.Check("before-commit"); !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("armed point must fire ErrInjectedCrash, got %v", err)
	}
	if err := r.Check("before-commit"); err != nil {
		t.Fatalf("point must fire only once, got %v", err)
	}
}

func TestCrashPointOnlyNamedFires(t *testing.T) {
	r := NewCrashRegistry()
	r.ArmAt("after-embed")
	if err := r.Check("before-commit"); err != nil {
		t.Fatalf("non-armed name must not fire: %v", err)
	}
	if err := r.Check("after-embed"); !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("armed name must fire, got %v", err)
	}
}
