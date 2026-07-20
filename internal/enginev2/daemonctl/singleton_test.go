package daemonctl

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestSingletonSecondAcquireFails(t *testing.T) {
	lp := filepath.Join(t.TempDir(), "d.lock")
	l1, err := Acquire(lp)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := Acquire(lp); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second acquire = %v; want ErrAlreadyRunning", err)
	}
	if err := l1.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	l2, err := Acquire(lp)
	if err != nil {
		t.Fatalf("re-acquire after release: %v", err)
	}
	if err := l2.Release(); err != nil {
		t.Fatalf("release2: %v", err)
	}
}
