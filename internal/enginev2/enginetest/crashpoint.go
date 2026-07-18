// internal/enginev2/enginetest/crashpoint.go
package enginetest

import (
	"errors"
	"sync"
)

// ErrInjectedCrash is returned by an armed crash point to simulate a process
// crash at a named durable-state boundary.
var ErrInjectedCrash = errors.New("enginetest: injected crash")

// CrashRegistry arms named injection points. Production durable-state code
// calls Check(name) at commit boundaries; a test arms a point to make the
// next Check for that name return ErrInjectedCrash exactly once.
type CrashRegistry struct {
	mu    sync.Mutex
	armed map[string]bool
}

// NewCrashRegistry returns an empty registry (all points disarmed).
func NewCrashRegistry() *CrashRegistry {
	return &CrashRegistry{armed: map[string]bool{}}
}

// ArmAt arms the injection point identified by name.
func (r *CrashRegistry) ArmAt(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.armed[name] = true
}

// Check returns ErrInjectedCrash if name is armed (disarming it), else nil.
func (r *CrashRegistry) Check(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.armed[name] {
		delete(r.armed, name)
		return ErrInjectedCrash
	}
	return nil
}
