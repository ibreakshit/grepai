//go:build !unix

// Windows (and any non-unix) stubs: the grepaid daemon lifecycle relies on
// flock + detached Setsid spawn, which are unix-only this release. The stubs
// keep the grepai CLI cross-compiling; every entry point fails loudly so an
// engine:v2 repo on an unsupported platform gets a clear error, never a silent
// v1 fallback.
package daemonctl

import (
	"context"
	"errors"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/rpc"
)

// ErrAlreadyRunning mirrors the unix sentinel so callers compile everywhere.
var ErrAlreadyRunning = errors.New("grepaid: already running")

// errUnsupported is returned by every daemon-lifecycle call on this platform.
var errUnsupported = errors.New("grepaid: the v2 daemon is not supported on this platform (unix only this release)")

// Lock is a stub; Acquire always fails on this platform.
type Lock struct{}

// Acquire is unsupported on this platform.
func Acquire(string) (*Lock, error) { return nil, errUnsupported }

// Release is a no-op stub.
func (l *Lock) Release() error { return nil }

// ReadPID is unsupported on this platform.
func ReadPID(string) int { return 0 }

// StopDaemon is unsupported on this platform.
func StopDaemon(string, time.Duration) error { return errUnsupported }

// EnsureDaemon is unsupported on this platform.
func EnsureDaemon(context.Context, string, string, time.Duration) (*rpc.Client, error) {
	return nil, errUnsupported
}
