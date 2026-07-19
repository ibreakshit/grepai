//go:build unix

// Package daemonctl provides the grepaid singleton lock and the client-side
// lazy-start (EnsureDaemon) helper.
package daemonctl

import (
	"errors"
	"os"
	"syscall"
)

// ErrAlreadyRunning means another process holds the singleton lock.
var ErrAlreadyRunning = errors.New("grepaid: already running")

// Lock is a held advisory flock, released on Release or process exit. The flock
// — not the socket file — is the authoritative liveness signal.
type Lock struct{ f *os.File }

// Acquire takes an exclusive non-blocking flock on lockPath.
func Acquire(lockPath string) (*Lock, error) {
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600) // #nosec G304 - operator's own state file
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrAlreadyRunning
		}
		return nil, err
	}
	return &Lock{f: f}, nil
}

// Release drops the lock and closes the file.
func (l *Lock) Release() error {
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	return l.f.Close()
}
