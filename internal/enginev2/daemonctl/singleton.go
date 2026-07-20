//go:build unix

// Package daemonctl provides the grepaid singleton lock and the client-side
// lazy-start (EnsureDaemon) helper.
package daemonctl

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ErrAlreadyRunning means another process holds the singleton lock.
var ErrAlreadyRunning = errors.New("grepaid: already running")

// Lock is a held advisory flock, released on Release or process exit. The flock
// — not the socket file — is the authoritative liveness signal. The lock file's
// contents are the holder's pid (so `grepai daemon stop` can signal it).
type Lock struct{ f *os.File }

// Acquire takes an exclusive non-blocking flock on lockPath and stamps the
// holder's pid into the file.
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
	if err := f.Truncate(0); err == nil {
		_, _ = f.WriteAt([]byte(strconv.Itoa(os.Getpid())), 0)
		_ = f.Sync()
	}
	return &Lock{f: f}, nil
}

// Release drops the lock and closes the file.
func (l *Lock) Release() error {
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	return l.f.Close()
}

// ReadPID returns the pid stamped in the lock file, or 0 if unreadable/empty.
func ReadPID(lockPath string) int {
	b, err := os.ReadFile(lockPath) // #nosec G304 - operator's own state file
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return 0
	}
	return pid
}

// StopDaemon SIGTERMs the daemon holding lockPath and waits (up to timeout) for
// it to exit, detected by the lock becoming free. It is a no-op if no daemon is
// running (the lock is already free).
func StopDaemon(lockPath string, timeout time.Duration) error {
	// Only a HELD lock means a daemon is running; any other Acquire failure
	// (permissions, IO) is its own error, not evidence of a live daemon.
	l, err := Acquire(lockPath)
	if err == nil {
		return l.Release()
	}
	if !errors.Is(err, ErrAlreadyRunning) {
		return err
	}
	pid := ReadPID(lockPath)
	if pid <= 0 {
		return errors.New("grepaid: running but pid unknown")
	}
	// Guard against pid reuse: only signal a process whose comm IS grepaid
	// (exact match; comm is the basename, kernel-truncated to 15 chars — which
	// still holds all of "grepaid"). If comm is unreadable (non-Linux /proc),
	// proceed — the held flock is strong evidence the pid is live and ours.
	if comm, cerr := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/comm"); cerr == nil { // #nosec G304 - fixed /proc path
		if strings.TrimSpace(string(comm)) != "grepaid" {
			return errors.New("grepaid: lock-file pid " + strconv.Itoa(pid) + " is not a grepaid process (pid reuse?); not signaling")
		}
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	for {
		l, aerr := Acquire(lockPath)
		if aerr == nil {
			return l.Release()
		}
		if !errors.Is(aerr, ErrAlreadyRunning) {
			// A permission/IO failure is not "still running" — surface it
			// instead of spinning until the timeout lies about the outcome.
			return aerr
		}
		if time.Now().After(deadline) {
			return errors.New("grepaid: did not exit before timeout")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
