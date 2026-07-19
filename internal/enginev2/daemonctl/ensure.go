//go:build unix

package daemonctl

import (
	"context"
	"errors"
	"os/exec"
	"syscall"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/rpc"
)

// pollInterval is how often EnsureDaemon re-dials while waiting for a spawned
// daemon to come up.
const pollInterval = 50 * time.Millisecond

// EnsureDaemon returns a connected client, lazily spawning grepaid (detached) if
// the socket is down. binPath is the grepaid executable. A spawn race is safe:
// the daemon's flock lets only one instance listen; a loser exits cleanly and
// both callers connect to the winner's socket.
func EnsureDaemon(ctx context.Context, socket, binPath string, timeout time.Duration) (*rpc.Client, error) {
	if c, err := rpc.Dial(socket); err == nil {
		return c, nil
	} else if !errors.Is(err, rpc.ErrDaemonDown) {
		return nil, err
	}
	if err := spawnDetached(binPath); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(timeout)
	for {
		if c, err := rpc.Dial(socket); err == nil {
			return c, nil
		}
		if time.Now().After(deadline) {
			return nil, errors.New("grepaid: did not become reachable before timeout")
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// spawnDetached starts grepaid in its own session so it outlives this process.
func spawnDetached(binPath string) error {
	cmd := exec.Command(binPath) // #nosec G204 - fixed daemon binary path resolved by the caller
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = nil, nil, nil
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Process.Release()
}
