// Command grepaid is the GrepAI v2 host daemon: a singleton process that serves
// every registered repository's catalog over a Unix-socket JSON-RPC API and runs
// the indexing scheduler continuously. It is started lazily by clients; see
// internal/enginev2/daemonctl.EnsureDaemon.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/yoanbernabeu/grepai/internal/enginev2/daemoncfg"
	"github.com/yoanbernabeu/grepai/internal/enginev2/daemonctl"
)

func main() {
	if err := daemonMain(); err != nil {
		fmt.Fprintf(os.Stderr, "grepaid: %v\n", err)
		os.Exit(1)
	}
}

func daemonMain() error {
	paths, err := daemoncfg.ResolvePaths()
	if err != nil {
		return err
	}
	cfg, existed, err := daemoncfg.Load(paths.Config)
	if err != nil {
		return err
	}
	// A socket in daemon.json overrides the XDG default, unless GREPAID_SOCKET
	// (which ResolvePaths already honored) is set. Applied BEFORE EnsureDirs so
	// the custom socket's parent directory is the one created.
	if cfg.Socket != "" && os.Getenv("GREPAID_SOCKET") == "" {
		paths.Socket = cfg.Socket
	}
	if err := paths.EnsureDirs(); err != nil {
		return err
	}
	if !existed {
		// Materialize defaults so the operator has a daemon.json to edit.
		if err := cfg.Save(paths.Config); err != nil {
			return err
		}
	}

	// The flock is the authoritative liveness signal. A lazy-start race loser
	// finds it held and exits cleanly; the winner owns the socket.
	lock, err := daemonctl.Acquire(paths.Lock)
	if err != nil {
		if errors.Is(err, daemonctl.ErrAlreadyRunning) {
			return nil
		}
		return err
	}
	defer func() { _ = lock.Release() }()

	logf, err := os.OpenFile(paths.Log, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) // #nosec G304 - operator's own log path
	if err != nil {
		return err
	}
	defer func() { _ = logf.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	return run(ctx, paths, cfg, logf)
}
