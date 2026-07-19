package cli

import (
	"context"
	"time"

	"github.com/yoanbernabeu/grepai/config"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/daemoncfg"
	"github.com/yoanbernabeu/grepai/internal/enginev2/daemonctl"
	"github.com/yoanbernabeu/grepai/internal/enginev2/rpc"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
)

// daemonDialTimeout bounds how long a client waits for a lazily-started daemon.
const daemonDialTimeout = 8 * time.Second

// daemonSocket resolves the Unix-socket path the CLI dials: a per-repo
// config override wins, else the host default (which itself honors GREPAID_SOCKET).
func daemonSocket(cfg *config.Config) (string, error) {
	if cfg != nil && cfg.Daemon.Socket != "" {
		return cfg.Daemon.Socket, nil
	}
	p, err := daemoncfg.ResolvePaths()
	if err != nil {
		return "", err
	}
	return p.Socket, nil
}

// ensureDaemonClient connects to the daemon, lazily starting grepaid if needed.
// A failure to start/reach the daemon is returned loudly — under engine:v2 there
// is no silent v1 fallback.
func ensureDaemonClient(ctx context.Context, cfg *config.Config) (*rpc.Client, error) {
	socket, err := daemonSocket(cfg)
	if err != nil {
		return nil, err
	}
	bin, err := daemonctl.LocateBinary()
	if err != nil {
		return nil, err
	}
	return daemonctl.EnsureDaemon(ctx, socket, bin, daemonDialTimeout)
}

// registerCwd resolves the current repo's project root and registers it with the
// daemon (idempotent), returning the canonical worktree id used for subsequent
// search/status/reconcile calls.
func registerCwd(ctx context.Context, client *rpc.Client) (core.WorktreeID, error) {
	root, err := config.FindProjectRoot()
	if err != nil {
		return "", err
	}
	resp, err := client.Register(ctx, service.RegisterRequest{Root: root})
	if err != nil {
		return "", err
	}
	return resp.WorktreeID, nil
}
