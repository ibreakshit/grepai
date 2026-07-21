package cli

import (
	"context"
	"time"

	"github.com/yoanbernabeu/grepai/config"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/daemonctl"
	"github.com/yoanbernabeu/grepai/internal/enginev2/rpc"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
)

// daemonDialTimeout bounds how long a client waits for a lazily-started daemon.
const daemonDialTimeout = 8 * time.Second

// ensureDaemonClient connects to the daemon, lazily starting grepaid if
// needed. Thin wrapper over daemonctl.Connect (shared with the MCP server) —
// socket precedence and loud-failure semantics are documented there.
func ensureDaemonClient(ctx context.Context) (*rpc.Client, error) {
	return daemonctl.Connect(ctx, daemonDialTimeout)
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
