package cli

import (
	"context"
	"fmt"
	"os"
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

// daemonSocket resolves the Unix-socket path the CLI dials, with the SAME
// host-level precedence the daemon itself applies so both ends always meet:
// GREPAID_SOCKET env > host daemon.json > XDG default. The socket is
// deliberately host-scoped (no per-repo override): there is ONE daemon per
// host, held by one singleton lock, so a per-repo socket could never have a
// daemon of its own to reach. A daemon.json read error is returned loudly — a
// malformed host config must not silently fall back to a different socket.
func daemonSocket() (string, error) {
	p, err := daemoncfg.ResolvePaths()
	if err != nil {
		return "", err
	}
	if os.Getenv("GREPAID_SOCKET") == "" {
		hostCfg, existed, lerr := daemoncfg.Load(p.Config)
		if lerr != nil {
			return "", fmt.Errorf("load host daemon config %s: %w", p.Config, lerr)
		}
		if existed && hostCfg.Socket != "" {
			return hostCfg.Socket, nil
		}
	}
	return p.Socket, nil
}

// ensureDaemonClient connects to the daemon, lazily starting grepaid if needed.
// A failure to start/reach the daemon is returned loudly — under engine:v2 there
// is no silent v1 fallback. Dial-first: a healthy running daemon is usable even
// if the grepaid binary has since been moved; the binary is located only when a
// spawn is actually needed.
func ensureDaemonClient(ctx context.Context) (*rpc.Client, error) {
	socket, err := daemonSocket()
	if err != nil {
		return nil, err
	}
	if c, derr := rpc.Dial(socket); derr == nil {
		return c, nil
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
