package cli

import (
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/yoanbernabeu/grepai/internal/enginev2/daemoncfg"
	"github.com/yoanbernabeu/grepai/internal/enginev2/daemonctl"
	"github.com/yoanbernabeu/grepai/internal/enginev2/registry"
	"github.com/yoanbernabeu/grepai/internal/enginev2/rpc"
)

var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "Manage the grepaid host daemon (v2 engine)",
	Long: `Control the grepaid host daemon that serves the v2 engine.

The daemon is normally started lazily by 'grepai v2 ...' commands; these
subcommands are for explicit control and inspection.`,
}

var daemonStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the grepaid daemon (if not already running)",
	RunE:  runDaemonStart,
}

var daemonStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop the running grepaid daemon",
	RunE:  runDaemonStop,
}

var daemonStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show grepaid daemon status",
	RunE:  runDaemonStatus,
}

func init() {
	daemonCmd.AddCommand(daemonStartCmd, daemonStopCmd, daemonStatusCmd)
	rootCmd.AddCommand(daemonCmd)
}

func runDaemonStart(cmd *cobra.Command, _ []string) error {
	client, err := ensureDaemonClient(cmd.Context())
	if err != nil {
		return err
	}
	_ = client.Close()
	paths, err := daemoncfg.ResolvePaths()
	if err != nil {
		return err
	}
	socket, err := daemonctl.Socket()
	if err != nil {
		return err
	}
	fmt.Printf("grepaid running (pid %d) on %s\n", daemonctl.ReadPID(paths.Lock), socket)
	return nil
}

func runDaemonStop(_ *cobra.Command, _ []string) error {
	paths, err := daemoncfg.ResolvePaths()
	if err != nil {
		return err
	}
	if err := daemonctl.StopDaemon(paths.Lock, 5*time.Second); err != nil {
		return err
	}
	fmt.Println("grepaid stopped")
	return nil
}

func runDaemonStatus(_ *cobra.Command, _ []string) error {
	paths, err := daemoncfg.ResolvePaths()
	if err != nil {
		return err
	}
	socket, err := daemonctl.Socket()
	if err != nil {
		return err
	}
	client, err := rpc.Dial(socket)
	if err != nil {
		if errors.Is(err, rpc.ErrDaemonDown) {
			fmt.Println("grepaid: not running")
			return nil
		}
		return err
	}
	defer client.Close()

	repos := 0
	if reg, lerr := registry.Load(paths.Registry); lerr == nil {
		repos = len(reg.Entries)
	}
	fmt.Printf("grepaid: running (pid %d)\n", daemonctl.ReadPID(paths.Lock))
	fmt.Printf("  socket:   %s\n", socket)
	fmt.Printf("  registry: %s (%d repos)\n", paths.Registry, repos)
	return nil
}
