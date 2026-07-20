package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/yoanbernabeu/grepai/config"
	"github.com/yoanbernabeu/grepai/daemon"
	"github.com/yoanbernabeu/grepai/git"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
	"github.com/yoanbernabeu/grepai/search"
)

// repoEngineV2 loads the current repo's config and reports whether it is
// configured for the v2 daemon engine. A missing project root is v1 (the
// default — not a grepai repo yet), but a root whose config EXISTS and fails to
// load is a hard error: a malformed engine:v2 config must not silently run v1.
func repoEngineV2() (*config.Config, bool, error) {
	root, err := config.FindProjectRoot()
	if err != nil {
		return nil, false, nil // no .grepai: plain v1 default
	}
	cfg, err := config.Load(root)
	if err != nil {
		return nil, false, fmt.Errorf("load %s config: %w", root, err)
	}
	return cfg, cfg.EngineV2(), nil
}

// warnIfV1WatcherRunning prints a gentle stderr note when a live v1 watcher is
// still running for this repo while it is on engine:v2 — the watcher is
// redundant (it keeps rewriting the inert v1 index) but harmless (separate
// files/processes). Best-effort: any detection error is silently ignored, and
// the v1 pid helpers clean up stale pidfiles as a side effect.
func warnIfV1WatcherRunning(cmd *cobra.Command) {
	root, err := config.FindProjectRoot()
	if err != nil {
		return
	}
	logDir, err := daemon.GetDefaultLogDir()
	if err != nil {
		return
	}
	// Only the per-repo worktree pidfile identifies THIS repo's watcher; the
	// legacy global pidfile is shared across non-git repos and would produce
	// false positives, so it is not consulted.
	pid := 0
	if info, derr := git.Detect(root); derr == nil && info.WorktreeID != "" {
		pid, _ = daemon.GetRunningWorktreePID(logDir, info.WorktreeID)
	}
	if pid > 0 {
		fmt.Fprintf(cmd.ErrOrStderr(),
			"note: a v1 grepai watcher (pid %d) is still running for this repo; under engine: v2 it is redundant — stop it with `kill %d` when convenient\n",
			pid, pid)
	}
}

// runSearchDaemon serves a top-level `grepai search` against the daemon (engine:v2).
// It fails loudly on any daemon/embedder error — there is no silent v1 fallback.
func runSearchDaemon(cmd *cobra.Command, args []string) error {
	if searchWorkspace != "" || len(searchProjects) > 0 {
		return fmt.Errorf("workspace/project search is not supported under engine: v2")
	}
	if searchTOON {
		return fmt.Errorf("--toon output is not supported under engine: v2 (use --json)")
	}
	warnIfV1WatcherRunning(cmd)
	ctx := context.Background()
	client, err := ensureDaemonClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()
	wt, err := registerCwd(ctx, client)
	if err != nil {
		return err
	}
	// Normalize --path exactly as v1 does (absolute paths become root-relative
	// prefixes); the daemon matches against repo-relative paths.
	pathPrefix := ""
	if searchPath != "" {
		root, rerr := config.FindProjectRoot()
		if rerr != nil {
			return rerr
		}
		pathPrefix, err = search.NormalizeProjectPathPrefix(searchPath, root)
		if err != nil {
			return fmt.Errorf("invalid --path value: %w", err)
		}
	}
	resp, err := client.Search(ctx, service.SearchRequest{
		WorktreeID: wt,
		Query:      strings.Join(args, " "),
		Limit:      searchLimit,
		PathPrefix: pathPrefix,
	})
	if err != nil {
		return fmt.Errorf("search: %w", err)
	}
	if !resp.Fresh {
		fmt.Fprintln(cmd.ErrOrStderr(), "note: index may be stale; run `grepai watch` to reconcile")
	}
	if len(resp.Results) == 0 && resp.Fresh {
		fmt.Fprintln(cmd.ErrOrStderr(), "note: no results on a fresh index — if this repo was just registered the index may still be empty; run `grepai watch`")
	}
	if searchJSON {
		if searchCompact {
			out := make([]SearchResultCompactJSON, 0, len(resp.Results))
			for _, h := range resp.Results {
				out = append(out, SearchResultCompactJSON{FilePath: h.Path, StartLine: h.StartLine, EndLine: h.EndLine, Score: h.Score})
			}
			return encodeJSON(cmd, out)
		}
		out := make([]SearchResultJSON, 0, len(resp.Results))
		for _, h := range resp.Results {
			out = append(out, SearchResultJSON{FilePath: h.Path, StartLine: h.StartLine, EndLine: h.EndLine, Score: h.Score, Content: h.Content})
		}
		return encodeJSON(cmd, out)
	}
	for _, h := range resp.Results {
		fmt.Fprintf(cmd.OutOrStdout(), "%s:%d-%d  (%.3f)\n", h.Path, h.StartLine, h.EndLine, h.Score)
	}
	return nil
}

func encodeJSON(cmd *cobra.Command, v any) error {
	enc := json.NewEncoder(cmd.OutOrStdout())
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// runStatusDaemon serves a top-level `grepai status` against the daemon.
func runStatusDaemon(cmd *cobra.Command) error {
	warnIfV1WatcherRunning(cmd)
	ctx := context.Background()
	client, err := ensureDaemonClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()
	wt, err := registerCwd(ctx, client)
	if err != nil {
		return err
	}
	st, err := client.Status(ctx, service.StatusRequest{WorktreeID: wt})
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "engine:      v2 (daemon)\n")
	fmt.Fprintf(cmd.OutOrStdout(), "generation:  %d\n", st.ActiveGeneration)
	fmt.Fprintf(cmd.OutOrStdout(), "fresh:       %t\n", st.Fresh)
	fmt.Fprintf(cmd.OutOrStdout(), "pending:     %d\n", st.Pending)
	fmt.Fprintf(cmd.OutOrStdout(), "dead-letter: %d\n", st.DeadLetters)
	return nil
}

// runWatchDaemon serves a top-level `grepai watch` against the daemon: it
// ensure-registers the repo, reconciles once, and tails freshness until the
// index is fresh. Continuous file-event watching is daemon-managed and a later
// slice; this is the reconcile-on-demand degradation.
func runWatchDaemon(cmd *cobra.Command) error {
	if watchBackground || watchStatus || watchStop {
		return fmt.Errorf("under engine: v2 the daemon manages indexing; use `grepai daemon start|status|stop` instead of `grepai watch --background|--status|--stop`")
	}
	fmt.Fprintln(cmd.ErrOrStderr(), "note: engine:v2 — `grepai watch` now reconciles via the grepaid daemon (per-repo watchers are retired)")
	warnIfV1WatcherRunning(cmd)
	ctx := context.Background()
	client, err := ensureDaemonClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()
	wt, err := registerCwd(ctx, client)
	if err != nil {
		return err
	}
	// Dead-letter baseline is captured BEFORE the explicit reconcile so a fast
	// permanent failure right after enqueue is still attributed to this run.
	// Known imprecision (documented in GREPAID_DAEMON.md): the count is
	// host-wide, and on a FIRST registration the register call above already
	// auto-reconciled — a failure in that window lands before this baseline. A
	// worktree-scoped dead-letter count is the follow-up that fixes both.
	dlStart := 0
	if st, serr := client.Status(ctx, service.StatusRequest{WorktreeID: wt}); serr == nil {
		dlStart = st.DeadLetters
	}
	resp, err := client.Reconcile(ctx, service.ReconcileRequest{WorktreeID: wt})
	if err != nil {
		return fmt.Errorf("reconcile: %w", err)
	}
	if resp.JobsQueued > 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "reconciled: %d files queued for indexing (Ctrl-C is safe — indexing continues in the daemon; check `grepai status`)\n", resp.JobsQueued)
	}
	// Wait until fresh with no fixed deadline: a large first index legitimately
	// takes long, and every queued job terminally resolves (commit, supersede, or
	// dead-letter after bounded retries), so pending reaches zero as long as the
	// daemon is serving. Progress is printed only when the count changes; Ctrl-C
	// is always safe (indexing continues daemon-side).
	lastPending := -1
	for {
		st, err := client.Status(ctx, service.StatusRequest{WorktreeID: wt})
		if err != nil {
			return err
		}
		if st.Fresh {
			fmt.Fprintf(cmd.OutOrStdout(), "index fresh (generation %d)\n", st.ActiveGeneration)
			if failed := st.DeadLetters - dlStart; failed > 0 {
				fmt.Fprintf(cmd.ErrOrStderr(), "warning: %d files failed indexing and were dead-lettered; see the daemon log\n", failed)
			}
			return nil
		}
		if st.Pending != lastPending {
			fmt.Fprintf(cmd.OutOrStdout(), "indexing... %d pending\n", st.Pending)
			lastPending = st.Pending
		}
		time.Sleep(500 * time.Millisecond)
	}
}
