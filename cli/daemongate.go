package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/yoanbernabeu/grepai/config"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
)

// repoEngineV2 loads the current repo's config and reports whether it is
// configured for the v2 daemon engine. A missing config/root is treated as v1
// (the default), so v1 repos never touch the daemon path.
func repoEngineV2() (*config.Config, bool) {
	root, err := config.FindProjectRoot()
	if err != nil {
		return nil, false
	}
	cfg, err := config.Load(root)
	if err != nil {
		return nil, false
	}
	return cfg, cfg.EngineV2()
}

// runSearchDaemon serves a top-level `grepai search` against the daemon (engine:v2).
// It fails loudly on any daemon/embedder error — there is no silent v1 fallback.
func runSearchDaemon(cmd *cobra.Command, args []string, cfg *config.Config) error {
	if searchWorkspace != "" {
		return fmt.Errorf("workspace search is not supported under engine: v2")
	}
	ctx := context.Background()
	client, err := ensureDaemonClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	wt, err := registerCwd(ctx, client)
	if err != nil {
		return err
	}
	resp, err := client.Search(ctx, service.SearchRequest{WorktreeID: wt, Query: strings.Join(args, " ")})
	if err != nil {
		return fmt.Errorf("search: %w", err)
	}
	if !resp.Fresh {
		fmt.Fprintln(cmd.ErrOrStderr(), "note: index may be stale; run `grepai watch` to reconcile")
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
	if searchTOON {
		fmt.Fprintln(cmd.ErrOrStderr(), "note: --toon is not supported under engine: v2; showing plain output")
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
func runStatusDaemon(cmd *cobra.Command, cfg *config.Config) error {
	ctx := context.Background()
	client, err := ensureDaemonClient(ctx, cfg)
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
func runWatchDaemon(cmd *cobra.Command, cfg *config.Config) error {
	if watchBackground || watchStatus || watchStop {
		return fmt.Errorf("under engine: v2 the daemon manages indexing; use `grepai daemon start|status|stop` instead of `grepai watch --background|--status|--stop`")
	}
	fmt.Fprintln(cmd.ErrOrStderr(), "note: engine:v2 — `grepai watch` now reconciles via the grepaid daemon (per-repo watchers are retired)")
	ctx := context.Background()
	client, err := ensureDaemonClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	wt, err := registerCwd(ctx, client)
	if err != nil {
		return err
	}
	if _, err := client.Reconcile(ctx, service.ReconcileRequest{WorktreeID: wt}); err != nil {
		return fmt.Errorf("reconcile: %w", err)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		st, err := client.Status(ctx, service.StatusRequest{WorktreeID: wt})
		if err != nil {
			return err
		}
		if st.Fresh {
			fmt.Fprintf(cmd.OutOrStdout(), "index fresh (generation %d)\n", st.ActiveGeneration)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("index did not become fresh within 2m (pending %d)", st.Pending)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "indexing... %d pending\n", st.Pending)
		time.Sleep(500 * time.Millisecond)
	}
}
