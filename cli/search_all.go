package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/yoanbernabeu/grepai/internal/enginev2/rpc"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
)

var (
	searchAllLimit   int
	searchAllJSON    bool
	searchAllCompact bool
	searchAllPath    string
)

var searchAllCmd = &cobra.Command{
	Use:   "search-all <query>",
	Short: "Search across ALL daemon-registered repos and worktrees",
	Long: `Search every repository and worktree registered with the grepaid daemon,
merged and ranked by relevance, each result tagged with its repo.

Scores are comparable across repos (one host-global embedder), and per-repo
isolation is preserved: results are explicitly multi-repo output — a plain
'grepai search' inside a repo still only ever sees that repo.

Runs from anywhere (no repo context needed); lazily starts the daemon.`,
	Args: cobra.MinimumNArgs(1),
	RunE: runSearchAll,
}

func init() {
	searchAllCmd.Flags().IntVarP(&searchAllLimit, "limit", "n", 15, "Maximum merged results across all repos")
	searchAllCmd.Flags().BoolVarP(&searchAllJSON, "json", "j", false, "Output results in JSON format (for AI agents)")
	searchAllCmd.Flags().BoolVarP(&searchAllCompact, "compact", "c", false, "Omit content in JSON output")
	searchAllCmd.Flags().StringVar(&searchAllPath, "path", "", "Repo-relative path prefix applied within every repo")
	rootCmd.AddCommand(searchAllCmd)
}

// repoHitJSON is the cross-repo JSON shape: SearchResultJSON plus a repo tag.
type repoHitJSON struct {
	Repo      string  `json:"repo"`
	FilePath  string  `json:"file_path"`
	StartLine int     `json:"start_line"`
	EndLine   int     `json:"end_line"`
	Score     float32 `json:"score"`
	Content   string  `json:"content,omitempty"`
}

// displayRepo shortens an absolute worktree id for humans (~/ for $HOME).
func displayRepo(wt string) string {
	if home, err := os.UserHomeDir(); err == nil && strings.HasPrefix(wt, home+"/") {
		return "~/" + strings.TrimPrefix(wt, home+"/")
	}
	return wt
}

func runSearchAll(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	client, err := ensureDaemonClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	resp, err := client.SearchAll(ctx, service.SearchAllRequest{
		Query:      strings.Join(args, " "),
		Limit:      searchAllLimit,
		PathPrefix: searchAllPath,
	})
	if err != nil {
		var rpcErr *rpc.Error
		if errors.As(err, &rpcErr) && rpcErr.Code == rpc.CodeMethodNotFound {
			return fmt.Errorf("search-all: the running grepaid predates this command — restart it (`grepai daemon stop` then any grepai command) after installing the new binary")
		}
		return fmt.Errorf("search-all: %w", err)
	}
	for _, wt := range resp.Skipped {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: %s skipped (its catalog errored); results are partial\n", displayRepo(string(wt)))
	}
	if len(resp.Stale) > 0 {
		names := make([]string, 0, len(resp.Stale))
		for _, wt := range resp.Stale {
			names = append(names, displayRepo(string(wt)))
		}
		fmt.Fprintf(cmd.ErrOrStderr(), "note: still indexing: %s\n", strings.Join(names, ", "))
	}

	if searchAllJSON {
		out := make([]repoHitJSON, 0, len(resp.Results))
		for _, r := range resp.Results {
			h := repoHitJSON{
				Repo: displayRepo(string(r.Worktree)), FilePath: r.Hit.Path,
				StartLine: r.Hit.StartLine, EndLine: r.Hit.EndLine, Score: r.Hit.Score,
			}
			if !searchAllCompact {
				h.Content = r.Hit.Content
			}
			out = append(out, h)
		}
		return encodeJSON(cmd, out)
	}
	for _, r := range resp.Results {
		fmt.Fprintf(cmd.OutOrStdout(), "%s › %s:%d-%d  (%.3f)\n",
			displayRepo(string(r.Worktree)), r.Hit.Path, r.Hit.StartLine, r.Hit.EndLine, r.Hit.Score)
	}
	return nil
}
