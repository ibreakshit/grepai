package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/yoanbernabeu/grepai/config"
	"github.com/yoanbernabeu/grepai/embedder"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/runtime"
)

var (
	v2SearchJSON  bool
	v2SearchLimit int
)

var v2Cmd = &cobra.Command{
	Use:   "v2",
	Short: "GrepAI v2 engine (experimental)",
	Long: `Experimental v2 indexing/search engine.

Isolated from the v1 engine: it uses its own catalog at .grepai/catalog_v2.db and
does not affect existing commands. Index a repository with 'grepai v2 index', then
query it with 'grepai v2 search'.`,
}

var v2IndexCmd = &cobra.Command{
	Use:   "index [dir]",
	Short: "Index a repository with the v2 engine",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runV2Index,
}

var v2SearchCmd = &cobra.Command{
	Use:   "search <query>",
	Short: "Search the v2 index (returns code snippets)",
	Args:  cobra.MinimumNArgs(1),
	RunE:  runV2Search,
}

func init() {
	v2SearchCmd.Flags().BoolVar(&v2SearchJSON, "json", false, "output results as JSON")
	v2SearchCmd.Flags().IntVar(&v2SearchLimit, "limit", 20, "maximum results")
	v2Cmd.AddCommand(v2IndexCmd, v2SearchCmd)
	rootCmd.AddCommand(v2Cmd)
}

// openV2Runtime loads config, builds the config-driven embedder, and assembles
// the v2 runtime over root. The legacy embedder satisfies the v2 embedder port.
func openV2Runtime(ctx context.Context, root string, searchLimit int) (*runtime.Engine, error) {
	cfg, err := config.Load(root)
	if err != nil {
		return nil, fmt.Errorf("load config (try `grepai init` first): %w", err)
	}
	emb, err := embedder.NewFromConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create embedder: %w", err)
	}
	catPath := filepath.Join(root, ".grepai", "catalog_v2.db")
	return runtime.Open(ctx, catPath, root, emb, runtime.Fingerprint(cfg), cfg.Chunking.Size, cfg.Chunking.Overlap, searchLimit)
}

func runV2Index(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	root, err := resolveV2Root(args)
	if err != nil {
		return err
	}
	eng, err := openV2Runtime(ctx, root, 20)
	if err != nil {
		return err
	}
	defer eng.Close()

	queued, dead, err := eng.Index(ctx)
	if err != nil {
		return fmt.Errorf("index: %w", err)
	}
	if dead > 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "indexed %d file(s) (%d dead-lettered)\n", queued, dead)
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "indexed %d file(s)\n", queued)
	}
	return nil
}

func runV2Search(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	eng, err := openV2Runtime(ctx, root, v2SearchLimit)
	if err != nil {
		return err
	}
	defer eng.Close()

	hits, gen, fresh, err := eng.Search(ctx, strings.Join(args, " "))
	if err != nil {
		return fmt.Errorf("search: %w", err)
	}
	if !fresh {
		fmt.Fprintln(cmd.ErrOrStderr(), "note: index may be stale; run `grepai v2 index`")
	}
	if v2SearchJSON {
		return writeV2JSON(cmd.OutOrStdout(), hits, gen, fresh)
	}
	writeV2Text(cmd.OutOrStdout(), hits)
	return nil
}

func resolveV2Root(args []string) (string, error) {
	if len(args) > 0 && args[0] != "" {
		return filepath.Abs(args[0])
	}
	return os.Getwd()
}

func writeV2Text(w interface{ Write([]byte) (int, error) }, hits []core.SearchHit) {
	if len(hits) == 0 {
		fmt.Fprintln(w, "no results")
		return
	}
	for _, h := range hits {
		fmt.Fprintf(w, "%s:%d-%d  (%.3f)\n", h.Path, h.StartLine, h.EndLine, h.Score)
		for _, line := range snippetLines(h.Content, 4) {
			fmt.Fprintf(w, "    %s\n", line)
		}
	}
}

// snippetLines returns up to max lines of content for a compact preview.
func snippetLines(content string, max int) []string {
	lines := strings.Split(strings.TrimRight(content, "\n"), "\n")
	if len(lines) > max {
		lines = append(lines[:max], "    …")
	}
	return lines
}

type v2JSONHit struct {
	Path      string  `json:"path"`
	Score     float32 `json:"score"`
	StartLine int     `json:"startLine"`
	EndLine   int     `json:"endLine"`
	Content   string  `json:"content"`
}

func writeV2JSON(w interface{ Write([]byte) (int, error) }, hits []core.SearchHit, gen core.Generation, fresh bool) error {
	out := struct {
		ActiveGeneration core.Generation `json:"activeGeneration"`
		Fresh            bool            `json:"fresh"`
		Results          []v2JSONHit     `json:"results"`
	}{ActiveGeneration: gen, Fresh: fresh, Results: make([]v2JSONHit, 0, len(hits))}
	for _, h := range hits {
		out.Results = append(out.Results, v2JSONHit{Path: h.Path, Score: h.Score, StartLine: h.StartLine, EndLine: h.EndLine, Content: h.Content})
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}
