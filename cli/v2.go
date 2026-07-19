package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/yoanbernabeu/grepai/config"
	"github.com/yoanbernabeu/grepai/embedder"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/legacyimport"
	"github.com/yoanbernabeu/grepai/internal/enginev2/runtime"
)

var (
	v2SearchJSON     bool
	v2SearchLimit    int
	v2SearchMigrated bool
	v2SearchCatalog  string
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
	v2SearchCmd.Flags().BoolVar(&v2SearchMigrated, "migrated", false, "search the migrated v1 index (.grepai/catalog_migrated.db)")
	v2SearchCmd.Flags().StringVar(&v2SearchCatalog, "catalog", "", "search a specific migrated catalog (matches `v2 migrate --catalog`)")
	v2Cmd.AddCommand(v2IndexCmd, v2SearchCmd)
	rootCmd.AddCommand(v2Cmd)
}

// openV2Runtime opens the NATIVE v2 index catalog for indexing (and for search
// when no migrated index applies). The legacy embedder satisfies the v2 port.
func openV2Runtime(ctx context.Context, root string, searchLimit int) (*runtime.Engine, error) {
	cfg, err := config.Load(root)
	if err != nil {
		return nil, fmt.Errorf("load config (try `grepai init` first): %w", err)
	}
	catPath := filepath.Join(root, ".grepai", "catalog_v2.db")
	return openV2RuntimeAt(ctx, root, cfg, catPath, runtime.Fingerprint(cfg), searchLimit)
}

// openV2RuntimeAt builds the config-driven embedder and assembles the runtime
// over a specific catalog + fingerprint (native or migrated).
func openV2RuntimeAt(ctx context.Context, root string, cfg *config.Config, catPath, fingerprint string, searchLimit int) (*runtime.Engine, error) {
	emb, err := embedder.NewFromConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create embedder: %w", err)
	}
	return runtime.Open(ctx, catPath, root, emb, fingerprint, cfg.Chunking.Size, cfg.Chunking.Overlap, searchLimit)
}

// openV2SearchRuntime opens the catalog a search should read (see
// resolveSearchTarget) with the matching fingerprint, and assembles the runtime.
func openV2SearchRuntime(ctx context.Context, root string, searchLimit int, forceMigrated bool, catalogOverride string, notice io.Writer) (*runtime.Engine, error) {
	cfg, err := config.Load(root)
	if err != nil {
		return nil, fmt.Errorf("load config (try `grepai init` first): %w", err)
	}
	catPath, fingerprint, migrated, err := resolveSearchTarget(cfg, filepath.Join(root, ".grepai"), forceMigrated, catalogOverride)
	if err != nil {
		return nil, err
	}
	if migrated {
		fmt.Fprintf(notice, "note: serving the migrated v1 index (%s)\n", catPath)
	}
	return openV2RuntimeAt(ctx, root, cfg, catPath, fingerprint, searchLimit)
}

// resolveSearchTarget decides which catalog a search reads and which fingerprint
// opens it — the one part of search-catalog selection that is pure and worth
// testing without an embedder. An explicit catalogOverride (matching
// `v2 migrate --catalog`) is treated as a migrated index. Otherwise it prefers
// the native v2 catalog, falling back to the migrated one when native is absent,
// or unconditionally when forceMigrated is set. A migrated target is opened with
// the migration fingerprint (legacyimport.DeriveFingerprint) so runtime.Open's
// fingerprint guard matches what the migration stored; a native target uses
// runtime.Fingerprint. A migrated target whose file is missing is an error
// (rather than letting sqlite.Open create an empty catalog and silently return
// nothing).
func resolveSearchTarget(cfg *config.Config, grepaiDir string, forceMigrated bool, catalogOverride string) (catPath, fingerprint string, migrated bool, err error) {
	if catalogOverride != "" {
		if !fileExists(catalogOverride) {
			return "", "", false, fmt.Errorf("no index at %s", catalogOverride)
		}
		return catalogOverride, legacyimport.DeriveFingerprint(cfg), true, nil
	}
	catPath, migrated = chooseSearchCatalog(grepaiDir, forceMigrated)
	if migrated {
		if !fileExists(catPath) {
			return "", "", false, fmt.Errorf("no migrated index at %s: run `grepai v2 migrate` first", catPath)
		}
		return catPath, legacyimport.DeriveFingerprint(cfg), true, nil
	}
	return catPath, runtime.Fingerprint(cfg), false, nil
}

// chooseSearchCatalog selects which default catalog a search reads. It prefers
// the native v2 index; it falls back to the migrated index only when no native
// index exists, or when forceMigrated is set.
func chooseSearchCatalog(grepaiDir string, forceMigrated bool) (path string, migrated bool) {
	migratedPath := filepath.Join(grepaiDir, "catalog_migrated.db")
	if forceMigrated {
		return migratedPath, true
	}
	nativePath := filepath.Join(grepaiDir, "catalog_v2.db")
	if !fileExists(nativePath) && fileExists(migratedPath) {
		return migratedPath, true
	}
	return nativePath, false
}

// fileExists reports whether path is an existing regular file.
func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func runV2Index(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	root, err := v2ProjectRoot(args)
	if err != nil {
		return err
	}
	eng, err := openV2Runtime(ctx, root, 20)
	if err != nil {
		return err
	}
	defer eng.Close()

	queued, failed, err := eng.Index(ctx)
	if err != nil {
		return fmt.Errorf("index: %w", err)
	}
	committed := queued - failed
	if failed > 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "indexed %d file(s), %d failed (dead-lettered)\n", committed, failed)
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "indexed %d file(s)\n", committed)
	}
	return nil
}

func runV2Search(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	root, err := v2ProjectRoot(nil)
	if err != nil {
		return err
	}
	eng, err := openV2SearchRuntime(ctx, root, v2SearchLimit, v2SearchMigrated, v2SearchCatalog, cmd.ErrOrStderr())
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

// v2ProjectRoot resolves a consistent repository root for both index and
// search, so running them from different subdirectories agrees on the worktree.
// An explicit dir argument wins; otherwise it walks up to the project root
// (the directory holding .grepai), falling back to the current directory for a
// first index before .grepai exists. runtime.Open resolves symlinks.
func v2ProjectRoot(args []string) (string, error) {
	if len(args) > 0 && args[0] != "" {
		return filepath.Abs(args[0])
	}
	if root, err := config.FindProjectRoot(); err == nil {
		return root, nil
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
