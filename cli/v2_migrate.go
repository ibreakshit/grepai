package cli

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/yoanbernabeu/grepai/config"
	"github.com/yoanbernabeu/grepai/embedder"
	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog/sqlite"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/legacyimport"
	"github.com/yoanbernabeu/grepai/store"
)

var (
	v2MigrateIndex   string
	v2ParityIndex    string
	v2ParityQueries  []string
	v2ParityFile     string
	v2ParityK        int
	v2ParityThresh   float64
	v2MigrateCatalog string
	v2ParityCatalog  string
)

var v2MigrateCmd = &cobra.Command{
	Use:   "migrate [dir]",
	Short: "Import a legacy v1 index into the v2 catalog (for search, no re-embedding)",
	Long: `Import a v1 GOB index into a dedicated v2 catalog (.grepai/catalog_migrated.db)
so the v2 engine can search it without re-embedding. Migration is import-for-search:
v1 embedded framework-transformed content that the v2 builder does not replicate, so a
v2 native re-index (grepai v2 index) is a separate generation. Prints a reconciliation
summary and exits non-zero if imported counts do not match the source.`,
	Args: cobra.MaximumNArgs(1),
	RunE: runV2Migrate,
}

var v2ParityCmd = &cobra.Command{
	Use:   "parity [dir]",
	Short: "Compare v1 vs v2 search ranking over a migrated index",
	Long: `Embed each query once and compare the legacy v1 ranking against the v2 ranking
over the migrated index, reporting per-query top-k unique-file overlap and the mean.
Requires 'grepai v2 migrate' to have run first, and a reachable embedder endpoint.
Exits non-zero if the mean overlap is below --threshold.`,
	Args: cobra.MaximumNArgs(1),
	RunE: runV2Parity,
}

func init() {
	v2MigrateCmd.Flags().StringVar(&v2MigrateIndex, "index", "", "path to the v1 index.gob (default <root>/.grepai/index.gob)")
	v2MigrateCmd.Flags().StringVar(&v2MigrateCatalog, "catalog", "", "path to the v2 migrated catalog (default <root>/.grepai/catalog_migrated.db)")

	v2ParityCmd.Flags().StringVar(&v2ParityIndex, "index", "", "path to the v1 index.gob (default <root>/.grepai/index.gob)")
	v2ParityCmd.Flags().StringVar(&v2ParityCatalog, "catalog", "", "path to the v2 migrated catalog (default <root>/.grepai/catalog_migrated.db)")
	v2ParityCmd.Flags().StringArrayVar(&v2ParityQueries, "query", nil, "a query to compare (repeatable)")
	v2ParityCmd.Flags().StringVar(&v2ParityFile, "queries-file", "", "file with one query per line")
	v2ParityCmd.Flags().IntVar(&v2ParityK, "k", 10, "top-k unique files to compare per query")
	v2ParityCmd.Flags().Float64Var(&v2ParityThresh, "threshold", 0.6, "minimum acceptable mean overlap")

	v2Cmd.AddCommand(v2MigrateCmd, v2ParityCmd)
}

// resolveMigrateRoot resolves the project root the same way for migrate and
// parity (symlinks resolved) so both agree on the worktree identity.
func resolveMigrateRoot(args []string) (string, error) {
	root, err := v2ProjectRoot(args)
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}
	return root, nil
}

func v1IndexPath(root, override string) string {
	if override != "" {
		return override
	}
	return filepath.Join(root, ".grepai", "index.gob")
}

func migratedCatalogPath(root, override string) string {
	if override != "" {
		return override
	}
	return filepath.Join(root, ".grepai", "catalog_migrated.db")
}

func runV2Migrate(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	root, err := resolveMigrateRoot(args)
	if err != nil {
		return err
	}
	cfg, err := config.Load(root)
	if err != nil {
		return fmt.Errorf("load config (try `grepai init` first): %w", err)
	}
	idx, err := legacyimport.Load(v1IndexPath(root, v2MigrateIndex))
	if err != nil {
		return err
	}
	cat, err := sqlite.Open(ctx, migratedCatalogPath(root, v2MigrateCatalog))
	if err != nil {
		return fmt.Errorf("open migrated catalog: %w", err)
	}
	defer func() { _ = cat.Close() }()

	repo := core.RepositoryID(root)
	wt := core.WorktreeID(root)
	fp := legacyimport.DeriveFingerprint(cfg)

	st, err := legacyimport.Import(ctx, cat, repo, wt, root, idx, fp)
	if err != nil {
		return fmt.Errorf("import: %w", err)
	}
	ok, detail := legacyimport.Reconcile(idx, st)
	fmt.Fprintln(cmd.OutOrStdout(), detail)
	if !ok {
		return fmt.Errorf("reconciliation failed")
	}
	fmt.Fprintf(cmd.OutOrStdout(), "migrated index ready for `grepai v2 parity` (catalog: %s)\n", migratedCatalogPath(root, v2MigrateCatalog))
	return nil
}

// gobV1Searcher adapts a v1 GOBStore to legacyimport.V1Searcher. It ranks over
// ALL chunks so the top-k unique files are exact — a single dominant file cannot
// occupy a fixed fetch window and hide lower-ranked distinct files (this is a
// validation tool, so completeness beats speed).
type gobV1Searcher struct{ s *store.GOBStore }

func (g gobV1Searcher) Search(ctx context.Context, query []float32, _ int) ([]string, error) {
	_, numChunks := g.s.Stats()
	if numChunks < 1 {
		numChunks = 1
	}
	res, err := g.s.Search(ctx, query, numChunks, store.SearchOptions{})
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(res))
	for _, r := range res {
		paths = append(paths, r.Chunk.FilePath)
	}
	return paths, nil
}

func runV2Parity(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if v2ParityK < 1 {
		return fmt.Errorf("--k must be >= 1")
	}
	if math.IsNaN(v2ParityThresh) || math.IsInf(v2ParityThresh, 0) || v2ParityThresh < 0 || v2ParityThresh > 1 {
		return fmt.Errorf("--threshold must be a finite value in [0,1]")
	}
	root, err := resolveMigrateRoot(args)
	if err != nil {
		return err
	}
	cfg, err := config.Load(root)
	if err != nil {
		return fmt.Errorf("load config (try `grepai init` first): %w", err)
	}
	queries, err := parityQueries()
	if err != nil {
		return err
	}
	if len(queries) == 0 {
		return fmt.Errorf("no queries: pass --query (repeatable) or --queries-file")
	}

	// The loaded GOBStore is an in-memory reader; it is NOT closed, because
	// GOBStore.Close() persists (rewrites) the file — closing it would mutate the
	// read-only legacy index.
	v1store := store.NewGOBStore(v1IndexPath(root, v2ParityIndex))
	if err := v1store.Load(ctx); err != nil {
		return fmt.Errorf("load v1 index: %w", err)
	}
	if _, numChunks := v1store.Stats(); numChunks == 0 {
		return fmt.Errorf("v1 index has no chunks (nothing to compare)")
	}

	cat, err := sqlite.Open(ctx, migratedCatalogPath(root, v2ParityCatalog))
	if err != nil {
		return fmt.Errorf("open migrated catalog (run `grepai v2 migrate` first): %w", err)
	}
	defer func() { _ = cat.Close() }()

	wt := core.WorktreeID(root)
	views, err := cat.WorktreeViewPaths(ctx, wt)
	if err != nil {
		return fmt.Errorf("read migrated views: %w", err)
	}
	if len(views) == 0 {
		return fmt.Errorf("no migrated index for %s: run `grepai v2 migrate` first", root)
	}

	emb, err := embedder.NewFromConfig(cfg)
	if err != nil {
		return fmt.Errorf("create embedder: %w", err)
	}
	defer func() { _ = emb.Close() }()

	rep, err := legacyimport.RunParity(ctx, emb, gobV1Searcher{s: v1store}, cat, wt, queries, v2ParityK)
	if err != nil {
		return err
	}

	out := cmd.OutOrStdout()
	for _, q := range rep.PerQuery {
		fmt.Fprintf(out, "%.3f  %q\n", q.Overlap, q.Query)
	}
	fmt.Fprintf(out, "mean top-%d overlap: %.3f (threshold %.3f)\n", v2ParityK, rep.Mean, v2ParityThresh)
	if rep.Mean < v2ParityThresh {
		return fmt.Errorf("parity below threshold: %.3f < %.3f", rep.Mean, v2ParityThresh)
	}
	return nil
}

// parityQueries collects queries from --query and --queries-file.
func parityQueries() ([]string, error) {
	queries := append([]string(nil), v2ParityQueries...)
	if v2ParityFile != "" {
		f, err := os.Open(v2ParityFile)
		if err != nil {
			return nil, fmt.Errorf("open queries file: %w", err)
		}
		defer func() { _ = f.Close() }()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line != "" {
				queries = append(queries, line)
			}
		}
		if err := sc.Err(); err != nil {
			return nil, fmt.Errorf("read queries file: %w", err)
		}
	}
	return queries, nil
}
