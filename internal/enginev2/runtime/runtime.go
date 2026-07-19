// Package runtime assembles the v2 engine components (catalog, reconciler,
// worker, service) into a runnable index+search runtime over a single repository
// worktree. It is the first production wiring of internal/enginev2; the CLI
// drives it with the config-derived embedder. The host-wide scheduler and the
// daemon are deferred — a one-shot index drains via worker.Run.
package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strconv"

	"github.com/yoanbernabeu/grepai/config"
	"github.com/yoanbernabeu/grepai/indexer"
	"github.com/yoanbernabeu/grepai/internal/enginev2/artifacts"
	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog/sqlite"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/embedder"
	"github.com/yoanbernabeu/grepai/internal/enginev2/reconcile"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
	"github.com/yoanbernabeu/grepai/internal/enginev2/worker"
)

// Fingerprint derives the v2 indexing fingerprint from config. index and search
// must use the same fingerprint so chunk ids match and query vectors compare.
func Fingerprint(cfg *config.Config) string {
	size, overlap := chunkParams(cfg)
	fp := core.IndexingFingerprint{
		EmbedderProvider:          cfg.Embedder.Provider,
		EmbedderModel:             cfg.Embedder.Model,
		Dimensions:                cfg.Embedder.GetDimensions(),
		ChunkerImplementation:     "indexer.Chunker",
		ChunkerSettings:           map[string]string{"size": strconv.Itoa(size), "overlap": strconv.Itoa(overlap)},
		FrameworkTransformVersion: "v2",
		EmbeddingInputFormat:      "text",
	}
	return fp.Hash()
}

// chunkParams returns the effective chunk size/overlap (config or defaults).
func chunkParams(cfg *config.Config) (size, overlap int) {
	size = cfg.Chunking.Size
	if size <= 0 {
		size = indexer.DefaultChunkSize
	}
	overlap = cfg.Chunking.Overlap
	if overlap < 0 {
		overlap = indexer.DefaultChunkOverlap
	}
	return size, overlap
}

// diskLoader loads a file's current bytes from the worktree root. For a clean
// tracked file this equals the committed content (so its git blob OID matches
// the reconciler's DesiredHash); for dirty/untracked files it is the current
// content the reconciler hashed.
type diskLoader struct{}

func (diskLoader) Load(_ context.Context, _ core.RepositoryID, root, rel, _ string) ([]byte, error) {
	return os.ReadFile(filepath.Join(root, rel)) // #nosec G304 - operator's own worktree file
}

// Engine is a runnable v2 index+search runtime for one worktree.
type Engine struct {
	cat *sqlite.Catalog
	wk  *worker.Worker
	rec *reconcile.Engine
	svc *service.Server
	wt  core.WorktreeID
}

// Open assembles the runtime over the repository at root, using a v2 catalog at
// catalogPath and the given embedder + fingerprint. It registers the worktree
// and bootstraps its active generation. The embedder must match the fingerprint.
func Open(ctx context.Context, catalogPath, root string, emb embedder.Embedder, fingerprint string, chunkSize, chunkOverlap, searchLimit int) (*Engine, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(catalogPath), 0o750); err != nil {
		return nil, err
	}
	cat, err := sqlite.Open(ctx, catalogPath)
	if err != nil {
		return nil, err
	}
	builder := artifacts.New(indexer.NewChunker(chunkSize, chunkOverlap), emb, cat)
	rec := reconcile.New(cat)
	svc := service.New(cat, rec, emb, fingerprint, searchLimit)
	e := &Engine{
		cat: cat,
		wk:  worker.New(cat, builder, diskLoader{}, worker.NoCrash, 5),
		rec: rec,
		svc: svc,
		wt:  core.WorktreeID(absRoot),
	}
	if _, err := svc.Register(ctx, service.RegisterRequest{Root: absRoot}); err != nil {
		_ = cat.Close()
		return nil, err
	}
	return e, nil
}

// Index reconciles the worktree and drains the resulting jobs (embedding
// cache-miss chunks and atomically committing). It returns the number of jobs
// queued and how many dead-lettered (e.g. a persistently unavailable endpoint).
func (e *Engine) Index(ctx context.Context) (queued, deadLettered int, err error) {
	plan, err := e.rec.Reconcile(ctx, e.wt)
	if err != nil {
		return 0, 0, err
	}
	for _, j := range plan.Jobs {
		if err := e.cat.UpsertJob(ctx, j); err != nil {
			return 0, 0, err
		}
	}
	if _, err := e.wk.Recover(ctx); err != nil {
		return 0, 0, err
	}
	if err := e.wk.Run(ctx); err != nil {
		return 0, 0, err
	}
	dl, err := e.cat.DeadLetterCount(ctx)
	if err != nil {
		return 0, 0, err
	}
	return len(plan.Jobs), dl, nil
}

// Search returns ranked results (with snippets) for the worktree, plus the
// active generation and whether the index is fresh (no pending jobs).
func (e *Engine) Search(ctx context.Context, query string) ([]core.SearchHit, core.Generation, bool, error) {
	resp, err := e.svc.Search(ctx, service.SearchRequest{WorktreeID: e.wt, Query: query})
	if err != nil {
		return nil, 0, false, err
	}
	return resp.Results, resp.ActiveGeneration, resp.Fresh, nil
}

// Close releases the catalog.
func (e *Engine) Close() error { return e.cat.Close() }
