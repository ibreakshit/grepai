// Package runtime assembles the v2 engine components (catalog, reconciler,
// worker, service) into a runnable index+search runtime over a single repository
// worktree. It is the first production wiring of internal/enginev2; the CLI
// drives it with the config-derived embedder. The host-wide scheduler and the
// daemon are deferred — a one-shot index drains via worker.Run.
package runtime

import (
	"context"
	"crypto/sha1" //nolint:gosec // G401/G505 - git blob object id format, not a security use
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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

func (diskLoader) Load(_ context.Context, _ core.RepositoryID, root, rel, desiredHash string) ([]byte, error) {
	b, err := os.ReadFile(filepath.Join(root, rel)) // #nosec G304 - operator's own worktree file
	if err != nil {
		return nil, err
	}
	// The worker requires the exact desired version's bytes. The reconciler's
	// DesiredHash is a git blob OID (tracked/dirty files) or a sha256 (non-git);
	// verify the on-disk bytes hash to it, so a file changed between
	// reconciliation and load is rejected rather than committed under the wrong
	// identity (the rejection is transient — a later reconcile re-derives it).
	if desiredHash != "" && gitBlobOID(b) != desiredHash && sha256Hex(b) != desiredHash {
		return nil, fmt.Errorf("content of %q changed since reconciliation", rel)
	}
	return b, nil
}

// gitBlobOID computes the git blob object id of content ("blob <len>\0<content>"
// hashed with SHA-1), matching git ls-files -s / hash-object.
func gitBlobOID(content []byte) string {
	h := sha1.New() //nolint:gosec // G401 - git object id format, not security
	fmt.Fprintf(h, "blob %d\x00", len(content))
	_, _ = h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}

func sha256Hex(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
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
	// Resolve symlinks so the same physical repository always maps to the same
	// worktree id (whether reached directly or via a symlink).
	if resolved, rerr := filepath.EvalSymlinks(absRoot); rerr == nil {
		absRoot = resolved
	}
	dataDir := filepath.Dir(catalogPath)
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return nil, err
	}
	// Self-ignore the data directory so reconciliation never indexes the v2
	// catalog's own files (a nested .gitignore of "*" is honored by git's
	// --exclude-standard untracked scan even if the repo root does not list it).
	if err := ensureSelfIgnore(dataDir); err != nil {
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
	// Guard against a fingerprint mismatch: Register bootstraps the active
	// generation with `fingerprint` only on first use, so if the existing index
	// was built with a different embedder/chunker (config changed), the runtime
	// embedder and the stored vectors are incompatible — search would silently
	// return nothing (or rank across vector spaces). Fail explicitly instead.
	repo := core.RepositoryID(absRoot)
	active, err := cat.ActiveGeneration(ctx, repo)
	if err != nil {
		_ = cat.Close()
		return nil, err
	}
	activeFP, err := cat.GenerationFingerprint(ctx, repo, active)
	if err != nil {
		_ = cat.Close()
		return nil, err
	}
	if activeFP != fingerprint {
		_ = cat.Close()
		return nil, fmt.Errorf("v2 index was built with a different embedder/chunker configuration; remove %q and re-run `grepai v2 index`", catalogPath)
	}
	return e, nil
}

// Index reconciles the worktree and drains the resulting jobs (embedding
// cache-miss chunks and atomically committing). It returns the number of jobs
// queued and how many dead-lettered (e.g. a persistently unavailable endpoint).
func (e *Engine) Index(ctx context.Context) (queued, deadLettered int, err error) {
	// Clear any leftover jobs (e.g. from an interrupted prior run) so a stale job
	// under an out-of-date desired identity is never processed; reconcile then
	// re-derives the exact desired set from current truth.
	if err := e.cat.DeleteJobsForWorktree(ctx, e.wt); err != nil {
		return 0, 0, err
	}
	dlBefore, err := e.cat.DeadLetterCount(ctx)
	if err != nil {
		return 0, 0, err
	}
	plan, err := e.rec.Reconcile(ctx, e.wt)
	if err != nil {
		return 0, 0, err
	}
	for _, j := range plan.Jobs {
		if err := e.cat.UpsertJob(ctx, j); err != nil {
			return 0, 0, err
		}
	}
	if err := e.wk.Run(ctx); err != nil {
		return 0, 0, err
	}
	dlAfter, err := e.cat.DeadLetterCount(ctx)
	if err != nil {
		return 0, 0, err
	}
	// Report this invocation's counts: committed = queued - newly dead-lettered.
	return len(plan.Jobs), dlAfter - dlBefore, nil
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

// ensureSelfIgnore makes sure the v2 data directory ignores its own catalog
// files, so reconciliation never indexes them. If no .gitignore exists it writes
// one ignoring everything; if one exists it is preserved and the catalog
// patterns are appended only when they are not already covered.
func ensureSelfIgnore(dir string) error {
	path := filepath.Join(dir, ".gitignore")
	patterns := []string{"catalog_v2.db", "catalog_v2.db-wal", "catalog_v2.db-shm"}
	existing, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return os.WriteFile(path, []byte("*\n"), 0o600)
	}
	if err != nil {
		return err
	}
	lines := map[string]bool{}
	for _, ln := range strings.Split(string(existing), "\n") {
		lines[strings.TrimSpace(ln)] = true
	}
	if lines["*"] {
		return nil // everything already ignored
	}
	var missing []string
	for _, p := range patterns {
		if !lines[p] {
			missing = append(missing, p)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	out := string(existing)
	if len(out) > 0 && !strings.HasSuffix(out, "\n") {
		out += "\n"
	}
	out += strings.Join(missing, "\n") + "\n"
	return os.WriteFile(path, []byte(out), 0o600)
}
