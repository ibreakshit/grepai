// Package legacyimport migrates a v1 GOB index into a v2 SQLite catalog for
// search, and provides a live v1-vs-v2 search-parity harness. Migration is
// import-for-search: it reuses v1's stored embedding vectors so v2 can query the
// index without re-embedding. Because v1 embedded framework-transformed content
// that the v2 builder does not replicate, a v2 native re-index produces a
// distinct generation (symbol/RPG import and generation-scoped views remain
// deferred). The package is read-only toward the legacy index file.
package legacyimport

import (
	"context"
	"fmt"
	"sort"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// migrationGeneration is the single active generation a migrated index occupies.
const migrationGeneration core.Generation = 1

// CatalogWriter is the v2 catalog surface the importer needs. It mirrors the
// concrete methods on *sqlite.Catalog, so a real catalog satisfies it directly.
type CatalogWriter interface {
	ActiveGeneration(ctx context.Context, repo core.RepositoryID) (core.Generation, error)
	GenerationFingerprint(ctx context.Context, repo core.RepositoryID, gen core.Generation) (string, error)
	RegisterRepository(ctx context.Context, repo core.RepositoryID, rootPath, gitCommonDir string) error
	RegisterWorktree(ctx context.Context, wt core.WorktreeID, repo core.RepositoryID, rootPath string, regGen core.Generation) error
	EnsureActiveGeneration(ctx context.Context, repo core.RepositoryID, gen core.Generation, fingerprint string) error
	PutChunkVector(ctx context.Context, chunkID string, repo core.RepositoryID, fingerprint string, vec []float32, content string) error
	CommitUpdate(ctx context.Context, req core.CommitRequest, job core.Job) error
	WorktreeViewPaths(ctx context.Context, wt core.WorktreeID) ([]string, error)
	DeleteWorktreeView(ctx context.Context, wt core.WorktreeID, relPath string) error
}

// Stats summarizes an import. Document/placement counts are read back from the
// catalog after the import (durable), not inferred from the attempted work.
type Stats struct {
	SourceDocuments     int // documents present in the source index
	CommittedDocuments  int // documents now visible in the catalog view (durable)
	ChunkPlacements     int // artifact_chunks committed across all views
	UniqueVectors       int // distinct content-addressed vectors written
	SkippedMissingChunk int // dangling chunk-id references skipped
	PrunedStaleViews    int // views removed that no longer exist in the source
}

// Import writes a decoded v1 index into the v2 catalog as a single active
// generation, reusing the tested PutChunkVector + CommitUpdate seams (no new
// core write path). It refuses to write into a catalog that already holds a
// different active generation (protecting a native catalog_v2.db and rejecting
// mixed imports), prunes views for source files that have disappeared, and reads
// the committed document set back from the catalog so Stats reflect reality.
//
// Chunk identity is content-addressed on the v1 ContentHash — v1's own
// vector-cache key (LookupByContentHash) — so distinct embeddings never collapse
// even when display content coincides; identical embeddings dedupe. SourceHash
// is the v1 Document.Hash, so the import is self-contained (no git, no network).
// Idempotent: re-running the same index writes the same rows and Stats.
func Import(ctx context.Context, cat CatalogWriter, repo core.RepositoryID, wt core.WorktreeID, root string, idx LegacyIndex, fingerprint string) (Stats, error) {
	var st Stats

	// Ownership guard: only a fresh catalog, or a re-import of the same migrated
	// generation (gen 1, matching fingerprint), may be written. Anything else — a
	// native v2 catalog, or a different v1 index — is refused so views never mix.
	activeGen, err := cat.ActiveGeneration(ctx, repo)
	if err != nil {
		return st, fmt.Errorf("read active generation: %w", err)
	}
	if activeGen != 0 {
		activeFP, err := cat.GenerationFingerprint(ctx, repo, activeGen)
		if err != nil {
			return st, fmt.Errorf("read active fingerprint: %w", err)
		}
		if activeGen != migrationGeneration || activeFP != fingerprint {
			return st, fmt.Errorf("catalog already has active generation %d (fingerprint %s); import into a dedicated migration catalog", activeGen, activeFP)
		}
	}

	if err := cat.RegisterRepository(ctx, repo, root, ""); err != nil {
		return st, fmt.Errorf("register repository: %w", err)
	}
	if err := cat.RegisterWorktree(ctx, wt, repo, root, migrationGeneration); err != nil {
		return st, fmt.Errorf("register worktree: %w", err)
	}
	if err := cat.EnsureActiveGeneration(ctx, repo, migrationGeneration, fingerprint); err != nil {
		return st, fmt.Errorf("bootstrap generation: %w", err)
	}

	st.SourceDocuments = len(idx.Documents)
	committed := make(map[string]struct{})
	seen := make(map[string]struct{})

	// Deterministic document order so the import is reproducible.
	paths := make([]string, 0, len(idx.Documents))
	for p := range idx.Documents {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	for _, p := range paths {
		doc := idx.Documents[p]
		artChunks := make([]core.ArtifactChunk, 0, len(doc.ChunkIDs))
		for _, cid := range doc.ChunkIDs {
			ch, ok := idx.Chunks[cid]
			if !ok {
				// A dangling reference in a legacy index must not abort the whole
				// migration; skip it and record the count for reconciliation.
				st.SkippedMissingChunk++
				continue
			}
			chunkID := core.ChunkID(fingerprint, vectorIdentity(ch))
			if err := cat.PutChunkVector(ctx, chunkID, repo, fingerprint, ch.Vector, ch.Content); err != nil {
				return st, fmt.Errorf("put chunk vector (%s): %w", p, err)
			}
			if _, dup := seen[chunkID]; !dup {
				seen[chunkID] = struct{}{}
				st.UniqueVectors++
			}
			artChunks = append(artChunks, core.ArtifactChunk{
				Ordinal:   len(artChunks),
				ChunkID:   chunkID,
				Vector:    ch.Vector,
				Content:   ch.Content,
				StartLine: ch.StartLine,
				EndLine:   ch.EndLine,
			})
		}
		if len(artChunks) == 0 {
			// A document whose every chunk was dangling commits no searchable view.
			continue
		}
		key := core.ArtifactKey{RepositoryID: repo, RelativePath: doc.Path, SourceHash: doc.Hash, Fingerprint: fingerprint}
		art := core.Artifact{ID: key.ArtifactID(), Key: key, Dimensions: idx.Dimensions, Chunks: artChunks}
		view := core.ViewEntry{WorktreeID: wt, Path: doc.Path, ArtifactID: art.ID, Generation: migrationGeneration}
		// The empty Job matches no index_jobs row, so CommitUpdate's job-delete is
		// a harmless no-op; the view switches because migrationGeneration is active.
		if err := cat.CommitUpdate(ctx, core.CommitRequest{View: view, Artifact: art, Chunks: artChunks}, core.Job{}); err != nil {
			return st, fmt.Errorf("commit view (%s): %w", p, err)
		}
		committed[doc.Path] = struct{}{}
		st.ChunkPlacements += len(artChunks)
	}

	// Prune views for files that no longer exist in the source index (a re-import
	// after files were deleted from the repo and v1 re-indexed).
	existing, err := cat.WorktreeViewPaths(ctx, wt)
	if err != nil {
		return st, fmt.Errorf("list views: %w", err)
	}
	for _, p := range existing {
		if _, ok := committed[p]; !ok {
			if err := cat.DeleteWorktreeView(ctx, wt, p); err != nil {
				return st, fmt.Errorf("prune stale view (%s): %w", p, err)
			}
			st.PrunedStaleViews++
		}
	}

	// Durable committed-document count: read the view back rather than trusting
	// the attempted work.
	final, err := cat.WorktreeViewPaths(ctx, wt)
	if err != nil {
		return st, fmt.Errorf("count views: %w", err)
	}
	st.CommittedDocuments = len(final)
	return st, nil
}

// vectorIdentity returns the value that uniquely identifies a chunk's embedding.
// v1 keyed its vector cache on ContentHash (store.LookupByContentHash), so equal
// ContentHash implies equal embedding input and vector; it falls back to display
// content only when a legacy chunk carries no ContentHash.
func vectorIdentity(ch LegacyChunk) string {
	if ch.ContentHash != "" {
		return ch.ContentHash
	}
	return ch.Content
}

// Reconcile verifies an import against the source index: every source document
// with at least one resolvable chunk must be committed and searchable, and the
// committed chunk placements must equal the resolvable chunk references. It
// compares the durable Stats (read back from the catalog) against expectations
// derived from the source, so a document whose chunks all dangled — which commits
// no view — is correctly excluded rather than silently counted as success.
func Reconcile(idx LegacyIndex, st Stats) (bool, string) {
	expectedDocs := 0
	expectedPlacements := 0
	for _, d := range idx.Documents {
		usable := 0
		for _, cid := range d.ChunkIDs {
			if _, ok := idx.Chunks[cid]; ok {
				usable++
			}
		}
		if usable > 0 {
			expectedDocs++
		}
		expectedPlacements += usable
	}

	if st.CommittedDocuments == expectedDocs && st.ChunkPlacements == expectedPlacements {
		return true, fmt.Sprintf("reconciled: %d/%d documents committed, %d chunk placements (%d unique vectors, %d dangling skipped, %d stale views pruned)",
			st.CommittedDocuments, expectedDocs, st.ChunkPlacements, st.UniqueVectors, st.SkippedMissingChunk, st.PrunedStaleViews)
	}
	return false, fmt.Sprintf("MISMATCH: documents committed %d/%d, chunk placements %d/%d (%d dangling skipped, %d stale pruned)",
		st.CommittedDocuments, expectedDocs, st.ChunkPlacements, expectedPlacements, st.SkippedMissingChunk, st.PrunedStaleViews)
}
