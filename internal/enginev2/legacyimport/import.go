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
	RegisterRepository(ctx context.Context, repo core.RepositoryID, rootPath, gitCommonDir string) error
	RegisterWorktree(ctx context.Context, wt core.WorktreeID, repo core.RepositoryID, rootPath string, regGen core.Generation) error
	EnsureActiveGeneration(ctx context.Context, repo core.RepositoryID, gen core.Generation, fingerprint string) error
	PutChunkVector(ctx context.Context, chunkID string, repo core.RepositoryID, fingerprint string, vec []float32, content string) error
	CommitUpdate(ctx context.Context, req core.CommitRequest, job core.Job) error
}

// Stats summarizes an import for reconciliation against the source.
type Stats struct {
	Documents           int // documents seen in the source index
	Chunks              int // chunk placements committed (artifact_chunks rows)
	UniqueVectors       int // distinct content-addressed vectors written
	SkippedMissingChunk int // dangling chunk-id references skipped
}

// Import writes a decoded v1 index into the v2 catalog as a single active
// generation, reusing the tested PutChunkVector + CommitUpdate seams (no new
// core write path). Chunks are content-addressed (core.ChunkID), so identical
// content across files collapses to one stored vector while each document keeps
// its own ordered composition. SourceHash is the v1 Document.Hash, so the import
// is self-contained (no git checkout, no network). Idempotent: re-running writes
// the same rows and yields the same Stats.
func Import(ctx context.Context, cat CatalogWriter, repo core.RepositoryID, wt core.WorktreeID, root string, idx LegacyIndex, fingerprint string) (Stats, error) {
	var st Stats
	if err := cat.RegisterRepository(ctx, repo, root, ""); err != nil {
		return st, fmt.Errorf("register repository: %w", err)
	}
	if err := cat.RegisterWorktree(ctx, wt, repo, root, migrationGeneration); err != nil {
		return st, fmt.Errorf("register worktree: %w", err)
	}
	if err := cat.EnsureActiveGeneration(ctx, repo, migrationGeneration, fingerprint); err != nil {
		return st, fmt.Errorf("bootstrap generation: %w", err)
	}

	st.Documents = len(idx.Documents)
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
			chunkID := core.ChunkID(fingerprint, ch.Content)
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
		st.Chunks += len(artChunks)
	}
	return st, nil
}

// Reconcile checks that an import accounted for every source document and every
// (non-dangling) chunk placement. It returns a human-readable detail either way.
func Reconcile(idx LegacyIndex, st Stats) (bool, string) {
	wantDocs := len(idx.Documents)
	wantChunks := 0
	for _, d := range idx.Documents {
		wantChunks += len(d.ChunkIDs)
	}
	wantChunks -= st.SkippedMissingChunk

	if st.Documents == wantDocs && st.Chunks == wantChunks {
		return true, fmt.Sprintf("reconciled: %d documents, %d chunk placements (%d unique vectors, %d dangling skipped)",
			st.Documents, st.Chunks, st.UniqueVectors, st.SkippedMissingChunk)
	}
	return false, fmt.Sprintf("MISMATCH: documents %d/%d, chunk placements %d/%d (%d dangling skipped)",
		st.Documents, wantDocs, st.Chunks, wantChunks, st.SkippedMissingChunk)
}
