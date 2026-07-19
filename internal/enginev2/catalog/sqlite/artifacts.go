package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// PutArtifact stores an immutable artifact row only. Re-inserting an identical
// artifact_id is a no-op (INSERT OR IGNORE), preserving immutability.
//
// It does NOT write the artifact_chunks mapping. It must not be used on the
// search/commit path: the whole-file cache-hit path relies on every artifact
// returned by GetArtifact already having a complete chunk mapping, which only
// CommitUpdate (via commitUpdateTx) establishes atomically with the view switch
// (invariant 6). PutArtifact is a low-level building block for tests and
// migration tooling that manage chunk rows separately.
func (c *Catalog) PutArtifact(ctx context.Context, a core.Artifact) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		return putArtifactTx(ctx, tx, a)
	})
}

// putArtifactTx is the transaction-scoped artifact insert, reused by
// CommitUpdate (Task 6) so the artifact store and view switch are atomic.
func putArtifactTx(ctx context.Context, tx *sql.Tx, a core.Artifact) error {
	_, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO file_artifacts(
			artifact_id, repository_id, relative_path, source_hash, fingerprint, dimensions, created_at)
		VALUES(?, ?, ?, ?, ?, ?, datetime('now'))`,
		string(a.ID), string(a.Key.RepositoryID), a.Key.RelativePath, a.Key.SourceHash,
		a.Key.Fingerprint, a.Dimensions)
	return err
}

// putArtifactChunksTx records the ordered (artifact, ordinal, chunk) mapping.
// Idempotent: re-committing an immutable artifact re-inserts the same rows.
func putArtifactChunksTx(ctx context.Context, tx *sql.Tx, artifactID core.ArtifactID, chunks []core.ArtifactChunk) error {
	for _, ch := range chunks {
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO artifact_chunks(artifact_id, ordinal, chunk_id, start_line, end_line)
			VALUES(?, ?, ?, ?, ?)`, string(artifactID), ch.Ordinal, ch.ChunkID, ch.StartLine, ch.EndLine); err != nil {
			return err
		}
	}
	return nil
}

// GetArtifact returns the artifact for an exact (repository, path, source hash,
// fingerprint) key. A differing fingerprint or source hash never matches.
func (c *Catalog) GetArtifact(ctx context.Context, key core.ArtifactKey) (core.Artifact, bool, error) {
	var id string
	var dims int
	err := c.db.QueryRowContext(ctx, `
		SELECT artifact_id, dimensions FROM file_artifacts
		WHERE repository_id=? AND relative_path=? AND source_hash=? AND fingerprint=?`,
		string(key.RepositoryID), key.RelativePath, key.SourceHash, key.Fingerprint).Scan(&id, &dims)
	if errors.Is(err, sql.ErrNoRows) {
		return core.Artifact{}, false, nil
	}
	if err != nil {
		return core.Artifact{}, false, err
	}
	return core.Artifact{ID: core.ArtifactID(id), Key: key, Dimensions: dims}, true, nil
}

// PutChunkVector stores a chunk's validated float32 vector and its display
// content. Content is content-addressed (stable per chunk_id), so INSERT OR
// IGNORE keeps the first writer's content.
func (c *Catalog) PutChunkVector(ctx context.Context, chunkID string, repo core.RepositoryID, fingerprint string, vec []float32, content string) error {
	blob := encodeVector(vec)
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		// Content is content-addressed and stable per chunk_id, so the vector is
		// never rewritten. But a chunk row created without content (e.g. an older
		// schema, or a first writer that had none) is repaired here: a later
		// writer's non-empty content backfills an empty one.
		_, err := tx.ExecContext(ctx, `
			INSERT INTO chunks(chunk_id, repository_id, fingerprint, dimensions, vector, content, created_at)
			VALUES(?, ?, ?, ?, ?, ?, datetime('now'))
			ON CONFLICT(chunk_id) DO UPDATE SET content=excluded.content
			WHERE chunks.content='' AND excluded.content<>''`,
			chunkID, string(repo), fingerprint, len(vec), blob, content)
		return err
	})
}

// GetChunkVector returns a chunk's vector, validating the stored blob length
// against its stored dimension count.
func (c *Catalog) GetChunkVector(ctx context.Context, chunkID string) ([]float32, bool, error) {
	var dims int
	var blob []byte
	err := c.db.QueryRowContext(ctx, `
		SELECT dimensions, vector FROM chunks WHERE chunk_id=?`, chunkID).Scan(&dims, &blob)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	v, err := decodeVector(blob, dims)
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}
