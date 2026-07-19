package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
)

// ArtifactChunk is one ordered chunk of an artifact: its content-addressed
// identity, the validated embedding vector, and display metadata (the chunk's
// text and its line range within this artifact) for search snippets. Ordinal
// preserves chunk order within the artifact so retrieval is stable.
type ArtifactChunk struct {
	Ordinal   int
	ChunkID   string
	Vector    []float32
	Content   string // display text (content-addressed, stable per ChunkID)
	StartLine int    // 1-based first line within the file (per-artifact)
	EndLine   int    // 1-based last line within the file (per-artifact)
}

// ChunkID derives a content-addressed identifier for one chunk's embedding
// input, scoped by the indexing fingerprint. Two chunks share an id only when
// both the fingerprint and the exact embedding input match (invariant 5 reuse,
// invariant 10 correctness). The encoding is length-prefixed so no component
// boundary can be confused with another.
func ChunkID(fingerprint, embedContent string) string {
	var buf bytes.Buffer
	writeCanonicalString(&buf, fingerprint)
	writeCanonicalString(&buf, embedContent)
	sum := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(sum[:])
}
