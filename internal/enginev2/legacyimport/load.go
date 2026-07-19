package legacyimport

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"os"

	"github.com/yoanbernabeu/grepai/store"
)

// LegacyChunk is one v1 chunk: identity, source location, display content, and
// its embedding vector. Mirrors the fields the v2 importer and parity harness
// need from store.Chunk.
type LegacyChunk struct {
	ID          string
	FilePath    string
	StartLine   int
	EndLine     int
	Content     string
	Vector      []float32
	ContentHash string
}

// LegacyDocument is one v1 indexed file: its path, content hash, and the ordered
// chunk ids that compose it.
type LegacyDocument struct {
	Path     string
	Hash     string
	ChunkIDs []string
}

// LegacyIndex is a fully decoded v1 GOB index.
type LegacyIndex struct {
	Chunks     map[string]LegacyChunk
	Documents  map[string]LegacyDocument
	Dimensions int // embedding width, taken from the first non-empty chunk vector
}

// Load decodes a v1 GOB index file (written by store.GOBStore) into a typed,
// read-only LegacyIndex. It never mutates the source file. It returns an error
// if the file cannot be opened or decoded, or if it contains neither chunks nor
// documents (an empty/corrupt index).
func Load(path string) (LegacyIndex, error) {
	f, err := os.Open(path)
	if err != nil {
		return LegacyIndex{}, fmt.Errorf("open legacy index: %w", err)
	}
	defer func() { _ = f.Close() }()

	// The on-disk shape is store's gobData: {Chunks, Documents}. gob matches by
	// field name/type, so decoding into this local struct with the real store
	// element types is exact and avoids re-declaring their fields.
	var raw struct {
		Chunks    map[string]store.Chunk
		Documents map[string]store.Document
	}
	if err := gob.NewDecoder(bufio.NewReader(f)).Decode(&raw); err != nil {
		return LegacyIndex{}, fmt.Errorf("decode legacy index %s: %w", path, err)
	}
	if len(raw.Chunks) == 0 && len(raw.Documents) == 0 {
		return LegacyIndex{}, fmt.Errorf("legacy index %s is empty", path)
	}

	idx := LegacyIndex{
		Chunks:    make(map[string]LegacyChunk, len(raw.Chunks)),
		Documents: make(map[string]LegacyDocument, len(raw.Documents)),
	}
	for id, c := range raw.Chunks {
		idx.Chunks[id] = LegacyChunk{
			ID:          c.ID,
			FilePath:    c.FilePath,
			StartLine:   c.StartLine,
			EndLine:     c.EndLine,
			Content:     c.Content,
			Vector:      c.Vector,
			ContentHash: c.ContentHash,
		}
		if idx.Dimensions == 0 && len(c.Vector) > 0 {
			idx.Dimensions = len(c.Vector)
		}
	}
	for p, d := range raw.Documents {
		idx.Documents[p] = LegacyDocument{Path: d.Path, Hash: d.Hash, ChunkIDs: d.ChunkIDs}
	}
	return idx, nil
}
