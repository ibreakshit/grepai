package legacyimport_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/legacyimport"
	"github.com/yoanbernabeu/grepai/store"
)

// writeFixture builds a small real v1 GOB index (3 chunks across 2 documents)
// and returns its path. Shared by the loader, importer, and reconcile tests.
func writeFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "index.gob")
	s := store.NewGOBStore(path)
	ctx := context.Background()
	if err := s.SaveChunks(ctx, []store.Chunk{
		{ID: "c1", FilePath: "a.go", StartLine: 1, EndLine: 3, Content: "func A() {}", Vector: []float32{1, 0, 0, 0}, ContentHash: "ha"},
		{ID: "c2", FilePath: "a.go", StartLine: 4, EndLine: 6, Content: "func B() {}", Vector: []float32{0, 1, 0, 0}, ContentHash: "hb"},
		{ID: "c3", FilePath: "b.go", StartLine: 1, EndLine: 2, Content: "package x", Vector: []float32{0, 0, 1, 0}, ContentHash: "hc"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveDocument(ctx, store.Document{Path: "a.go", Hash: "doc-a", ChunkIDs: []string{"c1", "c2"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveDocument(ctx, store.Document{Path: "b.go", Hash: "doc-b", ChunkIDs: []string{"c3"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Persist(ctx); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadDecodesChunksAndDocuments(t *testing.T) {
	idx, err := legacyimport.Load(writeFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Chunks) != 3 || len(idx.Documents) != 2 {
		t.Fatalf("counts: chunks=%d documents=%d", len(idx.Chunks), len(idx.Documents))
	}
	if idx.Dimensions != 4 {
		t.Fatalf("dims=%d want 4", idx.Dimensions)
	}
	c1 := idx.Chunks["c1"]
	if c1.FilePath != "a.go" || c1.StartLine != 1 || c1.EndLine != 3 || c1.Content != "func A() {}" || len(c1.Vector) != 4 {
		t.Fatalf("c1 not fully decoded: %+v", c1)
	}
	if got := idx.Documents["a.go"].ChunkIDs; len(got) != 2 || got[0] != "c1" || got[1] != "c2" {
		t.Fatalf("doc a chunk ids: %v", got)
	}
	if idx.Documents["a.go"].Hash != "doc-a" {
		t.Fatalf("doc a hash: %q", idx.Documents["a.go"].Hash)
	}
}

func TestLoadRejectsMissing(t *testing.T) {
	if _, err := legacyimport.Load(filepath.Join(t.TempDir(), "missing.gob")); err == nil {
		t.Fatal("expected error for missing index")
	}
}
