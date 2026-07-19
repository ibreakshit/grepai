package legacyimport_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/legacyimport"
	"github.com/yoanbernabeu/grepai/store"
)

func TestInspectGOBReadsLegacyStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	idx := filepath.Join(dir, "index.gob")
	s := store.NewGOBStore(idx)
	if err := s.SaveChunks(ctx, []store.Chunk{
		{ID: "c1", FilePath: "a.go", Vector: []float32{1, 2, 3, 4}, ContentHash: "ch1"},
		{ID: "c2", FilePath: "b.go", Vector: []float32{5, 6, 7, 8}, ContentHash: "ch2"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveDocument(ctx, store.Document{Path: "a.go", Hash: "h", ChunkIDs: []string{"c1"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Persist(ctx); err != nil {
		t.Fatal(err)
	}
	sum, err := legacyimport.InspectGOB(idx)
	if err != nil {
		t.Fatal(err)
	}
	if sum.ChunkCount != 2 || sum.DocumentCount != 1 || sum.Dimensions != 4 {
		t.Fatalf("summary wrong: %+v", sum)
	}
	if sum.SampleContentHash != "ch1" && sum.SampleContentHash != "ch2" {
		t.Fatalf("sample content hash not decoded: %+v", sum)
	}
}
