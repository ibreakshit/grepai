package legacyimport_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog/sqlite"
	"github.com/yoanbernabeu/grepai/internal/enginev2/legacyimport"
	"github.com/yoanbernabeu/grepai/store"
)

func openCatalog(t *testing.T) *sqlite.Catalog {
	t.Helper()
	c, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "cat.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestImportWritesSearchableViews(t *testing.T) {
	ctx := context.Background()
	idx, err := legacyimport.Load(writeFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	c := openCatalog(t)
	fp := "fp-v1"

	st, err := legacyimport.Import(ctx, c, "repo", "wt", "/root", idx, fp)
	if err != nil {
		t.Fatal(err)
	}
	if st.Documents != 2 || st.Chunks != 3 || st.UniqueVectors != 3 || st.SkippedMissingChunk != 0 {
		t.Fatalf("stats: %+v", st)
	}
	if id, ok, _ := c.ResolveView(ctx, "wt", "a.go"); !ok || id == "" {
		t.Fatal("a.go view not committed")
	}

	hits, err := c.SearchWorktree(ctx, "wt", []float32{1, 0, 0, 0}, 5)
	if err != nil || len(hits) == 0 {
		t.Fatalf("search: hits=%d err=%v", len(hits), err)
	}
	if hits[0].Path != "a.go" || hits[0].Content == "" {
		t.Fatalf("top hit: %+v", hits[0])
	}

	// Re-import must be idempotent.
	st2, err := legacyimport.Import(ctx, c, "repo", "wt", "/root", idx, fp)
	if err != nil || st2.Documents != st.Documents || st2.Chunks != st.Chunks || st2.UniqueVectors != st.UniqueVectors {
		t.Fatalf("re-import not idempotent: %+v err=%v", st2, err)
	}

	if ok, detail := legacyimport.Reconcile(idx, st); !ok {
		t.Fatalf("reconcile failed: %s", detail)
	}
}

func TestImportSkipsDanglingChunkID(t *testing.T) {
	ctx := context.Background()
	// A document references a chunk id that does not exist in Chunks.
	path := filepath.Join(t.TempDir(), "index.gob")
	s := store.NewGOBStore(path)
	if err := s.SaveChunks(ctx, []store.Chunk{
		{ID: "c1", FilePath: "a.go", Content: "real", Vector: []float32{1, 0}, ContentHash: "h1"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveDocument(ctx, store.Document{Path: "a.go", Hash: "doc-a", ChunkIDs: []string{"c1", "ghost"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Persist(ctx); err != nil {
		t.Fatal(err)
	}

	idx, err := legacyimport.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	c := openCatalog(t)
	st, err := legacyimport.Import(ctx, c, "repo", "wt", "/root", idx, "fp")
	if err != nil {
		t.Fatal(err)
	}
	if st.Chunks != 1 || st.SkippedMissingChunk != 1 {
		t.Fatalf("dangling handling: %+v", st)
	}
	// Reconcile must still succeed (skipped dangling is accounted for).
	if ok, detail := legacyimport.Reconcile(idx, st); !ok {
		t.Fatalf("reconcile after dangling: %s", detail)
	}
	// The document is still searchable via its one real chunk.
	if id, ok, _ := c.ResolveView(ctx, "wt", "a.go"); !ok || id == "" {
		t.Fatal("a.go view not committed despite one valid chunk")
	}
}

func TestReconcileDetectsMismatch(t *testing.T) {
	idx := legacyimport.LegacyIndex{
		Documents: map[string]legacyimport.LegacyDocument{
			"a.go": {ChunkIDs: []string{"c1", "c2"}},
			"b.go": {ChunkIDs: []string{"c3"}},
		},
	}
	if ok, _ := legacyimport.Reconcile(idx, legacyimport.Stats{Documents: 2, Chunks: 3}); !ok {
		t.Fatal("expected reconcile ok for matching stats")
	}
	if ok, _ := legacyimport.Reconcile(idx, legacyimport.Stats{Documents: 1, Chunks: 3}); ok {
		t.Fatal("document mismatch must not reconcile")
	}
	if ok, _ := legacyimport.Reconcile(idx, legacyimport.Stats{Documents: 2, Chunks: 2}); ok {
		t.Fatal("chunk mismatch must not reconcile")
	}
}
