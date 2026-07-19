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
	if st.SourceDocuments != 2 || st.CommittedDocuments != 2 || st.ChunkPlacements != 3 || st.UniqueVectors != 3 || st.SkippedMissingChunk != 0 {
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
	if err != nil || st2.CommittedDocuments != st.CommittedDocuments || st2.ChunkPlacements != st.ChunkPlacements || st2.UniqueVectors != st.UniqueVectors {
		t.Fatalf("re-import not idempotent: %+v err=%v", st2, err)
	}

	if ok, detail := legacyimport.Reconcile(idx, st); !ok {
		t.Fatalf("reconcile failed: %s", detail)
	}
}

func TestImportRefusesForeignActiveGeneration(t *testing.T) {
	ctx := context.Background()
	idx, err := legacyimport.Load(writeFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	c := openCatalog(t)
	// Seed a different active generation (e.g. a native v2 catalog).
	if err := c.RegisterRepository(ctx, "repo", "/root", ""); err != nil {
		t.Fatal(err)
	}
	if err := c.EnsureActiveGeneration(ctx, "repo", 1, "native-v2-fp"); err != nil {
		t.Fatal(err)
	}
	if _, err := legacyimport.Import(ctx, c, "repo", "wt", "/root", idx, "fp-v1"); err == nil {
		t.Fatal("import must refuse a catalog whose active generation has a different fingerprint")
	}
}

func TestImportPrunesStaleViewsOnShrink(t *testing.T) {
	ctx := context.Background()
	full, err := legacyimport.Load(writeFixture(t)) // a.go + b.go
	if err != nil {
		t.Fatal(err)
	}
	c := openCatalog(t)
	fp := "fp-v1"
	if _, err := legacyimport.Import(ctx, c, "repo", "wt", "/root", full, fp); err != nil {
		t.Fatal(err)
	}
	// Re-import a shrunk index (a.go only) at the same fingerprint.
	shrunk := legacyimport.LegacyIndex{
		Dimensions: full.Dimensions,
		Chunks:     map[string]legacyimport.LegacyChunk{"c1": full.Chunks["c1"]},
		Documents:  map[string]legacyimport.LegacyDocument{"a.go": {Path: "a.go", Hash: "doc-a", ChunkIDs: []string{"c1"}}},
	}
	st, err := legacyimport.Import(ctx, c, "repo", "wt", "/root", shrunk, fp)
	if err != nil {
		t.Fatal(err)
	}
	if st.CommittedDocuments != 1 || st.PrunedStaleViews != 1 {
		t.Fatalf("shrink re-import stats: %+v", st)
	}
	if _, ok, _ := c.ResolveView(ctx, "wt", "b.go"); ok {
		t.Fatal("stale b.go view should have been pruned")
	}
	if ok, detail := legacyimport.Reconcile(shrunk, st); !ok {
		t.Fatalf("reconcile after shrink: %s", detail)
	}
}

func TestImportKeepsDistinctVectorsForEqualContent(t *testing.T) {
	ctx := context.Background()
	// Two chunks with identical display content but different ContentHash (a
	// different embed input in v1) must NOT collapse to one vector.
	path := filepath.Join(t.TempDir(), "index.gob")
	s := store.NewGOBStore(path)
	if err := s.SaveChunks(ctx, []store.Chunk{
		{ID: "c1", FilePath: "a.go", Content: "same text", Vector: []float32{1, 0}, ContentHash: "hashA"},
		{ID: "c2", FilePath: "a.go", Content: "same text", Vector: []float32{0, 1}, ContentHash: "hashB"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveDocument(ctx, store.Document{Path: "a.go", Hash: "doc-a", ChunkIDs: []string{"c1", "c2"}}); err != nil {
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
	if st.UniqueVectors != 2 {
		t.Fatalf("distinct embeddings must not collapse: UniqueVectors=%d", st.UniqueVectors)
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
	if st.ChunkPlacements != 1 || st.SkippedMissingChunk != 1 || st.CommittedDocuments != 1 {
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
		Chunks: map[string]legacyimport.LegacyChunk{
			"c1": {ID: "c1"}, "c2": {ID: "c2"}, "c3": {ID: "c3"},
		},
		Documents: map[string]legacyimport.LegacyDocument{
			"a.go": {ChunkIDs: []string{"c1", "c2"}},
			"b.go": {ChunkIDs: []string{"c3"}},
		},
	}
	// Expected: 2 committed documents, 3 chunk placements.
	if ok, _ := legacyimport.Reconcile(idx, legacyimport.Stats{CommittedDocuments: 2, ChunkPlacements: 3}); !ok {
		t.Fatal("expected reconcile ok for matching stats")
	}
	if ok, _ := legacyimport.Reconcile(idx, legacyimport.Stats{CommittedDocuments: 1, ChunkPlacements: 3}); ok {
		t.Fatal("document mismatch must not reconcile")
	}
	if ok, _ := legacyimport.Reconcile(idx, legacyimport.Stats{CommittedDocuments: 2, ChunkPlacements: 2}); ok {
		t.Fatal("chunk mismatch must not reconcile")
	}
}
