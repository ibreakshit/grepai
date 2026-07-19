package runtime_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog/sqlite"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
	"github.com/yoanbernabeu/grepai/internal/enginev2/legacyimport"
	"github.com/yoanbernabeu/grepai/internal/enginev2/runtime"
)

// buildMigratedCatalog imports a tiny fixed index into a migrated catalog under
// root/.grepai and returns the catalog path and the fingerprint it was stored
// with. It mirrors what `grepai v2 migrate` produces.
func buildMigratedCatalog(t *testing.T, root, fingerprint string) string {
	t.Helper()
	ctx := context.Background()
	if err := os.MkdirAll(filepath.Join(root, ".grepai"), 0o750); err != nil {
		t.Fatal(err)
	}
	catPath := filepath.Join(root, ".grepai", "catalog_migrated.db")
	cat, err := sqlite.Open(ctx, catPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cat.Close() }()

	// repo/wt must equal what runtime.Open derives: RepositoryID/WorktreeID of
	// the symlink-resolved absolute root.
	absRoot := resolvedRoot(t, root)
	idx := legacyimport.LegacyIndex{
		Dimensions: 4,
		Chunks: map[string]legacyimport.LegacyChunk{
			"c1": {Content: "alpha", Vector: []float32{1, 0, 0, 0}, StartLine: 1, EndLine: 1, ContentHash: "a"},
			"c2": {Content: "beta", Vector: []float32{0, 1, 0, 0}, StartLine: 1, EndLine: 1, ContentHash: "b"},
		},
		Documents: map[string]legacyimport.LegacyDocument{
			"a.txt": {Path: "a.txt", Hash: "ha", ChunkIDs: []string{"c1"}},
			"b.txt": {Path: "b.txt", Hash: "hb", ChunkIDs: []string{"c2"}},
		},
	}
	if _, err := legacyimport.Import(ctx, cat, core.RepositoryID(absRoot), core.WorktreeID(absRoot), absRoot, idx, fingerprint); err != nil {
		t.Fatal(err)
	}
	return catPath
}

func resolvedRoot(t *testing.T, root string) string {
	t.Helper()
	abs, err := filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	if r, err := filepath.EvalSymlinks(abs); err == nil {
		abs = r
	}
	return abs
}

func TestOpenServesMigratedCatalog(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const fp = "migration-fingerprint"
	catPath := buildMigratedCatalog(t, root, fp)

	// Opened with the SAME fingerprint the migration stored (as the CLI does for
	// a migrated index), the runtime serves the migrated views.
	emb := enginetest.NewFakeEmbedder(4)
	eng, err := runtime.Open(ctx, catPath, root, emb, fp, 512, 50, 10)
	if err != nil {
		t.Fatalf("open migrated catalog: %v", err)
	}
	defer func() { _ = eng.Close() }()

	hits, gen, _, err := eng.Search(ctx, "anything")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if gen != 1 {
		t.Fatalf("active generation = %d, want 1", gen)
	}
	if len(hits) == 0 {
		t.Fatal("expected the migrated views to be searchable")
	}
	for _, h := range hits {
		if h.Path != "a.txt" && h.Path != "b.txt" {
			t.Fatalf("unexpected hit path %q", h.Path)
		}
		if h.Content == "" {
			t.Fatalf("hit %q has no snippet", h.Path)
		}
	}
}

func TestOpenRejectsMigratedCatalogWithWrongFingerprint(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	catPath := buildMigratedCatalog(t, root, "migration-fingerprint")

	// Opening with the native fingerprint (what a naive search would pass) must be
	// refused by the fingerprint guard, not silently return nothing.
	emb := enginetest.NewFakeEmbedder(4)
	if _, err := runtime.Open(ctx, catPath, root, emb, "native-fingerprint", 512, 50, 10); err == nil {
		t.Fatal("opening a migrated catalog with a mismatched fingerprint must error")
	}
}
