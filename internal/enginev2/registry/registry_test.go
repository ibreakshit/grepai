package registry

import (
	"path/filepath"
	"testing"
)

func TestLoadMissingReturnsEmpty(t *testing.T) {
	r, err := Load(filepath.Join(t.TempDir(), "registry.json"))
	if err != nil {
		t.Fatalf("Load missing: %v", err)
	}
	if len(r.Entries) != 0 {
		t.Fatalf("missing file should load empty, got %d entries", len(r.Entries))
	}
}

func TestSaveLoadRoundTripAndUpsert(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	r := &Registry{}
	r.Upsert(Entry{RepositoryID: "/a", Root: "/a", CatalogPath: "/a/.grepai/catalog_v2.db", ActiveGeneration: 1})
	r.Upsert(Entry{RepositoryID: "/b", Root: "/b", CatalogPath: "/b/.grepai/catalog_v2.db", ActiveGeneration: 3})
	r.Upsert(Entry{RepositoryID: "/a", Root: "/a", CatalogPath: "/a/.grepai/catalog_v2.db", ActiveGeneration: 2}) // replace /a
	if err := r.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got.Entries) != 2 {
		t.Fatalf("want 2 entries after upsert-replace, got %d", len(got.Entries))
	}
	var genA int64
	for _, e := range got.Entries {
		if e.RepositoryID == "/a" {
			genA = e.ActiveGeneration
		}
	}
	if genA != 2 {
		t.Fatalf("upsert should have replaced /a generation with 2, got %d", genA)
	}
}
