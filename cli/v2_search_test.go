package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/config"
	"github.com/yoanbernabeu/grepai/internal/enginev2/legacyimport"
	"github.com/yoanbernabeu/grepai/internal/enginev2/runtime"
)

func searchTestConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Embedder.Provider = "openai"
	cfg.Embedder.Model = "qwen3-embedding-8b"
	cfg.Chunking.Size = 512
	cfg.Chunking.Overlap = 50
	return cfg
}

func touchFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestChooseSearchCatalog(t *testing.T) {
	t.Run("native present -> native", func(t *testing.T) {
		d := t.TempDir()
		touchFile(t, filepath.Join(d, "catalog_v2.db"))
		p, migrated := chooseSearchCatalog(d, false)
		if migrated || filepath.Base(p) != "catalog_v2.db" {
			t.Fatalf("got %s migrated=%v", p, migrated)
		}
	})
	t.Run("only migrated -> migrated", func(t *testing.T) {
		d := t.TempDir()
		touchFile(t, filepath.Join(d, "catalog_migrated.db"))
		p, migrated := chooseSearchCatalog(d, false)
		if !migrated || filepath.Base(p) != "catalog_migrated.db" {
			t.Fatalf("got %s migrated=%v", p, migrated)
		}
	})
	t.Run("both present -> native by default", func(t *testing.T) {
		d := t.TempDir()
		touchFile(t, filepath.Join(d, "catalog_v2.db"))
		touchFile(t, filepath.Join(d, "catalog_migrated.db"))
		p, migrated := chooseSearchCatalog(d, false)
		if migrated || filepath.Base(p) != "catalog_v2.db" {
			t.Fatalf("native must win when both exist: got %s migrated=%v", p, migrated)
		}
	})
	t.Run("force migrated overrides native", func(t *testing.T) {
		d := t.TempDir()
		touchFile(t, filepath.Join(d, "catalog_v2.db"))
		touchFile(t, filepath.Join(d, "catalog_migrated.db"))
		p, migrated := chooseSearchCatalog(d, true)
		if !migrated || filepath.Base(p) != "catalog_migrated.db" {
			t.Fatalf("--migrated must force migrated: got %s migrated=%v", p, migrated)
		}
	})
	t.Run("neither -> native (empty)", func(t *testing.T) {
		d := t.TempDir()
		p, migrated := chooseSearchCatalog(d, false)
		if migrated || filepath.Base(p) != "catalog_v2.db" {
			t.Fatalf("got %s migrated=%v", p, migrated)
		}
	})
}

func TestResolveSearchTarget(t *testing.T) {
	cfg := searchTestConfig()
	nativeFP := runtime.Fingerprint(cfg)
	migratedFP := legacyimport.DeriveFingerprint(cfg)
	if nativeFP == migratedFP {
		t.Fatal("native and migration fingerprints must differ, else the guard is moot")
	}

	t.Run("native default uses native fingerprint", func(t *testing.T) {
		d := t.TempDir()
		touchFile(t, filepath.Join(d, "catalog_v2.db"))
		p, fp, migrated, err := resolveSearchTarget(cfg, d, false, "")
		if err != nil || migrated || fp != nativeFP || filepath.Base(p) != "catalog_v2.db" {
			t.Fatalf("p=%s fp==native:%v migrated=%v err=%v", p, fp == nativeFP, migrated, err)
		}
	})
	t.Run("migrated fallback uses migration fingerprint", func(t *testing.T) {
		d := t.TempDir()
		touchFile(t, filepath.Join(d, "catalog_migrated.db"))
		p, fp, migrated, err := resolveSearchTarget(cfg, d, false, "")
		if err != nil || !migrated || fp != migratedFP || filepath.Base(p) != "catalog_migrated.db" {
			t.Fatalf("p=%s fp==migrated:%v migrated=%v err=%v", p, fp == migratedFP, migrated, err)
		}
	})
	t.Run("forced migrated but missing errors", func(t *testing.T) {
		d := t.TempDir()
		if _, _, _, err := resolveSearchTarget(cfg, d, true, ""); err == nil {
			t.Fatal("forced migrated with no catalog must error")
		}
	})
	t.Run("catalog override present uses migration fingerprint", func(t *testing.T) {
		d := t.TempDir()
		cp := filepath.Join(d, "custom_migrated.db")
		touchFile(t, cp)
		p, fp, migrated, err := resolveSearchTarget(cfg, d, false, cp)
		if err != nil || !migrated || fp != migratedFP || p != cp {
			t.Fatalf("p=%s fp==migrated:%v migrated=%v err=%v", p, fp == migratedFP, migrated, err)
		}
	})
	t.Run("catalog override missing errors", func(t *testing.T) {
		d := t.TempDir()
		if _, _, _, err := resolveSearchTarget(cfg, d, false, filepath.Join(d, "nope.db")); err == nil {
			t.Fatal("missing override catalog must error")
		}
	})
}
