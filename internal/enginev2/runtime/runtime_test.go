package runtime_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
	"github.com/yoanbernabeu/grepai/internal/enginev2/runtime"
)

// TestRuntimeIndexAndSearch exercises the full v2 runtime end-to-end over a real
// git repository: reconcile -> worker -> catalog -> service search. FakeEmbedder
// vectors are content-hash-based (no semantics), so this asserts the plumbing —
// every indexed file is searchable with a non-empty snippet and line range, and
// re-indexing an unchanged repo is idle — not semantic ranking (which the real
// config-driven embedder provides, verified manually).
func TestRuntimeIndexAndSearch(t *testing.T) {
	ctx := context.Background()
	fx := enginetest.NewGitFixture(t)
	fx.WriteFile("alpha.go", "package main\n\n// alpha unique marker token\nfunc Alpha() int { return 1 }\n")
	fx.WriteFile("beta.go", "package main\n\n// beta distinct marker token\nfunc Beta() int { return 2 }\n")
	fx.Commit("init")

	emb := enginetest.NewFakeEmbedder(4)
	catPath := filepath.Join(t.TempDir(), "catalog_v2.db")
	eng, err := runtime.Open(ctx, catPath, fx.Root(), emb, "test-fp", 512, 50, 20)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()

	queued, dead, err := eng.Index(ctx)
	if err != nil {
		t.Fatalf("index: %v", err)
	}
	if queued < 2 {
		t.Fatalf("expected to index at least 2 files, queued=%d", queued)
	}
	if dead != 0 {
		t.Fatalf("no jobs should dead-letter, got %d", dead)
	}

	hits, gen, fresh, err := eng.Search(ctx, "marker token")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if gen != 1 {
		t.Fatalf("active generation = %d, want 1", gen)
	}
	if !fresh {
		t.Fatal("index should be fresh right after indexing")
	}
	// Both indexed files are searchable, each with a snippet + line range.
	paths := map[string]bool{}
	for _, h := range hits {
		paths[h.Path] = true
		if h.Content == "" {
			t.Fatalf("result %s has no snippet content", h.Path)
		}
		if h.StartLine < 1 || h.EndLine < h.StartLine {
			t.Fatalf("result %s has bad line range [%d,%d]", h.Path, h.StartLine, h.EndLine)
		}
	}
	if !paths["alpha.go"] || !paths["beta.go"] {
		t.Fatalf("both indexed files must be searchable, got %v", paths)
	}

	// Re-indexing the unchanged repo is idle (invariant 1 through the runtime).
	queued2, _, err := eng.Index(ctx)
	if err != nil {
		t.Fatalf("re-index: %v", err)
	}
	if queued2 != 0 {
		t.Fatalf("re-indexing an unchanged repo must be idle, queued=%d", queued2)
	}
}

// Opening an existing index with a different fingerprint (config/model changed)
// must fail explicitly rather than silently returning no results.
func TestRuntimeFingerprintMismatchFails(t *testing.T) {
	ctx := context.Background()
	fx := enginetest.NewGitFixture(t)
	fx.WriteFile("a.go", "package main\nfunc A() {}\n")
	fx.Commit("init")
	emb := enginetest.NewFakeEmbedder(4)
	catPath := filepath.Join(t.TempDir(), "catalog_v2.db")

	eng, err := runtime.Open(ctx, catPath, fx.Root(), emb, "fp-A", 512, 50, 20)
	if err != nil {
		t.Fatalf("open A: %v", err)
	}
	if _, _, err := eng.Index(ctx); err != nil {
		t.Fatalf("index A: %v", err)
	}
	_ = eng.Close()

	// Reopening the same catalog with a changed fingerprint must error.
	if _, err := runtime.Open(ctx, catPath, fx.Root(), emb, "fp-B", 512, 50, 20); err == nil {
		t.Fatal("opening an index with a changed fingerprint must fail")
	}
}
