package sqlite

import (
	"context"
	"math"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
)

// putArtifact commits a one-chunk artifact for (wt, path, content) at gen 1,
// using FakeEmbedder vectors so a query embedded from the same text ranks it.
func putArtifact(t *testing.T, c *Catalog, emb *enginetest.FakeEmbedder, wt core.WorktreeID, path, content string) {
	t.Helper()
	ctx := context.Background()
	vec, err := emb.Embed(ctx, content)
	if err != nil {
		t.Fatal(err)
	}
	key := core.ArtifactKey{RepositoryID: "r", RelativePath: path, SourceHash: path + content, Fingerprint: "fp"}
	chID := core.ChunkID("fp", content)
	must(t, c.PutChunkVector(ctx, chID, "r", "fp", vec, content))
	art := core.Artifact{ID: key.ArtifactID(), Key: key, Dimensions: 4, Chunks: []core.ArtifactChunk{{Ordinal: 0, ChunkID: chID, Vector: vec}}}
	req := core.CommitRequest{
		View:     core.ViewEntry{WorktreeID: wt, Path: path, ArtifactID: key.ArtifactID(), Generation: 1},
		Artifact: art,
		Chunks:   art.Chunks,
	}
	must(t, c.CommitUpdate(ctx, req, core.Job{WorktreeID: wt, Path: path, DesiredHash: path + content, Generation: 1, Operation: core.OpUpsert}))
}

func TestSearchWorktreeIsolationAndRanking(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	emb := enginetest.NewFakeEmbedder(4)
	seedRepoWorktree(t, c, "r", "w1")
	seedRepoWorktree(t, c, "r", "w2")
	seedGeneration(t, c, "r", 1, "fp")

	putArtifact(t, c, emb, "w1", "a.go", "alpha")
	putArtifact(t, c, emb, "w2", "secret.go", "beta") // only in w2

	q, err := emb.Embed(ctx, "alpha")
	if err != nil {
		t.Fatal(err)
	}
	hits, err := c.SearchWorktree(ctx, "w1", q, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Path != "a.go" {
		t.Fatalf("w1 search wrong: %+v", hits)
	}
	if hits[0].Content != "alpha" {
		t.Fatalf("search must return the matching chunk's content, got %q", hits[0].Content)
	}
	for _, h := range hits {
		if h.Path == "secret.go" {
			t.Fatal("worktree isolation violated: w1 saw w2's file")
		}
	}
	h2, err := c.SearchWorktree(ctx, "w2", q, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(h2) != 1 || h2[0].Path != "secret.go" {
		t.Fatalf("w2 search wrong: %+v", h2)
	}
}

// A stored non-finite (NaN) vector must be skipped, never becoming a path's
// best score nor corrupting the ranking (Codex Phase 5 review #4).
func TestSearchWorktreeSkipsNonFiniteVectors(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	emb := enginetest.NewFakeEmbedder(4)
	seedRepoWorktree(t, c, "r", "w")
	seedGeneration(t, c, "r", 1, "fp")
	putArtifact(t, c, emb, "w", "good.go", "good")

	// Commit a NaN-vector artifact for bad.go directly.
	nan := []float32{float32(math.NaN()), 0, 0, 0}
	chID := core.ChunkID("fp", "badcontent")
	must(t, c.PutChunkVector(ctx, chID, "r", "fp", nan, "nan-chunk"))
	key := core.ArtifactKey{RepositoryID: "r", RelativePath: "bad.go", SourceHash: "hbad", Fingerprint: "fp"}
	art := core.Artifact{ID: key.ArtifactID(), Key: key, Dimensions: 4, Chunks: []core.ArtifactChunk{{Ordinal: 0, ChunkID: chID, Vector: nan}}}
	must(t, c.CommitUpdate(ctx,
		core.CommitRequest{View: core.ViewEntry{WorktreeID: "w", Path: "bad.go", ArtifactID: key.ArtifactID(), Generation: 1}, Artifact: art, Chunks: art.Chunks},
		core.Job{WorktreeID: "w", Path: "bad.go", DesiredHash: "hbad", Generation: 1, Operation: core.OpUpsert}))

	q, err := emb.Embed(ctx, "good")
	if err != nil {
		t.Fatal(err)
	}
	hits, err := c.SearchWorktree(ctx, "w", q, 10)
	if err != nil {
		t.Fatal(err)
	}
	good := false
	for _, h := range hits {
		if h.Path == "bad.go" {
			t.Fatal("a non-finite stored vector must be skipped")
		}
		if h.Path == "good.go" {
			good = true
		}
	}
	if !good {
		t.Fatal("good.go should still rank")
	}
}

func TestWorktreeFreshnessReads(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "r", "w")
	seedGeneration(t, c, "r", 1, "fp")
	if n, _ := c.WorktreePendingCount(ctx, "w"); n != 0 {
		t.Fatalf("empty worktree should have 0 pending, got %d", n)
	}
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	if n, _ := c.WorktreePendingCount(ctx, "w"); n != 1 {
		t.Fatalf("expected 1 pending, got %d", n)
	}
	pend, _ := c.WorktreePathsPending(ctx, "w", []string{"a.go", "b.go"})
	if !pend {
		t.Fatal("a.go is pending")
	}
	none, _ := c.WorktreePathsPending(ctx, "w", []string{"b.go"})
	if none {
		t.Fatal("b.go is not pending")
	}
	empty, _ := c.WorktreePathsPending(ctx, "w", nil)
	if empty {
		t.Fatal("empty paths must report not-pending")
	}
}
