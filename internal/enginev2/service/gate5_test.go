package service_test

import (
	"context"
	"sync"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
	"github.com/yoanbernabeu/grepai/internal/enginev2/reconcile"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
)

// countingEmbedder counts Embed/EmbedBatch calls to prove query paths make no
// indexing (batch) embeds.
type countingEmbedder struct {
	inner   *enginetest.FakeEmbedder
	mu      sync.Mutex
	embeds  int
	batches int
}

func (e *countingEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.mu.Lock()
	e.embeds++
	e.mu.Unlock()
	return e.inner.Embed(ctx, text)
}
func (e *countingEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	e.mu.Lock()
	e.batches++
	e.mu.Unlock()
	return e.inner.EmbedBatch(ctx, texts)
}
func (e *countingEmbedder) Dimensions() int { return e.inner.Dimensions() }
func (e *countingEmbedder) Close() error    { return e.inner.Close() }
func (e *countingEmbedder) counts() (int, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.embeds, e.batches
}

func TestGate5_WorktreeIsolation(t *testing.T) {
	ctx := context.Background()
	c, emb, s := newServer(t)
	// Same path a.go, different content per worktree; plus a file only in w2.
	seedWorktreeArtifact(t, c, emb, "r", "w1", "a.go", "v1")
	seedWorktreeArtifact(t, c, emb, "r", "w2", "a.go", "v2")
	seedWorktreeArtifact(t, c, emb, "r", "w2", "secret.go", "onlyw2")

	// Each worktree's a.go resolves to its OWN artifact version.
	id1, _, _ := c.ResolveView(ctx, "w1", "a.go")
	id2, _, _ := c.ResolveView(ctx, "w2", "a.go")
	if id1 == id2 {
		t.Fatal("distinct worktree versions of a.go must resolve to distinct artifacts")
	}
	// A search from w1 never returns w2's exclusive file.
	resp, err := s.Search(ctx, service.SearchRequest{WorktreeID: "w1", Query: "onlyw2"})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range resp.Results {
		if h.Path == "secret.go" {
			t.Fatal("worktree isolation violated: w1 returned w2's file")
		}
	}
	// w2 can see its own exclusive file.
	r2, _ := s.Search(ctx, service.SearchRequest{WorktreeID: "w2", Query: "onlyw2"})
	found := false
	for _, h := range r2.Results {
		if h.Path == "secret.go" {
			found = true
		}
	}
	if !found {
		t.Fatal("w2 should see its own file")
	}
}

func TestGate5_QueryMakesNoIndexingCalls(t *testing.T) {
	ctx := context.Background()
	c := newCatalog(t)
	fake := enginetest.NewFakeEmbedder(4)
	ce := &countingEmbedder{inner: fake}
	s := service.New(c, reconcile.New(c), ce, "fp", 10)
	seedWorktreeArtifact(t, c, fake, "r", "w", "a.go", "alpha") // seeds via fake, not the server

	before, _ := c.WorktreePendingCount(ctx, "w")
	if before != 0 {
		t.Fatalf("precondition: expected 0 pending, got %d", before)
	}
	// Query paths: none may enqueue a job or batch-embed (index).
	if _, err := s.Search(ctx, service.SearchRequest{WorktreeID: "w", Query: "alpha"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Status(ctx, service.StatusRequest{WorktreeID: "w"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WaitFresh(ctx, service.WaitFreshRequest{WorktreeID: "w", Paths: []string{"a.go"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Trace(ctx, service.TraceRequest{WorktreeID: "w", Symbol: "X"}); err != nil {
		t.Fatal(err)
	}
	after, _ := c.WorktreePendingCount(ctx, "w")
	if after != 0 {
		t.Fatalf("query paths must enqueue no jobs, pending went to %d", after)
	}
	embeds, batches := ce.counts()
	if batches != 0 {
		t.Fatalf("query paths must never batch-embed (index): %d batch calls", batches)
	}
	if embeds != 1 {
		t.Fatalf("only Search should embed (the query), got %d embeds", embeds)
	}
}

func TestGate5_ActiveGenerationQueryableDuringRebuild(t *testing.T) {
	ctx := context.Background()
	c, emb, s := newServer(t)
	seedWorktreeArtifact(t, c, emb, "r", "w", "a.go", "v1")

	// Baseline: a.go is searchable at gen 1.
	base, _ := s.Search(ctx, service.SearchRequest{WorktreeID: "w", Query: "v1"})
	if len(base.Results) != 1 || base.Results[0].Path != "a.go" || base.ActiveGeneration != 1 {
		t.Fatalf("baseline wrong: %+v", base)
	}

	// Start a controlled rebuild (creates generation 2, building).
	rb, err := s.Rebuild(ctx, service.RebuildRequest{RepositoryID: "r"})
	if err != nil || rb.Generation != 2 {
		t.Fatalf("rebuild: gen=%d err=%v", rb.Generation, err)
	}
	// Stage a generation-2 artifact + chunk (as a rebuild would build) WITHOUT
	// committing any active view row for it.
	newVec, _ := emb.Embed(ctx, "brandnew")
	newCh := core.ChunkID("fp", "brandnew")
	if err := c.PutChunkVector(ctx, newCh, "r", "fp", newVec); err != nil {
		t.Fatal(err)
	}
	newKey := core.ArtifactKey{RepositoryID: "r", RelativePath: "new.go", SourceHash: "hnew", Fingerprint: "fp"}
	if err := c.PutArtifact(ctx, core.Artifact{ID: newKey.ArtifactID(), Key: newKey, Dimensions: 4}); err != nil {
		t.Fatal(err)
	}

	// The active generation stays queryable: the staged (building) artifact,
	// referenced by no active view row, never surfaces in search.
	q, _ := s.Search(ctx, service.SearchRequest{WorktreeID: "w", Query: "brandnew"})
	for _, h := range q.Results {
		if h.Path == "new.go" {
			t.Fatal("a building generation's artifact leaked into search")
		}
	}
	// The original view remains searchable and the active generation is still 1.
	still, _ := s.Search(ctx, service.SearchRequest{WorktreeID: "w", Query: "v1"})
	if len(still.Results) != 1 || still.Results[0].Path != "a.go" {
		t.Fatalf("active generation stopped being queryable during rebuild: %+v", still)
	}
	st, _ := s.Status(ctx, service.StatusRequest{WorktreeID: "w"})
	if st.ActiveGeneration != 1 {
		t.Fatalf("active generation must stay 1 during a rebuild, got %d", st.ActiveGeneration)
	}
}
