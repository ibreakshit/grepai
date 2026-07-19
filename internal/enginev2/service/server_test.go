package service_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog/sqlite"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
	"github.com/yoanbernabeu/grepai/internal/enginev2/reconcile"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
)

func newCatalog(t *testing.T) *sqlite.Catalog {
	t.Helper()
	c, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func newServer(t *testing.T) (*sqlite.Catalog, *enginetest.FakeEmbedder, *service.Server) {
	t.Helper()
	c := newCatalog(t)
	emb := enginetest.NewFakeEmbedder(4)
	return c, emb, service.New(c, reconcile.New(c), emb, "fp", 10)
}

// seedWorktreeArtifact registers repo/wt, ensures an active gen 1 (fp "fp"), and
// commits a one-chunk artifact for (wt, path, content). Duplicate registration
// errors are ignored so it can be called repeatedly in one repo.
func seedWorktreeArtifact(t *testing.T, c *sqlite.Catalog, emb *enginetest.FakeEmbedder, repo core.RepositoryID, wt core.WorktreeID, path, content string) {
	t.Helper()
	ctx := context.Background()
	_ = c.RegisterRepository(ctx, repo, "/"+string(repo), "")
	_ = c.RegisterWorktree(ctx, wt, repo, "/"+string(wt), 1)
	_ = c.CreateGeneration(ctx, repo, 1, "fp")
	if err := c.SetActiveGeneration(ctx, repo, 1); err != nil {
		t.Fatal(err)
	}
	vec, err := emb.Embed(ctx, content)
	if err != nil {
		t.Fatal(err)
	}
	key := core.ArtifactKey{RepositoryID: repo, RelativePath: path, SourceHash: path + content, Fingerprint: "fp"}
	chID := core.ChunkID("fp", content)
	if err := c.PutChunkVector(ctx, chID, repo, "fp", vec); err != nil {
		t.Fatal(err)
	}
	art := core.Artifact{ID: key.ArtifactID(), Key: key, Dimensions: 4, Chunks: []core.ArtifactChunk{{Ordinal: 0, ChunkID: chID, Vector: vec}}}
	req := core.CommitRequest{
		View:     core.ViewEntry{WorktreeID: wt, Path: path, ArtifactID: key.ArtifactID(), Generation: 1},
		Artifact: art,
		Chunks:   art.Chunks,
	}
	if err := c.CommitUpdate(ctx, req, core.Job{WorktreeID: wt, Path: path, DesiredHash: path + content, Generation: 1, Operation: core.OpUpsert}); err != nil {
		t.Fatal(err)
	}
}

func TestSearchReturnsWorktreeResults(t *testing.T) {
	ctx := context.Background()
	c, emb, s := newServer(t)
	seedWorktreeArtifact(t, c, emb, "r", "w", "a.go", "alpha")
	resp, err := s.Search(ctx, service.SearchRequest{WorktreeID: "w", Query: "alpha"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) != 1 || resp.Results[0].Path != "a.go" {
		t.Fatalf("search results wrong: %+v", resp.Results)
	}
	if resp.ActiveGeneration != 1 || !resp.Fresh {
		t.Fatalf("freshness/gen wrong: %+v", resp)
	}
}

func TestStatusReportsFreshnessAndGeneration(t *testing.T) {
	ctx := context.Background()
	c, emb, s := newServer(t)
	seedWorktreeArtifact(t, c, emb, "r", "w", "a.go", "alpha")
	resp, err := s.Status(ctx, service.StatusRequest{WorktreeID: "w"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.ActiveGeneration != 1 || resp.Pending != 0 || !resp.Fresh {
		t.Fatalf("status wrong: %+v", resp)
	}
	// A pending job makes it not fresh.
	if err := c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "b.go", DesiredHash: "h", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}); err != nil {
		t.Fatal(err)
	}
	resp, _ = s.Status(ctx, service.StatusRequest{WorktreeID: "w"})
	if resp.Pending != 1 || resp.Fresh {
		t.Fatalf("expected not-fresh: %+v", resp)
	}
}

func TestWaitFreshImmediateAndTimeout(t *testing.T) {
	ctx := context.Background()
	c, emb, s := newServer(t)
	seedWorktreeArtifact(t, c, emb, "r", "w", "a.go", "alpha")
	// No pending job for a.go => immediately fresh.
	resp, err := s.WaitFresh(ctx, service.WaitFreshRequest{WorktreeID: "w", Paths: []string{"a.go"}})
	if err != nil || !resp.Fresh {
		t.Fatalf("expected immediately fresh: resp=%+v err=%v", resp, err)
	}
	// A pending path with a short deadline => returns not-fresh (timeout), no error.
	if err := c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "b.go", DesiredHash: "h", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}); err != nil {
		t.Fatal(err)
	}
	dctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	resp, err = s.WaitFresh(dctx, service.WaitFreshRequest{WorktreeID: "w", Paths: []string{"b.go"}})
	if err != nil || resp.Fresh {
		t.Fatalf("expected timeout not-fresh: resp=%+v err=%v", resp, err)
	}
}

func TestRegisterAndRebuild(t *testing.T) {
	ctx := context.Background()
	_, _, s := newServer(t)
	reg, err := s.Register(ctx, service.RegisterRequest{Root: "/repo/x"})
	if err != nil {
		t.Fatal(err)
	}
	if reg.RepositoryID != "/repo/x" || reg.WorktreeID != "/repo/x" {
		t.Fatalf("register ids wrong: %+v", reg)
	}
	// Register bootstrapped an active generation 1, so Status/Rebuild work.
	st, err := s.Status(ctx, service.StatusRequest{WorktreeID: reg.WorktreeID})
	if err != nil || st.ActiveGeneration != 1 {
		t.Fatalf("register should bootstrap active gen 1: st=%+v err=%v", st, err)
	}
	rb, err := s.Rebuild(ctx, service.RebuildRequest{RepositoryID: reg.RepositoryID})
	if err != nil {
		t.Fatal(err)
	}
	if rb.Generation != 2 {
		t.Fatalf("rebuild should create generation 2, got %d", rb.Generation)
	}
}

func TestWaitFreshUnknownWorktreeErrors(t *testing.T) {
	ctx := context.Background()
	_, _, s := newServer(t)
	// An unknown worktree must not be reported "fresh" just because it has no jobs.
	if _, err := s.WaitFresh(ctx, service.WaitFreshRequest{WorktreeID: "nope", Paths: []string{"a.go"}}); err == nil {
		t.Fatal("WaitFresh on an unregistered worktree must error, not report fresh")
	}
}

// badEmbedder returns a wrong-dimension vector to exercise query validation.
type badEmbedder struct{ dims int }

func (b badEmbedder) Embed(context.Context, string) ([]float32, error) {
	return make([]float32, b.dims-1), nil // one short
}
func (b badEmbedder) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	return make([][]float32, len(texts)), nil
}
func (b badEmbedder) Dimensions() int { return b.dims }
func (b badEmbedder) Close() error    { return nil }

func TestSearchRejectsBadQueryVector(t *testing.T) {
	ctx := context.Background()
	c := newCatalog(t)
	s := service.New(c, reconcile.New(c), badEmbedder{dims: 4}, "fp", 10)
	if err := c.RegisterRepository(ctx, "r", "/r", ""); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterWorktree(ctx, "w", "r", "/w", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Search(ctx, service.SearchRequest{WorktreeID: "w", Query: "x"}); err == nil {
		t.Fatal("Search must reject a query embedding of the wrong dimension")
	}
}

func TestTraceInert(t *testing.T) {
	ctx := context.Background()
	_, _, s := newServer(t)
	resp, err := s.Trace(ctx, service.TraceRequest{WorktreeID: "w", Symbol: "X"})
	if err != nil || len(resp.Symbols) != 0 {
		t.Fatalf("trace must be inert: resp=%+v err=%v", resp, err)
	}
}
