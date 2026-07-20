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
	if err := c.PutChunkVector(ctx, chID, repo, "fp", vec, content); err != nil {
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

// Register is idempotent and retry-safe: repeated calls neither error nor
// create a second generation (Codex Phase 5 re-review).
func TestRegisterIdempotent(t *testing.T) {
	ctx := context.Background()
	_, _, s := newServer(t)
	for i := 0; i < 3; i++ {
		if _, err := s.Register(ctx, service.RegisterRequest{Root: "/repo/y"}); err != nil {
			t.Fatalf("register %d must be idempotent: %v", i, err)
		}
	}
	st, err := s.Status(ctx, service.StatusRequest{WorktreeID: "/repo/y"})
	if err != nil || st.ActiveGeneration != 1 {
		t.Fatalf("repeated register must leave exactly active gen 1: st=%+v err=%v", st, err)
	}
	// Rebuild must produce generation 2 (proving only one generation 1 exists).
	rb, err := s.Rebuild(ctx, service.RebuildRequest{RepositoryID: "/repo/y"})
	if err != nil || rb.Generation != 2 {
		t.Fatalf("rebuild after repeated register: gen=%d err=%v", rb.Generation, err)
	}
}

// Register must recover a half-bootstrapped repository (a 'building' generation
// 1 that was created but never activated) by activating it — proving the fix,
// not just the already-active fast path (Codex Phase 5 re-review).
func TestRegisterActivatesExistingBuildingGeneration(t *testing.T) {
	ctx := context.Background()
	c, _, s := newServer(t)
	// Simulate a crash after CreateGeneration but before activation.
	if err := c.RegisterRepository(ctx, "/repo/z", "/repo/z", ""); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateGeneration(ctx, "/repo/z", 1, "fp"); err != nil { // 'building', not active
		t.Fatal(err)
	}
	if active, _ := c.ActiveGeneration(ctx, "/repo/z"); active != 0 {
		t.Fatalf("precondition: no active generation yet, got %d", active)
	}
	if _, err := s.Register(ctx, service.RegisterRequest{Root: "/repo/z"}); err != nil {
		t.Fatal(err)
	}
	if active, _ := c.ActiveGeneration(ctx, "/repo/z"); active != 1 {
		t.Fatalf("Register must activate the existing building generation 1, got %d", active)
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

// seedSymbols commits an artifact for (wt, path) carrying the given symbol
// defs/edges (extracted=true), reusing the repo/generation setup from
// seedWorktreeArtifact's conventions.
func seedSymbols(t *testing.T, c *sqlite.Catalog, repo core.RepositoryID, wt core.WorktreeID, path string, defs []core.SymbolDef, edges []core.SymbolEdge) {
	t.Helper()
	ctx := context.Background()
	_ = c.RegisterRepository(ctx, repo, "/"+string(repo), "")
	_ = c.RegisterWorktree(ctx, wt, repo, "/"+string(wt), 1)
	_ = c.CreateGeneration(ctx, repo, 1, "fp")
	if err := c.SetActiveGeneration(ctx, repo, 1); err != nil {
		t.Fatal(err)
	}
	key := core.ArtifactKey{RepositoryID: repo, RelativePath: path, SourceHash: "h-" + path, Fingerprint: "fp"}
	art := core.Artifact{ID: key.ArtifactID(), Key: key, Dimensions: 4}
	req := core.CommitRequest{
		View:             core.ViewEntry{WorktreeID: wt, Path: path, ArtifactID: art.ID, Generation: 1},
		Artifact:         art,
		Symbols:          defs,
		SymbolEdges:      edges,
		SymbolsExtracted: true,
	}
	if err := c.CommitUpdate(ctx, req, core.Job{WorktreeID: wt, Path: path, DesiredHash: "h-" + path, Generation: 1, Operation: core.OpUpsert}); err != nil {
		t.Fatal(err)
	}
}

func TestTraceCallersCalleesAndGraph(t *testing.T) {
	ctx := context.Background()
	c, _, s := newServer(t)
	// a.go: HandleReq -> Validate ; b.go: Validate -> log ; c.go defines log.
	seedSymbols(t, c, "r", "w", "a.go",
		[]core.SymbolDef{{Name: "HandleReq", Kind: "function", Line: 3}},
		[]core.SymbolEdge{{Caller: "HandleReq", Callee: "Validate", Line: 5}})
	seedSymbols(t, c, "r", "w", "b.go",
		[]core.SymbolDef{{Name: "Validate", Kind: "function", Line: 2}},
		[]core.SymbolEdge{{Caller: "Validate", Callee: "log", Line: 4}})
	seedSymbols(t, c, "r", "w", "c.go",
		[]core.SymbolDef{{Name: "log", Kind: "function", Line: 1}}, nil)

	callers, err := s.Trace(ctx, service.TraceRequest{WorktreeID: "w", Symbol: "Validate", Direction: service.TraceCallers})
	if err != nil {
		t.Fatal(err)
	}
	if len(callers.Definitions) != 1 || callers.Definitions[0].Path != "b.go" {
		t.Fatalf("Validate definitions wrong: %+v", callers.Definitions)
	}
	if len(callers.Edges) != 1 || callers.Edges[0].Caller != "HandleReq" || callers.Edges[0].Path != "a.go" {
		t.Fatalf("callers wrong: %+v", callers.Edges)
	}
	callees, err := s.Trace(ctx, service.TraceRequest{WorktreeID: "w", Symbol: "Validate", Direction: service.TraceCallees})
	if err != nil {
		t.Fatal(err)
	}
	if len(callees.Edges) != 1 || callees.Edges[0].Callee != "log" {
		t.Fatalf("callees wrong: %+v", callees.Edges)
	}
	// Graph depth 2 from Validate reaches both edges.
	graph, err := s.Trace(ctx, service.TraceRequest{WorktreeID: "w", Symbol: "Validate", Direction: service.TraceGraph, Depth: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.Edges) != 2 {
		t.Fatalf("graph should have 2 edges, got %+v", graph.Edges)
	}
	if graph.BackfillPending != 0 {
		t.Fatalf("no backfill expected, got %d", graph.BackfillPending)
	}
	// Empty symbol is a loud error; unknown direction too.
	if _, err := s.Trace(ctx, service.TraceRequest{WorktreeID: "w"}); err == nil {
		t.Fatal("empty symbol must error")
	}
	if _, err := s.Trace(ctx, service.TraceRequest{WorktreeID: "w", Symbol: "X", Direction: "sideways"}); err == nil {
		t.Fatal("unknown direction must error")
	}
}

func TestTraceIsWorktreeIsolated(t *testing.T) {
	ctx := context.Background()
	c, _, s := newServer(t)
	seedSymbols(t, c, "ra", "wa", "a.go", []core.SymbolDef{{Name: "Dup", Kind: "function", Line: 1}},
		[]core.SymbolEdge{{Caller: "Other", Callee: "Dup", Line: 2}})
	seedSymbols(t, c, "rb", "wb", "b.go", []core.SymbolDef{{Name: "Dup", Kind: "function", Line: 9}}, nil)
	resp, err := s.Trace(ctx, service.TraceRequest{WorktreeID: "wb", Symbol: "Dup", Direction: service.TraceCallers})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Definitions) != 1 || resp.Definitions[0].Path != "b.go" {
		t.Fatalf("wb must only see its own Dup: %+v", resp.Definitions)
	}
	if len(resp.Edges) != 0 {
		t.Fatalf("wa's edges must not leak into wb: %+v", resp.Edges)
	}
}

func TestSearchAllMergesAcrossWorktreesWithTags(t *testing.T) {
	ctx := context.Background()
	c, emb, s := newServer(t)
	// Two repos/worktrees in one catalog; FakeEmbedder vectors are content-
	// derived, so querying with wa's exact content ranks wa's hit first.
	seedWorktreeArtifact(t, c, emb, "ra", "wa", "a.go", "alpha content")
	seedWorktreeArtifact(t, c, emb, "rb", "wb", "b.go", "beta material")

	resp, err := s.SearchAll(ctx, service.SearchAllRequest{Query: "alpha content", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) != 2 {
		t.Fatalf("want hits from both worktrees, got %d: %+v", len(resp.Results), resp.Results)
	}
	// Tagged correctly and ranked: the exact-match repo first.
	if resp.Results[0].Worktree != "wa" || resp.Results[0].Hit.Path != "a.go" {
		t.Fatalf("top hit should be wa/a.go, got %s/%s", resp.Results[0].Worktree, resp.Results[0].Hit.Path)
	}
	if resp.Results[1].Worktree != "wb" {
		t.Fatalf("second hit should be tagged wb, got %s", resp.Results[1].Worktree)
	}
	if resp.Results[0].Hit.Score <= resp.Results[1].Hit.Score {
		t.Fatalf("results not score-ordered: %f <= %f", resp.Results[0].Hit.Score, resp.Results[1].Hit.Score)
	}
	if len(resp.Skipped) != 0 || len(resp.Stale) != 0 {
		t.Fatalf("unexpected skipped/stale: %+v / %+v", resp.Skipped, resp.Stale)
	}
}

func TestSearchAllLimitAndStale(t *testing.T) {
	ctx := context.Background()
	c, emb, s := newServer(t)
	seedWorktreeArtifact(t, c, emb, "ra", "wa", "a.go", "gamma one")
	seedWorktreeArtifact(t, c, emb, "rb", "wb", "b.go", "gamma two")
	// Pending job in wb -> it must be reported stale.
	if err := c.UpsertJob(ctx, core.Job{WorktreeID: "wb", Path: "pending.go", DesiredHash: "h", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}); err != nil {
		t.Fatal(err)
	}
	resp, err := s.SearchAll(ctx, service.SearchAllRequest{Query: "gamma", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("limit=1 must cap merged results, got %d", len(resp.Results))
	}
	if len(resp.Stale) != 1 || resp.Stale[0] != "wb" {
		t.Fatalf("wb should be reported stale, got %+v", resp.Stale)
	}
}
