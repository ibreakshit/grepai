package worker_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/indexer"
	"github.com/yoanbernabeu/grepai/internal/enginev2/artifacts"
	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog/sqlite"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
	"github.com/yoanbernabeu/grepai/internal/enginev2/worker"
)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// newTestCatalog opens a fresh on-disk catalog in a temp dir.
func newTestCatalog(t *testing.T) *sqlite.Catalog {
	t.Helper()
	c, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// seedRepoWorktree registers repo "r", worktree "w", and an active generation 1
// with fingerprint "fp".
func seedRepoWorktree(t *testing.T, c *sqlite.Catalog) {
	t.Helper()
	ctx := context.Background()
	must(t, c.RegisterRepository(ctx, "r", ".", ""))
	must(t, c.RegisterWorktree(ctx, "w", "r", ".", 1))
	must(t, c.CreateGeneration(ctx, "r", 1, "fp"))
	must(t, c.SetActiveGeneration(ctx, "r", 1))
}

// staticLoader returns fixed content regardless of hash (unit tests).
type staticLoader struct{ content []byte }

func (l staticLoader) Load(_ context.Context, _ core.RepositoryID, _, _, _ string) ([]byte, error) {
	return l.content, nil
}

// stubBuilder returns a fixed artifact or error, exercising classification.
type stubBuilder struct {
	art core.Artifact
	err error
}

func (b stubBuilder) Build(_ context.Context, _ artifacts.BuildRequest) (core.Artifact, artifacts.EndpointResult, error) {
	if b.err != nil {
		return b.art, artifacts.EndpointFailed, b.err
	}
	return b.art, artifacts.EndpointNotContacted, nil
}

func realBuilder(emb *enginetest.FakeEmbedder, c *sqlite.Catalog) worker.Builder {
	return artifacts.New(indexer.NewChunker(512, 50), emb, c)
}

func TestProcessClaimedClassifies(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)
	w := worker.New(c, realBuilder(emb, c), staticLoader{content: []byte("func main() {}")}, worker.NoCrash, 5)
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	job, ok, err := c.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil || !ok {
		t.Fatalf("claim: %v", err)
	}
	oc, _, cause := w.ProcessClaimed(ctx, job)
	if oc != worker.OutcomeCommitted || cause != nil {
		t.Fatalf("want committed, got oc=%d cause=%v", oc, cause)
	}
	if id, ok, _ := c.ResolveView(ctx, "w", "a.go"); !ok || id == "" {
		t.Fatal("view not committed by ProcessClaimed")
	}
}

func TestProcessClaimedPermanentClassification(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)
	w := worker.New(c, stubBuilder{err: artifacts.ErrDimensionMismatch}, staticLoader{content: []byte("x")}, worker.NoCrash, 5)
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	job, ok, err := c.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil || !ok {
		t.Fatalf("claim: %v", err)
	}
	oc, _, cause := w.ProcessClaimed(ctx, job)
	if oc != worker.OutcomePermanent || cause == nil {
		t.Fatalf("want permanent+cause, got oc=%d cause=%v", oc, cause)
	}
	// ProcessClaimed must NOT dead-letter itself (the caller owns that).
	if dlc, _ := c.DeadLetterCount(ctx); dlc != 0 {
		t.Fatalf("ProcessClaimed must not dead-letter: %d", dlc)
	}
}

func TestProcessOneCommitsUpsert(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)
	w := worker.New(c, realBuilder(emb, c), staticLoader{content: []byte("func main() {}")}, worker.NoCrash, 5)
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	did, err := w.ProcessOne(ctx)
	if err != nil || !did {
		t.Fatalf("did=%v err=%v", did, err)
	}
	if id, ok, _ := c.ResolveView(ctx, "w", "a.go"); !ok || id == "" {
		t.Fatal("view not committed")
	}
	if dlc, _ := c.DeadLetterCount(ctx); dlc != 0 {
		t.Fatalf("unexpected dead-letters: %d", dlc)
	}
}

func TestTransientRetryThenSucceeds(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	emb.FailNext(2, errors.New("503 upstream")) // transient; recovers on 3rd
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)
	w := worker.New(c, realBuilder(emb, c), staticLoader{content: []byte("func main() {}")}, worker.NoCrash, 5)
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	for i := 0; i < 3; i++ {
		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
	}
	if _, ok, _ := c.ResolveView(ctx, "w", "a.go"); !ok {
		t.Fatal("expected eventual commit after transient retries")
	}
	if dlc, _ := c.DeadLetterCount(ctx); dlc != 0 {
		t.Fatalf("should not dead-letter a recoverable transient: %d", dlc)
	}
}

func TestPermanentFailureDeadLetters(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)
	// A builder that returns a permanent (dimension) error dead-letters at once.
	b := stubBuilder{err: artifacts.ErrDimensionMismatch}
	w := worker.New(c, b, staticLoader{content: []byte("x")}, worker.NoCrash, 5)
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	did, err := w.ProcessOne(ctx)
	if err != nil || !did {
		t.Fatalf("did=%v err=%v", did, err)
	}
	if dlc, _ := c.DeadLetterCount(ctx); dlc != 1 {
		t.Fatalf("permanent failure should dead-letter immediately: dlc=%d", dlc)
	}
	// No retries: the job is gone from the active queue.
	if _, ok, _ := c.ClaimNextJob(ctx, core.PriorityBootstrap); ok {
		t.Fatal("dead-lettered job must not remain claimable")
	}
}

func TestDeleteOpRemovesView(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)
	w := worker.New(c, realBuilder(emb, c), staticLoader{content: []byte("func main() {}")}, worker.NoCrash, 5)
	// Commit an upsert first.
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatal(err)
	}
	// Then a delete op at the active generation (a real delete reconciles at the
	// active generation; a non-active/rebuild delete must not touch the active view).
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "", Generation: 1, Operation: core.OpDelete, Priority: core.PriorityReconcile}))
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := c.ResolveView(ctx, "w", "a.go"); ok {
		t.Fatal("delete op should remove the view")
	}
}

// fakeExtractor returns canned symbol data (and counts calls).
type fakeExtractor struct{ calls int }

func (f *fakeExtractor) Extract(_ context.Context, relPath, _ string) ([]core.SymbolDef, []core.SymbolEdge, error) {
	f.calls++
	return []core.SymbolDef{{Name: "main", Kind: "function", Line: 1}},
		[]core.SymbolEdge{{Caller: "main", Callee: "helper", Line: 1}}, nil
}

func TestProcessCommitsSymbolsAtomically(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)
	w := worker.New(c, realBuilder(emb, c), staticLoader{content: []byte("func main() { helper() }")}, worker.NoCrash, 5)
	fx := &fakeExtractor{}
	w.SetSymbolExtractor(fx)
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	if did, err := w.ProcessOne(ctx); err != nil || !did {
		t.Fatalf("did=%v err=%v", did, err)
	}
	// Symbols landed with the commit and are trace-readable through the view.
	defs, err := c.SymbolDefinitions(ctx, "w", "main")
	if err != nil || len(defs) != 1 || defs[0].Path != "a.go" {
		t.Fatalf("symbols not committed with artifact: %+v err=%v", defs, err)
	}
	callers, err := c.SymbolEdges(ctx, "w", "helper", true)
	if err != nil || len(callers) != 1 || callers[0].Caller != "main" {
		t.Fatalf("edges not committed: %+v err=%v", callers, err)
	}
	// The artifact is no longer in the backfill worklist.
	miss, err := c.ArtifactsMissingSymbols(ctx, "w")
	if err != nil || len(miss) != 0 {
		t.Fatalf("committed artifact still needs backfill: %+v err=%v", miss, err)
	}
	if fx.calls != 1 {
		t.Fatalf("extractor calls = %d, want 1", fx.calls)
	}
}

func TestWholeFileCacheHitSkipsExtraction(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)
	fx := &fakeExtractor{}
	w := worker.New(c, realBuilder(emb, c), staticLoader{content: []byte("func main() {}")}, worker.NoCrash, 5)
	w.SetSymbolExtractor(fx)
	// First commit extracts; a re-index of identical content is a whole-file
	// cache hit whose artifact already carries symbols — no second extraction.
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatal(err)
	}
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatal(err)
	}
	if fx.calls != 1 {
		t.Fatalf("cache hit must skip extraction; extractor calls = %d", fx.calls)
	}
}

// TestCacheHitRepairsStaleSymbols guards the revert scenario: an artifact
// committed by a pre-trace binary (no extractor → symbols_version=0) can be
// RE-ACTIVATED later via a whole-file cache hit; the worker must notice the
// stale symbol version and extract, because the daemon's one-shot backfill
// will not rescan the worktree.
func TestCacheHitRepairsStaleSymbols(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)

	// Pre-trace commit: no extractor wired.
	w1 := worker.New(c, realBuilder(emb, c), staticLoader{content: []byte("func main() {}")}, worker.NoCrash, 5)
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	if _, err := w1.ProcessOne(ctx); err != nil {
		t.Fatal(err)
	}
	if miss, err := c.ArtifactsMissingSymbols(ctx, "w"); err != nil || len(miss) != 1 {
		t.Fatalf("pre-trace artifact should be missing symbols: %+v err=%v", miss, err)
	}

	// Same content re-indexed by a trace-capable worker: whole-file cache hit,
	// but the stale symbol version must trigger extraction.
	fx := &fakeExtractor{}
	w2 := worker.New(c, realBuilder(emb, c), staticLoader{content: []byte("func main() {}")}, worker.NoCrash, 5)
	w2.SetSymbolExtractor(fx)
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	if _, err := w2.ProcessOne(ctx); err != nil {
		t.Fatal(err)
	}
	if fx.calls != 1 {
		t.Fatalf("stale-symbol cache hit must extract; extractor calls = %d", fx.calls)
	}
	if miss, err := c.ArtifactsMissingSymbols(ctx, "w"); err != nil || len(miss) != 0 {
		t.Fatalf("cache-hit repair left artifact stale: %+v err=%v", miss, err)
	}
}
