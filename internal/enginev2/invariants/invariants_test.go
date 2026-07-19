// internal/enginev2/invariants/invariants_test.go
// Package invariants_test compiles the v2 release-blocking invariants (spec
// §3) against the engine interfaces. Invariants already satisfiable against
// the fakes assert real behavior; the rest are compiled scaffolds that skip
// with a pointer to the phase that implements them, so the interfaces are
// proven sufficient before production code exists (Gate 0).
package invariants_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yoanbernabeu/grepai/indexer"
	"github.com/yoanbernabeu/grepai/internal/enginev2/artifacts"
	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog"
	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog/sqlite"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
	"github.com/yoanbernabeu/grepai/internal/enginev2/reconcile"
	"github.com/yoanbernabeu/grepai/internal/enginev2/scheduler"
	"github.com/yoanbernabeu/grepai/internal/enginev2/worker"
)

// Invariant 4 (worktree isolation): a search from one worktree cannot return a
// file version that exists only in another worktree. Satisfiable against the
// fake catalog today.
func TestInvariant_WorktreeIsolation(t *testing.T) {
	var c catalog.Catalog = enginetest.NewFakeCatalog()
	ctx := context.Background()
	key := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "oid", Fingerprint: "fp"}
	art := core.Artifact{ID: key.ArtifactID(), Key: key}
	if err := c.CommitUpdate(ctx, core.CommitRequest{
		View:     core.ViewEntry{WorktreeID: "wt1", Path: "a.go", ArtifactID: art.ID, Generation: 1},
		Artifact: art,
	}, core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Operation: core.OpUpsert}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, ok, _ := c.ResolveView(ctx, "wt2", "a.go"); ok {
		t.Fatal("invariant 4 violated: wt2 resolved wt1's private view")
	}
	if id, ok, _ := c.ResolveView(ctx, "wt1", "a.go"); !ok || id != art.ID {
		t.Fatal("invariant 4: wt1 must resolve its own committed view")
	}
}

// Invariant 5 (shared immutable work): identical artifacts are stored once and
// reused. Satisfiable against the fake catalog today.
func TestInvariant_SharedImmutableWork(t *testing.T) {
	var c catalog.Catalog = enginetest.NewFakeCatalog()
	ctx := context.Background()
	key := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "oid", Fingerprint: "fp"}
	art := core.Artifact{ID: key.ArtifactID(), Key: key}
	for _, wt := range []core.WorktreeID{"wt1", "wt2"} {
		_ = c.CommitUpdate(ctx, core.CommitRequest{
			View:     core.ViewEntry{WorktreeID: wt, Path: "a.go", ArtifactID: art.ID, Generation: 1},
			Artifact: art,
		}, core.Job{WorktreeID: wt, Path: "a.go", Generation: 1, Operation: core.OpUpsert})
	}
	if _, ok, _ := c.GetArtifact(ctx, key); !ok {
		t.Fatal("invariant 5 violated: shared artifact not reusable by key")
	}
}

// Invariant 10 (fingerprint correctness): incompatible fingerprints never
// share a cache key. Satisfiable against core today.
func TestInvariant_FingerprintCorrectness(t *testing.T) {
	base := core.IndexingFingerprint{EmbedderProvider: "openai", EmbedderModel: "m", Dimensions: 1536}
	other := base
	other.Dimensions = 768
	if base.Hash() == other.Hash() {
		t.Fatal("invariant 10 violated: differing dimensions collide")
	}
	kBase := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "oid", Fingerprint: base.Hash()}
	kOther := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "oid", Fingerprint: other.Hash()}
	if kBase.ArtifactID() == kOther.ArtifactID() {
		t.Fatal("invariant 10 violated: differing fingerprints share an ArtifactID")
	}
}

// --- integration harness for the invariants that need production impls ---

func openCatalog(t *testing.T) *sqlite.Catalog {
	t.Helper()
	c, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "cat.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func newWorker(cat *sqlite.Catalog, emb *enginetest.FakeEmbedder, load worker.ContentLoader, crash worker.CrashHook) *worker.Worker {
	return worker.New(cat, artifacts.New(indexer.NewChunker(512, 50), emb, cat), load, crash, 5)
}

// seedRepoWorktree registers repo "r", worktree "w", and an active generation 1.
func seedRepoWorktree(t *testing.T, cat *sqlite.Catalog) {
	t.Helper()
	ctx := context.Background()
	mustErr(t, cat.RegisterRepository(ctx, "r", ".", ""))
	mustErr(t, cat.RegisterWorktree(ctx, "w", "r", ".", 1))
	mustErr(t, cat.CreateGeneration(ctx, "r", 1, "fp"))
	mustErr(t, cat.SetActiveGeneration(ctx, "r", 1))
}

func mustErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// diskLoader reads a file from the worktree root (git-clean content).
type diskLoader struct{}

func (diskLoader) Load(_ context.Context, _ core.RepositoryID, root, rel, _ string) ([]byte, error) {
	return os.ReadFile(filepath.Join(root, rel))
}

// staticLoader returns fixed bytes regardless of hash.
type staticLoader struct{ content []byte }

func (l staticLoader) Load(_ context.Context, _ core.RepositoryID, _, _, _ string) ([]byte, error) {
	return l.content, nil
}

// hashLoader returns content chosen by the desired hash.
type hashLoader struct{ byHash map[string][]byte }

func (l hashLoader) Load(_ context.Context, _ core.RepositoryID, _, _, desiredHash string) ([]byte, error) {
	if b, ok := l.byHash[desiredHash]; ok {
		return b, nil
	}
	return nil, errors.New("no content for hash")
}

// Invariant 1 (idle means idle) — the project's defining guarantee: after a
// repository is indexed, reconciling it unchanged produces zero jobs and issues
// zero further embedding calls. Real end-to-end: git truth → reconcile → worker
// → catalog, then reconcile again.
func TestInvariant_IdleMeansIdle(t *testing.T) {
	ctx := context.Background()
	fx := enginetest.NewGitFixture(t)
	fx.WriteFile("a.go", "package main\n\nfunc main() {}\n")
	fx.Commit("init")

	cat := openCatalog(t)
	repo := core.RepositoryID("r")
	wt := core.WorktreeID("w")
	mustErr(t, cat.RegisterRepository(ctx, repo, fx.Root(), ""))
	mustErr(t, cat.RegisterWorktree(ctx, wt, repo, fx.Root(), 1))
	mustErr(t, cat.CreateGeneration(ctx, repo, 1, "fp"))
	mustErr(t, cat.SetActiveGeneration(ctx, repo, 1))

	emb := enginetest.NewFakeEmbedder(4)
	w := newWorker(cat, emb, diskLoader{}, worker.NoCrash)
	rec := reconcile.New(cat)

	// First reconcile discovers the new file; enqueue + drain (this embeds).
	p1, err := rec.Reconcile(ctx, wt)
	mustErr(t, err)
	if len(p1.Jobs) == 0 {
		t.Fatal("first reconcile should discover the new file")
	}
	for _, j := range p1.Jobs {
		mustErr(t, cat.UpsertJob(ctx, j))
	}
	mustErr(t, w.Run(ctx))
	embedded := emb.EmbedCalls()
	if embedded == 0 {
		t.Fatal("indexing should have issued embedding calls")
	}

	// Reconciling the UNCHANGED repository must yield zero jobs and zero new embeds.
	p2, err := rec.Reconcile(ctx, wt)
	mustErr(t, err)
	if len(p2.Jobs) != 0 {
		t.Fatalf("invariant 1 violated: idle repo produced %d jobs", len(p2.Jobs))
	}
	if emb.EmbedCalls() != embedded {
		t.Fatalf("invariant 1 violated: idle reconcile issued embeddings: %d -> %d", embedded, emb.EmbedCalls())
	}
}

// Invariant 6 (atomic visibility): a failed embedding leaves the previously
// committed artifact searchable — the view never regresses to a partial state.
func TestInvariant_AtomicVisibility(t *testing.T) {
	ctx := context.Background()
	cat := openCatalog(t)
	seedRepoWorktree(t, cat)
	emb := enginetest.NewFakeEmbedder(4)
	load := hashLoader{byHash: map[string][]byte{"h1": []byte("func v1() {}"), "h2": []byte("func v2() {}")}}
	w := newWorker(cat, emb, load, worker.NoCrash)

	mustErr(t, cat.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	if did, err := w.ProcessOne(ctx); err != nil || !did {
		t.Fatalf("first index: did=%v err=%v", did, err)
	}
	v1, ok, _ := cat.ResolveView(ctx, "w", "a.go")
	if !ok {
		t.Fatal("v1 must be committed")
	}

	// The second version's embedding fails on every attempt.
	emb.SetError(errors.New("endpoint down"))
	embedsBefore := emb.EmbedCalls()
	mustErr(t, cat.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h2", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	deadLettered := false
	for i := 0; i < 12; i++ {
		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatal(err)
		}
		if n, _ := cat.DeadLetterCount(ctx); n > 0 {
			deadLettered = true
			break
		}
	}
	// Prove the failure actually occurred (else v1==v2 would pass vacuously).
	if !deadLettered {
		t.Fatal("second embedding should have failed and dead-lettered")
	}
	if n, _ := cat.DeadLetterCount(ctx); n != 1 {
		t.Fatalf("expected exactly one dead-letter, got %d", n)
	}
	if emb.EmbedCalls() <= embedsBefore {
		t.Fatalf("the failing second job never attempted an embed: %d -> %d", embedsBefore, emb.EmbedCalls())
	}
	// Invariant 6: the view still resolves the first (fully-committed) artifact.
	v2, ok, _ := cat.ResolveView(ctx, "w", "a.go")
	if !ok || v2 != v1 {
		t.Fatalf("invariant 6 violated: a failed embedding changed the searchable view (v1=%s v2=%s ok=%v)", v1, v2, ok)
	}
}

// Invariant 7 (durable progress): committed work survives a crash at a durable
// boundary — an injected crash before commit is invisible, and recovery
// reprocesses to a valid committed state.
func TestInvariant_DurableProgress(t *testing.T) {
	ctx := context.Background()
	cat := openCatalog(t)
	seedRepoWorktree(t, cat)
	emb := enginetest.NewFakeEmbedder(4)
	reg := enginetest.NewCrashRegistry()
	reg.ArmAt("after-chunks")
	load := staticLoader{content: []byte("func main() {}")}
	w := newWorker(cat, emb, load, reg.Check)

	mustErr(t, cat.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	if _, err := w.ProcessOne(ctx); !errors.Is(err, enginetest.ErrInjectedCrash) {
		t.Fatalf("expected injected crash, got %v", err)
	}
	if _, ok, _ := cat.ResolveView(ctx, "w", "a.go"); ok {
		t.Fatal("crashed (uncommitted) work must not be visible")
	}

	// Restart: recover the claimed job and reprocess.
	w2 := newWorker(cat, emb, load, worker.NoCrash)
	if _, err := w2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	mustErr(t, w2.Run(ctx))
	if _, ok, _ := cat.ResolveView(ctx, "w", "a.go"); !ok {
		t.Fatal("invariant 7 violated: committed progress lost across a crash + recovery")
	}
}

// Invariant 8 (bounded failure): a persistently unavailable backend trips the
// circuit breaker (bounded calls) and the scheduler keeps running — no restart
// loop.
func TestInvariant_BoundedFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cat := openCatalog(t)
	seedRepoWorktree(t, cat)
	emb := enginetest.NewFakeEmbedder(4)
	emb.SetError(errors.New("503 service unavailable"))
	w := newWorker(cat, emb, staticLoader{content: []byte("x")}, worker.NoCrash)

	cfg := scheduler.DefaultConfig()
	cfg.CircuitOpenAfter = 3
	cfg.MaxIndexInflight = 1
	e, err := scheduler.New(cfg, cat, w, enginetest.NewFakeClock(time.Unix(0, 0)), 1)
	mustErr(t, err)
	for _, p := range []string{"a.go", "b.go", "c.go", "d.go", "e.go"} {
		mustErr(t, cat.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: p, DesiredHash: p, Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	}
	done := make(chan struct{})
	go func() { _ = e.Run(ctx); close(done) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && e.Stats().Circuit != "open" {
		time.Sleep(2 * time.Millisecond)
	}
	if e.Stats().Circuit != "open" {
		t.Fatal("invariant 8 violated: breaker should open under a persistent outage")
	}
	// Calls are bounded, not an unbounded retry storm: at most CircuitOpenAfter
	// failures (+ in-flight) reached the backend before the breaker opened.
	callsAtOpen := emb.EmbedCalls()
	if callsAtOpen > cfg.CircuitOpenAfter+cfg.MaxIndexInflight {
		t.Fatalf("invariant 8 violated: unbounded calls before open (%d)", callsAtOpen)
	}
	// Observe a real window with fake time frozen: no further backend calls are
	// made while the breaker is open, and the scheduler keeps running.
	time.Sleep(150 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("invariant 8 violated: scheduler exited/restarted on backend failure")
	default:
	}
	if got := emb.EmbedCalls(); got != callsAtOpen {
		t.Fatalf("invariant 8 violated: backend calls continued while breaker open (%d -> %d)", callsAtOpen, got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("scheduler did not stop after cancel")
	}
}
