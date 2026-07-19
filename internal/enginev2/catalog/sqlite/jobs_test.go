package sqlite

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// seedGeneration creates and activates a generation with a fingerprint on top
// of the repo/worktree registered by seedRepoWorktree (defined in views_test.go).
func seedGeneration(t *testing.T, c *Catalog, repo core.RepositoryID, gen core.Generation, fp string) {
	t.Helper()
	ctx := context.Background()
	if err := c.CreateGeneration(ctx, repo, gen, fp); err != nil {
		t.Fatal(err)
	}
	if err := c.SetActiveGeneration(ctx, repo, gen); err != nil {
		t.Fatal(err)
	}
}

func TestCommitUpdatePersistsArtifactChunks(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "r", "w")
	seedGeneration(t, c, "r", 1, "fp")

	art := core.Artifact{
		ID:         core.ArtifactID("art-1"),
		Key:        core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "h1", Fingerprint: "fp"},
		Dimensions: 3,
	}
	// Pre-store chunk vectors (cache warming), as the worker does before commit.
	if err := c.PutChunkVector(ctx, "ch-0", "r", "fp", []float32{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	req := core.CommitRequest{
		View:     core.ViewEntry{WorktreeID: "w", Path: "a.go", ArtifactID: "art-1", Generation: 1},
		Artifact: art,
		Chunks:   []core.ArtifactChunk{{Ordinal: 0, ChunkID: "ch-0", Vector: []float32{1, 2, 3}}},
	}
	job := core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert}
	if err := c.CommitUpdate(ctx, req, job); err != nil {
		t.Fatal(err)
	}
	ids, err := c.ArtifactChunkIDs(ctx, "art-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "ch-0" {
		t.Fatalf("artifact_chunks mapping wrong: %v", ids)
	}
}

func TestCommitDeleteRemovesView(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "r", "w")
	seedGeneration(t, c, "r", 1, "fp")
	// Establish a view at gen 1.
	req := core.CommitRequest{
		View:     core.ViewEntry{WorktreeID: "w", Path: "a.go", ArtifactID: "art-1", Generation: 1},
		Artifact: core.Artifact{ID: "art-1", Key: core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "h1", Fingerprint: "fp"}, Dimensions: 3},
	}
	if err := c.CommitUpdate(ctx, req, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert}); err != nil {
		t.Fatal(err)
	}
	// A delete job must exist for CommitDelete to fulfill (mirrors the worker,
	// which only calls CommitDelete for a claimed OpDelete job).
	del := core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "", Generation: 1, Operation: core.OpDelete, Priority: core.PriorityReconcile}
	if err := c.UpsertJob(ctx, del); err != nil {
		t.Fatal(err)
	}
	if err := c.CommitDelete(ctx, "w", "a.go", 1, del); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := c.ResolveView(ctx, "w", "a.go"); err != nil || ok {
		t.Fatalf("view should be gone: ok=%v err=%v", ok, err)
	}
}

// A commit for a non-active generation (a controlled rebuild building a new
// generation) must NOT switch the active worktree view — the active generation
// stays queryable during a rebuild (invariant 12, Codex Phase 5 review #1).
func TestCommitAtNonActiveGenerationDoesNotSwitchView(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "r", "w")
	seedGeneration(t, c, "r", 1, "fp") // gen 1 active

	artA := mkArtifact("r", "a.go", "h1", "fp")
	must(t, c.CommitUpdate(ctx,
		core.CommitRequest{View: core.ViewEntry{WorktreeID: "w", Path: "a.go", ArtifactID: artA.ID, Generation: 1}, Artifact: artA},
		core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert}))
	if id, ok, _ := c.ResolveView(ctx, "w", "a.go"); !ok || id != artA.ID {
		t.Fatal("gen 1 view should be set")
	}

	// gen 2 is created but NOT activated (a building rebuild generation).
	must(t, c.CreateGeneration(ctx, "r", 2, "fp2"))
	artB := mkArtifact("r", "a.go", "h2", "fp2")
	must(t, c.CommitUpdate(ctx,
		core.CommitRequest{View: core.ViewEntry{WorktreeID: "w", Path: "a.go", ArtifactID: artB.ID, Generation: 2}, Artifact: artB},
		core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h2", Generation: 2, Operation: core.OpUpsert}))
	// The active view must still resolve to the gen-1 artifact.
	if id, _, _ := c.ResolveView(ctx, "w", "a.go"); id != artA.ID {
		t.Fatalf("active view must stay gen 1 during rebuild: got %s want %s", id, artA.ID)
	}
}

// A delete for a non-active generation (a controlled rebuild) completes its job
// but must NOT remove the active worktree view (invariant 12; symmetric to the
// upsert guard — Codex Phase 5 re-review).
func TestCommitDeleteAtNonActiveGenerationKeepsView(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "r", "w")
	seedGeneration(t, c, "r", 1, "fp") // gen 1 active
	artA := mkArtifact("r", "a.go", "h1", "fp")
	must(t, c.CommitUpdate(ctx,
		core.CommitRequest{View: core.ViewEntry{WorktreeID: "w", Path: "a.go", ArtifactID: artA.ID, Generation: 1}, Artifact: artA},
		core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert}))

	// A gen-2 (building, non-active) delete job for the same path.
	must(t, c.CreateGeneration(ctx, "r", 2, "fp2"))
	del := core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "", Generation: 2, Operation: core.OpDelete}
	must(t, c.UpsertJob(ctx, del))
	must(t, c.CommitDelete(ctx, "w", "a.go", 2, del))
	// The active gen-1 view must survive the non-active delete.
	if id, ok, _ := c.ResolveView(ctx, "w", "a.go"); !ok || id != artA.ID {
		t.Fatalf("active view must survive a non-active-generation delete: ok=%v id=%s", ok, id)
	}
	// The delete job itself must still have been consumed (its build/work is done).
	if _, _, ok, _ := c.CurrentJob(ctx, "w", "a.go"); ok {
		t.Fatal("the non-active delete job should have been removed")
	}
}

func TestCommitDeleteSupersededKeepsNewerUpsert(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "r", "w")
	seedGeneration(t, c, "r", 1, "fp")
	// A delete job is claimed, then the file is re-created (same generation,
	// non-empty desired hash) — the row is overwritten to an upsert.
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "", Generation: 1, Operation: core.OpDelete, Priority: core.PriorityReconcile}))
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h9", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	// The stale delete must not drop the newer upsert.
	del := core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "", Generation: 1, Operation: core.OpDelete}
	if err := c.CommitDelete(ctx, "w", "a.go", 1, del); err != nil {
		t.Fatal(err)
	}
	gen, hash, ok, err := c.CurrentJob(ctx, "w", "a.go")
	if err != nil || !ok || gen != 1 || hash != "h9" {
		t.Fatalf("newer upsert must survive: gen=%d hash=%q ok=%v err=%v", gen, hash, ok, err)
	}
}

func TestDeadLetterSupersededKeepsNewerUpsert(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "r", "w")
	seedGeneration(t, c, "r", 1, "fp")
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	// Re-save supersedes h1 with h2 (same generation).
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h2", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	// A dead-letter for the stale h1 must not drop h2 or record a dead-letter.
	if err := c.DeadLetterJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1}, "stale permanent"); err != nil {
		t.Fatal(err)
	}
	if dlc, _ := c.DeadLetterCount(ctx); dlc != 0 {
		t.Fatalf("stale dead-letter must not record: dlc=%d", dlc)
	}
	_, hash, ok, err := c.CurrentJob(ctx, "w", "a.go")
	if err != nil || !ok || hash != "h2" {
		t.Fatalf("newer upsert must survive dead-letter: hash=%q ok=%v err=%v", hash, ok, err)
	}
}

func TestFailJobAttemptSupersededIsNoop(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "r", "w")
	seedGeneration(t, c, "r", 1, "fp")
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h2", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	// A stale h1 transient failure must not charge an attempt against h2.
	att, err := c.FailJobAttempt(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if att != 0 {
		t.Fatalf("stale attempt must report the untouched newer count 0, got %d", att)
	}
	job, ok, err := c.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil || !ok || job.DesiredHash != "h2" || job.Attempts != 0 {
		t.Fatalf("h2 must be intact: hash=%q attempts=%d ok=%v err=%v", job.DesiredHash, job.Attempts, ok, err)
	}
}

func TestDeadLetterAndRequeueAndAttempt(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "r", "w")
	seedGeneration(t, c, "r", 1, "fp")
	job := core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}
	if err := c.UpsertJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	// Claim it (marks claimed=1), then simulate a crash by requeueing.
	if _, ok, err := c.ClaimNextJob(ctx, core.PriorityBootstrap); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	n, err := c.RequeueClaimedJobs(ctx)
	if err != nil || n != 1 {
		t.Fatalf("requeue n=%d err=%v", n, err)
	}
	// It must be claimable again after requeue.
	claimed, ok, err := c.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil || !ok {
		t.Fatalf("reclaim: ok=%v err=%v", ok, err)
	}
	// Record a transient attempt: attempts increments, becomes claimable again.
	att, err := c.FailJobAttempt(ctx, claimed)
	if err != nil || att != 1 {
		t.Fatalf("attempt=%d err=%v", att, err)
	}
	// Dead-letter it.
	if _, _, err := c.ClaimNextJob(ctx, core.PriorityBootstrap); err != nil {
		t.Fatal(err)
	}
	if err := c.DeadLetterJob(ctx, claimed, "permanent: bad dims"); err != nil {
		t.Fatal(err)
	}
	dlc, err := c.DeadLetterCount(ctx)
	if err != nil || dlc != 1 {
		t.Fatalf("dead-letter count=%d err=%v", dlc, err)
	}
	if _, ok, _ := c.ClaimNextJob(ctx, core.PriorityBootstrap); ok {
		t.Fatal("dead-lettered job should not be claimable")
	}
}

func TestGenerationFingerprintAndCurrentJob(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "r", "w")
	seedGeneration(t, c, "r", 1, "fp") // generation 1 with fingerprint "fp"
	fp, err := c.GenerationFingerprint(ctx, "r", 1)
	if err != nil || fp != "fp" {
		t.Fatalf("fingerprint=%q err=%v", fp, err)
	}
	if _, _, ok, _ := c.CurrentJob(ctx, "w", "a.go"); ok {
		t.Fatal("no job yet => not ok")
	}
	if err := c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h7", Generation: 7, Operation: core.OpUpsert, Priority: core.PriorityReconcile}); err != nil {
		t.Fatal(err)
	}
	g, hash, ok, err := c.CurrentJob(ctx, "w", "a.go")
	if err != nil || !ok || g != 7 || hash != "h7" {
		t.Fatalf("current job gen=%d hash=%q ok=%v err=%v", g, hash, ok, err)
	}
}
