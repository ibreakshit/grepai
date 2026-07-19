package sqlite

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

func seedRepoWorktree(t *testing.T, c *Catalog, repo core.RepositoryID, wt core.WorktreeID) {
	t.Helper()
	ctx := context.Background()
	if err := c.RegisterRepository(ctx, repo, "/"+string(repo), ""); err != nil {
		t.Fatalf("repo: %v", err)
	}
	if err := c.RegisterWorktree(ctx, wt, repo, "/"+string(wt), 1); err != nil {
		t.Fatalf("worktree: %v", err)
	}
}

func mkArtifact(repo core.RepositoryID, path, oid, fp string) core.Artifact {
	k := core.ArtifactKey{RepositoryID: repo, RelativePath: path, SourceHash: oid, Fingerprint: fp}
	return core.Artifact{ID: k.ArtifactID(), Key: k, Dimensions: 4}
}

func TestCommitThenResolve(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "repo1", "wt1")
	art := mkArtifact("repo1", "a.go", "oid1", "fp")
	req := core.CommitRequest{
		View:     core.ViewEntry{WorktreeID: "wt1", Path: "a.go", ArtifactID: art.ID, Generation: 1},
		Artifact: art,
	}
	job := core.Job{WorktreeID: "wt1", Path: "a.go", DesiredHash: "oid1", Generation: 1, Operation: core.OpUpsert}
	if err := c.CommitUpdate(ctx, req, job); err != nil {
		t.Fatalf("commit: %v", err)
	}
	id, ok, err := c.ResolveView(ctx, "wt1", "a.go")
	if err != nil || !ok || id != art.ID {
		t.Fatalf("resolve: id=%v ok=%v err=%v", id, ok, err)
	}
}

func TestCommitUpdateCompletesJob(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "repo1", "wt1")
	if err := c.UpsertJob(ctx, core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Priority: core.PriorityLiveChange, Operation: core.OpUpsert}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	art := mkArtifact("repo1", "a.go", "oid1", "fp")
	req := core.CommitRequest{View: core.ViewEntry{WorktreeID: "wt1", Path: "a.go", ArtifactID: art.ID, Generation: 1}, Artifact: art}
	job := core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Operation: core.OpUpsert}
	if err := c.CommitUpdate(ctx, req, job); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// The job is complete: nothing claimable remains.
	if _, ok, _ := c.ClaimNextJob(ctx, core.PriorityBootstrap); ok {
		t.Fatal("committed job must not remain claimable")
	}
}

func TestWorktreeViewIsolation(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "repo1", "wt1")
	seedRepoWorktree(t, c, "repo1", "wt2")
	art := mkArtifact("repo1", "a.go", "oid1", "fp")
	req := core.CommitRequest{View: core.ViewEntry{WorktreeID: "wt1", Path: "a.go", ArtifactID: art.ID, Generation: 1}, Artifact: art}
	if err := c.CommitUpdate(ctx, req, core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Operation: core.OpUpsert}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, ok, _ := c.ResolveView(ctx, "wt2", "a.go"); ok {
		t.Fatal("wt2 must not resolve a path only committed under wt1")
	}
}

func TestUpsertJobSupersedesOlderGeneration(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "repo1", "wt1")
	if err := c.UpsertJob(ctx, core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, DesiredHash: "old", Priority: core.PriorityLiveChange, Operation: core.OpUpsert}); err != nil {
		t.Fatalf("upsert1: %v", err)
	}
	if err := c.UpsertJob(ctx, core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 2, DesiredHash: "new", Priority: core.PriorityLiveChange, Operation: core.OpUpsert}); err != nil {
		t.Fatalf("upsert2: %v", err)
	}
	job, ok, err := c.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if job.Generation != 2 || job.DesiredHash != "new" {
		t.Fatalf("expected superseding gen 2/new, got gen %d/%q", job.Generation, job.DesiredHash)
	}
	// Only one row survived: no second claim.
	if _, ok, _ := c.ClaimNextJob(ctx, core.PriorityBootstrap); ok {
		t.Fatal("supersede must leave exactly one job per (worktree,path)")
	}
}

func TestClaimNextJobPriorityOrder(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "repo1", "wt1")
	_ = c.UpsertJob(ctx, core.Job{WorktreeID: "wt1", Path: "b.go", Generation: 1, Priority: core.PriorityBootstrap, Operation: core.OpUpsert})
	_ = c.UpsertJob(ctx, core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Priority: core.PriorityLiveChange, Operation: core.OpUpsert})
	job, ok, err := c.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if job.Priority != core.PriorityLiveChange {
		t.Fatalf("expected highest-priority (live change) first, got %v", job.Priority)
	}
	// minPriority gating: a claim at InteractiveQuery only sees priority<=1.
	if _, ok, _ := c.ClaimNextJob(ctx, core.PriorityInteractiveQuery); ok {
		t.Fatal("no job at/above interactive priority should be claimable")
	}
}
