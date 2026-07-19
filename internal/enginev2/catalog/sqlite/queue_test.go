package sqlite

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

func TestClaimNextJobInRepoAndPending(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "rA", "wA")
	seedRepoWorktree(t, c, "rB", "wB")
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "wA", Path: "a.go", DesiredHash: "h", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "wB", Path: "b.go", DesiredHash: "h", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))

	repos, err := c.RepositoriesWithPendingJobs(ctx)
	if err != nil || len(repos) != 2 {
		t.Fatalf("repos=%v err=%v", repos, err)
	}
	// Claiming in repo rA yields only rA's job.
	job, ok, err := c.ClaimNextJobInRepo(ctx, "rA", core.PriorityBootstrap)
	if err != nil || !ok || job.WorktreeID != "wA" {
		t.Fatalf("claim rA: job=%+v ok=%v err=%v", job, ok, err)
	}
	// rA now has no unclaimed jobs; rB still does.
	if _, ok, _ := c.ClaimNextJobInRepo(ctx, "rA", core.PriorityBootstrap); ok {
		t.Fatal("rA should be drained")
	}
	depth, err := c.QueueDepthByPriority(ctx)
	if err != nil || depth[core.PriorityReconcile] != 1 {
		t.Fatalf("depth=%v err=%v", depth, err)
	}
}

func TestClaimNextJobInRepoPriorityOrder(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "rA", "wA")
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "wA", Path: "boot.go", DesiredHash: "h", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityBootstrap}))
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "wA", Path: "live.go", DesiredHash: "h", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityLiveChange}))
	job, ok, err := c.ClaimNextJobInRepo(ctx, "rA", core.PriorityBootstrap)
	if err != nil || !ok || job.Priority != core.PriorityLiveChange {
		t.Fatalf("expected live-change first, got %+v ok=%v err=%v", job, ok, err)
	}
}
