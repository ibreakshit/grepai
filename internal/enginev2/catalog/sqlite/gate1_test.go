package sqlite

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// Gate 1: a transaction rollback leaves the prior view searchable.
func TestGate1_RollbackPreservesPriorView(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c, "repo1", "wt1")

	// Commit v1 normally.
	v1 := mkArtifact("repo1", "a.go", "oid1", "fp")
	if err := c.CommitUpdate(ctx,
		core.CommitRequest{View: core.ViewEntry{WorktreeID: "wt1", Path: "a.go", ArtifactID: v1.ID, Generation: 1}, Artifact: v1},
		core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Operation: core.OpUpsert}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}

	// Attempt v2 inside a transaction that we force to roll back (simulating a
	// crash after the writes but before commit).
	v2 := mkArtifact("repo1", "a.go", "oid2", "fp")
	c.writeMu.Lock()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		c.writeMu.Unlock()
		t.Fatalf("begin: %v", err)
	}
	if err := commitUpdateTx(ctx,
		tx,
		core.CommitRequest{View: core.ViewEntry{WorktreeID: "wt1", Path: "a.go", ArtifactID: v2.ID, Generation: 2}, Artifact: v2},
		core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 2, Operation: core.OpUpsert}); err != nil {
		t.Fatalf("commitUpdateTx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	c.writeMu.Unlock()

	// The view must still resolve to v1, not the rolled-back v2.
	id, ok, err := c.ResolveView(ctx, "wt1", "a.go")
	if err != nil || !ok {
		t.Fatalf("resolve after rollback: ok=%v err=%v", ok, err)
	}
	if id != v1.ID {
		t.Fatalf("view = %v after rollback, want prior v1 %v", id, v1.ID)
	}
}

// Gate 1: incompatible fingerprints never produce a cache hit.
func TestGate1_NoCrossFingerprintCacheHit(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepo(t, c, "repo1")
	base := core.ArtifactKey{RepositoryID: "repo1", RelativePath: "a.go", SourceHash: "oid", Fingerprint: "fp-1536"}
	if err := c.PutArtifact(ctx, core.Artifact{ID: base.ArtifactID(), Key: base, Dimensions: 1536}); err != nil {
		t.Fatalf("put: %v", err)
	}
	other := base
	other.Fingerprint = "fp-768"
	if _, ok, _ := c.GetArtifact(ctx, other); ok {
		t.Fatal("Gate 1: differing fingerprint returned a cache hit")
	}
}

// Gate 1: repository and worktree isolation hold under concurrent writers
// (run the package with -race to exercise the single-writer serialization).
func TestGate1_ConcurrentWorktreeIsolation(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	if err := c.RegisterRepository(ctx, "repo1", "/repo1", ""); err != nil {
		t.Fatalf("repo: %v", err)
	}
	const n = 8
	for i := 0; i < n; i++ {
		if err := c.RegisterWorktree(ctx, core.WorktreeID(fmt.Sprintf("wt%d", i)), "repo1", "/w", 1); err != nil {
			t.Fatalf("worktree %d: %v", i, err)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wt := core.WorktreeID(fmt.Sprintf("wt%d", i))
			// Each worktree commits its own distinct artifact for the same path.
			art := mkArtifact("repo1", "shared.go", fmt.Sprintf("oid%d", i), "fp")
			_ = c.CommitUpdate(ctx,
				core.CommitRequest{View: core.ViewEntry{WorktreeID: wt, Path: "shared.go", ArtifactID: art.ID, Generation: 1}, Artifact: art},
				core.Job{WorktreeID: wt, Path: "shared.go", Generation: 1, Operation: core.OpUpsert})
		}(i)
	}
	wg.Wait()

	// Each worktree resolves to its own artifact — no cross-contamination.
	for i := 0; i < n; i++ {
		wt := core.WorktreeID(fmt.Sprintf("wt%d", i))
		want := mkArtifact("repo1", "shared.go", fmt.Sprintf("oid%d", i), "fp").ID
		id, ok, err := c.ResolveView(ctx, wt, "shared.go")
		if err != nil || !ok {
			t.Fatalf("resolve %s: ok=%v err=%v", wt, ok, err)
		}
		if id != want {
			t.Fatalf("worktree %s resolved %v, want its own %v", wt, id, want)
		}
	}
}
