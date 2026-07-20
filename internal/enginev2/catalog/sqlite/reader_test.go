// internal/enginev2/catalog/sqlite/reader_test.go
package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

func TestWorktreeInfo(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	if err := c.RegisterRepository(ctx, "repo1", "/repo1", ""); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterWorktree(ctx, "wt1", "repo1", "/repo1/wtA", 1); err != nil {
		t.Fatal(err)
	}
	root, repo, err := c.WorktreeInfo(ctx, "wt1")
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if root != "/repo1/wtA" || repo != "repo1" {
		t.Fatalf("got root=%q repo=%q", root, repo)
	}
	if _, _, err := c.WorktreeInfo(ctx, "ghost"); !errors.Is(err, ErrNoSuchWorktree) {
		t.Fatalf("expected ErrNoSuchWorktree, got %v", err)
	}
}

func TestWorktreeIndexedHashes(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	if err := c.RegisterRepository(ctx, "repo1", "/repo1", ""); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterWorktree(ctx, "wt1", "repo1", "/repo1", 1); err != nil {
		t.Fatal(err)
	}
	// Commit two views for wt1.
	for _, f := range []struct{ path, oid string }{{"a.go", "oidA"}, {"b.go", "oidB"}} {
		key := core.ArtifactKey{RepositoryID: "repo1", RelativePath: f.path, SourceHash: f.oid, Fingerprint: "fp"}
		art := core.Artifact{ID: key.ArtifactID(), Key: key, Dimensions: 4}
		req := core.CommitRequest{View: core.ViewEntry{WorktreeID: "wt1", Path: f.path, ArtifactID: art.ID, Generation: 1}, Artifact: art}
		if err := c.CommitUpdate(ctx, req, core.Job{WorktreeID: "wt1", Path: f.path, Generation: 1, Operation: core.OpUpsert}); err != nil {
			t.Fatalf("commit %s: %v", f.path, err)
		}
	}
	hashes, err := c.WorktreeIndexedHashes(ctx, "wt1")
	if err != nil {
		t.Fatalf("hashes: %v", err)
	}
	if len(hashes) != 2 || hashes["a.go"] != "oidA" || hashes["b.go"] != "oidB" {
		t.Fatalf("got %v", hashes)
	}
	// A different worktree sees none of wt1's views (isolation).
	other, err := c.WorktreeIndexedHashes(ctx, "wt2")
	if err != nil {
		t.Fatalf("other: %v", err)
	}
	if len(other) != 0 {
		t.Fatalf("wt2 should have no indexed hashes, got %v", other)
	}
}

func TestWorktreesListsSorted(t *testing.T) {
	ctx := context.Background()
	c, err := Open(ctx, filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, id := range []string{"/b", "/a"} {
		if err := c.RegisterRepository(ctx, core.RepositoryID(id), id, ""); err != nil {
			t.Fatal(err)
		}
		if err := c.RegisterWorktree(ctx, core.WorktreeID(id), core.RepositoryID(id), id, 1); err != nil {
			t.Fatal(err)
		}
	}
	wts, err := c.Worktrees(ctx)
	if err != nil {
		t.Fatalf("Worktrees: %v", err)
	}
	if len(wts) != 2 || wts[0] != "/a" || wts[1] != "/b" {
		t.Fatalf("Worktrees = %v, want [/a /b] sorted", wts)
	}
}
