package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

func newTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	c, err := Open(context.Background(), filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestRegistrationAndGenerations(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)

	if err := c.RegisterRepository(ctx, "repo1", "/repo1", "/repo1/.git"); err != nil {
		t.Fatalf("register repo: %v", err)
	}
	// Idempotent: registering again does not error.
	if err := c.RegisterRepository(ctx, "repo1", "/repo1", "/repo1/.git"); err != nil {
		t.Fatalf("re-register repo: %v", err)
	}
	if err := c.RegisterWorktree(ctx, "wt1", "repo1", "/repo1", 1); err != nil {
		t.Fatalf("register worktree: %v", err)
	}

	// No active generation yet.
	if g, err := c.ActiveGeneration(ctx, "repo1"); err != nil || g != 0 {
		t.Fatalf("active gen = %d, err %v; want 0, nil", g, err)
	}

	if err := c.CreateGeneration(ctx, "repo1", 1, "fp-a"); err != nil {
		t.Fatalf("create gen 1: %v", err)
	}
	if err := c.SetActiveGeneration(ctx, "repo1", 1); err != nil {
		t.Fatalf("activate gen 1: %v", err)
	}
	if g, err := c.ActiveGeneration(ctx, "repo1"); err != nil || g != 1 {
		t.Fatalf("active gen = %d, err %v; want 1, nil", g, err)
	}

	// A second generation supersedes the first as active.
	if err := c.CreateGeneration(ctx, "repo1", 2, "fp-b"); err != nil {
		t.Fatalf("create gen 2: %v", err)
	}
	if err := c.SetActiveGeneration(ctx, "repo1", 2); err != nil {
		t.Fatalf("activate gen 2: %v", err)
	}
	if g, err := c.ActiveGeneration(ctx, "repo1"); err != nil || g != core.Generation(2) {
		t.Fatalf("active gen = %d; want 2", g)
	}
}

func TestRegisterWorktreeRequiresRepository(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	// Foreign key: worktree for an unregistered repo must fail.
	if err := c.RegisterWorktree(ctx, "wt1", "ghost", "/x", 1); err == nil {
		t.Fatal("expected FK error registering worktree for unknown repository")
	}
}
