package catalogset

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

func TestAddOpensCatalogAndGetRoutes(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	p := filepath.Join(t.TempDir(), "a.db")
	if err := s.Add(ctx, core.RepositoryID("/a"), p); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := s.get(core.RepositoryID("/a")); err != nil {
		t.Fatalf("get registered repo: %v", err)
	}
	if _, err := s.get(core.RepositoryID("/nope")); !errors.Is(err, ErrUnknownRepo) {
		t.Fatalf("get unknown repo should be ErrUnknownRepo, got %v", err)
	}
}

func TestGetByWTRequiresBinding(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	if err := s.Add(ctx, core.RepositoryID("/a"), filepath.Join(t.TempDir(), "a.db")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := s.getByWT(core.WorktreeID("/a")); !errors.Is(err, ErrUnknownWorktree) {
		t.Fatalf("unbound worktree should be ErrUnknownWorktree, got %v", err)
	}
	s.bindWorktree(core.WorktreeID("/a"), core.RepositoryID("/a"))
	if _, err := s.getByWT(core.WorktreeID("/a")); err != nil {
		t.Fatalf("bound worktree should route: %v", err)
	}
}
