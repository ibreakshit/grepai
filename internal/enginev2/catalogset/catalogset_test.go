package catalogset

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog/sqlite"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	_ "modernc.org/sqlite" // raw driver for the schema-too-new fixture
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

func TestRoutingIsolationAndFanout(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()

	for _, id := range []string{"/a", "/b"} {
		if err := s.Add(ctx, core.RepositoryID(id), filepath.Join(t.TempDir(), "c.db")); err != nil {
			t.Fatalf("Add %s: %v", id, err)
		}
		// Register repo + worktree + active generation (mirrors service.Register).
		if err := s.RegisterRepository(ctx, core.RepositoryID(id), id, ""); err != nil {
			t.Fatalf("RegisterRepository %s: %v", id, err)
		}
		if err := s.RegisterWorktree(ctx, core.WorktreeID(id), core.RepositoryID(id), id, 1); err != nil {
			t.Fatalf("RegisterWorktree %s: %v", id, err)
		}
		if err := s.EnsureActiveGeneration(ctx, core.RepositoryID(id), 1, "fp-"+id); err != nil {
			t.Fatalf("EnsureActiveGeneration %s: %v", id, err)
		}
	}

	// Routing: WorktreeInfo resolves through the wt->repo map to the right repo.
	if _, repo, err := s.WorktreeInfo(ctx, core.WorktreeID("/a")); err != nil || repo != "/a" {
		t.Fatalf("WorktreeInfo(/a) = %q, %v; want /a, nil", repo, err)
	}
	// Per-repo fingerprint stays isolated.
	fpB, err := s.GenerationFingerprint(ctx, core.RepositoryID("/b"), 1)
	if err != nil || fpB != "fp-/b" {
		t.Fatalf("GenerationFingerprint(/b) = %q, %v; want fp-/b", fpB, err)
	}
	// Unknown worktree errors, never falls back to another repo.
	if _, _, err := s.WorktreeInfo(ctx, core.WorktreeID("/zzz")); !errors.Is(err, ErrUnknownWorktree) {
		t.Fatalf("WorktreeInfo(unknown) = %v; want ErrUnknownWorktree", err)
	}

	// Fan-out: enqueue one job in /a and one in /b, expect both repos pending.
	job := func(id string) core.Job {
		return core.Job{WorktreeID: core.WorktreeID(id), Path: "x.go", DesiredHash: "h", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}
	}
	if err := s.UpsertJob(ctx, job("/a")); err != nil {
		t.Fatalf("UpsertJob(/a): %v", err)
	}
	if err := s.UpsertJob(ctx, job("/b")); err != nil {
		t.Fatalf("UpsertJob(/b): %v", err)
	}
	repos, err := s.RepositoriesWithPendingJobs(ctx)
	if err != nil {
		t.Fatalf("RepositoriesWithPendingJobs: %v", err)
	}
	if len(repos) != 2 {
		t.Fatalf("want 2 repos with pending jobs (fan-out), got %d: %v", len(repos), repos)
	}
}

func TestAddRejectsSchemaTooNew(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "toonew.db")
	// Create a normal catalog (schema at LatestSchemaVersion) and close it.
	s0 := New()
	if err := s0.Add(ctx, core.RepositoryID("/x"), path); err != nil {
		t.Fatalf("initial Add: %v", err)
	}
	s0.Close()
	// Stamp a schema version beyond what this binary supports.
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"INSERT INTO schema_migrations(version, applied_at) VALUES(?, datetime('now'))",
		sqlite.LatestSchemaVersion+1); err != nil {
		db.Close()
		t.Fatalf("bump version: %v", err)
	}
	db.Close()

	s := New()
	defer s.Close()
	if err := s.Add(ctx, core.RepositoryID("/x"), path); !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("Add of a too-new catalog = %v; want ErrSchemaTooNew", err)
	}
	// A rejected catalog must not be registered (no leaked handle, no routing).
	if _, gerr := s.get(core.RepositoryID("/x")); !errors.Is(gerr, ErrUnknownRepo) {
		t.Fatalf("rejected catalog must not be registered; get = %v", gerr)
	}
}

func TestChunkCacheSharesHandleAndErrorsForUnknown(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	if err := s.Add(ctx, core.RepositoryID("/a"), filepath.Join(t.TempDir(), "a.db")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	cache, err := s.ChunkCache(core.RepositoryID("/a"))
	if err != nil {
		t.Fatalf("ChunkCache: %v", err)
	}
	// The returned cache is usable: a miss is (nil, false, nil), not an error.
	if _, ok, err := cache.GetChunkVector(ctx, "nonexistent"); err != nil || ok {
		t.Fatalf("GetChunkVector miss = ok:%v err:%v; want ok:false err:nil", ok, err)
	}
	if _, err := s.ChunkCache(core.RepositoryID("/nope")); !errors.Is(err, ErrUnknownRepo) {
		t.Fatalf("ChunkCache(unknown) = %v; want ErrUnknownRepo", err)
	}
}

func TestWorktreesUnionAcrossMembers(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	for _, id := range []string{"/b", "/a"} {
		if err := s.Add(ctx, core.RepositoryID(id), filepath.Join(t.TempDir(), "c.db")); err != nil {
			t.Fatal(err)
		}
		if err := s.RegisterRepository(ctx, core.RepositoryID(id), id, ""); err != nil {
			t.Fatal(err)
		}
		if err := s.RegisterWorktree(ctx, core.WorktreeID(id), core.RepositoryID(id), id, 1); err != nil {
			t.Fatal(err)
		}
	}
	wts, err := s.Worktrees(ctx)
	if err != nil {
		t.Fatalf("Worktrees: %v", err)
	}
	if len(wts) != 2 || wts[0] != "/a" || wts[1] != "/b" {
		t.Fatalf("Worktrees union = %v, want sorted [/a /b]", wts)
	}
}
