// Package catalogset serves many per-repo SQLite catalogs from one process. Set
// implements the union of the catalog-facing interfaces (scheduler.Queue,
// service.Catalog, worker.Catalog, reconcile.CatalogReader) by routing each
// single-repo call to the owning catalog and fanning out the host-wide
// aggregates. Cross-repo isolation is structural: an op for an unregistered
// repo/worktree errors, it never touches another repo's catalog.
package catalogset

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/yoanbernabeu/grepai/internal/enginev2/artifacts"
	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog/sqlite"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

var (
	// ErrUnknownRepo is returned for an op targeting an unregistered repository.
	ErrUnknownRepo = errors.New("catalogset: unknown repository")
	// ErrUnknownWorktree is returned for an op whose worktree is not bound to a
	// registered repository.
	ErrUnknownWorktree = errors.New("catalogset: unknown worktree")
	// ErrSchemaTooNew is returned by Add when a catalog's schema is newer than
	// this binary supports (guard against corruption from an older daemon).
	ErrSchemaTooNew = errors.New("catalogset: catalog schema newer than supported")
)

// Set is the live map of registered repositories to their open catalogs, plus
// the worktree->repo routing map. Safe for concurrent use.
type Set struct {
	mu    sync.RWMutex
	cats  map[core.RepositoryID]*sqlite.Catalog
	wtToR map[core.WorktreeID]core.RepositoryID
}

// New returns an empty Set.
func New() *Set {
	return &Set{
		cats:  make(map[core.RepositoryID]*sqlite.Catalog),
		wtToR: make(map[core.WorktreeID]core.RepositoryID),
	}
}

// Add opens the repository's catalog at catalogPath and registers it. It applies
// the schema guard: a catalog stamped newer than sqlite.LatestSchemaVersion is
// closed and rejected with ErrSchemaTooNew (the daemon skips + logs it).
func (s *Set) Add(ctx context.Context, repo core.RepositoryID, catalogPath string) error {
	cat, err := sqlite.Open(ctx, catalogPath)
	if err != nil {
		return err
	}
	v, err := cat.SchemaVersion(ctx)
	if err != nil {
		_ = cat.Close()
		return err
	}
	if v > sqlite.LatestSchemaVersion {
		_ = cat.Close()
		return fmt.Errorf("%w: catalog %q at v%d, binary supports v%d", ErrSchemaTooNew, catalogPath, v, sqlite.LatestSchemaVersion)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cats[repo]; ok {
		_ = cat.Close() // already registered; keep the first handle, drop this one
		return nil
	}
	s.cats[repo] = cat
	return nil
}

// Close closes every catalog. The first error is returned; all are attempted.
func (s *Set) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var first error
	for repo, c := range s.cats {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
		delete(s.cats, repo)
	}
	return first
}

// bindWorktree records that wt belongs to repo (called from RegisterWorktree and
// at daemon startup rehydration). Idempotent.
func (s *Set) bindWorktree(wt core.WorktreeID, repo core.RepositoryID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wtToR[wt] = repo
}

// ChunkCache returns repo's catalog as an artifacts.ChunkCache so the daemon
// can build one artifacts.DefaultBuilder per repo that shares Set's
// already-open handle — no second Open of the same SQLite file, and no extra
// handle for anyone else to close (Set.Close owns it). Errors with
// ErrUnknownRepo for an unregistered repo.
func (s *Set) ChunkCache(repo core.RepositoryID) (artifacts.ChunkCache, error) {
	c, err := s.get(repo)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// get returns the catalog for repo, or ErrUnknownRepo.
func (s *Set) get(repo core.RepositoryID) (*sqlite.Catalog, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.cats[repo]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownRepo, repo)
	}
	return c, nil
}

// getByWT resolves wt->repo then returns that repo's catalog.
func (s *Set) getByWT(wt core.WorktreeID) (*sqlite.Catalog, error) {
	s.mu.RLock()
	repo, ok := s.wtToR[wt]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownWorktree, wt)
	}
	return s.get(repo)
}

// snapshot returns the current catalogs for fan-out reads without holding the
// lock during delegation.
func (s *Set) snapshot() []*sqlite.Catalog {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*sqlite.Catalog, 0, len(s.cats))
	for _, c := range s.cats {
		out = append(out, c)
	}
	return out
}
