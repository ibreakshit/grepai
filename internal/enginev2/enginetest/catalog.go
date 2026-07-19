// internal/enginev2/enginetest/catalog.go
package enginetest

import (
	"context"
	"sort"
	"sync"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

type viewKey struct {
	wt   core.WorktreeID
	path string
}

// FakeCatalog is an in-memory catalog.Catalog for invariant tests. It models
// artifact reuse, per-worktree view isolation, atomic commit, and priority
// job claiming; it does not persist or enforce SQL constraints.
type FakeCatalog struct {
	mu         sync.Mutex
	artifacts  map[core.ArtifactKey]core.Artifact
	views      map[viewKey]core.ViewEntry
	generation map[core.RepositoryID]core.Generation
	jobs       []core.Job
}

// NewFakeCatalog returns an empty FakeCatalog.
func NewFakeCatalog() *FakeCatalog {
	return &FakeCatalog{
		artifacts:  map[core.ArtifactKey]core.Artifact{},
		views:      map[viewKey]core.ViewEntry{},
		generation: map[core.RepositoryID]core.Generation{},
	}
}

// SeedGeneration sets the active generation for a repository.
func (c *FakeCatalog) SeedGeneration(repo core.RepositoryID, gen core.Generation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation[repo] = gen
}

// ActiveGeneration returns the seeded active generation (0 if unset).
func (c *FakeCatalog) ActiveGeneration(ctx context.Context, repo core.RepositoryID) (core.Generation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation[repo], nil
}

// GetArtifact returns a stored artifact for a key.
func (c *FakeCatalog) GetArtifact(ctx context.Context, key core.ArtifactKey) (core.Artifact, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	a, ok := c.artifacts[key]
	return a, ok, nil
}

// ResolveView returns the artifact a worktree path currently resolves to.
func (c *FakeCatalog) ResolveView(ctx context.Context, wt core.WorktreeID, relPath string) (core.ArtifactID, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.views[viewKey{wt, relPath}]
	if !ok {
		return "", false, nil
	}
	return v.ArtifactID, true, nil
}

// CommitUpdate atomically stores the artifact and switches the worktree view.
func (c *FakeCatalog) CommitUpdate(ctx context.Context, req core.CommitRequest, job core.Job) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.artifacts[req.Artifact.Key] = req.Artifact
	c.views[viewKey{req.View.WorktreeID, req.View.Path}] = req.View
	// Mark the job complete: a committed (worktree, path) is no longer queued,
	// regardless of whether it was claimed first (crash-recovery replay may
	// commit an upserted-but-unclaimed job).
	for i, existing := range c.jobs {
		if existing.WorktreeID == job.WorktreeID && existing.Path == job.Path {
			c.jobs = append(c.jobs[:i], c.jobs[i+1:]...)
			break
		}
	}
	return nil
}

// UpsertJob records desired file state, superseding older generations for the
// same (worktree, path).
func (c *FakeCatalog) UpsertJob(ctx context.Context, job core.Job) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, existing := range c.jobs {
		if existing.WorktreeID == job.WorktreeID && existing.Path == job.Path {
			if existing.Generation <= job.Generation {
				c.jobs[i] = job
			}
			return nil
		}
	}
	c.jobs = append(c.jobs, job)
	return nil
}

// ClaimNextJob returns the highest-priority eligible job at or above
// minPriority. Lower Priority value = higher priority.
func (c *FakeCatalog) ClaimNextJob(ctx context.Context, minPriority core.Priority) (core.Job, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	eligible := make([]int, 0, len(c.jobs))
	for i, j := range c.jobs {
		if j.Priority <= minPriority {
			eligible = append(eligible, i)
		}
	}
	if len(eligible) == 0 {
		return core.Job{}, false, nil
	}
	sort.SliceStable(eligible, func(a, b int) bool {
		return c.jobs[eligible[a]].Priority < c.jobs[eligible[b]].Priority
	})
	idx := eligible[0]
	job := c.jobs[idx]
	c.jobs = append(c.jobs[:idx], c.jobs[idx+1:]...)
	return job, true, nil
}
