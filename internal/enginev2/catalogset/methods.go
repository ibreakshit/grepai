package catalogset

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/reconcile"
	"github.com/yoanbernabeu/grepai/internal/enginev2/scheduler"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
	"github.com/yoanbernabeu/grepai/internal/enginev2/worker"
)

// Compile-time proof that Set satisfies every catalog-facing interface. If any
// method is missing or mis-typed, the package fails to build.
var (
	_ scheduler.Queue         = (*Set)(nil)
	_ service.Catalog         = (*Set)(nil)
	_ worker.Catalog          = (*Set)(nil)
	_ reconcile.CatalogReader = (*Set)(nil)
	_ worker.Builder          = (*BuilderRouter)(nil)
)

// --- routed by explicit repo id ---

func (s *Set) ActiveGeneration(ctx context.Context, repo core.RepositoryID) (core.Generation, error) {
	c, err := s.get(repo)
	if err != nil {
		return 0, err
	}
	return c.ActiveGeneration(ctx, repo)
}

func (s *Set) GenerationFingerprint(ctx context.Context, repo core.RepositoryID, gen core.Generation) (string, error) {
	c, err := s.get(repo)
	if err != nil {
		return "", err
	}
	return c.GenerationFingerprint(ctx, repo, gen)
}

func (s *Set) CreateGeneration(ctx context.Context, repo core.RepositoryID, gen core.Generation, fingerprint string) error {
	c, err := s.get(repo)
	if err != nil {
		return err
	}
	return c.CreateGeneration(ctx, repo, gen, fingerprint)
}

func (s *Set) EnsureActiveGeneration(ctx context.Context, repo core.RepositoryID, gen core.Generation, fingerprint string) error {
	c, err := s.get(repo)
	if err != nil {
		return err
	}
	return c.EnsureActiveGeneration(ctx, repo, gen, fingerprint)
}

func (s *Set) ClaimNextJobInRepo(ctx context.Context, repo core.RepositoryID, minPriority core.Priority) (core.Job, bool, error) {
	c, err := s.get(repo)
	if err != nil {
		return core.Job{}, false, err
	}
	return c.ClaimNextJobInRepo(ctx, repo, minPriority)
}

func (s *Set) PutChunkVector(ctx context.Context, chunkID string, repo core.RepositoryID, fingerprint string, vec []float32, content string) error {
	c, err := s.get(repo)
	if err != nil {
		return err
	}
	return c.PutChunkVector(ctx, chunkID, repo, fingerprint, vec, content)
}

func (s *Set) RegisterRepository(ctx context.Context, repo core.RepositoryID, rootPath, gitCommonDir string) error {
	c, err := s.get(repo)
	if err != nil {
		return err
	}
	return c.RegisterRepository(ctx, repo, rootPath, gitCommonDir)
}

// SetActiveGeneration is beyond the strict interface union; the Phase B
// fingerprint-roll uses it. Routed by repo.
func (s *Set) SetActiveGeneration(ctx context.Context, repo core.RepositoryID, gen core.Generation) error {
	c, err := s.get(repo)
	if err != nil {
		return err
	}
	return c.SetActiveGeneration(ctx, repo, gen)
}

// RegisterWorktree routes by repo AND records the worktree->repo binding so
// later wt-keyed calls resolve.
func (s *Set) RegisterWorktree(ctx context.Context, wt core.WorktreeID, repo core.RepositoryID, rootPath string, regGen core.Generation) error {
	c, err := s.get(repo)
	if err != nil {
		return err
	}
	if err := c.RegisterWorktree(ctx, wt, repo, rootPath, regGen); err != nil {
		return err
	}
	s.bindWorktree(wt, repo)
	return nil
}

// --- routed by worktree id (through the wt->repo map) ---

func (s *Set) WorktreeInfo(ctx context.Context, wt core.WorktreeID) (string, core.RepositoryID, error) {
	c, err := s.getByWT(wt)
	if err != nil {
		return "", "", err
	}
	return c.WorktreeInfo(ctx, wt)
}

func (s *Set) SearchWorktree(ctx context.Context, wt core.WorktreeID, query []float32, limit int) ([]core.SearchHit, error) {
	c, err := s.getByWT(wt)
	if err != nil {
		return nil, err
	}
	return c.SearchWorktree(ctx, wt, query, limit)
}

func (s *Set) WorktreePendingCount(ctx context.Context, wt core.WorktreeID) (int, error) {
	c, err := s.getByWT(wt)
	if err != nil {
		return 0, err
	}
	return c.WorktreePendingCount(ctx, wt)
}

func (s *Set) WorktreePathsPending(ctx context.Context, wt core.WorktreeID, paths []string) (bool, error) {
	c, err := s.getByWT(wt)
	if err != nil {
		return false, err
	}
	return c.WorktreePathsPending(ctx, wt, paths)
}

func (s *Set) CommitDelete(ctx context.Context, wt core.WorktreeID, relPath string, gen core.Generation, job core.Job) error {
	c, err := s.getByWT(wt)
	if err != nil {
		return err
	}
	return c.CommitDelete(ctx, wt, relPath, gen, job)
}

func (s *Set) CurrentJob(ctx context.Context, wt core.WorktreeID, relPath string) (core.Generation, string, bool, error) {
	c, err := s.getByWT(wt)
	if err != nil {
		return 0, "", false, err
	}
	return c.CurrentJob(ctx, wt, relPath)
}

func (s *Set) WorktreeIndexedHashes(ctx context.Context, wt core.WorktreeID) (map[string]string, error) {
	c, err := s.getByWT(wt)
	if err != nil {
		return nil, err
	}
	return c.WorktreeIndexedHashes(ctx, wt)
}

// --- routed by job.WorktreeID ---

func (s *Set) UpsertJob(ctx context.Context, job core.Job) error {
	c, err := s.getByWT(job.WorktreeID)
	if err != nil {
		return err
	}
	return c.UpsertJob(ctx, job)
}

func (s *Set) DeadLetterJob(ctx context.Context, job core.Job, reason string) error {
	c, err := s.getByWT(job.WorktreeID)
	if err != nil {
		return err
	}
	return c.DeadLetterJob(ctx, job, reason)
}

func (s *Set) FailJobAttempt(ctx context.Context, job core.Job) (int, error) {
	c, err := s.getByWT(job.WorktreeID)
	if err != nil {
		return 0, err
	}
	return c.FailJobAttempt(ctx, job)
}

func (s *Set) CommitUpdate(ctx context.Context, req core.CommitRequest, job core.Job) error {
	c, err := s.getByWT(job.WorktreeID)
	if err != nil {
		return err
	}
	return c.CommitUpdate(ctx, req, job)
}

// --- routed by ArtifactKey.RepositoryID ---

func (s *Set) GetArtifact(ctx context.Context, key core.ArtifactKey) (core.Artifact, bool, error) {
	c, err := s.get(key.RepositoryID)
	if err != nil {
		return core.Artifact{}, false, err
	}
	return c.GetArtifact(ctx, key)
}

// --- fan-out aggregates ---

func (s *Set) RepositoriesWithPendingJobs(ctx context.Context) ([]core.RepositoryID, error) {
	var out []core.RepositoryID
	seen := map[core.RepositoryID]bool{}
	for _, c := range s.snapshot() {
		repos, err := c.RepositoriesWithPendingJobs(ctx)
		if err != nil {
			return nil, err
		}
		for _, r := range repos {
			if !seen[r] {
				seen[r] = true
				out = append(out, r)
			}
		}
	}
	return out, nil
}

func (s *Set) QueueDepthByPriority(ctx context.Context) (map[core.Priority]int, error) {
	out := map[core.Priority]int{}
	for _, c := range s.snapshot() {
		m, err := c.QueueDepthByPriority(ctx)
		if err != nil {
			return nil, err
		}
		for p, n := range m {
			out[p] += n
		}
	}
	return out, nil
}

func (s *Set) DeadLetterCount(ctx context.Context) (int, error) {
	total := 0
	for _, c := range s.snapshot() {
		n, err := c.DeadLetterCount(ctx)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func (s *Set) RequeueClaimedJobs(ctx context.Context) (int, error) {
	total := 0
	for _, c := range s.snapshot() {
		n, err := c.RequeueClaimedJobs(ctx)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// ClaimNextJob (host-wide, no repo arg) exists only to satisfy worker.Catalog.
// The daemon's scheduler claims per-repo via ClaimNextJobInRepo, so this is
// never called in the daemon path; implemented as a first-non-empty fan-out.
func (s *Set) ClaimNextJob(ctx context.Context, minPriority core.Priority) (core.Job, bool, error) {
	for _, c := range s.snapshot() {
		job, ok, err := c.ClaimNextJob(ctx, minPriority)
		if err != nil {
			return core.Job{}, false, err
		}
		if ok {
			return job, true, nil
		}
	}
	return core.Job{}, false, nil
}
