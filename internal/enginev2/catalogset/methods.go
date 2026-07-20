package catalogset

import (
	"context"
	"fmt"
	"sort"

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

// ClaimNextJobInRepo follows the quarantine-lite contract: a member whose claim
// fails is reported via OnAggregateError and answers "no job" instead of
// erroring — the scheduler's round-robin pass aborts on a claim error, so a
// fail-fast here would let one broken catalog stall every healthy repo. The
// skipped repo's jobs stay durably queued and are retried on later passes.
// An UNREGISTERED repo still errors (that is a routing bug, not a bad catalog).
func (s *Set) ClaimNextJobInRepo(ctx context.Context, repo core.RepositoryID, minPriority core.Priority) (core.Job, bool, error) {
	c, err := s.get(repo)
	if err != nil {
		return core.Job{}, false, err
	}
	job, ok, err := c.ClaimNextJobInRepo(ctx, repo, minPriority)
	if err != nil {
		s.reportErr(repo, err)
		return core.Job{}, false, nil
	}
	return job, ok, nil
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

// RollWorktreeGeneration is beyond the strict interface union; the daemon's
// fingerprint rollover uses it (atomic clear-view+clear-jobs+activate). Routed
// by worktree.
func (s *Set) RollWorktreeGeneration(ctx context.Context, wt core.WorktreeID, repo core.RepositoryID, gen core.Generation) error {
	c, err := s.getByWT(wt)
	if err != nil {
		return err
	}
	return c.RollWorktreeGeneration(ctx, wt, repo, gen)
}

func (s *Set) WorktreeIndexedHashes(ctx context.Context, wt core.WorktreeID) (map[string]string, error) {
	c, err := s.getByWT(wt)
	if err != nil {
		return nil, err
	}
	return c.WorktreeIndexedHashes(ctx, wt)
}

// SymbolDefinitions resolves trace definitions through the worktree's view.
func (s *Set) SymbolDefinitions(ctx context.Context, wt core.WorktreeID, name string) ([]core.SymbolAt, error) {
	c, err := s.getByWT(wt)
	if err != nil {
		return nil, err
	}
	return c.SymbolDefinitions(ctx, wt, name)
}

// SymbolEdges resolves trace call edges through the worktree's view.
func (s *Set) SymbolEdges(ctx context.Context, wt core.WorktreeID, name string, callersOf bool) ([]core.EdgeAt, error) {
	c, err := s.getByWT(wt)
	if err != nil {
		return nil, err
	}
	return c.SymbolEdges(ctx, wt, name, callersOf)
}

// ArtifactsMissingSymbols lists the worktree's pending symbol backfill.
func (s *Set) ArtifactsMissingSymbols(ctx context.Context, wt core.WorktreeID) ([]core.MissingSymbolArtifact, error) {
	c, err := s.getByWT(wt)
	if err != nil {
		return nil, err
	}
	return c.ArtifactsMissingSymbols(ctx, wt)
}

// BackfillSymbols writes backfilled symbol data for an artifact in wt's repo
// (beyond the strict interface union; the daemon's backfill uses it — the
// worktree parameter exists solely to route to the owning catalog).
func (s *Set) BackfillSymbols(ctx context.Context, wt core.WorktreeID, artifactID core.ArtifactID, defs []core.SymbolDef, edges []core.SymbolEdge) error {
	c, err := s.getByWT(wt)
	if err != nil {
		return err
	}
	return c.PutArtifactSymbols(ctx, artifactID, defs, edges)
}

// --- routed by job.WorktreeID ---

func (s *Set) UpsertJob(ctx context.Context, job core.Job) error {
	c, err := s.getByWT(job.WorktreeID)
	if err != nil {
		return err
	}
	return c.UpsertJob(ctx, job)
}

// UpsertJobs routes an atomic whole-plan enqueue. A reconcile plan is always
// for a single worktree; mixed-worktree batches are rejected (they could not be
// atomic across separate per-repo catalogs).
func (s *Set) UpsertJobs(ctx context.Context, jobs []core.Job) error {
	if len(jobs) == 0 {
		return nil
	}
	wt := jobs[0].WorktreeID
	for _, j := range jobs[1:] {
		if j.WorktreeID != wt {
			return fmt.Errorf("catalogset: UpsertJobs batch spans worktrees %q and %q", wt, j.WorktreeID)
		}
	}
	c, err := s.getByWT(wt)
	if err != nil {
		return err
	}
	return c.UpsertJobs(ctx, jobs)
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

func (s *Set) ArtifactSymbolsCurrent(ctx context.Context, key core.ArtifactKey) (bool, error) {
	c, err := s.get(key.RepositoryID)
	if err != nil {
		return false, err
	}
	return c.ArtifactSymbolsCurrent(ctx, key)
}

// --- fan-out aggregates ---
//
// Aggregates SKIP a member whose read fails (reporting it via OnAggregateError)
// rather than failing the whole call: the scheduler consults these before every
// claim, so one broken catalog must not stall indexing for every healthy repo
// (quarantine-lite; the spec's Phase B obligation). Results are best-effort
// partial when a member errors. snapshot() returns members sorted by repo id —
// the scheduler's round-robin resume point assumes ascending order.

func (s *Set) RepositoriesWithPendingJobs(ctx context.Context) ([]core.RepositoryID, error) {
	var out []core.RepositoryID
	seen := map[core.RepositoryID]bool{}
	for _, m := range s.snapshot() {
		repos, err := m.cat.RepositoriesWithPendingJobs(ctx)
		if err != nil {
			s.reportErr(m.repo, err)
			continue
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
	for _, m := range s.snapshot() {
		depths, err := m.cat.QueueDepthByPriority(ctx)
		if err != nil {
			s.reportErr(m.repo, err)
			continue
		}
		for p, n := range depths {
			out[p] += n
		}
	}
	return out, nil
}

func (s *Set) DeadLetterCount(ctx context.Context) (int, error) {
	total := 0
	for _, m := range s.snapshot() {
		n, err := m.cat.DeadLetterCount(ctx)
		if err != nil {
			s.reportErr(m.repo, err)
			continue
		}
		total += n
	}
	return total, nil
}

// Worktrees unions every member's registered worktrees (sorted). A failing
// member is skipped + reported (quarantine-lite): one broken catalog must not
// take cross-repo search down for the healthy ones.
func (s *Set) Worktrees(ctx context.Context) ([]core.WorktreeID, error) {
	var out []core.WorktreeID
	for _, m := range s.snapshot() {
		wts, err := m.cat.Worktrees(ctx)
		if err != nil {
			s.reportErr(m.repo, err)
			continue
		}
		out = append(out, wts...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func (s *Set) RequeueClaimedJobs(ctx context.Context) (int, error) {
	total := 0
	for _, m := range s.snapshot() {
		n, err := m.cat.RequeueClaimedJobs(ctx)
		if err != nil {
			s.reportErr(m.repo, err)
			continue
		}
		total += n
	}
	return total, nil
}

// ClaimNextJob (host-wide, no repo arg) exists only to satisfy worker.Catalog.
// The daemon's scheduler claims per-repo via ClaimNextJobInRepo, so this is
// never called in the daemon path; implemented as a first-non-empty fan-out.
//
// WARNING: do NOT drive a worker.Worker.Run loop with a Set. This fan-out has
// no cross-repo fairness (a perpetually busy catalog starves the rest). The
// scheduler is the only supported multi-repo drainer.
func (s *Set) ClaimNextJob(ctx context.Context, minPriority core.Priority) (core.Job, bool, error) {
	for _, m := range s.snapshot() {
		job, ok, err := m.cat.ClaimNextJob(ctx, minPriority)
		if err != nil {
			return core.Job{}, false, err
		}
		if ok {
			return job, true, nil
		}
	}
	return core.Job{}, false, nil
}
