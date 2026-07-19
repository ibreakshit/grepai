package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/embedder"
	"github.com/yoanbernabeu/grepai/internal/enginev2/reconcile"
)

// waitFreshPoll is how often WaitFresh re-checks freshness while blocking.
const waitFreshPoll = 5 * time.Millisecond

// Catalog is the durable-read surface the Server needs.
type Catalog interface {
	RegisterRepository(ctx context.Context, repo core.RepositoryID, rootPath, gitCommonDir string) error
	RegisterWorktree(ctx context.Context, wt core.WorktreeID, repo core.RepositoryID, rootPath string, regGen core.Generation) error
	WorktreeInfo(ctx context.Context, wt core.WorktreeID) (string, core.RepositoryID, error)
	ActiveGeneration(ctx context.Context, repo core.RepositoryID) (core.Generation, error)
	GenerationFingerprint(ctx context.Context, repo core.RepositoryID, gen core.Generation) (string, error)
	CreateGeneration(ctx context.Context, repo core.RepositoryID, gen core.Generation, fingerprint string) error
	SetActiveGeneration(ctx context.Context, repo core.RepositoryID, gen core.Generation) error
	SearchWorktree(ctx context.Context, wt core.WorktreeID, query []float32, limit int) ([]core.SearchHit, error)
	WorktreePendingCount(ctx context.Context, wt core.WorktreeID) (int, error)
	WorktreePathsPending(ctx context.Context, wt core.WorktreeID, paths []string) (bool, error)
	DeadLetterCount(ctx context.Context) (int, error)
	UpsertJob(ctx context.Context, job core.Job) error
}

// Reconciler computes a worktree's convergence plan (satisfied by *reconcile.Engine).
type Reconciler interface {
	Reconcile(ctx context.Context, wt core.WorktreeID) (reconcile.Plan, error)
}

// Server is the in-process implementation of Service. Query methods (Search,
// Status, WaitFresh, Trace) only read and embed the query — they never enqueue
// index jobs or reconcile (invariant 3). Only Register/Reconcile/Rebuild mutate.
type Server struct {
	cat         Catalog
	rec         Reconciler
	emb         embedder.Embedder
	fingerprint string
	limit       int
}

var _ Service = (*Server)(nil)

// New constructs a Server. fingerprint is the current indexing fingerprint used
// to bootstrap a repository's first generation at registration; it must match
// emb (provider/model/dimensions/chunker). searchLimit below 1 defaults to 10.
func New(cat Catalog, rec Reconciler, emb embedder.Embedder, fingerprint string, searchLimit int) *Server {
	if searchLimit < 1 {
		searchLimit = 10
	}
	return &Server{cat: cat, rec: rec, emb: emb, fingerprint: fingerprint, limit: searchLimit}
}

// Register registers a repository and worktree from a canonical root path and
// bootstraps an active generation 1 (so reconcile, indexing, status, and
// rebuild work immediately). This phase derives both ids from the root
// (single-worktree registration); richer git-common-dir identity derivation is
// a later refinement.
func (s *Server) Register(ctx context.Context, req RegisterRequest) (RegisterResponse, error) {
	repo := core.RepositoryID(req.Root)
	wt := core.WorktreeID(req.Root)
	if err := s.cat.RegisterRepository(ctx, repo, req.Root, ""); err != nil {
		return RegisterResponse{}, err
	}
	if err := s.cat.RegisterWorktree(ctx, wt, repo, req.Root, 1); err != nil {
		return RegisterResponse{}, err
	}
	active, err := s.cat.ActiveGeneration(ctx, repo)
	if err != nil {
		return RegisterResponse{}, err
	}
	if active == 0 {
		if err := s.cat.CreateGeneration(ctx, repo, 1, s.fingerprint); err != nil {
			return RegisterResponse{}, err
		}
		if err := s.cat.SetActiveGeneration(ctx, repo, 1); err != nil {
			return RegisterResponse{}, err
		}
	}
	return RegisterResponse{RepositoryID: repo, WorktreeID: wt}, nil
}

// Reconcile computes the worktree's plan and durably enqueues it. This is an
// administrative path — enqueueing jobs is expected (unlike query paths).
func (s *Server) Reconcile(ctx context.Context, req ReconcileRequest) (ReconcileResponse, error) {
	plan, err := s.rec.Reconcile(ctx, req.WorktreeID)
	if err != nil {
		return ReconcileResponse{}, err
	}
	for _, job := range plan.Jobs {
		if err := s.cat.UpsertJob(ctx, job); err != nil {
			return ReconcileResponse{}, err
		}
	}
	return ReconcileResponse{JobsQueued: len(plan.Jobs)}, nil
}

// Search embeds the query once and ranks the worktree's active-view chunks. It
// enqueues no work.
func (s *Server) Search(ctx context.Context, req SearchRequest) (SearchResponse, error) {
	q, err := s.emb.Embed(ctx, req.Query)
	if err != nil {
		return SearchResponse{}, err
	}
	if err := validateQueryVector(q, s.emb.Dimensions()); err != nil {
		return SearchResponse{}, err
	}
	hits, err := s.cat.SearchWorktree(ctx, req.WorktreeID, q, s.limit)
	if err != nil {
		return SearchResponse{}, err
	}
	gen, pending, err := s.genAndPending(ctx, req.WorktreeID)
	if err != nil {
		return SearchResponse{}, err
	}
	return SearchResponse{
		WorktreeID:       req.WorktreeID,
		Results:          hits,
		ActiveGeneration: gen,
		Fresh:            pending == 0,
	}, nil
}

// Trace is inert this phase: symbol extraction is deferred, so it returns no
// symbols. It contacts no backend and enqueues nothing.
func (s *Server) Trace(_ context.Context, req TraceRequest) (TraceResponse, error) {
	return TraceResponse{WorktreeID: req.WorktreeID, Symbols: nil}, nil
}

// Status reports the worktree's active generation and freshness. Read-only.
func (s *Server) Status(ctx context.Context, req StatusRequest) (StatusResponse, error) {
	gen, pending, err := s.genAndPending(ctx, req.WorktreeID)
	if err != nil {
		return StatusResponse{}, err
	}
	dl, err := s.cat.DeadLetterCount(ctx)
	if err != nil {
		return StatusResponse{}, err
	}
	return StatusResponse{ActiveGeneration: gen, Pending: pending, Fresh: pending == 0, DeadLetters: dl}, nil
}

// WaitFresh blocks until none of the requested paths has a pending job, or the
// context deadline is reached (in which case it returns Fresh:false, nil — a
// timeout is not an error). An empty Paths slice is immediately fresh.
func (s *Server) WaitFresh(ctx context.Context, req WaitFreshRequest) (WaitFreshResponse, error) {
	// Validate the worktree first: an unknown worktree must not report "fresh"
	// just because it has no jobs.
	if _, _, err := s.cat.WorktreeInfo(ctx, req.WorktreeID); err != nil {
		return WaitFreshResponse{}, err
	}
	ticker := time.NewTicker(waitFreshPoll)
	defer ticker.Stop()
	for {
		pending, err := s.cat.WorktreePathsPending(ctx, req.WorktreeID, req.Paths)
		if err != nil {
			// A deadline/cancel that lands mid-check is a timeout, not a failure.
			if ctx.Err() != nil {
				return WaitFreshResponse{Fresh: false}, nil
			}
			return WaitFreshResponse{}, err
		}
		if !pending {
			return WaitFreshResponse{Fresh: true}, nil
		}
		select {
		case <-ctx.Done():
			return WaitFreshResponse{Fresh: false}, nil
		case <-ticker.C:
		}
	}
}

// Rebuild creates a building generation handle (or cancels one). Building,
// validating, and activating the generation is deferred to the Phase 6
// migration/cutover flow (which needs generation-scoped views); this records
// only the next generation number, carrying the active fingerprint forward.
func (s *Server) Rebuild(ctx context.Context, req RebuildRequest) (RebuildResponse, error) {
	active, err := s.cat.ActiveGeneration(ctx, req.RepositoryID)
	if err != nil {
		return RebuildResponse{}, err
	}
	if req.Cancel {
		return RebuildResponse{Generation: active}, nil
	}
	fp, err := s.cat.GenerationFingerprint(ctx, req.RepositoryID, active)
	if err != nil {
		return RebuildResponse{}, err
	}
	next := active + 1
	if err := s.cat.CreateGeneration(ctx, req.RepositoryID, next, fp); err != nil {
		return RebuildResponse{}, err
	}
	return RebuildResponse{Generation: next}, nil
}

// DeadLetters lists dead-letter work. A per-worktree path listing is deferred
// (Phase 1 exposes only a host-wide count, surfaced via Status.DeadLetters);
// this returns an empty path list.
func (s *Server) DeadLetters(_ context.Context, req DeadLetterRequest) (DeadLetterResponse, error) {
	_ = req
	return DeadLetterResponse{Paths: nil}, nil
}

// validateQueryVector rejects a query embedding whose length does not match the
// embedder's dimension or that contains non-finite values — either would
// silently corrupt (empty or NaN) ranking rather than surface the error.
func validateQueryVector(q []float32, dims int) error {
	if len(q) != dims {
		return fmt.Errorf("service: query embedding has %d dimensions, want %d", len(q), dims)
	}
	for _, v := range q {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return errors.New("service: query embedding contains a non-finite value")
		}
	}
	return nil
}

// genAndPending resolves a worktree's repository, its active generation, and its
// pending-job count in one place.
func (s *Server) genAndPending(ctx context.Context, wt core.WorktreeID) (core.Generation, int, error) {
	_, repo, err := s.cat.WorktreeInfo(ctx, wt)
	if err != nil {
		return 0, 0, err
	}
	gen, err := s.cat.ActiveGeneration(ctx, repo)
	if err != nil {
		return 0, 0, err
	}
	pending, err := s.cat.WorktreePendingCount(ctx, wt)
	if err != nil {
		return 0, 0, err
	}
	return gen, pending, nil
}
