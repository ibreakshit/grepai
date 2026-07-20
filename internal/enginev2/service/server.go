package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
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
	EnsureActiveGeneration(ctx context.Context, repo core.RepositoryID, gen core.Generation, fingerprint string) error
	SearchWorktree(ctx context.Context, wt core.WorktreeID, query []float32, limit int) ([]core.SearchHit, error)
	WorktreePendingCount(ctx context.Context, wt core.WorktreeID) (int, error)
	WorktreePathsPending(ctx context.Context, wt core.WorktreeID, paths []string) (bool, error)
	DeadLetterCount(ctx context.Context) (int, error)
	UpsertJob(ctx context.Context, job core.Job) error
	// UpsertJobs enqueues a whole reconcile plan atomically: all or nothing, so
	// a midway failure can never leave a partially queued plan behind.
	UpsertJobs(ctx context.Context, jobs []core.Job) error
	// Worktrees lists every registered worktree (cross-repo fan-out queries).
	Worktrees(ctx context.Context) ([]core.WorktreeID, error)
	// SymbolDefinitions/SymbolEdges resolve trace data through a worktree's
	// ACTIVE view (isolation + generation scoping structural).
	SymbolDefinitions(ctx context.Context, wt core.WorktreeID, name string) ([]core.SymbolAt, error)
	SymbolEdges(ctx context.Context, wt core.WorktreeID, name string, callersOf bool) ([]core.EdgeAt, error)
	// SymbolDefinitionsBulk resolves many names in one pass (Related assembly).
	SymbolDefinitionsBulk(ctx context.Context, wt core.WorktreeID, names []string) (map[string][]core.SymbolAt, error)
	// ArtifactsMissingSymbols sizes the symbol backfill still pending for wt.
	ArtifactsMissingSymbols(ctx context.Context, wt core.WorktreeID) ([]core.MissingSymbolArtifact, error)
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
	// Bootstrap an active generation 1 atomically and idempotently, so a
	// freshly-registered repo can reconcile/index/rebuild immediately and
	// concurrent/retried Register calls are safe.
	if err := s.cat.EnsureActiveGeneration(ctx, repo, 1, s.fingerprint); err != nil {
		return RegisterResponse{}, err
	}
	return RegisterResponse{RepositoryID: repo, WorktreeID: wt}, nil
}

// Reconcile computes the worktree's plan and durably enqueues it ATOMICALLY
// (one transaction for the whole plan): an interrupted reconcile leaves either
// every desired intent queued or none — never a partial plan that a later
// retry could mistake for complete. This is an administrative path — enqueueing
// jobs is expected (unlike query paths).
func (s *Server) Reconcile(ctx context.Context, req ReconcileRequest) (ReconcileResponse, error) {
	plan, err := s.rec.Reconcile(ctx, req.WorktreeID)
	if err != nil {
		return ReconcileResponse{}, err
	}
	if req.Live {
		// Watcher-triggered: the file someone just saved indexes ahead of any
		// bootstrap backfill. UpsertJob's conflict-update carries priority, so a
		// live re-save of an already-queued file upgrades it.
		for i := range plan.Jobs {
			plan.Jobs[i].Priority = core.PriorityLiveChange
		}
	}
	if err := s.cat.UpsertJobs(ctx, plan.Jobs); err != nil {
		return ReconcileResponse{}, err
	}
	return ReconcileResponse{JobsQueued: len(plan.Jobs)}, nil
}

// Search embeds the query once and ranks the worktree's active-view chunks. It
// enqueues no work. Limit<=0 uses the server default; PathPrefix filters hits
// to paths under the prefix (the catalog is over-fetched when filtering so a
// narrow prefix still fills the limit).
func (s *Server) Search(ctx context.Context, req SearchRequest) (SearchResponse, error) {
	q, err := s.emb.Embed(ctx, req.Query)
	if err != nil {
		return SearchResponse{}, err
	}
	if err := validateQueryVector(q, s.emb.Dimensions()); err != nil {
		return SearchResponse{}, err
	}
	const maxFetch = 2000
	limit := req.Limit
	if limit <= 0 {
		limit = s.limit
	}
	if limit > maxFetch {
		limit = maxFetch // also bounds a hostile/huge -n
	}
	fetch := limit
	if req.PathPrefix != "" {
		// Over-fetch so post-filtering can still fill the limit; bounded (and
		// computed overflow-safely) so a large limit cannot wrap or force an
		// unbounded scan.
		if limit >= maxFetch/20 {
			fetch = maxFetch
		} else {
			fetch = limit * 20
		}
	}
	hits, err := s.cat.SearchWorktree(ctx, req.WorktreeID, q, fetch)
	if err != nil {
		return SearchResponse{}, err
	}
	if req.PathPrefix != "" {
		filtered := hits[:0]
		for _, h := range hits {
			if strings.HasPrefix(h.Path, req.PathPrefix) {
				filtered = append(filtered, h)
			}
		}
		hits = filtered
	}
	if len(hits) > limit {
		hits = hits[:limit]
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

// SearchAll embeds the query ONCE and fans it out across every registered
// worktree, merging by score. Scores are comparable across repos because the
// daemon indexes everything under one host-global embedder+fingerprint.
// Isolation stays structural: each per-worktree search runs against that
// worktree's own view; this is explicit multi-repo OUTPUT, tagged per hit —
// never leakage into a single repo's results. A worktree whose search fails is
// skipped and reported (quarantine-lite); it never sinks the whole query.
// Query path: embeds + reads only, never enqueues (invariant 3).
func (s *Server) SearchAll(ctx context.Context, req SearchAllRequest) (SearchAllResponse, error) {
	q, err := s.emb.Embed(ctx, req.Query)
	if err != nil {
		return SearchAllResponse{}, err
	}
	if err := validateQueryVector(q, s.emb.Dimensions()); err != nil {
		return SearchAllResponse{}, err
	}
	const maxFetch = 2000
	limit := req.Limit
	if limit <= 0 {
		limit = s.limit
	}
	if limit > maxFetch {
		limit = maxFetch
	}
	perRepo := limit // each repo can contribute at most the global limit
	if req.PathPrefix != "" {
		if perRepo >= maxFetch/20 {
			perRepo = maxFetch
		} else {
			perRepo = perRepo * 20
		}
	}

	wts, err := s.cat.Worktrees(ctx)
	if err != nil {
		return SearchAllResponse{}, err
	}
	var resp SearchAllResponse
	for _, wt := range wts {
		hits, herr := s.cat.SearchWorktree(ctx, wt, q, perRepo)
		if herr != nil {
			resp.Skipped = append(resp.Skipped, wt)
			continue
		}
		if req.PathPrefix != "" {
			filtered := hits[:0]
			for _, h := range hits {
				if strings.HasPrefix(h.Path, req.PathPrefix) {
					filtered = append(filtered, h)
				}
			}
			hits = filtered
		}
		if len(hits) > limit {
			hits = hits[:limit]
		}
		for _, h := range hits {
			resp.Results = append(resp.Results, RepoHit{Worktree: wt, Hit: h})
		}
		if pending, perr := s.cat.WorktreePendingCount(ctx, wt); perr == nil && pending > 0 {
			resp.Stale = append(resp.Stale, wt)
		}
	}
	sort.SliceStable(resp.Results, func(i, j int) bool {
		return resp.Results[i].Hit.Score > resp.Results[j].Hit.Score
	})
	if len(resp.Results) > limit {
		resp.Results = resp.Results[:limit]
	}
	return resp, nil
}

// Trace answers callers/callees/graph queries from artifact-scoped symbol data
// through the worktree's active view. Query-only: reads, never enqueues
// (invariant 3); no embedding backend involved.
func (s *Server) Trace(ctx context.Context, req TraceRequest) (TraceResponse, error) {
	if req.Symbol == "" {
		return TraceResponse{}, errors.New("trace: symbol required")
	}
	dir := req.Direction
	if dir == "" {
		dir = TraceCallers
	}
	resp := TraceResponse{WorktreeID: req.WorktreeID, Served: true, Protocol: TraceProtocolCurrent}
	defs, err := s.cat.SymbolDefinitions(ctx, req.WorktreeID, req.Symbol)
	if err != nil {
		return TraceResponse{}, err
	}
	resp.Definitions = defs

	switch dir {
	case TraceCallers, TraceCallees:
		edges, err := s.cat.SymbolEdges(ctx, req.WorktreeID, req.Symbol, dir == TraceCallers)
		if err != nil {
			return TraceResponse{}, err
		}
		resp.Edges = edges
	case TraceGraph:
		depth := req.Depth
		if depth <= 0 {
			depth = 2
		}
		if depth > 5 {
			depth = 5
		}
		edges, err := s.traceGraph(ctx, req.WorktreeID, req.Symbol, depth)
		if err != nil {
			return TraceResponse{}, err
		}
		resp.Edges = edges
	default:
		return TraceResponse{}, fmt.Errorf("trace: unknown direction %q", dir)
	}

	// Resolve definitions for every distinct edge endpoint (v1-parity: the CLI
	// mirrors v1's caller/callee symbol resolution and graph node table from
	// these). Bounded by the edge caps above; the root's defs are already in
	// Definitions and are not duplicated here.
	if len(resp.Edges) > 0 {
		seen := map[string]bool{req.Symbol: true}
		var names []string
		for _, e := range resp.Edges {
			for _, n := range []string{e.Caller, e.Callee} {
				if !seen[n] {
					seen[n] = true
					names = append(names, n)
				}
			}
		}
		related, derr := s.cat.SymbolDefinitionsBulk(ctx, req.WorktreeID, names)
		if derr != nil {
			return TraceResponse{}, derr
		}
		resp.Related = related
	}

	if missing, err := s.cat.ArtifactsMissingSymbols(ctx, req.WorktreeID); err == nil {
		resp.BackfillPending = len(missing)
	}
	return resp, nil
}

// maxGraphSymbols / maxGraphEdges strictly bound BFS breadth and response
// size: no symbol is admitted to the frontier once maxGraphSymbols names have
// been visited (admission-checked, so a single high-fanout symbol cannot blow
// past it), and expansion stops entirely at maxGraphEdges output edges. A
// truncated graph is a prefix, not an error.
const (
	maxGraphSymbols = 500
	maxGraphEdges   = 2000
)

// traceGraph BFS-expands both directions from root up to depth levels,
// deduplicating edges. Level-by-level catalog queries keep it simple; depth is
// capped by the caller and breadth by maxGraphSymbols.
func (s *Server) traceGraph(ctx context.Context, wt core.WorktreeID, root string, depth int) ([]core.EdgeAt, error) {
	type ekey struct {
		caller, callee, path string
		line                 int
	}
	seenEdge := map[ekey]bool{}
	visited := map[string]bool{root: true}
	frontier := []string{root}
	var out []core.EdgeAt
	for level := 0; level < depth && len(frontier) > 0; level++ {
		var next []string
		for _, sym := range frontier {
			if len(visited) >= maxGraphSymbols {
				return out, nil
			}
			for _, callersOf := range []bool{true, false} {
				edges, err := s.cat.SymbolEdges(ctx, wt, sym, callersOf)
				if err != nil {
					return nil, err
				}
				for _, e := range edges {
					k := ekey{e.Caller, e.Callee, e.Path, e.Line}
					if seenEdge[k] {
						continue
					}
					seenEdge[k] = true
					out = append(out, e)
					if len(out) >= maxGraphEdges {
						return out, nil
					}
					for _, n := range []string{e.Caller, e.Callee} {
						if !visited[n] && len(visited) < maxGraphSymbols {
							visited[n] = true
							next = append(next, n)
						}
					}
				}
			}
		}
		frontier = next
	}
	return out, nil
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
