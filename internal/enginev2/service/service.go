// Package service defines the daemon's request-oriented API (spec §7),
// independent of the wire transport. Phase 5 implements it against the
// catalog, reconciler, and scheduler. CLI and MCP become thin clients.
package service

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// RegisterRequest registers a repository or worktree with the daemon.
type RegisterRequest struct {
	Root string // canonical filesystem path
}

// RegisterResponse returns the assigned identities.
type RegisterResponse struct {
	RepositoryID core.RepositoryID
	WorktreeID   core.WorktreeID
}

// ReconcileRequest requests reconciliation of one worktree. Live marks the
// resulting jobs as live-change priority (watcher-triggered edits index ahead
// of background backfills).
type ReconcileRequest struct {
	WorktreeID core.WorktreeID
	Live       bool
}

// ReconcileResponse reports how many jobs reconciliation produced.
type ReconcileResponse struct {
	JobsQueued int
}

// SearchRequest issues a query within an explicit worktree view (invariant 3:
// MCP is read/query oriented — this never launches a full scan).
type SearchRequest struct {
	WorktreeID core.WorktreeID
	Query      string
	// Limit caps returned results; <=0 uses the server default.
	Limit int
	// PathPrefix restricts results to paths under the given prefix ("" = all).
	PathPrefix string
}

// SearchResponse carries worktree-scoped ranked results plus freshness metadata.
type SearchResponse struct {
	WorktreeID       core.WorktreeID
	Results          []core.SearchHit
	ActiveGeneration core.Generation
	Fresh            bool // true when the worktree has no pending index jobs
}

// SearchAllRequest searches every registered worktree (cross-repo fan-out).
type SearchAllRequest struct {
	Query      string
	Limit      int    // global cap across all repos; <=0 uses the server default
	PathPrefix string // repo-relative prefix applied within every repo ("" = all)
}

// RepoHit is one cross-repo result, tagged with its owning worktree.
type RepoHit struct {
	Worktree core.WorktreeID
	Hit      core.SearchHit
}

// SearchAllResponse carries merged, score-ranked results across worktrees.
// Skipped lists worktrees whose search failed (quarantine-lite: one broken
// catalog does not take the whole query down — but the caller is TOLD).
// Stale lists worktrees that served results while having pending index jobs.
type SearchAllResponse struct {
	Results []RepoHit
	Stale   []core.WorktreeID
	Skipped []core.WorktreeID
}

// Trace directions.
const (
	TraceCallers = "callers"
	TraceCallees = "callees"
	TraceGraph   = "graph"
)

// TraceRequest issues a call-graph query within an explicit worktree view.
// Direction is one of TraceCallers/TraceCallees/TraceGraph; Depth applies to
// graph only (default 2, capped at 5).
type TraceRequest struct {
	WorktreeID core.WorktreeID
	Symbol     string
	Direction  string
	Depth      int
}

// TraceResponse carries the symbol's definitions and the call edges in the
// requested direction, all resolved through the worktree's ACTIVE view.
// BackfillPending>0 means symbol coverage is still building for this worktree
// (artifacts committed before extraction existed) — results may be incomplete.
type TraceResponse struct {
	WorktreeID      core.WorktreeID
	Definitions     []core.SymbolAt
	Edges           []core.EdgeAt
	BackfillPending int
	// Served is the capability marker: a trace-capable daemon always sets it.
	// Pre-trace daemons registered the method but answered inertly; their JSON
	// decodes here with Served=false, which the CLI turns into a loud
	// restart-the-daemon error instead of a silent false-negative "no symbols".
	Served bool
}

// StatusRequest asks for indexing/freshness status.
type StatusRequest struct {
	WorktreeID core.WorktreeID
}

// StatusResponse reports indexing/freshness status for a worktree.
type StatusResponse struct {
	ActiveGeneration core.Generation
	Pending          int  // active index jobs for the worktree
	Fresh            bool // true when Pending == 0
	DeadLetters      int  // host-wide dead-letter count (coarse this phase)
}

// WaitFreshRequest waits for selected paths to become fresh with a deadline.
type WaitFreshRequest struct {
	WorktreeID core.WorktreeID
	Paths      []string
}

// WaitFreshResponse reports whether all requested paths became fresh.
type WaitFreshResponse struct {
	Fresh bool
}

// RebuildRequest starts or cancels a controlled generation rebuild.
type RebuildRequest struct {
	RepositoryID core.RepositoryID
	Cancel       bool
}

// RebuildResponse reports the rebuild generation.
type RebuildResponse struct {
	Generation core.Generation
}

// DeadLetterRequest inspects, retries, or clears dead-letter work.
type DeadLetterRequest struct {
	WorktreeID core.WorktreeID
}

// DeadLetterResponse lists dead-letter paths.
type DeadLetterResponse struct {
	Paths []string
}

// Service is the daemon's transport-independent API surface.
type Service interface {
	Register(ctx context.Context, req RegisterRequest) (RegisterResponse, error)
	Reconcile(ctx context.Context, req ReconcileRequest) (ReconcileResponse, error)
	Search(ctx context.Context, req SearchRequest) (SearchResponse, error)
	SearchAll(ctx context.Context, req SearchAllRequest) (SearchAllResponse, error)
	Trace(ctx context.Context, req TraceRequest) (TraceResponse, error)
	Status(ctx context.Context, req StatusRequest) (StatusResponse, error)
	WaitFresh(ctx context.Context, req WaitFreshRequest) (WaitFreshResponse, error)
	Rebuild(ctx context.Context, req RebuildRequest) (RebuildResponse, error)
	DeadLetters(ctx context.Context, req DeadLetterRequest) (DeadLetterResponse, error)
}
