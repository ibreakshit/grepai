// Package catalog defines the durable source-of-truth contract for the v2
// engine. Phase 1 implements it over SQLite (modernc.org/sqlite, WAL mode).
package catalog

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// Catalog is the durable catalog contract. All methods are safe for
// concurrent readers; writes flow through a single serialized writer.
type Catalog interface {
	// ActiveGeneration returns the generation currently serving searches for a
	// repository (invariant 12: search availability).
	ActiveGeneration(ctx context.Context, repo core.RepositoryID) (core.Generation, error)

	// GetArtifact returns an existing immutable artifact for a key, if present
	// (invariant 5: shared immutable work; invariant 10: fingerprint correctness).
	GetArtifact(ctx context.Context, key core.ArtifactKey) (core.Artifact, bool, error)

	// ResolveView returns the artifact a worktree path currently resolves to
	// (invariant 4: worktree isolation).
	ResolveView(ctx context.Context, wt core.WorktreeID, relPath string) (core.ArtifactID, bool, error)

	// CommitUpdate atomically stores any missing artifact, switches the
	// worktree view, and marks the job complete in one transaction
	// (invariant 6: atomic visibility; invariant 7: durable progress).
	CommitUpdate(ctx context.Context, req core.CommitRequest, job core.Job) error

	// UpsertJob records desired file state, superseding older generations for
	// the same (worktree, path).
	UpsertJob(ctx context.Context, job core.Job) error

	// ClaimNextJob atomically claims the highest-priority eligible job at or
	// above minPriority, or returns ok=false if none are ready.
	ClaimNextJob(ctx context.Context, minPriority core.Priority) (core.Job, bool, error)
}
