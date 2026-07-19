// Package reconcile defines the desired-state reconciliation contract (spec
// §5.5). Phase 2 implements Git tree/blob + fsnotify-hinted reconciliation.
package reconcile

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// Plan is the set of jobs a reconciliation determined are needed to make a
// worktree view match truth. An empty Plan means the view is already fresh
// (invariant 1: idle means idle).
type Plan struct {
	Jobs []core.Job
}

// Reconciler computes the jobs required to converge one worktree's indexed
// view to its current on-disk / Git truth.
type Reconciler interface {
	Reconcile(ctx context.Context, wt core.WorktreeID) (Plan, error)
}
