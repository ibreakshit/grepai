// Package scheduler defines the host-wide priority scheduler contract (spec
// §5.4). Phase 4 implements one scheduler governing all repositories.
package scheduler

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// Stats is a point-in-time scheduler snapshot for observability (spec §11).
type Stats struct {
	InFlight             int
	ReservedQuery        int
	QueueDepthByPriority map[core.Priority]int
	Circuit              string // "closed" | "open" | "half-open"
}

// Scheduler admits and paces all backend work under one host-wide budget
// (invariant 2: one host, one scheduler).
type Scheduler interface {
	// Submit enqueues a job at its Priority.
	Submit(ctx context.Context, job core.Job) error
	// Run drives the scheduler loop until ctx is canceled.
	Run(ctx context.Context) error
	// Stats returns a current snapshot.
	Stats() Stats
}
