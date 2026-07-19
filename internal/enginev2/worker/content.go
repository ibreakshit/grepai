// Package worker implements the durable in-process indexing loop: claim a job,
// load its desired content, build (cache-miss-only) an immutable artifact,
// persist chunk vectors, and atomically commit the artifact + view + job. It
// classifies failures into transient (bounded retry), permanent (dead-letter),
// and superseded (dropped), and recovers a crashed worker's in-flight jobs at
// startup. Scheduling/pacing (timed backoff, budgets) belongs to Phase 4.
package worker

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// ContentLoader fetches the exact bytes for a job's desired file version.
// Production reads the git blob (clean tracked) or the on-disk file (dirty/
// untracked); tests supply fakes. A returned error is treated as transient.
type ContentLoader interface {
	Load(ctx context.Context, repo core.RepositoryID, worktreeRoot, relPath, desiredHash string) ([]byte, error)
}
