// internal/enginev2/reconcile/catalogreader.go
package reconcile

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// CatalogReader is the read surface the reconciler needs from the catalog.
// The Phase 1 SQLite catalog satisfies it.
type CatalogReader interface {
	WorktreeInfo(ctx context.Context, wt core.WorktreeID) (root string, repo core.RepositoryID, err error)
	ActiveGeneration(ctx context.Context, repo core.RepositoryID) (core.Generation, error)
	WorktreeIndexedHashes(ctx context.Context, wt core.WorktreeID) (map[string]string, error)
}
