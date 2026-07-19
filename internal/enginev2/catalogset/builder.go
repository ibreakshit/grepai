package catalogset

import (
	"context"
	"fmt"
	"sync"

	"github.com/yoanbernabeu/grepai/internal/enginev2/artifacts"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// RepoBuilder builds one artifact for a single repository (satisfied by
// *artifacts.DefaultBuilder). Declared locally so the router does not import
// worker and so tests can substitute a fake.
type RepoBuilder interface {
	Build(ctx context.Context, req artifacts.BuildRequest) (core.Artifact, artifacts.EndpointResult, error)
}

// BuilderRouter dispatches a build to the per-repo builder named by
// req.Key.RepositoryID. It satisfies worker.Builder. Needed because chunk-cache
// reads (ChunkCache.GetChunkVector) carry no repo id and so cannot route through
// a single shared cache.
type BuilderRouter struct {
	mu       sync.RWMutex
	builders map[core.RepositoryID]RepoBuilder
}

// NewBuilderRouter returns an empty router.
func NewBuilderRouter() *BuilderRouter {
	return &BuilderRouter{builders: make(map[core.RepositoryID]RepoBuilder)}
}

// Add registers repo's builder.
func (r *BuilderRouter) Add(repo core.RepositoryID, b RepoBuilder) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.builders[repo] = b
}

// Build routes by req.Key.RepositoryID. An unregistered repo errors (ErrUnknownRepo).
func (r *BuilderRouter) Build(ctx context.Context, req artifacts.BuildRequest) (core.Artifact, artifacts.EndpointResult, error) {
	r.mu.RLock()
	b, ok := r.builders[req.Key.RepositoryID]
	r.mu.RUnlock()
	if !ok {
		return core.Artifact{}, 0, fmt.Errorf("%w: %q", ErrUnknownRepo, req.Key.RepositoryID)
	}
	return b.Build(ctx, req)
}
