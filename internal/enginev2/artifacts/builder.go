// Package artifacts defines the artifact construction contract: transform +
// cache-miss-only embedding + validation. Phase 3 implements it.
package artifacts

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// BuildRequest carries the desired artifact identity and its raw content.
type BuildRequest struct {
	Key     core.ArtifactKey
	Content []byte
}

// ArtifactBuilder transforms content, reuses compatible cached chunk vectors,
// embeds only cache misses, validates returned dimensions, and returns the
// immutable artifact ready for an atomic catalog commit.
type ArtifactBuilder interface {
	Build(ctx context.Context, req BuildRequest) (core.Artifact, error)
}
