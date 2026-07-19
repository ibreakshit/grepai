// Package artifacts defines the artifact construction contract: transform +
// cache-miss-only embedding + validation. Phase 3 implements it.
package artifacts

import (
	"context"
	"errors"

	"github.com/yoanbernabeu/grepai/indexer"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/embedder"
)

// BuildRequest carries the desired artifact identity and its raw content.
type BuildRequest struct {
	Key     core.ArtifactKey
	Content []byte
}

// EndpointResult reports how the embedding backend was involved in a build, so
// the scheduler's circuit breaker reacts only to genuine endpoint health
// signals — independent of any later local (catalog) error. A successful embed
// stays EndpointSucceeded even if a subsequent commit fails locally.
type EndpointResult uint8

const (
	// EndpointNotContacted: no EmbedBatch call (empty, or fully cache-served).
	EndpointNotContacted EndpointResult = iota
	// EndpointSucceeded: an EmbedBatch call returned (endpoint reachable), even
	// if its output was then rejected as a dimension mismatch.
	EndpointSucceeded
	// EndpointFailed: an EmbedBatch call failed with an availability error.
	EndpointFailed
)

// ArtifactBuilder transforms content, reuses compatible cached chunk vectors,
// embeds only cache misses, validates returned dimensions, and returns the
// immutable artifact ready for an atomic catalog commit. The EndpointResult
// reports how the embedding backend was involved, so the scheduler's circuit
// breaker reacts only to real endpoint signals.
type ArtifactBuilder interface {
	Build(ctx context.Context, req BuildRequest) (core.Artifact, EndpointResult, error)
}

// ErrDimensionMismatch signals an embedding (or cached vector) whose length
// does not match the embedder's declared dimension. The worker treats it as a
// permanent failure — retrying cannot fix a shape mismatch.
var ErrDimensionMismatch = errors.New("artifacts: embedding dimension mismatch")

// Chunker is the transform surface the builder needs (the top-level chunker).
type Chunker interface {
	Chunk(filePath, content string) []indexer.ChunkInfo
}

// ChunkCache is the read side of the chunk vector cache (the SQLite catalog).
type ChunkCache interface {
	GetChunkVector(ctx context.Context, chunkID string) ([]float32, bool, error)
}

// DefaultBuilder implements ArtifactBuilder over a chunker, an embedder, and a
// chunk-vector cache. It embeds only cache misses and validates every vector.
type DefaultBuilder struct {
	chunker Chunker
	emb     embedder.Embedder
	cache   ChunkCache
}

// New returns a DefaultBuilder.
func New(ch Chunker, emb embedder.Embedder, cache ChunkCache) *DefaultBuilder {
	return &DefaultBuilder{chunker: ch, emb: emb, cache: cache}
}

var _ ArtifactBuilder = (*DefaultBuilder)(nil)

// Build transforms content into an immutable artifact, reusing compatible
// cached chunk vectors and embedding only the misses (spec §5.5). The
// EndpointResult reports how the embedding backend was involved.
func (b *DefaultBuilder) Build(ctx context.Context, req BuildRequest) (core.Artifact, EndpointResult, error) {
	dims := b.emb.Dimensions()
	art := core.Artifact{ID: req.Key.ArtifactID(), Key: req.Key, Dimensions: dims}

	infos := b.chunker.Chunk(req.Key.RelativePath, string(req.Content))
	if len(infos) == 0 {
		return art, EndpointNotContacted, nil // valid empty artifact
	}

	art.Chunks = make([]core.ArtifactChunk, len(infos))
	var (
		missText []string
		missByID = map[string]int{} // chunk id -> index into missText (dedup)
		missOrds = make([]int, 0, len(infos))
		idByOrd  = make([]string, len(infos))
	)
	for i, info := range infos {
		id := core.ChunkID(req.Key.Fingerprint, info.EmbedContent)
		idByOrd[i] = id
		vec, ok, err := b.cache.GetChunkVector(ctx, id)
		if err != nil {
			return core.Artifact{}, EndpointNotContacted, err // local cache read error
		}
		if ok {
			if len(vec) != dims {
				return core.Artifact{}, EndpointNotContacted, ErrDimensionMismatch
			}
			art.Chunks[i] = core.ArtifactChunk{Ordinal: i, ChunkID: id, Vector: vec, Content: info.Content, StartLine: info.StartLine, EndLine: info.EndLine}
			continue
		}
		if _, seen := missByID[id]; !seen {
			missByID[id] = len(missText)
			missText = append(missText, info.EmbedContent)
		}
		missOrds = append(missOrds, i)
	}

	if len(missText) == 0 {
		return art, EndpointNotContacted, nil // fully cache-served
	}

	vecs, err := b.emb.EmbedBatch(ctx, missText)
	if err != nil {
		return core.Artifact{}, EndpointFailed, err // availability failure
	}
	// The endpoint answered; it is reachable even if the answer is malformed.
	if len(vecs) != len(missText) {
		return core.Artifact{}, EndpointSucceeded, ErrDimensionMismatch
	}
	for _, v := range vecs {
		if len(v) != dims {
			return core.Artifact{}, EndpointSucceeded, ErrDimensionMismatch
		}
	}
	for _, ord := range missOrds {
		id := idByOrd[ord]
		vec := vecs[missByID[id]]
		info := infos[ord]
		art.Chunks[ord] = core.ArtifactChunk{Ordinal: ord, ChunkID: id, Vector: vec, Content: info.Content, StartLine: info.StartLine, EndLine: info.EndLine}
	}
	return art, EndpointSucceeded, nil
}
