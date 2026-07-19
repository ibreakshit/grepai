// Package embedder defines the v2 engine's embedding port: the minimal surface
// the artifact builder needs. Both enginetest.FakeEmbedder and the legacy
// top-level embedder implementations satisfy it structurally.
package embedder

import "context"

// Embedder converts text into fixed-dimension vectors.
type Embedder interface {
	// Embed returns the vector for one text.
	Embed(ctx context.Context, text string) ([]float32, error)
	// EmbedBatch returns vectors for many texts, index-aligned with the input.
	EmbedBatch(ctx context.Context, texts []string) ([][]float32, error)
	// Dimensions returns the vector length this embedder produces.
	Dimensions() int
	// Close releases any underlying resources.
	Close() error
}
