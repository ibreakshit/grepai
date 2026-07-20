package artifacts_test

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/indexer"
	"github.com/yoanbernabeu/grepai/internal/enginev2/artifacts"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
)

// mapCache is an in-memory ChunkCache for unit tests.
type mapCache struct{ m map[string][]float32 }

func (c mapCache) GetChunkVector(_ context.Context, id string) ([]float32, bool, error) {
	v, ok := c.m[id]
	return v, ok, nil
}

// stubChunker returns a fixed chunk set with known EmbedContent, decoupling the
// mismatch test from the real chunker's internal EmbedContent construction.
type stubChunker struct{ infos []indexer.ChunkInfo }

func (s stubChunker) Chunk(_ string, _ string) []indexer.ChunkInfo { return s.infos }

func TestBuildEmbedsMissesOnlyAndReusesCache(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	ch := indexer.NewChunker(512, 50)
	key := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "h1", Fingerprint: "fp"}
	content := []byte("package main\n\nfunc main() {}\n")

	// First build: cold cache => embeds, artifact carries chunks.
	b1 := artifacts.New(ch, emb, mapCache{m: map[string][]float32{}})
	art, contacted, err := b1.Build(ctx, artifacts.BuildRequest{Key: key, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	if contacted != artifacts.EndpointSucceeded {
		t.Fatalf("cold build must report endpoint success, got %d", contacted)
	}
	if len(art.Chunks) == 0 {
		t.Fatal("expected chunks")
	}
	if art.Chunks[0].Content == "" {
		t.Fatal("built chunk must carry display content for search snippets")
	}
	if art.Dimensions != 4 {
		t.Fatalf("dims=%d", art.Dimensions)
	}
	if emb.TextsEmbedded() == 0 {
		t.Fatal("cold build should embed")
	}

	// Warm cache with the produced vectors; second build embeds nothing.
	warm := map[string][]float32{}
	for _, c := range art.Chunks {
		warm[c.ChunkID] = c.Vector
	}
	emb2 := enginetest.NewFakeEmbedder(4)
	b2 := artifacts.New(ch, emb2, mapCache{m: warm})
	art2, contacted2, err := b2.Build(ctx, artifacts.BuildRequest{Key: key, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	if contacted2 != artifacts.EndpointNotContacted {
		t.Fatalf("fully cache-served build must report no contact, got %d", contacted2)
	}
	if emb2.TextsEmbedded() != 0 {
		t.Fatalf("warm build must not embed, embedded=%d", emb2.TextsEmbedded())
	}
	if art2.ID != art.ID || len(art2.Chunks) != len(art.Chunks) {
		t.Fatal("warm build must reproduce identical artifact")
	}
}

func TestBuildRejectsCachedDimensionMismatch(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	// One chunk with a known EmbedContent; its id is derived deterministically.
	embedContent := "func main() {}"
	id := core.ChunkID("fp", embedContent)
	ch := stubChunker{infos: []indexer.ChunkInfo{{ID: "c0", FilePath: "a.go", EmbedContent: embedContent}}}
	// Cache holds a wrong-dimension vector (3 != 4) for that chunk id.
	cache := mapCache{m: map[string][]float32{id: {1, 2, 3}}}
	b := artifacts.New(ch, emb, cache)
	_, contacted, err := b.Build(ctx, artifacts.BuildRequest{Key: core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "h1", Fingerprint: "fp"}, Content: []byte("x")})
	if contacted != artifacts.EndpointNotContacted {
		t.Fatalf("a cached-vector mismatch reaches no backend, got %d", contacted)
	}
	if err != artifacts.ErrDimensionMismatch {
		t.Fatalf("want ErrDimensionMismatch, got %v", err)
	}
}

func TestBuildEmptyContentIsValidEmptyArtifact(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	ch := indexer.NewChunker(512, 50)
	key := core.ArtifactKey{RepositoryID: "r", RelativePath: "empty.go", SourceHash: "h0", Fingerprint: "fp"}
	b := artifacts.New(ch, emb, mapCache{m: map[string][]float32{}})
	art, contacted, err := b.Build(ctx, artifacts.BuildRequest{Key: key, Content: []byte("")})
	_ = contacted
	if err != nil {
		t.Fatal(err)
	}
	if len(art.Chunks) != 0 || art.Dimensions != 4 || art.ID != key.ArtifactID() {
		t.Fatalf("empty artifact wrong: %+v", art)
	}
	if emb.TextsEmbedded() != 0 {
		t.Fatal("empty content must not embed")
	}
}

func TestBuildBinaryContentSkipsEmbedding(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	ch := indexer.NewChunker(512, 50)
	b := artifacts.New(ch, emb, mapCache{m: map[string][]float32{}})

	// PNG-like bytes (NUL) and invalid UTF-8: both are binary per the v1
	// scanner heuristic and must become valid EMPTY artifacts — recorded as
	// indexed (reconcile stays idle) but never sent to the embedder.
	cases := map[string][]byte{
		"logo.png":  {0x89, 'P', 'N', 'G', 0x00, 0x1a, 0x0a, 0x00, 0xff},
		"weird.bin": {0xff, 0xfe, 0xfd, 'a', 'b', 0xc0, 0x01},
	}
	for name, content := range cases {
		key := core.ArtifactKey{RepositoryID: "r", RelativePath: name, SourceHash: "h-" + name, Fingerprint: "fp"}
		art, ep, err := b.Build(ctx, artifacts.BuildRequest{Key: key, Content: content})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(art.Chunks) != 0 || art.ID != key.ArtifactID() {
			t.Fatalf("%s: binary should build a valid empty artifact, got %+v", name, art)
		}
		if ep != artifacts.EndpointNotContacted {
			t.Fatalf("%s: binary must not contact the endpoint, got %v", name, ep)
		}
	}
	if emb.TextsEmbedded() != 0 {
		t.Fatalf("binary content must never embed; embedded %d texts", emb.TextsEmbedded())
	}
}
