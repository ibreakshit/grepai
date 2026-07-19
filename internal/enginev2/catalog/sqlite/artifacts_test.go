package sqlite

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

func seedRepo(t *testing.T, c *Catalog, repo core.RepositoryID) {
	t.Helper()
	if err := c.RegisterRepository(context.Background(), repo, "/"+string(repo), ""); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
}

func TestArtifactCacheFingerprintScoped(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepo(t, c, "repo1")

	keyA := core.ArtifactKey{RepositoryID: "repo1", RelativePath: "a.go", SourceHash: "oid", Fingerprint: "fp-a"}
	artA := core.Artifact{ID: keyA.ArtifactID(), Key: keyA, Dimensions: 8}
	if err := c.PutArtifact(ctx, artA); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Exact key hits.
	got, ok, err := c.GetArtifact(ctx, keyA)
	if err != nil || !ok {
		t.Fatalf("GetArtifact ok=%v err=%v", ok, err)
	}
	if got.ID != artA.ID || got.Dimensions != 8 {
		t.Fatalf("artifact mismatch: %+v", got)
	}

	// A differing fingerprint must NOT hit (Gate 1).
	keyB := keyA
	keyB.Fingerprint = "fp-b"
	if _, ok, _ := c.GetArtifact(ctx, keyB); ok {
		t.Fatal("incompatible fingerprint must not produce a cache hit")
	}
	// A differing source hash must NOT hit.
	keyC := keyA
	keyC.SourceHash = "oid2"
	if _, ok, _ := c.GetArtifact(ctx, keyC); ok {
		t.Fatal("differing source hash must not produce a cache hit")
	}
}

func TestPutArtifactIsImmutable(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepo(t, c, "repo1")
	key := core.ArtifactKey{RepositoryID: "repo1", RelativePath: "a.go", SourceHash: "oid", Fingerprint: "fp"}
	art := core.Artifact{ID: key.ArtifactID(), Key: key, Dimensions: 4}
	if err := c.PutArtifact(ctx, art); err != nil {
		t.Fatalf("put1: %v", err)
	}
	// Re-putting the same immutable artifact is a no-op, not an error.
	if err := c.PutArtifact(ctx, art); err != nil {
		t.Fatalf("put2: %v", err)
	}
}

func TestChunkVectorRoundTrip(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepo(t, c, "repo1")
	vec := []float32{1, 2, 3, 4}
	if err := c.PutChunkVector(ctx, "chunk1", "repo1", "fp", vec, "content1"); err != nil {
		t.Fatalf("put chunk: %v", err)
	}
	got, ok, err := c.GetChunkVector(ctx, "chunk1")
	if err != nil || !ok {
		t.Fatalf("get chunk ok=%v err=%v", ok, err)
	}
	if len(got) != 4 || got[0] != 1 || got[3] != 4 {
		t.Fatalf("vector mismatch: %v", got)
	}
	// Missing chunk -> ok=false, no error.
	if _, ok, err := c.GetChunkVector(ctx, "nope"); ok || err != nil {
		t.Fatalf("missing chunk: ok=%v err=%v", ok, err)
	}
}
