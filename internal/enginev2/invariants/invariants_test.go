// internal/enginev2/invariants/invariants_test.go
// Package invariants_test compiles the v2 release-blocking invariants (spec
// §3) against the engine interfaces. Invariants already satisfiable against
// the fakes assert real behavior; the rest are compiled scaffolds that skip
// with a pointer to the phase that implements them, so the interfaces are
// proven sufficient before production code exists (Gate 0).
package invariants_test

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
)

// Invariant 4 (worktree isolation): a search from one worktree cannot return a
// file version that exists only in another worktree. Satisfiable against the
// fake catalog today.
func TestInvariant_WorktreeIsolation(t *testing.T) {
	var c catalog.Catalog = enginetest.NewFakeCatalog()
	ctx := context.Background()
	key := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "oid", Fingerprint: "fp"}
	art := core.Artifact{ID: key.ArtifactID(), Key: key}
	if err := c.CommitUpdate(ctx, core.CommitRequest{
		View:     core.ViewEntry{WorktreeID: "wt1", Path: "a.go", ArtifactID: art.ID, Generation: 1},
		Artifact: art,
	}, core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Operation: core.OpUpsert}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, ok, _ := c.ResolveView(ctx, "wt2", "a.go"); ok {
		t.Fatal("invariant 4 violated: wt2 resolved wt1's private view")
	}
}

// Invariant 5 (shared immutable work): identical artifacts are stored once and
// reused. Satisfiable against the fake catalog today.
func TestInvariant_SharedImmutableWork(t *testing.T) {
	var c catalog.Catalog = enginetest.NewFakeCatalog()
	ctx := context.Background()
	key := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "oid", Fingerprint: "fp"}
	art := core.Artifact{ID: key.ArtifactID(), Key: key}
	for _, wt := range []core.WorktreeID{"wt1", "wt2"} {
		_ = c.CommitUpdate(ctx, core.CommitRequest{
			View:     core.ViewEntry{WorktreeID: wt, Path: "a.go", ArtifactID: art.ID, Generation: 1},
			Artifact: art,
		}, core.Job{WorktreeID: wt, Path: "a.go", Generation: 1, Operation: core.OpUpsert})
	}
	if _, ok, _ := c.GetArtifact(ctx, key); !ok {
		t.Fatal("invariant 5 violated: shared artifact not reusable by key")
	}
}

// Invariant 10 (fingerprint correctness): incompatible fingerprints never
// share a cache key. Satisfiable against core today.
func TestInvariant_FingerprintCorrectness(t *testing.T) {
	base := core.IndexingFingerprint{EmbedderProvider: "openai", EmbedderModel: "m", Dimensions: 1536}
	other := base
	other.Dimensions = 768
	if base.Hash() == other.Hash() {
		t.Fatal("invariant 10 violated: differing dimensions collide")
	}
	kBase := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "oid", Fingerprint: base.Hash()}
	kOther := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "oid", Fingerprint: other.Hash()}
	if kBase.ArtifactID() == kOther.ArtifactID() {
		t.Fatal("invariant 10 violated: differing fingerprints share an ArtifactID")
	}
}

// The following invariants require production implementations. They are
// compiled scaffolds proving the interfaces are sufficient to express them.

// Invariant 1 (idle means idle): startup reconciliation of an unchanged
// repository issues zero embedding calls. Implemented in Phase 2 + Phase 3.
func TestInvariant_IdleMeansIdle(t *testing.T) {
	t.Skip("Phase 2/3: reconciler + artifact indexer required")
	// Scaffold shape: reconcile a fixture with no changes, assert
	// enginetest.FakeEmbedder.EmbedCalls() == 0.
	_ = enginetest.NewFakeEmbedder(8)
}

// Invariant 6 (atomic visibility): a failed embedding leaves the prior
// artifact searchable. Implemented in Phase 3.
func TestInvariant_AtomicVisibility(t *testing.T) {
	t.Skip("Phase 3: artifact indexer commit protocol required")
}

// Invariant 7 (durable progress): committed work survives a crash at any
// durable-state boundary. Implemented in Phase 1 + Phase 3 (uses CrashRegistry).
func TestInvariant_DurableProgress(t *testing.T) {
	t.Skip("Phase 1/3: SQLite catalog + crash-point wiring required")
	_ = enginetest.NewCrashRegistry()
}

// Invariant 8 (bounded failure): unavailable backends produce bounded calls,
// no restart loop. Implemented in Phase 4 (scheduler + circuit breaker).
func TestInvariant_BoundedFailure(t *testing.T) {
	t.Skip("Phase 4: scheduler circuit breaker required")
}
