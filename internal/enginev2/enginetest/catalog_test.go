// internal/enginev2/enginetest/catalog_test.go
package enginetest

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

var _ catalog.Catalog = (*FakeCatalog)(nil)

func TestFakeCatalogArtifactReuse(t *testing.T) {
	c := NewFakeCatalog()
	ctx := context.Background()
	key := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "oid", Fingerprint: "fp"}
	art := core.Artifact{ID: key.ArtifactID(), Key: key, Dimensions: 8}
	req := core.CommitRequest{
		View:     core.ViewEntry{WorktreeID: "wt1", Path: "a.go", ArtifactID: art.ID, Generation: 1},
		Artifact: art,
	}
	job := core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Operation: core.OpUpsert}
	if err := c.CommitUpdate(ctx, req, job); err != nil {
		t.Fatalf("commit: %v", err)
	}
	got, ok, err := c.GetArtifact(ctx, key)
	if err != nil || !ok {
		t.Fatalf("GetArtifact ok=%v err=%v", ok, err)
	}
	if got.ID != art.ID {
		t.Fatalf("artifact id mismatch")
	}
}

func TestFakeCatalogViewIsolation(t *testing.T) {
	c := NewFakeCatalog()
	ctx := context.Background()
	key := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "oidA", Fingerprint: "fp"}
	art := core.Artifact{ID: key.ArtifactID(), Key: key}
	commit := func(wt core.WorktreeID) {
		_ = c.CommitUpdate(ctx, core.CommitRequest{
			View:     core.ViewEntry{WorktreeID: wt, Path: "a.go", ArtifactID: art.ID, Generation: 1},
			Artifact: art,
		}, core.Job{WorktreeID: wt, Path: "a.go", Generation: 1, Operation: core.OpUpsert})
	}
	commit("wt1")
	// wt2 has no view for a.go.
	if _, ok, _ := c.ResolveView(ctx, "wt2", "a.go"); ok {
		t.Fatal("wt2 must not resolve a path only committed in wt1")
	}
	if id, ok, _ := c.ResolveView(ctx, "wt1", "a.go"); !ok || id != art.ID {
		t.Fatal("wt1 must resolve its own committed view")
	}
}

func TestFakeCatalogJobPriorityClaim(t *testing.T) {
	c := NewFakeCatalog()
	ctx := context.Background()
	_ = c.UpsertJob(ctx, core.Job{WorktreeID: "wt1", Path: "b.go", Generation: 1, Priority: core.PriorityBootstrap})
	_ = c.UpsertJob(ctx, core.Job{WorktreeID: "wt1", Path: "a.go", Generation: 1, Priority: core.PriorityLiveChange})
	job, ok, err := c.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	if job.Priority != core.PriorityLiveChange {
		t.Fatalf("expected highest-priority (live change) first, got %v", job.Priority)
	}
}
