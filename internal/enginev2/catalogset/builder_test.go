package catalogset

import (
	"context"
	"errors"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/artifacts"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

type fakeBuilder struct{ id string }

func (f fakeBuilder) Build(_ context.Context, _ artifacts.BuildRequest) (core.Artifact, artifacts.EndpointResult, error) {
	return core.Artifact{ID: core.ArtifactID(f.id)}, 0, nil
}

func TestBuilderRouterDispatchesByRepo(t *testing.T) {
	r := NewBuilderRouter()
	r.Add(core.RepositoryID("/a"), fakeBuilder{id: "A"})
	r.Add(core.RepositoryID("/b"), fakeBuilder{id: "B"})

	art, _, err := r.Build(context.Background(), artifacts.BuildRequest{Key: core.ArtifactKey{RepositoryID: "/b"}})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if art.ID != "B" {
		t.Fatalf("routed to wrong builder: got artifact %q, want B", art.ID)
	}
	if _, _, err := r.Build(context.Background(), artifacts.BuildRequest{Key: core.ArtifactKey{RepositoryID: "/x"}}); !errors.Is(err, ErrUnknownRepo) {
		t.Fatalf("unknown repo build should be ErrUnknownRepo, got %v", err)
	}
}
