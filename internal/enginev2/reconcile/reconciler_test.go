// internal/enginev2/reconcile/reconciler_test.go
package reconcile

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

var _ Reconciler = (*Engine)(nil)

// fakeReader implements CatalogReader for unit tests.
type fakeReader struct {
	root    string
	repo    core.RepositoryID
	gen     core.Generation
	indexed map[string]string
}

func (f *fakeReader) WorktreeInfo(_ context.Context, _ core.WorktreeID) (string, core.RepositoryID, error) {
	return f.root, f.repo, nil
}
func (f *fakeReader) ActiveGeneration(_ context.Context, _ core.RepositoryID) (core.Generation, error) {
	return f.gen, nil
}
func (f *fakeReader) WorktreeIndexedHashes(_ context.Context, _ core.WorktreeID) (map[string]string, error) {
	return f.indexed, nil
}

func plan(t *testing.T, indexed, desired map[string]string, gen core.Generation) Plan {
	t.Helper()
	r := NewWithTruth(&fakeReader{root: "/x", repo: "repo1", gen: gen, indexed: indexed},
		func(context.Context, string) (map[string]string, error) { return desired, nil })
	p, err := r.Reconcile(context.Background(), "wt1")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return p
}

func TestReconcileUnchangedYieldsNoJobs(t *testing.T) {
	m := map[string]string{"a.go": "o1", "b.go": "o2"}
	p := plan(t, m, m, 1)
	if len(p.Jobs) != 0 {
		t.Fatalf("unchanged reconcile must yield 0 jobs, got %d: %+v", len(p.Jobs), p.Jobs)
	}
}

func TestReconcileEmitsUpsertsAndDeletes(t *testing.T) {
	indexed := map[string]string{"keep.go": "o1", "change.go": "old", "gone.go": "o3"}
	desired := map[string]string{"keep.go": "o1", "change.go": "new", "added.go": "o4"}
	p := plan(t, indexed, desired, 7)

	byPath := map[string]core.Job{}
	for _, j := range p.Jobs {
		byPath[j.Path] = j
	}
	if _, ok := byPath["keep.go"]; ok {
		t.Fatal("unchanged file must not produce a job")
	}
	if j := byPath["change.go"]; j.Operation != core.OpUpsert || j.DesiredHash != "new" || j.Generation != 7 || j.Priority != core.PriorityReconcile {
		t.Fatalf("change.go job wrong: %+v", j)
	}
	if j := byPath["added.go"]; j.Operation != core.OpUpsert || j.DesiredHash != "o4" {
		t.Fatalf("added.go job wrong: %+v", j)
	}
	if j := byPath["gone.go"]; j.Operation != core.OpDelete {
		t.Fatalf("gone.go should be a delete, got %+v", j)
	}
	if len(p.Jobs) != 3 {
		t.Fatalf("want 3 jobs, got %d", len(p.Jobs))
	}
}

func TestReconcileDefaultsGenerationToOne(t *testing.T) {
	// No active generation (0) -> jobs target generation 1 (bootstrap).
	p := plan(t, map[string]string{}, map[string]string{"a.go": "o1"}, 0)
	if len(p.Jobs) != 1 || p.Jobs[0].Generation != 1 {
		t.Fatalf("expected 1 job at generation 1, got %+v", p.Jobs)
	}
}

func TestReconcileJobsDeterministicOrder(t *testing.T) {
	indexed := map[string]string{"z.go": "x", "a.go": "x"}
	desired := map[string]string{}
	p := plan(t, indexed, desired, 1)
	// Both deletes, sorted by path: a.go before z.go.
	if len(p.Jobs) != 2 || p.Jobs[0].Path != "a.go" || p.Jobs[1].Path != "z.go" {
		t.Fatalf("jobs not deterministically ordered: %+v", p.Jobs)
	}
}
