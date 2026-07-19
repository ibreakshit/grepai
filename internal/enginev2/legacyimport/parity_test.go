package legacyimport_test

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
	"github.com/yoanbernabeu/grepai/internal/enginev2/legacyimport"
)

func TestTopKOverlap(t *testing.T) {
	cases := []struct {
		name string
		a, b []string
		k    int
		want float64
	}{
		{"jaccard-half", []string{"a", "b", "c"}, []string{"a", "b", "d"}, 3, 2.0 / 4.0}, // ∩=2 ∪=4
		{"identical", []string{"a", "b", "c"}, []string{"a", "b", "c"}, 3, 1.0},
		{"disjoint", []string{"a", "b", "c"}, []string{"x", "y", "z"}, 3, 0.0},
		{"both-empty", nil, nil, 3, 1.0},
		{"one-empty", []string{"a"}, nil, 3, 0.0},
		{"cardinality-penalized", []string{"a"}, []string{"a", "b", "c"}, 3, 1.0 / 3.0}, // ∩=1 ∪=3
		{"fewer-than-k-identical", []string{"a", "b"}, []string{"a", "b"}, 5, 1.0},
		{"dedup-order", []string{"a", "a", "b"}, []string{"a", "b"}, 2, 1.0},
		{"k-zero", []string{"a"}, []string{"a"}, 0, 0.0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := legacyimport.TopKOverlap(tc.a, tc.b, tc.k); got != tc.want {
				t.Fatalf("TopKOverlap=%v want %v", got, tc.want)
			}
		})
	}
}

// fakeV1 returns a fixed path list regardless of the query vector.
type fakeV1 struct{ paths []string }

func (f fakeV1) Search(_ context.Context, _ []float32, _ int) ([]string, error) {
	return f.paths, nil
}

// fakeV2 returns fixed hits regardless of the query vector.
type fakeV2 struct{ paths []string }

func (f fakeV2) SearchWorktree(_ context.Context, _ core.WorktreeID, _ []float32, _ int) ([]core.SearchHit, error) {
	hits := make([]core.SearchHit, 0, len(f.paths))
	for _, p := range f.paths {
		hits = append(hits, core.SearchHit{Path: p})
	}
	return hits, nil
}

func TestRunParityMeanAcrossQueries(t *testing.T) {
	ctx := context.Background()
	emb := enginetest.NewFakeEmbedder(4)
	v1 := fakeV1{paths: []string{"a.go", "b.go", "c.go"}}
	v2 := fakeV2{paths: []string{"a.go", "b.go", "d.go"}} // overlap 2/3 per query

	rep, err := legacyimport.RunParity(ctx, emb, v1, v2, "wt", []string{"q1", "q2"}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.PerQuery) != 2 {
		t.Fatalf("per-query len=%d", len(rep.PerQuery))
	}
	want := 2.0 / 4.0 // Jaccard: ∩={a,b}=2, ∪={a,b,c,d}=4
	if rep.Mean != want {
		t.Fatalf("mean=%v want %v", rep.Mean, want)
	}
	if rep.PerQuery[0].Overlap != want {
		t.Fatalf("q1 overlap=%v want %v", rep.PerQuery[0].Overlap, want)
	}
}
