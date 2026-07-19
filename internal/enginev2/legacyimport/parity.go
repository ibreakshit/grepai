package legacyimport

import (
	"context"
	"fmt"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/embedder"
)

// V1Searcher ranks a query vector against the legacy index, returning file paths
// in descending relevance (chunk-level results may repeat a path; callers
// de-duplicate). k is the desired number of unique files.
type V1Searcher interface {
	Search(ctx context.Context, query []float32, k int) ([]string, error)
}

// V2Searcher ranks a query vector against a worktree's v2 active view.
type V2Searcher interface {
	SearchWorktree(ctx context.Context, wt core.WorktreeID, query []float32, limit int) ([]core.SearchHit, error)
}

// QueryParity is one query's v1-vs-v2 comparison.
type QueryParity struct {
	Query   string
	Overlap float64
	V1Paths []string
	V2Paths []string
}

// ParityReport aggregates per-query overlap and its mean.
type ParityReport struct {
	PerQuery []QueryParity
	Mean     float64
}

// TopKOverlap is the Jaccard similarity of the top-k unique files of each ranked
// list: |A∩B| / |A∪B|. Using the union as the denominator penalizes cardinality
// differences — ["a"] versus ["a","b","c"] scores 1/3, not 1.0 — so a truncated
// or near-empty result cannot masquerade as agreement. Two empty lists are a
// perfect 1.0; one empty list is 0.0. Order within k does not matter. k<1 yields
// 0 (nothing compared).
func TopKOverlap(a, b []string, k int) float64 {
	if k < 1 {
		return 0.0
	}
	ua := uniqueTopK(a, k)
	ub := uniqueTopK(b, k)
	if len(ua) == 0 && len(ub) == 0 {
		return 1.0
	}
	set := make(map[string]struct{}, len(ua))
	for _, p := range ua {
		set[p] = struct{}{}
	}
	inter := 0
	for _, p := range ub {
		if _, ok := set[p]; ok {
			inter++
		}
	}
	union := len(ua) + len(ub) - inter
	if union == 0 {
		return 0.0
	}
	return float64(inter) / float64(union)
}

// uniqueTopK returns the first k distinct paths preserving input order.
func uniqueTopK(paths []string, k int) []string {
	out := make([]string, 0, k)
	seen := make(map[string]struct{})
	for _, p := range paths {
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
		if len(out) == k {
			break
		}
	}
	return out
}

// RunParity embeds each query once and compares the v1 and v2 top-k unique-file
// rankings, returning per-query overlap and the mean across all queries.
func RunParity(ctx context.Context, emb embedder.Embedder, v1 V1Searcher, v2 V2Searcher, wt core.WorktreeID, queries []string, k int) (ParityReport, error) {
	var rep ParityReport
	var sum float64
	for _, q := range queries {
		vec, err := emb.Embed(ctx, q)
		if err != nil {
			return rep, fmt.Errorf("embed %q: %w", q, err)
		}
		v1paths, err := v1.Search(ctx, vec, k)
		if err != nil {
			return rep, fmt.Errorf("v1 search %q: %w", q, err)
		}
		hits, err := v2.SearchWorktree(ctx, wt, vec, k)
		if err != nil {
			return rep, fmt.Errorf("v2 search %q: %w", q, err)
		}
		v2paths := make([]string, 0, len(hits))
		for _, h := range hits {
			v2paths = append(v2paths, h.Path)
		}
		ov := TopKOverlap(v1paths, v2paths, k)
		sum += ov
		rep.PerQuery = append(rep.PerQuery, QueryParity{
			Query:   q,
			Overlap: ov,
			V1Paths: uniqueTopK(v1paths, k),
			V2Paths: uniqueTopK(v2paths, k),
		})
	}
	if len(queries) > 0 {
		rep.Mean = sum / float64(len(queries))
	}
	return rep, nil
}
