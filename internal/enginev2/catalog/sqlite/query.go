package sqlite

import (
	"context"
	"math"
	"sort"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// SearchWorktree ranks the chunks reachable from a worktree's current view by
// cosine similarity to query, returning the top `limit` distinct file paths by
// their best chunk score. The candidate set is scoped to this worktree's view,
// so results never include another worktree's file versions (invariant 4) nor a
// building generation's not-yet-referenced artifacts (invariant 12). A stored
// vector whose length differs from the query's is skipped as incompatible.
func (c *Catalog) SearchWorktree(ctx context.Context, wt core.WorktreeID, query []float32, limit int) ([]core.SearchHit, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT wf.relative_path, ch.dimensions, ch.vector
		FROM worktree_files wf
		JOIN artifact_chunks ac ON ac.artifact_id = wf.artifact_id
		JOIN chunks ch ON ch.chunk_id = ac.chunk_id
		WHERE wf.worktree_id=?`, string(wt))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	best := map[string]float32{}
	for rows.Next() {
		var path string
		var dims int
		var blob []byte
		if err := rows.Scan(&path, &dims, &blob); err != nil {
			return nil, err
		}
		if dims != len(query) {
			continue // incompatible fingerprint/dimension
		}
		vec, err := decodeVector(blob, dims)
		if err != nil {
			return nil, err
		}
		s := cosine(query, vec)
		if cur, ok := best[path]; !ok || s > cur {
			best[path] = s
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	hits := make([]core.SearchHit, 0, len(best))
	for p, s := range best {
		hits = append(hits, core.SearchHit{Path: p, Score: s})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].Path < hits[j].Path // stable tie-break
	})
	if limit > 0 && len(hits) > limit {
		hits = hits[:limit]
	}
	return hits, nil
}

func cosine(a, b []float32) float32 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return float32(dot / (math.Sqrt(na) * math.Sqrt(nb)))
}

// WorktreePendingCount returns the number of active index jobs for a worktree.
func (c *Catalog) WorktreePendingCount(ctx context.Context, wt core.WorktreeID) (int, error) {
	var n int
	err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM index_jobs WHERE worktree_id=?`, string(wt)).Scan(&n)
	return n, err
}

// WorktreePathsPending reports whether any of paths has a pending job for the
// worktree. An empty paths slice reports false (nothing to wait on).
func (c *Catalog) WorktreePathsPending(ctx context.Context, wt core.WorktreeID, paths []string) (bool, error) {
	for _, p := range paths {
		var n int
		if err := c.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM index_jobs WHERE worktree_id=? AND relative_path=?`,
			string(wt), p).Scan(&n); err != nil {
			return false, err
		}
		if n > 0 {
			return true, nil
		}
	}
	return false, nil
}
