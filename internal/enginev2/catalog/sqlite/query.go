package sqlite

import (
	"context"
	"math"
	"sort"
	"strings"

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
		SELECT wf.relative_path, ch.dimensions, ch.vector, ch.content, ac.start_line, ac.end_line
		FROM worktree_files wf
		JOIN artifact_chunks ac ON ac.artifact_id = wf.artifact_id
		JOIN chunks ch ON ch.chunk_id = ac.chunk_id
		WHERE wf.worktree_id=?`, string(wt))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	best := map[string]core.SearchHit{}
	for rows.Next() {
		var path string
		var dims int
		var blob []byte
		var content string
		var startLine, endLine int
		if err := rows.Scan(&path, &dims, &blob, &content, &startLine, &endLine); err != nil {
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
		// Skip non-finite scores (a NaN/Inf in a stored or query vector) so they
		// can never become a path's "best" score or break the sort's total order.
		if math.IsNaN(float64(s)) || math.IsInf(float64(s), 0) {
			continue
		}
		// Keep the best-scoring chunk per path, carrying its snippet.
		if cur, ok := best[path]; !ok || s > cur.Score {
			best[path] = core.SearchHit{Path: path, Score: s, Content: content, StartLine: startLine, EndLine: endLine}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	hits := make([]core.SearchHit, 0, len(best))
	for _, h := range best {
		hits = append(hits, h)
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
// worktree, evaluated in a single statement so the set is checked against one
// consistent snapshot (a job appearing on one path while another completes can
// never be missed). An empty paths slice reports false (nothing to wait on).
func (c *Catalog) WorktreePathsPending(ctx context.Context, wt core.WorktreeID, paths []string) (bool, error) {
	if len(paths) == 0 {
		return false, nil
	}
	placeholders := make([]string, len(paths))
	args := make([]any, 0, len(paths)+1)
	args = append(args, string(wt))
	for i, p := range paths {
		placeholders[i] = "?"
		args = append(args, p)
	}
	q := `SELECT COUNT(*) FROM index_jobs WHERE worktree_id=? AND relative_path IN (` +
		strings.Join(placeholders, ",") + `)`
	var n int
	if err := c.db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}
