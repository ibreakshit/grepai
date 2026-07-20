// internal/enginev2/catalog/sqlite/reader.go
package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// ErrNoSuchWorktree is returned when a worktree id is not registered.
var ErrNoSuchWorktree = errors.New("catalog/sqlite: worktree not registered")

// WorktreeInfo returns a worktree's root path and repository namespace.
func (c *Catalog) WorktreeInfo(ctx context.Context, wt core.WorktreeID) (string, core.RepositoryID, error) {
	var root, repo string
	err := c.db.QueryRowContext(ctx, `
		SELECT root_path, repository_id FROM worktrees WHERE worktree_id=?`,
		string(wt)).Scan(&root, &repo)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", ErrNoSuchWorktree
	}
	if err != nil {
		return "", "", err
	}
	return root, core.RepositoryID(repo), nil
}

// WorktreeIndexedHashes returns the currently-indexed source hash for every
// path in a worktree's view (invariant 4: only this worktree's rows).
func (c *Catalog) WorktreeIndexedHashes(ctx context.Context, wt core.WorktreeID) (map[string]string, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT wf.relative_path, fa.source_hash
		FROM worktree_files wf
		JOIN file_artifacts fa ON fa.artifact_id = wf.artifact_id
		WHERE wf.worktree_id=?`, string(wt))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := map[string]string{}
	for rows.Next() {
		var path, hash string
		if err := rows.Scan(&path, &hash); err != nil {
			return nil, err
		}
		res[path] = hash
	}
	return res, rows.Err()
}

// Worktrees lists every registered worktree id, sorted, for cross-repo
// fan-out queries.
func (c *Catalog) Worktrees(ctx context.Context) ([]core.WorktreeID, error) {
	rows, err := c.db.QueryContext(ctx, `SELECT worktree_id FROM worktrees ORDER BY worktree_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.WorktreeID
	for rows.Next() {
		var wt string
		if err := rows.Scan(&wt); err != nil {
			return nil, err
		}
		out = append(out, core.WorktreeID(wt))
	}
	return out, rows.Err()
}
