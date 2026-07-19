package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// RepositoriesWithPendingJobs returns the repositories that currently have at
// least one unclaimed job, for round-robin fair scheduling.
func (c *Catalog) RepositoriesWithPendingJobs(ctx context.Context) ([]core.RepositoryID, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT DISTINCT w.repository_id
		FROM index_jobs j JOIN worktrees w ON w.worktree_id = j.worktree_id
		WHERE j.claimed=0
		ORDER BY w.repository_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var repos []core.RepositoryID
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			return nil, err
		}
		repos = append(repos, core.RepositoryID(r))
	}
	return repos, rows.Err()
}

// ClaimNextJobInRepo claims the highest-priority unclaimed job (at or above
// minPriority) whose worktree belongs to repo. Same claim discipline as
// ClaimNextJob, scoped to one repository for fair round-robin dispatch.
func (c *Catalog) ClaimNextJobInRepo(ctx context.Context, repo core.RepositoryID, minPriority core.Priority) (core.Job, bool, error) {
	var job core.Job
	var found bool
	err := c.withWriteTx(ctx, func(tx *sql.Tx) error {
		var (
			jobID int64
			wt    string
			path  string
			hash  string
			gen   int64
			op    int
			prio  int
			att   int
		)
		row := tx.QueryRowContext(ctx, `
			SELECT j.job_id, j.worktree_id, j.relative_path, j.desired_hash, j.generation, j.operation, j.priority, j.attempts
			FROM index_jobs j JOIN worktrees w ON w.worktree_id = j.worktree_id
			WHERE j.claimed=0 AND j.priority<=? AND w.repository_id=?
			ORDER BY j.priority ASC, j.job_id ASC
			LIMIT 1`, int(minPriority), string(repo))
		if err := row.Scan(&jobID, &wt, &path, &hash, &gen, &op, &prio, &att); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE index_jobs SET claimed=1 WHERE job_id=?`, jobID); err != nil {
			return err
		}
		job = core.Job{
			WorktreeID:  core.WorktreeID(wt),
			Path:        path,
			DesiredHash: hash,
			Generation:  core.Generation(gen),
			Operation:   core.Operation(op),  //nolint:gosec // #nosec G115 - small enum persisted by this package
			Priority:    core.Priority(prio), //nolint:gosec // #nosec G115 - small enum persisted by this package
			Attempts:    att,
		}
		found = true
		return nil
	})
	if err != nil {
		return core.Job{}, false, err
	}
	return job, found, nil
}

// QueueDepthByPriority returns the count of unclaimed jobs per priority.
func (c *Catalog) QueueDepthByPriority(ctx context.Context) (map[core.Priority]int, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT priority, COUNT(*) FROM index_jobs WHERE claimed=0 GROUP BY priority`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[core.Priority]int{}
	for rows.Next() {
		var prio, n int
		if err := rows.Scan(&prio, &n); err != nil {
			return nil, err
		}
		out[core.Priority(prio)] = n //nolint:gosec // #nosec G115 - small enum persisted by this package
	}
	return out, rows.Err()
}
