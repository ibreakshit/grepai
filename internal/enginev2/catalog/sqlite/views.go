package sqlite

import (
	"context"
	"database/sql"

	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// Compile-time assertion that the SQLite catalog satisfies the contract.
var _ catalog.Catalog = (*Catalog)(nil)

// ResolveView returns the artifact a worktree path currently resolves to.
func (c *Catalog) ResolveView(ctx context.Context, wt core.WorktreeID, relPath string) (core.ArtifactID, bool, error) {
	var id string
	err := c.db.QueryRowContext(ctx, `
		SELECT artifact_id FROM worktree_files WHERE worktree_id=? AND relative_path=?`,
		string(wt), relPath).Scan(&id)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return core.ArtifactID(id), true, nil
}

// CommitUpdate atomically stores the artifact, switches the worktree view, and
// completes the job. Any failure rolls the whole transaction back, leaving the
// prior view searchable (invariant 6).
func (c *Catalog) CommitUpdate(ctx context.Context, req core.CommitRequest, job core.Job) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		return commitUpdateTx(ctx, tx, req, job)
	})
}

// commitUpdateTx performs the artifact store + view switch + job completion
// within a caller-provided transaction. It is the internal seam used by both
// CommitUpdate and the Gate 1 rollback test.
func commitUpdateTx(ctx context.Context, tx *sql.Tx, req core.CommitRequest, job core.Job) error {
	if err := putArtifactTx(ctx, tx, req.Artifact); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO worktree_files(worktree_id, relative_path, artifact_id, generation, updated_at)
		VALUES(?, ?, ?, ?, datetime('now'))
		ON CONFLICT(worktree_id, relative_path) DO UPDATE SET
			artifact_id=excluded.artifact_id, generation=excluded.generation, updated_at=excluded.updated_at`,
		string(req.View.WorktreeID), req.View.Path, string(req.View.ArtifactID), int64(req.View.Generation)); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		DELETE FROM index_jobs WHERE worktree_id=? AND relative_path=?`,
		string(job.WorktreeID), job.Path)
	return err
}

// UpsertJob records desired file state, superseding an existing job for the
// same (worktree, path) only when the incoming generation is at least as new.
func (c *Catalog) UpsertJob(ctx context.Context, job core.Job) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO index_jobs(worktree_id, relative_path, desired_hash, generation, operation, priority, attempts, claimed, created_at)
			VALUES(?, ?, ?, ?, ?, ?, ?, 0, datetime('now'))
			ON CONFLICT(worktree_id, relative_path) DO UPDATE SET
				desired_hash=excluded.desired_hash,
				generation=excluded.generation,
				operation=excluded.operation,
				priority=excluded.priority,
				attempts=excluded.attempts,
				claimed=0
			WHERE excluded.generation >= index_jobs.generation`,
			string(job.WorktreeID), job.Path, job.DesiredHash, int64(job.Generation),
			int(job.Operation), int(job.Priority), job.Attempts)
		return err
	})
}

// ClaimNextJob claims the highest-priority unclaimed job at or above
// minPriority (lower Priority value = higher priority), marking it claimed so
// it is not handed out twice.
func (c *Catalog) ClaimNextJob(ctx context.Context, minPriority core.Priority) (core.Job, bool, error) {
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
			SELECT job_id, worktree_id, relative_path, desired_hash, generation, operation, priority, attempts
			FROM index_jobs
			WHERE claimed=0 AND priority<=?
			ORDER BY priority ASC, job_id ASC
			LIMIT 1`, int(minPriority))
		if err := row.Scan(&jobID, &wt, &path, &hash, &gen, &op, &prio, &att); err != nil {
			if err == sql.ErrNoRows {
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
			Operation:   core.Operation(op),  //nolint:gosec // #nosec G115 - operation is a small enum persisted by this package
			Priority:    core.Priority(prio), //nolint:gosec // #nosec G115 - priority is a small enum persisted by this package
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
