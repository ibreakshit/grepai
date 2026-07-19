package sqlite

import (
	"context"
	"database/sql"
	"errors"

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
	if errors.Is(err, sql.ErrNoRows) {
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
	if err := putArtifactChunksTx(ctx, tx, req.Artifact.ID, req.Chunks); err != nil {
		return err
	}
	// Invariant 12 (search availability): a commit for a generation other than
	// the repository's active one — e.g. a controlled rebuild building a new
	// generation — stores the shared immutable artifact but must NOT switch the
	// active worktree view, so the active generation stays queryable until the
	// rebuild is activated. When no generation is active yet (fresh/bootstrap),
	// the switch proceeds.
	switchView, err := commitTouchesActiveView(ctx, tx, req.View.WorktreeID, req.View.Generation)
	if err != nil {
		return err
	}
	// Switch the view only if this generation is at least as new as the one
	// currently recorded — a superseded (older-generation) commit must not
	// regress the worktree view (spec §5.6: only the newest generation commits).
	if switchView {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO worktree_files(worktree_id, relative_path, artifact_id, generation, updated_at)
			VALUES(?, ?, ?, ?, datetime('now'))
			ON CONFLICT(worktree_id, relative_path) DO UPDATE SET
				artifact_id=excluded.artifact_id, generation=excluded.generation, updated_at=excluded.updated_at
			WHERE excluded.generation >= worktree_files.generation`,
			string(req.View.WorktreeID), req.View.Path, string(req.View.ArtifactID), int64(req.View.Generation)); err != nil {
			return err
		}
	}
	// Complete only the exact job this commit fulfills: same generation and the
	// same desired hash. A newer pending intent — a higher generation, or the
	// same generation with a different desired hash (a rapid re-save) — must
	// survive so its own commit can run (spec §5.6: only the newest desired
	// state commits). Guarding on desired_hash is essential: without it, a
	// stale same-generation commit would delete the newer save's job.
	_, err = tx.ExecContext(ctx, `
		DELETE FROM index_jobs
		WHERE worktree_id=? AND relative_path=? AND generation<=? AND desired_hash=?`,
		string(job.WorktreeID), job.Path, int64(req.View.Generation), job.DesiredHash)
	return err
}

// commitTouchesActiveView reports whether a commit at gen may modify the active
// worktree view: true when gen is the repository's active generation, or when no
// generation is active yet (bootstrap). A commit for a non-active generation (a
// building rebuild) stores its artifact but must not disturb the active view
// (invariant 12).
func commitTouchesActiveView(ctx context.Context, tx *sql.Tx, wt core.WorktreeID, gen core.Generation) (bool, error) {
	var active sql.NullInt64
	err := tx.QueryRowContext(ctx, `
		SELECT g.generation FROM index_generations g
		JOIN worktrees w ON w.repository_id = g.repository_id
		WHERE w.worktree_id=? AND g.status='active'`, string(wt)).Scan(&active)
	if errors.Is(err, sql.ErrNoRows) || !active.Valid {
		return true, nil // no active generation yet: bootstrap commit switches
	}
	if err != nil {
		return false, err
	}
	return active.Int64 == int64(gen), nil
}

// CommitDelete atomically fulfills a delete job: it removes the exact job row
// (same generation and desired hash — an OpDelete carries an empty desired
// hash) and, only if that job was still present, removes the worktree view. If
// a newer desired intent superseded the delete (e.g. the file was re-created,
// replacing the row with an OpUpsert whose desired hash differs), the job
// delete matches nothing and the view is left untouched so the surviving upsert
// job can refresh it — the newer save is never dropped (spec §5.6). The
// artifact itself is retained (it may still be referenced by other worktrees;
// invariant 5).
func (c *Catalog) CommitDelete(ctx context.Context, wt core.WorktreeID, relPath string, gen core.Generation, job core.Job) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		// Key the job delete on the same (worktree, path) as the view delete so
		// the two can never diverge; job.DesiredHash pins the exact intent.
		res, err := tx.ExecContext(ctx, `
			DELETE FROM index_jobs
			WHERE worktree_id=? AND relative_path=? AND generation<=? AND desired_hash=?`,
			string(wt), relPath, int64(gen), job.DesiredHash)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return nil // superseded: leave the view for the surviving upsert job
		}
		// Invariant 12: only an active-generation delete may remove the active
		// view. A non-active (building rebuild) delete completes its job but
		// leaves the active generation's view intact.
		touchesActive, err := commitTouchesActiveView(ctx, tx, wt, gen)
		if err != nil {
			return err
		}
		if !touchesActive {
			return nil
		}
		_, err = tx.ExecContext(ctx, `
			DELETE FROM worktree_files
			WHERE worktree_id=? AND relative_path=? AND generation<=?`,
			string(wt), relPath, int64(gen))
		return err
	})
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
