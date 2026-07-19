package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// DeadLetterJob atomically records a permanently-failed job and removes it from
// the active queue. Guarded by generation so a newer supersede survives.
func (c *Catalog) DeadLetterJob(ctx context.Context, job core.Job, reason string) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO dead_letter_jobs(worktree_id, relative_path, reason, created_at)
			VALUES(?, ?, ?, datetime('now'))`,
			string(job.WorktreeID), job.Path, reason); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `
			DELETE FROM index_jobs
			WHERE worktree_id=? AND relative_path=? AND generation<=?`,
			string(job.WorktreeID), job.Path, int64(job.Generation))
		return err
	})
}

// RequeueClaimedJobs releases every claimed job so a restarted worker can
// re-claim work a crashed worker left in flight (invariant 7 recovery).
func (c *Catalog) RequeueClaimedJobs(ctx context.Context) (int, error) {
	var n int64
	err := c.withWriteTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE index_jobs SET claimed=0 WHERE claimed=1`)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	return int(n), err
}

// FailJobAttempt increments the attempt counter and releases the claim for a
// transient failure, but only while the row is still at the job's generation
// (a newer supersede leaves attempts alone). Returns the new attempt count.
func (c *Catalog) FailJobAttempt(ctx context.Context, job core.Job) (int, error) {
	var attempts int
	err := c.withWriteTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			UPDATE index_jobs SET attempts=attempts+1, claimed=0
			WHERE worktree_id=? AND relative_path=? AND generation=?`,
			string(job.WorktreeID), job.Path, int64(job.Generation)); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `
			SELECT attempts FROM index_jobs WHERE worktree_id=? AND relative_path=?`,
			string(job.WorktreeID), job.Path).Scan(&attempts)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil // row was superseded/removed; nothing to retry
	}
	return attempts, err
}

// DesiredGeneration returns the pending job generation for a path, if any.
func (c *Catalog) DesiredGeneration(ctx context.Context, wt core.WorktreeID, relPath string) (core.Generation, bool, error) {
	var gen int64
	err := c.db.QueryRowContext(ctx, `
		SELECT generation FROM index_jobs WHERE worktree_id=? AND relative_path=?`,
		string(wt), relPath).Scan(&gen)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return core.Generation(gen), true, nil
}

// GenerationFingerprint returns the fingerprint recorded for a generation.
func (c *Catalog) GenerationFingerprint(ctx context.Context, repo core.RepositoryID, gen core.Generation) (string, error) {
	var fp string
	err := c.db.QueryRowContext(ctx, `
		SELECT fingerprint FROM index_generations WHERE repository_id=? AND generation=?`,
		string(repo), int64(gen)).Scan(&fp)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNoSuchGeneration
	}
	return fp, err
}

// DeadLetterCount returns the number of dead-letter rows (test/status read).
func (c *Catalog) DeadLetterCount(ctx context.Context) (int, error) {
	var n int
	err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letter_jobs`).Scan(&n)
	return n, err
}

// ArtifactChunkIDs returns an artifact's chunk ids in ordinal order (test read).
func (c *Catalog) ArtifactChunkIDs(ctx context.Context, artifactID core.ArtifactID) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT chunk_id FROM artifact_chunks WHERE artifact_id=? ORDER BY ordinal ASC`,
		string(artifactID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
