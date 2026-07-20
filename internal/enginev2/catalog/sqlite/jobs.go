package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// DeadLetterJob atomically records a permanently-failed job and removes it from
// the active queue. It deletes only the exact job it is dead-lettering (same
// generation and desired hash); if a newer desired intent has superseded that
// row (a higher generation, or the same generation with a different desired
// hash from a rapid re-save), the delete matches nothing and no dead-letter row
// is written — the surviving newer job is neither dropped nor spuriously
// dead-lettered.
func (c *Catalog) DeadLetterJob(ctx context.Context, job core.Job, reason string) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			DELETE FROM index_jobs
			WHERE worktree_id=? AND relative_path=? AND generation<=? AND desired_hash=?`,
			string(job.WorktreeID), job.Path, int64(job.Generation), job.DesiredHash)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return nil // superseded by a newer intent: nothing to dead-letter
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO dead_letter_jobs(worktree_id, relative_path, reason, created_at)
			VALUES(?, ?, ?, datetime('now'))`,
			string(job.WorktreeID), job.Path, reason)
		return err
	})
}

// DeleteJobsForWorktree removes all pending jobs for a worktree. A one-shot
// index reconciles the full desired state fresh, so it clears any leftover jobs
// (e.g. from an interrupted prior run whose desired state has since changed)
// before re-reconciling, rather than processing a stale job under an out-of-date
// desired identity. Not for a running daemon (which must not drop live work).
func (c *Catalog) DeleteJobsForWorktree(ctx context.Context, wt core.WorktreeID) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM index_jobs WHERE worktree_id=?`, string(wt))
		return err
	})
}

// RollWorktreeGeneration atomically drops a worktree's file view AND its queued
// jobs AND activates the given generation — the daemon's fingerprint rollover.
// All three must be ONE write transaction: without clearing, reconciliation
// would see the old generation's indexed hashes (the view is not
// generation-filtered) and search would keep serving incompatible vectors; and
// if clear and activate were separate transactions, an in-flight worker commit
// under the still-active old generation could repopulate the view between them
// (it passes the invariant-12 active-generation guard), permanently stranding
// stale rows once activation makes the fingerprint match. Single-writer
// serialization (writeMu + one tx) means a concurrent commit lands either
// before (its rows are cleared here) or after (the guard rejects its
// now-inactive generation). Artifacts and cached chunk vectors are left in
// place — content+fingerprint addressed, so the new fingerprint simply misses
// them.
func (c *Catalog) RollWorktreeGeneration(ctx context.Context, wt core.WorktreeID, repo core.RepositoryID, gen core.Generation) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM worktree_files WHERE worktree_id=?`, string(wt)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM index_jobs WHERE worktree_id=?`, string(wt)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE index_generations SET status='retired'
			WHERE repository_id=? AND status='active'`, string(repo)); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE index_generations SET status='active'
			WHERE repository_id=? AND generation=?`, string(repo), int64(gen))
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNoSuchGeneration
		}
		return nil
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
// transient failure, but only while the row is still the exact job that failed
// (same generation and desired hash). A newer supersede leaves the newer row's
// attempt count untouched — the returned count reflects the current row, so a
// stale failure never pushes a valid newer save toward a premature dead-letter.
func (c *Catalog) FailJobAttempt(ctx context.Context, job core.Job) (int, error) {
	var attempts int
	err := c.withWriteTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			UPDATE index_jobs SET attempts=attempts+1, claimed=0
			WHERE worktree_id=? AND relative_path=? AND generation=? AND desired_hash=?`,
			string(job.WorktreeID), job.Path, int64(job.Generation), job.DesiredHash); err != nil {
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

// CurrentJob returns the current desired intent (generation and desired hash)
// for a path, if a job row exists. The worker compares this against the job it
// claimed to detect supersession on either axis: a newer generation, or the
// same generation with a different desired hash (a rapid re-save).
func (c *Catalog) CurrentJob(ctx context.Context, wt core.WorktreeID, relPath string) (core.Generation, string, bool, error) {
	var gen int64
	var hash string
	err := c.db.QueryRowContext(ctx, `
		SELECT generation, desired_hash FROM index_jobs WHERE worktree_id=? AND relative_path=?`,
		string(wt), relPath).Scan(&gen, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", false, nil
	}
	if err != nil {
		return 0, "", false, err
	}
	return core.Generation(gen), hash, true, nil
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
