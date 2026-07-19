package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// ErrNoSuchGeneration is returned when activating a generation that was never created.
var ErrNoSuchGeneration = errors.New("catalog/sqlite: generation does not exist")

// RegisterRepository idempotently records a repository namespace.
func (c *Catalog) RegisterRepository(ctx context.Context, repo core.RepositoryID, rootPath, gitCommonDir string) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO repositories(repository_id, root_path, git_common_dir, created_at)
			VALUES(?, ?, ?, datetime('now'))
			ON CONFLICT(repository_id) DO UPDATE SET root_path=excluded.root_path, git_common_dir=excluded.git_common_dir`,
			string(repo), rootPath, gitCommonDir)
		return err
	})
}

// RegisterWorktree records a worktree bound to a repository namespace.
func (c *Catalog) RegisterWorktree(ctx context.Context, wt core.WorktreeID, repo core.RepositoryID, rootPath string, regGen core.Generation) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO worktrees(worktree_id, repository_id, root_path, registration_generation, created_at)
			VALUES(?, ?, ?, ?, datetime('now'))
			ON CONFLICT(worktree_id) DO UPDATE SET root_path=excluded.root_path, registration_generation=excluded.registration_generation`,
			string(wt), string(repo), rootPath, int64(regGen))
		return err
	})
}

// CreateGeneration records a new 'building' index generation.
func (c *Catalog) CreateGeneration(ctx context.Context, repo core.RepositoryID, gen core.Generation, fingerprint string) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO index_generations(repository_id, generation, fingerprint, status, created_at)
			VALUES(?, ?, ?, 'building', datetime('now'))`,
			string(repo), int64(gen), fingerprint)
		return err
	})
}

// EnsureActiveGeneration idempotently and atomically establishes gen as an
// active generation for a repository: it creates the generation if absent, then
// activates it only if the repository has no active generation yet. Safe under
// concurrent callers (the single serialized writer + INSERT OR IGNORE + the
// NOT EXISTS guard mean exactly one activation, no duplicate-key failure, and no
// wedged half-bootstrapped state). Used to bootstrap a freshly registered repo.
func (c *Catalog) EnsureActiveGeneration(ctx context.Context, repo core.RepositoryID, gen core.Generation, fingerprint string) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO index_generations(repository_id, generation, fingerprint, status, created_at)
			VALUES(?, ?, ?, 'building', datetime('now'))`,
			string(repo), int64(gen), fingerprint); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `
			UPDATE index_generations SET status='active'
			WHERE repository_id=? AND generation=?
			AND NOT EXISTS (SELECT 1 FROM index_generations WHERE repository_id=? AND status='active')`,
			string(repo), int64(gen), string(repo))
		return err
	})
}

// SetActiveGeneration retires any current active generation and makes gen active.
func (c *Catalog) SetActiveGeneration(ctx context.Context, repo core.RepositoryID, gen core.Generation) error {
	return c.withWriteTx(ctx, func(tx *sql.Tx) error {
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

// ActiveGeneration returns the repository's active generation, or 0 if none.
func (c *Catalog) ActiveGeneration(ctx context.Context, repo core.RepositoryID) (core.Generation, error) {
	var gen sql.NullInt64
	err := c.db.QueryRowContext(ctx, `
		SELECT generation FROM index_generations
		WHERE repository_id=? AND status='active'`, string(repo)).Scan(&gen)
	if errors.Is(err, sql.ErrNoRows) || !gen.Valid {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return core.Generation(gen.Int64), nil
}
