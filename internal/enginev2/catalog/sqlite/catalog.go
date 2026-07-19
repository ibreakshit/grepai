// Package sqlite implements the enginev2 catalog.Catalog contract over a
// local SQLite database (modernc.org/sqlite, WAL mode). It is the durable
// source of truth for the v2 engine: repositories, worktrees, generations,
// immutable file artifacts, chunk vectors, worktree views, and jobs.
//
// All writes are serialized through a single process-level mutex (the
// "single serialized writer"); reads use WAL snapshot reads on the shared
// *sql.DB and never block commits.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	_ "modernc.org/sqlite" // pure-Go driver, registered as "sqlite"
)

// Catalog is a SQLite-backed durable catalog.
type Catalog struct {
	db      *sql.DB
	writeMu sync.Mutex
}

// Open opens (creating if needed) the SQLite database at path, applies WAL,
// busy_timeout, and foreign-keys pragmas, and runs pending migrations.
func Open(ctx context.Context, path string) (*Catalog, error) {
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
	// Establish the connection early so pragmas/migrations run against a live DB.
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite ping: %w", err)
	}
	c := &Catalog{db: db}
	if err := c.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return c, nil
}

// Close closes the underlying database.
func (c *Catalog) Close() error {
	return c.db.Close()
}

// withWriteTx runs fn inside a serialized write transaction. writeMu enforces
// the single-writer invariant; a non-nil error from fn rolls the whole
// transaction back, leaving prior state intact.
func (c *Catalog) withWriteTx(ctx context.Context, fn func(*sql.Tx) error) (err error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()
	if err = fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
