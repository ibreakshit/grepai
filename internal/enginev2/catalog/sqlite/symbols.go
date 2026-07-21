package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// ArtifactsMissingSymbols lists active-view artifacts whose symbols_version is
// behind SymbolsVersionCurrent for a worktree, sorted by path (deterministic
// backfill order). Bumping SymbolsVersionCurrent therefore triggers a
// re-backfill, and putArtifactSymbolsTx's replace semantics make it correct.
func (c *Catalog) ArtifactsMissingSymbols(ctx context.Context, wt core.WorktreeID) ([]core.MissingSymbolArtifact, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT wf.relative_path, fa.artifact_id, fa.source_hash
		FROM worktree_files wf
		JOIN file_artifacts fa ON fa.artifact_id = wf.artifact_id
		WHERE wf.worktree_id=? AND fa.symbols_version < ?
		ORDER BY wf.relative_path`, string(wt), SymbolsVersionCurrent)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.MissingSymbolArtifact
	for rows.Next() {
		var m core.MissingSymbolArtifact
		var id string
		if err := rows.Scan(&m.Path, &id, &m.SourceHash); err != nil {
			return nil, err
		}
		m.ArtifactID = core.ArtifactID(id)
		out = append(out, m)
	}
	return out, rows.Err()
}

// SymbolDefinitions returns definitions of name within the worktree's ACTIVE
// view — the view join is what provides worktree isolation and generation
// scoping (a retired generation's artifacts are unreachable).
func (c *Catalog) SymbolDefinitions(ctx context.Context, wt core.WorktreeID, name string) ([]core.SymbolAt, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT wf.relative_path, s.name, s.kind, s.line, s.end_line, s.signature,
			s.receiver, s.package, s.exported, s.language, s.docstring
		FROM worktree_files wf
		JOIN symbols s ON s.artifact_id = wf.artifact_id
		WHERE wf.worktree_id=? AND s.name=?
		ORDER BY wf.relative_path, s.line`, string(wt), name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.SymbolAt
	for rows.Next() {
		var s core.SymbolAt
		var exported int
		if err := rows.Scan(&s.Path, &s.Name, &s.Kind, &s.Line, &s.EndLine, &s.Signature,
			&s.Receiver, &s.Package, &exported, &s.Language, &s.Docstring); err != nil {
			return nil, err
		}
		s.Exported = exported != 0
		out = append(out, s)
	}
	return out, rows.Err()
}

// SymbolEdges returns call edges touching name within the worktree's active
// view. callersOf=true returns edges WHERE callee=name (who calls it);
// callersOf=false returns edges WHERE caller=name (what it calls).
func (c *Catalog) SymbolEdges(ctx context.Context, wt core.WorktreeID, name string, callersOf bool) ([]core.EdgeAt, error) {
	const byCallee = `
		SELECT e.caller, e.callee, wf.relative_path, e.line, e.context
		FROM worktree_files wf
		JOIN symbol_edges e ON e.artifact_id = wf.artifact_id
		WHERE wf.worktree_id=? AND e.callee=?
		ORDER BY wf.relative_path, e.line`
	const byCaller = `
		SELECT e.caller, e.callee, wf.relative_path, e.line, e.context
		FROM worktree_files wf
		JOIN symbol_edges e ON e.artifact_id = wf.artifact_id
		WHERE wf.worktree_id=? AND e.caller=?
		ORDER BY wf.relative_path, e.line`
	query := byCallee
	if !callersOf {
		query = byCaller
	}
	rows, err := c.db.QueryContext(ctx, query, string(wt), name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.EdgeAt
	for rows.Next() {
		var e core.EdgeAt
		if err := rows.Scan(&e.Caller, &e.Callee, &e.Path, &e.Line, &e.Context); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ArtifactSymbolsCurrent reports whether the artifact's symbols were extracted
// by the current extractor version. Missing artifact reads as not-current (the
// caller then extracts, which is always safe under replace semantics).
func (c *Catalog) ArtifactSymbolsCurrent(ctx context.Context, key core.ArtifactKey) (bool, error) {
	var v int
	err := c.db.QueryRowContext(ctx,
		`SELECT symbols_version FROM file_artifacts WHERE artifact_id=?`,
		string(key.ArtifactID())).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return v >= SymbolsVersionCurrent, nil
}

// SymbolDefinitionsBulk resolves definitions for many names in one pass
// (chunked IN-clause; SQLite's default variable limit is 999, so chunks stay
// well under it). Names with no definitions are simply absent from the map.
func (c *Catalog) SymbolDefinitionsBulk(ctx context.Context, wt core.WorktreeID, names []string) (map[string][]core.SymbolAt, error) {
	out := map[string][]core.SymbolAt{}
	const chunk = 500
	for start := 0; start < len(names); start += chunk {
		end := start + chunk
		if end > len(names) {
			end = len(names)
		}
		batch := names[start:end]
		placeholders := strings.Repeat("?,", len(batch))
		placeholders = placeholders[:len(placeholders)-1]
		args := make([]any, 0, len(batch)+1)
		args = append(args, string(wt))
		for _, n := range batch {
			args = append(args, n)
		}
		//nolint:gosec // #nosec G202 - placeholders is "?,?..." built from Repeat; all values are bound parameters
		query := `
			SELECT wf.relative_path, s.name, s.kind, s.line, s.end_line, s.signature,
				s.receiver, s.package, s.exported, s.language, s.docstring
			FROM worktree_files wf
			JOIN symbols s ON s.artifact_id = wf.artifact_id
			WHERE wf.worktree_id=? AND s.name IN (` + placeholders + `)
			ORDER BY s.name, wf.relative_path, s.line`
		rows, err := c.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var s core.SymbolAt
			var exported int
			if err := rows.Scan(&s.Path, &s.Name, &s.Kind, &s.Line, &s.EndLine, &s.Signature,
				&s.Receiver, &s.Package, &exported, &s.Language, &s.Docstring); err != nil {
				rows.Close()
				return nil, err
			}
			s.Exported = exported != 0
			out[s.Name] = append(out[s.Name], s)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return out, nil
}

// CountArtifactsMissingSymbols is the count-only form of
// ArtifactsMissingSymbols for hot paths (Status is polled every 500ms by
// `grepai watch`): no row materialization, single aggregate.
func (c *Catalog) CountArtifactsMissingSymbols(ctx context.Context, wt core.WorktreeID) (int, error) {
	var n int
	err := c.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM worktree_files wf
		JOIN file_artifacts fa ON fa.artifact_id = wf.artifact_id
		WHERE wf.worktree_id=? AND fa.symbols_version < ?`, string(wt), SymbolsVersionCurrent).Scan(&n)
	return n, err
}
