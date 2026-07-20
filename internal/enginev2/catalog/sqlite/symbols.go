package sqlite

import (
	"context"

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
		SELECT wf.relative_path, s.name, s.kind, s.line, s.end_line, s.signature
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
		if err := rows.Scan(&s.Path, &s.Name, &s.Kind, &s.Line, &s.EndLine, &s.Signature); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// SymbolEdges returns call edges touching name within the worktree's active
// view. callersOf=true returns edges WHERE callee=name (who calls it);
// callersOf=false returns edges WHERE caller=name (what it calls).
func (c *Catalog) SymbolEdges(ctx context.Context, wt core.WorktreeID, name string, callersOf bool) ([]core.EdgeAt, error) {
	const byCallee = `
		SELECT e.caller, e.callee, wf.relative_path, e.line
		FROM worktree_files wf
		JOIN symbol_edges e ON e.artifact_id = wf.artifact_id
		WHERE wf.worktree_id=? AND e.callee=?
		ORDER BY wf.relative_path, e.line`
	const byCaller = `
		SELECT e.caller, e.callee, wf.relative_path, e.line
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
		if err := rows.Scan(&e.Caller, &e.Callee, &e.Path, &e.Line); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
