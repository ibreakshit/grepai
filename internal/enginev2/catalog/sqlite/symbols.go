package sqlite

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// MissingSymbolArtifact identifies one active-view file whose artifact has not
// had symbols extracted yet (the backfill work list).
type MissingSymbolArtifact struct {
	Path       string
	ArtifactID core.ArtifactID
	SourceHash string
}

// ArtifactsMissingSymbols lists active-view artifacts with symbols_version=0
// for a worktree, sorted by path (deterministic backfill order).
func (c *Catalog) ArtifactsMissingSymbols(ctx context.Context, wt core.WorktreeID) ([]MissingSymbolArtifact, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT wf.relative_path, fa.artifact_id, fa.source_hash
		FROM worktree_files wf
		JOIN file_artifacts fa ON fa.artifact_id = wf.artifact_id
		WHERE wf.worktree_id=? AND fa.symbols_version=0
		ORDER BY wf.relative_path`, string(wt))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MissingSymbolArtifact
	for rows.Next() {
		var m MissingSymbolArtifact
		var id string
		if err := rows.Scan(&m.Path, &id, &m.SourceHash); err != nil {
			return nil, err
		}
		m.ArtifactID = core.ArtifactID(id)
		out = append(out, m)
	}
	return out, rows.Err()
}

// SymbolAt is one symbol definition resolved through a worktree's active view.
type SymbolAt struct {
	Path      string
	Name      string
	Kind      string
	Line      int
	EndLine   int
	Signature string
}

// SymbolDefinitions returns definitions of name within the worktree's ACTIVE
// view — the view join is what provides worktree isolation and generation
// scoping (a retired generation's artifacts are unreachable).
func (c *Catalog) SymbolDefinitions(ctx context.Context, wt core.WorktreeID, name string) ([]SymbolAt, error) {
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
	var out []SymbolAt
	for rows.Next() {
		var s SymbolAt
		if err := rows.Scan(&s.Path, &s.Name, &s.Kind, &s.Line, &s.EndLine, &s.Signature); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// EdgeAt is one call edge resolved through a worktree's active view.
type EdgeAt struct {
	Caller string
	Callee string
	Path   string
	Line   int
}

// SymbolEdges returns call edges touching name within the worktree's active
// view. callersOf=true returns edges WHERE callee=name (who calls it);
// callersOf=false returns edges WHERE caller=name (what it calls).
func (c *Catalog) SymbolEdges(ctx context.Context, wt core.WorktreeID, name string, callersOf bool) ([]EdgeAt, error) {
	where := "e.callee=?"
	if !callersOf {
		where = "e.caller=?"
	}
	rows, err := c.db.QueryContext(ctx, `
		SELECT e.caller, e.callee, wf.relative_path, e.line
		FROM worktree_files wf
		JOIN symbol_edges e ON e.artifact_id = wf.artifact_id
		WHERE wf.worktree_id=? AND `+where+`
		ORDER BY wf.relative_path, e.line`, string(wt), name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EdgeAt
	for rows.Next() {
		var e EdgeAt
		if err := rows.Scan(&e.Caller, &e.Callee, &e.Path, &e.Line); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
