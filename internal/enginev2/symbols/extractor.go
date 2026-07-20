// Package symbols adapts the v1 trace extractors (tree-sitter with a regex
// fallback) to the v2 engine's artifact-scoped symbol model. Extraction LOGIC
// is reused per the architecture plan; only persistence moved to the catalog.
package symbols

import (
	"context"
	"sync"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/trace"
)

// Extractor converts file content into artifact-scoped symbol defs + call
// edges. Safe for concurrent use (the underlying extractor is guarded — tree-
// sitter parsers are not concurrency-safe).
type Extractor struct {
	mu    sync.Mutex
	inner trace.SymbolExtractor
}

// New builds the production extractor via the build-tag-selected constructor:
// the default CGO_ENABLED=0 build uses the regex extractor (exactly what v1's
// shipped binaries used); a `-tags treesitter` build upgrades to tree-sitter.
func New() *Extractor {
	return &Extractor{inner: newInner()}
}

// Extract returns defs and call edges for one file. Unsupported languages
// yield empty results, not errors — the artifact is still marked extracted.
func (e *Extractor) Extract(ctx context.Context, relPath, content string) ([]core.SymbolDef, []core.SymbolEdge, error) {
	e.mu.Lock()
	syms, refs, err := e.inner.ExtractAll(ctx, relPath, content)
	e.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	defs := make([]core.SymbolDef, 0, len(syms))
	for _, s := range syms {
		defs = append(defs, core.SymbolDef{
			Name: s.Name, Kind: string(s.Kind), Line: s.Line, EndLine: s.EndLine, Signature: s.Signature,
		})
	}
	// Only call references become edges; read/write refs are out of scope for
	// the v2 model (issue #9 non-goal — grepai refs stays v1).
	var edges []core.SymbolEdge
	seen := map[core.SymbolEdge]bool{}
	for _, r := range refs {
		if r.Kind != "" && r.Kind != trace.RefKindCall {
			continue
		}
		if r.CallerName == "" || r.SymbolName == "" {
			continue
		}
		ed := core.SymbolEdge{Caller: r.CallerName, Callee: r.SymbolName, Line: r.Line}
		if !seen[ed] {
			seen[ed] = true
			edges = append(edges, ed)
		}
	}
	return defs, edges, nil
}
