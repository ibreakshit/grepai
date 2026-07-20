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
			Receiver: s.Receiver, Package: s.Package, Exported: s.Exported,
			Language: s.Language, Docstring: s.Docstring,
		})
	}
	// Only call references become edges; read/write refs are out of scope for
	// the v2 model (issue #9 non-goal — grepai refs stays v1). Dedup keys on
	// (caller, callee, line) — context is derived from the line, so it cannot
	// differ within one key.
	var edges []core.SymbolEdge
	type edgeKey struct {
		caller, callee string
		line           int
	}
	seen := map[edgeKey]bool{}
	for _, r := range refs {
		if r.Kind != "" && r.Kind != trace.RefKindCall {
			continue
		}
		if r.CallerName == "" || r.SymbolName == "" {
			continue
		}
		k := edgeKey{r.CallerName, r.SymbolName, r.Line}
		if !seen[k] {
			seen[k] = true
			edges = append(edges, core.SymbolEdge{Caller: r.CallerName, Callee: r.SymbolName, Line: r.Line, Context: r.Context})
		}
	}
	return defs, edges, nil
}
