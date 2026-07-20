//go:build treesitter

package symbols

import "github.com/yoanbernabeu/grepai/trace"

// newInner selects tree-sitter when built with -tags treesitter, falling back
// to regex if initialization fails.
func newInner() trace.SymbolExtractor {
	if ts, err := trace.NewTreeSitterExtractor(); err == nil {
		return ts
	}
	return trace.NewRegexExtractor()
}
