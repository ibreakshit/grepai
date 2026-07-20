//go:build !treesitter

package symbols

import "github.com/yoanbernabeu/grepai/trace"

// newInner selects the regex extractor: the default CGO_ENABLED=0 build has no
// tree-sitter (cgo), matching v1's shipped behavior.
func newInner() trace.SymbolExtractor { return trace.NewRegexExtractor() }
