// Package traceview assembles v1 trace.TraceResult values from daemon trace
// responses (issue #20 parity), shared by the CLI and the MCP server so both
// render byte-compatible v1 output from one code path.
package traceview

import (
	"fmt"
	"sort"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
	"github.com/yoanbernabeu/grepai/trace"
)

// CheckProtocol validates that the responding daemon serves the current trace
// protocol. The initial #9 daemon set only Served; a pre-trace daemon sets
// neither — both must be rejected loudly instead of rendering degraded output
// from zero-valued fields.
func CheckProtocol(resp service.TraceResponse) error {
	if !resp.Served || resp.Protocol < service.TraceProtocolCurrent {
		return fmt.Errorf("the running grepaid daemon predates this trace protocol (got %d, need %d); restart it with `grepai daemon stop` — the next command auto-starts the new binary", resp.Protocol, service.TraceProtocolCurrent)
	}
	return nil
}

// Assemble builds a v1 trace.TraceResult from a daemon trace response,
// mirroring v1's assembly (pickBestTargetSymbol for the callers target,
// pickBestSymbolForFile for caller resolution, first-definition for callees
// and graph nodes). Pure. mode is stamped verbatim (v1 used the --mode flag
// value; the daemon extracts in fast mode).
func Assemble(symbol, direction string, depth int, mode string, resp service.TraceResponse) trace.TraceResult {
	symbols := SymbolsToV1(resp.Definitions)
	// The queried symbol's own definitions live in Definitions, not Related —
	// a self-caller (recursion, or the extractor attributing a call site to a
	// same-named symbol in another file) must resolve like any other endpoint,
	// exactly as v1's LookupSymbol did.
	related := func(name string) []trace.Symbol {
		if name == symbol {
			return symbols
		}
		return SymbolsToV1(resp.Related[name])
	}

	switch direction {
	case service.TraceCallees:
		result := trace.TraceResult{Query: symbol, Mode: mode}
		if len(symbols) == 0 {
			return result
		}
		result.Symbol = &symbols[0]
		for _, e := range resp.Edges {
			calleeSym := trace.Symbol{Name: e.Callee}
			if calleeSyms := related(e.Callee); len(calleeSyms) > 0 {
				calleeSym = calleeSyms[0]
			}
			result.Callees = append(result.Callees, trace.CalleeInfo{
				Symbol:   calleeSym,
				CallSite: trace.CallSite{File: e.Path, Line: e.Line, Context: e.Context},
			})
		}
		return result

	case service.TraceGraph:
		g := &trace.CallGraph{Root: symbol, Nodes: map[string]trace.Symbol{}, Edges: []trace.CallEdge{}, Depth: depth}
		if len(symbols) > 0 {
			g.Nodes[symbol] = symbols[0]
		}
		for _, e := range resp.Edges {
			g.Edges = append(g.Edges, trace.CallEdge{Caller: e.Caller, Callee: e.Callee, File: e.Path, Line: e.Line})
			for _, n := range []string{e.Caller, e.Callee} {
				if _, ok := g.Nodes[n]; !ok {
					if syms := related(n); len(syms) > 0 {
						g.Nodes[n] = syms[0]
					}
				}
			}
		}
		return trace.TraceResult{Query: symbol, Mode: mode, Graph: g}

	default: // service.TraceCallers
		result := trace.TraceResult{Query: symbol, Mode: mode}
		if len(symbols) == 0 {
			return result
		}
		refs := make([]trace.Reference, 0, len(resp.Edges))
		for _, e := range resp.Edges {
			refs = append(refs, trace.Reference{
				SymbolName: e.Callee, Kind: trace.RefKindCall,
				File: e.Path, Line: e.Line, Context: e.Context,
				CallerName: e.Caller, CallerFile: e.Path,
			})
		}
		target := PickBestTargetSymbol(symbols, refs)
		if target == nil {
			target = &symbols[0]
		}
		result.Symbol = target
		for _, e := range resp.Edges {
			callerSym := trace.Symbol{Name: e.Caller, File: e.Path}
			if callerSyms := related(e.Caller); len(callerSyms) > 0 {
				if picked := PickBestSymbolForFile(callerSyms, e.Path); picked != nil {
					callerSym = *picked
				} else {
					callerSym = callerSyms[0]
				}
			}
			result.Callers = append(result.Callers, trace.CallerInfo{
				Symbol:   callerSym,
				CallSite: trace.CallSite{File: e.Path, Line: e.Line, Context: e.Context},
			})
		}
		return result
	}
}

// SymbolsToV1 converts daemon symbol definitions to v1 trace.Symbol values
// (File <- Path; feature_path stays empty — RPG is not daemon-served).
func SymbolsToV1(defs []core.SymbolAt) []trace.Symbol {
	out := make([]trace.Symbol, 0, len(defs))
	for _, d := range defs {
		out = append(out, trace.Symbol{
			Name: d.Name, Kind: trace.SymbolKind(d.Kind), File: d.Path,
			Line: d.Line, EndLine: d.EndLine, Signature: d.Signature,
			Receiver: d.Receiver, Package: d.Package, Exported: d.Exported,
			Language: d.Language, Docstring: d.Docstring,
		})
	}
	return out
}

// PickBestTargetSymbol mirrors v1's target pick for ambiguous names: prefer
// the definition most referenced from its own file; deterministic file/line
// ordering when all scores tie.
func PickBestTargetSymbol(candidates []trace.Symbol, refs []trace.Reference) *trace.Symbol {
	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		return &candidates[0]
	}

	bestIdx := 0
	bestScore := -1
	for i, sym := range candidates {
		score := 0
		for _, ref := range refs {
			// Prefer symbols referenced from their own file first (component-local/private usage).
			if ref.File == sym.File || ref.CallerFile == sym.File {
				score++
			}
		}
		if score > bestScore {
			bestScore = score
			bestIdx = i
		}
	}

	// Deterministic fallback when all scores are equal.
	if bestScore <= 0 {
		sorted := append([]trace.Symbol(nil), candidates...)
		sort.SliceStable(sorted, func(i, j int) bool {
			if sorted[i].File == sorted[j].File {
				return sorted[i].Line < sorted[j].Line
			}
			return sorted[i].File < sorted[j].File
		})
		return &sorted[0]
	}

	return &candidates[bestIdx]
}

// PickBestSymbolForFile prefers the candidate defined in preferredFile,
// falling back to the first candidate (v1's caller-resolution rule).
func PickBestSymbolForFile(candidates []trace.Symbol, preferredFile string) *trace.Symbol {
	if len(candidates) == 0 {
		return nil
	}
	if preferredFile != "" {
		for i := range candidates {
			if candidates[i].File == preferredFile {
				return &candidates[i]
			}
		}
	}
	return &candidates[0]
}
