package cli

import (
	"path/filepath"
	"strings"
	"testing"

	"encoding/json"
	"github.com/yoanbernabeu/grepai/config"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
)

func TestRepoEngineV2RoutingDetection(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)

	cfg := config.DefaultConfig()

	// engine: v2 -> daemon path.
	cfg.Engine = "v2"
	if err := cfg.Save(dir); err != nil {
		t.Fatalf("save v2: %v", err)
	}
	if _, v2, err := repoEngineV2(); err != nil || !v2 {
		t.Fatalf("engine:v2 config must route to the daemon path (v2=%v err=%v)", v2, err)
	}

	// engine: v1 -> v1 path (no daemon).
	cfg.Engine = "v1"
	if err := cfg.Save(dir); err != nil {
		t.Fatalf("save v1: %v", err)
	}
	if _, v2, err := repoEngineV2(); err != nil || v2 {
		t.Fatalf("engine:v1 must NOT route to the daemon path (v2=%v err=%v)", v2, err)
	}

	// unset engine -> v1 default (no daemon).
	cfg.Engine = ""
	if err := cfg.Save(dir); err != nil {
		t.Fatalf("save default: %v", err)
	}
	if _, v2, err := repoEngineV2(); err != nil || v2 {
		t.Fatalf("unset engine must default to v1, no daemon (v2=%v err=%v)", v2, err)
	}
}

// TestTopLevelSearchEngineGating is the load-bearing invariant test: with
// engine:v1 the top-level `grepai search` command must NEVER reach the daemon
// path; with engine:v2 it MUST, and a missing daemon fails loudly (no v1
// fallback). We detect "took the daemon path" by making the daemon unreachable
// and un-locatable so that path fails with a distinctive "grepaid not found".
func TestTopLevelSearchEngineGating(t *testing.T) {
	// No grepaid on PATH and a bogus socket: if (and only if) the daemon path is
	// taken, ensureDaemonClient fails with "grepaid not found".
	t.Setenv("PATH", t.TempDir())
	t.Setenv("XDG_RUNTIME_DIR", "")

	newRepo := func(engine string) string {
		dir := t.TempDir()
		cfg := config.DefaultConfig()
		cfg.Engine = engine
		if err := cfg.Save(dir); err != nil {
			t.Fatalf("save cfg: %v", err)
		}
		return dir
	}

	// engine:v1 -> must NOT take the daemon path (never "grepaid not found").
	t.Chdir(newRepo("v1"))
	t.Setenv("GREPAID_SOCKET", filepath.Join(t.TempDir(), "none.sock"))
	if err := runSearch(searchCmd, []string{"query"}); err != nil && strings.Contains(err.Error(), "grepaid not found") {
		t.Fatalf("engine:v1 wrongly dialed the daemon: %v", err)
	}

	// engine:v2 -> MUST take the daemon path and fail loudly when it can't start.
	t.Chdir(newRepo("v2"))
	err := runSearch(searchCmd, []string{"query"})
	if err == nil {
		t.Fatal("engine:v2 with no daemon should fail loudly, got nil")
	}
	if !strings.Contains(err.Error(), "grepaid not found") {
		t.Fatalf("engine:v2 should take the daemon path and fail loudly with grepaid-not-found; got: %v", err)
	}
}

// TestBuildTraceResultV1Shape asserts the daemon trace response assembles into
// v1's exact TraceResult JSON surface (issue #20 parity): lowercase v1 keys,
// resolved caller/callee symbols, call-site context, graph node table.
func TestBuildTraceResultV1Shape(t *testing.T) {
	resp := service.TraceResponse{
		Served: true,
		Definitions: []core.SymbolAt{{
			Path: "store/store.go", Name: "Get", Kind: "method", Line: 10, EndLine: 20,
			Signature: "func (s *Store) Get(k string) string", Receiver: "*Store",
			Package: "store", Exported: true, Language: "go", Docstring: "Get returns k.",
		}},
		Edges: []core.EdgeAt{{Caller: "HandleReq", Callee: "Get", Path: "api/handler.go", Line: 42, Context: "\tv := s.Get(k)"}},
		Related: map[string][]core.SymbolAt{
			"HandleReq": {{Path: "api/handler.go", Name: "HandleReq", Kind: "function", Line: 30, Language: "go", Exported: true}},
		},
	}

	result, view, _, count := buildTraceResult("Get", service.TraceCallers, 0, resp)
	if view != traceViewCallers || count != 1 {
		t.Fatalf("view/count wrong: %v %d", view, count)
	}
	if result.Symbol == nil || result.Symbol.Receiver != "*Store" || !result.Symbol.Exported ||
		result.Symbol.Docstring != "Get returns k." || result.Symbol.File != "store/store.go" {
		t.Fatalf("target symbol fields wrong: %+v", result.Symbol)
	}
	if len(result.Callers) != 1 {
		t.Fatalf("callers: %+v", result.Callers)
	}
	c := result.Callers[0]
	if c.Symbol.Name != "HandleReq" || c.Symbol.Line != 30 || c.CallSite.Context != "\tv := s.Get(k)" ||
		c.CallSite.File != "api/handler.go" || c.CallSite.Line != 42 {
		t.Fatalf("caller resolution wrong: %+v", c)
	}

	// JSON keys must be v1's (lowercase snake_case from trace.TraceResult tags).
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"query"`, `"mode"`, `"symbol"`, `"callers"`, `"call_site"`, `"receiver"`, `"docstring"`, `"context"`, `"exported"`} {
		if !strings.Contains(string(raw), key) {
			t.Fatalf("v1 JSON key %s missing in %s", key, raw)
		}
	}
	for _, key := range []string{`"Path"`, `"Definitions"`, `"Edges"`} {
		if strings.Contains(string(raw), key) {
			t.Fatalf("v2-shaped key %s leaked into output: %s", key, raw)
		}
	}

	// Graph: node table includes root and resolved endpoints; edges are v1 CallEdge.
	gRes, gView, _, gCount := buildTraceResult("Get", service.TraceGraph, 3, resp)
	if gView != traceViewGraph || gRes.Graph == nil {
		t.Fatalf("graph missing: %+v", gRes)
	}
	if gRes.Graph.Root != "Get" || gRes.Graph.Depth != 3 || gCount != len(gRes.Graph.Nodes) {
		t.Fatalf("graph meta wrong: %+v", gRes.Graph)
	}
	if _, ok := gRes.Graph.Nodes["Get"]; !ok {
		t.Fatal("root node missing")
	}
	if n, ok := gRes.Graph.Nodes["HandleReq"]; !ok || n.Line != 30 {
		t.Fatalf("endpoint node unresolved: %+v", gRes.Graph.Nodes)
	}
	if len(gRes.Graph.Edges) != 1 || gRes.Graph.Edges[0].File != "api/handler.go" {
		t.Fatalf("graph edges wrong: %+v", gRes.Graph.Edges)
	}

	// Callees: first-definition resolution (v1 semantics), unknown callee falls
	// back to a bare name.
	respCallees := resp
	respCallees.Edges = []core.EdgeAt{{Caller: "Get", Callee: "unknownFn", Path: "store/store.go", Line: 12, Context: "\tunknownFn()"}}
	cRes, _, _, _ := buildTraceResult("Get", service.TraceCallees, 0, respCallees)
	if len(cRes.Callees) != 1 || cRes.Callees[0].Symbol.Name != "unknownFn" || cRes.Callees[0].Symbol.File != "" {
		t.Fatalf("callee fallback wrong: %+v", cRes.Callees)
	}

	// Empty definitions: v1 renders an empty result (Symbol nil), never errors.
	eRes, _, _, eCount := buildTraceResult("Nope", service.TraceCallers, 0, service.TraceResponse{Served: true})
	if eRes.Symbol != nil || eCount != 0 {
		t.Fatalf("empty result wrong: %+v", eRes)
	}
}
