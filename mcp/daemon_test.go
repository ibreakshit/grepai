package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/yoanbernabeu/grepai/config"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
)

// fakeBackend implements daemonBackend for handler tests.
type fakeBackend struct {
	searchResp service.SearchResponse
	traceResp  service.TraceResponse
	statusResp service.StatusResponse
	err        error

	lastQuery     string
	lastLimit     int
	lastPrefix    string
	lastSymbol    string
	lastDirection string
	lastDepth     int
}

func (f *fakeBackend) Search(_ context.Context, query string, limit int, pathPrefix string) (service.SearchResponse, error) {
	f.lastQuery, f.lastLimit, f.lastPrefix = query, limit, pathPrefix
	return f.searchResp, f.err
}

func (f *fakeBackend) Trace(_ context.Context, symbol, direction string, depth int) (service.TraceResponse, error) {
	f.lastSymbol, f.lastDirection, f.lastDepth = symbol, direction, depth
	return f.traceResp, f.err
}

func (f *fakeBackend) Status(_ context.Context) (service.StatusResponse, error) {
	return f.statusResp, f.err
}

func newV2TestServer(t *testing.T, fb *fakeBackend) *Server {
	t.Helper()
	root := t.TempDir()
	s, err := NewServerV2(root)
	if err != nil {
		t.Fatal(err)
	}
	s.daemon = fb
	// Stats recording is fire-and-forget (recordMCPStats spawns a goroutine)
	// and would race t.TempDir cleanup; nil disables it deterministically.
	s.recorder = nil
	return s
}

func toolRequest(args map[string]any) mcp.CallToolRequest {
	return mcp.CallToolRequest{Params: mcp.CallToolParams{Arguments: args}}
}

func v2TraceResp() service.TraceResponse {
	return service.TraceResponse{
		Served:   true,
		Protocol: service.TraceProtocolCurrent,
		Definitions: []core.SymbolAt{{
			Path: "store/store.go", Name: "Get", Kind: "method", Line: 10,
			Signature: "func (s *Store) Get(k string) string", Exported: true, Language: "go",
		}},
		Edges: []core.EdgeAt{{Caller: "HandleReq", Callee: "Get", Path: "api/handler.go", Line: 42, Context: "\tv := s.Get(k)"}},
		Related: map[string][]core.SymbolAt{
			"HandleReq": {{Path: "api/handler.go", Name: "HandleReq", Kind: "function", Line: 30, Language: "go"}},
		},
	}
}

func TestV2SearchServedFromDaemon(t *testing.T) {
	fb := &fakeBackend{searchResp: service.SearchResponse{
		Results: []core.SearchHit{{Path: "a.go", StartLine: 1, EndLine: 5, Score: 0.9, Content: "func A() {}"}},
		Fresh:   true,
	}}
	s := newV2TestServer(t, fb)
	res, err := s.handleSearch(context.Background(), toolRequest(map[string]any{"query": "alpha", "limit": 3.0}))
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("unexpected error result: %+v", res)
	}
	if fb.lastQuery != "alpha" || fb.lastLimit != 3 {
		t.Fatalf("daemon not called with tool params: %q %d", fb.lastQuery, fb.lastLimit)
	}
	payload := textResultPayload(t, res)
	var hits []SearchResult
	if jerr := json.Unmarshal([]byte(payload), &hits); jerr != nil {
		t.Fatalf("output not v1 SearchResult JSON: %v\n%s", jerr, payload)
	}
	if len(hits) != 1 || hits[0].FilePath != "a.go" || hits[0].Content != "func A() {}" {
		t.Fatalf("hit mapping wrong: %+v", hits)
	}
}

func TestV2SearchRejectsWorkspaceParams(t *testing.T) {
	s := newV2TestServer(t, &fakeBackend{})
	res, err := s.handleSearch(context.Background(), toolRequest(map[string]any{"query": "q", "workspace": "acme"}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || !strings.Contains(textResultPayload(t, res), "engine: v2") {
		t.Fatalf("workspace search must reject loudly under v2: %+v", res)
	}
}

func TestV2TraceCallersV1Shape(t *testing.T) {
	fb := &fakeBackend{traceResp: v2TraceResp()}
	s := newV2TestServer(t, fb)
	res, err := s.handleTraceCallers(context.Background(), toolRequest(map[string]any{"symbol": "Get"}))
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("unexpected error: %+v", res)
	}
	if fb.lastSymbol != "Get" || fb.lastDirection != service.TraceCallers {
		t.Fatalf("daemon called wrong: %q %q", fb.lastSymbol, fb.lastDirection)
	}
	payload := textResultPayload(t, res)
	for _, key := range []string{`"query"`, `"symbol"`, `"callers"`, `"call_site"`, `"context"`} {
		if !strings.Contains(payload, key) {
			t.Fatalf("v1 key %s missing: %s", key, payload)
		}
	}
	if !strings.Contains(payload, `"HandleReq"`) || !strings.Contains(payload, `"line": 30`) {
		t.Fatalf("caller not resolved from Related: %s", payload)
	}
}

func TestV2TraceCompactStripsContext(t *testing.T) {
	fb := &fakeBackend{traceResp: v2TraceResp()}
	s := newV2TestServer(t, fb)
	res, err := s.handleTraceCallers(context.Background(), toolRequest(map[string]any{"symbol": "Get", "compact": true}))
	if err != nil {
		t.Fatal(err)
	}
	payload := textResultPayload(t, res)
	if strings.Contains(payload, `"context"`) {
		t.Fatalf("compact output must omit call-site context: %s", payload)
	}
	if !strings.Contains(payload, `"call_site"`) {
		t.Fatalf("compact output must keep call_site file/line: %s", payload)
	}
}

func TestV2TraceGraphNodesAndEdges(t *testing.T) {
	fb := &fakeBackend{traceResp: v2TraceResp()}
	s := newV2TestServer(t, fb)
	res, err := s.handleTraceGraph(context.Background(), toolRequest(map[string]any{"symbol": "Get", "depth": 3.0}))
	if err != nil {
		t.Fatal(err)
	}
	if fb.lastDirection != service.TraceGraph || fb.lastDepth != 3 {
		t.Fatalf("graph call wrong: %q depth=%d", fb.lastDirection, fb.lastDepth)
	}
	payload := textResultPayload(t, res)
	for _, key := range []string{`"graph"`, `"root"`, `"nodes"`, `"edges"`} {
		if !strings.Contains(payload, key) {
			t.Fatalf("v1 graph key %s missing: %s", key, payload)
		}
	}
}

func TestV2TraceRejectsOldDaemon(t *testing.T) {
	fb := &fakeBackend{traceResp: service.TraceResponse{Served: true, Protocol: 0}}
	s := newV2TestServer(t, fb)
	res, err := s.handleTraceCallers(context.Background(), toolRequest(map[string]any{"symbol": "X"}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || !strings.Contains(textResultPayload(t, res), "restart") {
		t.Fatalf("old daemon must be rejected with a restart hint: %+v", res)
	}
}

func TestV2IndexStatusFromDaemon(t *testing.T) {
	zero := 0
	fb := &fakeBackend{statusResp: service.StatusResponse{ActiveGeneration: 2, Pending: 5, Fresh: false, DeadLetters: 1, SymbolsBackfillPending: &zero}}
	s := newV2TestServer(t, fb)
	res, err := s.handleIndexStatus(context.Background(), toolRequest(map[string]any{}))
	if err != nil {
		t.Fatal(err)
	}
	var st V2IndexStatus
	if jerr := json.Unmarshal([]byte(textResultPayload(t, res)), &st); jerr != nil {
		t.Fatal(jerr)
	}
	if st.Engine != "v2" || st.ActiveGeneration != 2 || st.PendingJobs != 5 || st.Fresh || st.DeadLetters != 1 {
		t.Fatalf("status mapping wrong: %+v", st)
	}
}

func TestV2RefsAndRPGRejectLoudly(t *testing.T) {
	s := newV2TestServer(t, &fakeBackend{})
	for name, h := range map[string]func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error){
		"refs_readers": s.handleRefsReaders,
		"refs_writers": s.handleRefsWriters,
		"refs_graph":   s.handleRefsGraph,
		"rpg_search":   s.handleRPGSearch,
		"rpg_fetch":    s.handleRPGFetch,
		"rpg_explore":  s.handleRPGExplore,
	} {
		res, err := h(context.Background(), toolRequest(map[string]any{"symbol": "X", "query": "q", "node_id": "n"}))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !res.IsError || !strings.Contains(textResultPayload(t, res), "engine: v2") {
			t.Fatalf("%s must reject loudly under v2: %+v", name, res)
		}
	}
}

func TestV2DaemonErrorSurfacesLoudly(t *testing.T) {
	fb := &fakeBackend{err: errors.New("daemon unreachable")}
	s := newV2TestServer(t, fb)
	res, err := s.handleSearch(context.Background(), toolRequest(map[string]any{"query": "q"}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || !strings.Contains(textResultPayload(t, res), "daemon unreachable") {
		t.Fatalf("daemon errors must surface, not fall back: %+v", res)
	}
}

// TestV1UnscopedWorkspaceWithV2MemberRejected closes the #8 review's gap: a
// v1-mode MCP server must reject a TOOL-CALL-selected workspace containing
// engine:v2 members (the startup gate only sees the startup workspace).
func TestV1UnscopedWorkspaceWithV2MemberRejected(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)

	v2dir := t.TempDir()
	v2cfg := config.DefaultConfig()
	v2cfg.Engine = "v2"
	if err := v2cfg.Save(v2dir); err != nil {
		t.Fatal(err)
	}
	if err := config.SaveWorkspaceConfig(&config.WorkspaceConfig{
		Version: 1,
		Workspaces: map[string]config.Workspace{
			"acme": {Name: "acme", Projects: []config.ProjectEntry{{Name: "retired", Path: v2dir}}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Plain v1 server (no workspace at startup) — the tool call selects one.
	v1root := t.TempDir()
	s, err := NewServer(v1root)
	if err != nil {
		t.Fatal(err)
	}
	for name, call := range map[string]func() (*mcp.CallToolResult, error){
		"search": func() (*mcp.CallToolResult, error) {
			return s.handleSearch(context.Background(), toolRequest(map[string]any{"query": "q", "workspace": "acme"}))
		},
		"trace_callers": func() (*mcp.CallToolResult, error) {
			return s.handleTraceCallers(context.Background(), toolRequest(map[string]any{"symbol": "X", "workspace": "acme"}))
		},
		"trace_callees": func() (*mcp.CallToolResult, error) {
			return s.handleTraceCallees(context.Background(), toolRequest(map[string]any{"symbol": "X", "workspace": "acme"}))
		},
		"trace_graph": func() (*mcp.CallToolResult, error) {
			return s.handleTraceGraph(context.Background(), toolRequest(map[string]any{"symbol": "X", "workspace": "acme"}))
		},
		"refs_readers": func() (*mcp.CallToolResult, error) {
			return s.handleRefsReaders(context.Background(), toolRequest(map[string]any{"symbol": "X", "workspace": "acme"}))
		},
		"refs_graph": func() (*mcp.CallToolResult, error) {
			return s.handleRefsGraph(context.Background(), toolRequest(map[string]any{"symbol": "X", "workspace": "acme"}))
		},
		"index_status": func() (*mcp.CallToolResult, error) {
			return s.handleIndexStatus(context.Background(), toolRequest(map[string]any{"workspace": "acme"}))
		},
	} {
		res, cerr := call()
		if cerr != nil {
			t.Fatalf("%s: %v", name, cerr)
		}
		if !res.IsError || !strings.Contains(textResultPayload(t, res), "retired") {
			t.Fatalf("%s must reject the v2-member workspace at tool-call time: %+v", name, res)
		}
	}
}

// Codex #10 merge-gate finding 2: a project-only selector must reject, not
// silently query the local repo.
func TestV2TraceRejectsProjectParam(t *testing.T) {
	s := newV2TestServer(t, &fakeBackend{traceResp: v2TraceResp()})
	for name, call := range map[string]func() (*mcp.CallToolResult, error){
		"callers": func() (*mcp.CallToolResult, error) {
			return s.handleTraceCallers(context.Background(), toolRequest(map[string]any{"symbol": "X", "project": "backend"}))
		},
		"callees": func() (*mcp.CallToolResult, error) {
			return s.handleTraceCallees(context.Background(), toolRequest(map[string]any{"symbol": "X", "project": "backend"}))
		},
		"graph": func() (*mcp.CallToolResult, error) {
			return s.handleTraceGraph(context.Background(), toolRequest(map[string]any{"symbol": "X", "project": "backend"}))
		},
	} {
		res, err := call()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !res.IsError || !strings.Contains(textResultPayload(t, res), "engine: v2") {
			t.Fatalf("%s: project param must reject loudly under v2: %+v", name, res)
		}
	}
}

// Codex #10 merge-gate finding 3: backfill state must be visible — in index
// status always, in trace output while pending (and absent at steady state so
// the v1 shape stays byte-identical).
func TestV2BackfillVisibility(t *testing.T) {
	// Index status carries the count.
	pending42 := 42
	fb := &fakeBackend{statusResp: service.StatusResponse{Fresh: true, SymbolsBackfillPending: &pending42}}
	s := newV2TestServer(t, fb)
	res, err := s.handleIndexStatus(context.Background(), toolRequest(map[string]any{}))
	if err != nil {
		t.Fatal(err)
	}
	var st V2IndexStatus
	if jerr := json.Unmarshal([]byte(textResultPayload(t, res)), &st); jerr != nil {
		t.Fatal(jerr)
	}
	if !st.Fresh || st.SymbolsBackfillPending != 42 {
		t.Fatalf("fresh index must still expose pending symbol backfill: %+v", st)
	}

	// Trace: marker present while pending...
	tr := v2TraceResp()
	tr.BackfillPending = 7
	s2 := newV2TestServer(t, &fakeBackend{traceResp: tr})
	res2, err := s2.handleTraceCallers(context.Background(), toolRequest(map[string]any{"symbol": "Get"}))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(textResultPayload(t, res2), `"symbols_backfill_pending": 7`) {
		t.Fatalf("pending backfill must mark trace output: %s", textResultPayload(t, res2))
	}
	// ...and absent at steady state (v1 byte-parity).
	s3 := newV2TestServer(t, &fakeBackend{traceResp: v2TraceResp()})
	res3, err := s3.handleTraceCallers(context.Background(), toolRequest(map[string]any{"symbol": "Get"}))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(textResultPayload(t, res3), "symbols_backfill_pending") {
		t.Fatalf("steady-state trace output must stay v1-identical: %s", textResultPayload(t, res3))
	}
	// Compact carries the marker too.
	s4 := newV2TestServer(t, &fakeBackend{traceResp: tr})
	res4, err := s4.handleTraceCallers(context.Background(), toolRequest(map[string]any{"symbol": "Get", "compact": true}))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(textResultPayload(t, res4), `"symbols_backfill_pending": 7`) {
		t.Fatalf("compact trace must carry the backfill marker: %s", textResultPayload(t, res4))
	}
}

// Codex #10 round-2 finding 1: TOON output must not nest under an embedded
// struct name, must use the exact marker key, and must omit the marker at
// steady state — for full and compact, pending and zero.
func TestV2TraceTOONShapes(t *testing.T) {
	pending := v2TraceResp()
	pending.BackfillPending = 7
	// NOTE: v1's own structs carry ",omitempty" tags that gotoon renders
	// literally — that noise is v1 TOON behavior and thus parity. Only the
	// MARKER key must be clean, and nothing may nest under "TraceResult".
	for name, tc := range map[string]struct {
		resp   service.TraceResponse
		args   map[string]any
		want   []string
		forbid []string
	}{
		"full_zero":       {v2TraceResp(), map[string]any{"symbol": "Get", "format": "toon"}, []string{"query", "callers"}, []string{"symbols_backfill_pending", "TraceResult"}},
		"full_pending":    {pending, map[string]any{"symbol": "Get", "format": "toon"}, []string{"symbols_backfill_pending"}, []string{"TraceResult", "symbols_backfill_pending,omitempty"}},
		"compact_zero":    {v2TraceResp(), map[string]any{"symbol": "Get", "format": "toon", "compact": true}, []string{"call_site"}, []string{"symbols_backfill_pending"}},
		"compact_pending": {pending, map[string]any{"symbol": "Get", "format": "toon", "compact": true}, []string{"symbols_backfill_pending"}, []string{"symbols_backfill_pending,omitempty"}},
	} {
		s := newV2TestServer(t, &fakeBackend{traceResp: tc.resp})
		res, err := s.handleTraceCallers(context.Background(), toolRequest(tc.args))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		payload := textResultPayload(t, res)
		for _, w := range tc.want {
			if !strings.Contains(payload, w) {
				t.Fatalf("%s: %q missing in TOON output:\n%s", name, w, payload)
			}
		}
		for _, f := range tc.forbid {
			if strings.Contains(payload, f) {
				t.Fatalf("%s: %q must not appear in TOON output:\n%s", name, f, payload)
			}
		}
	}
	// Graph pending via JSON keeps the flat marker too.
	s := newV2TestServer(t, &fakeBackend{traceResp: pending})
	res, err := s.handleTraceGraph(context.Background(), toolRequest(map[string]any{"symbol": "Get"}))
	if err != nil {
		t.Fatal(err)
	}
	p := textResultPayload(t, res)
	if !strings.Contains(p, `"graph"`) || !strings.Contains(p, `"symbols_backfill_pending": 7`) {
		t.Fatalf("graph pending output wrong: %s", p)
	}
}

// Codex #10 round-2 finding 2: a resident daemon predating backfill reporting
// (nil sentinel) must be rejected loudly, never read as zero.
func TestV2IndexStatusRejectsOldDaemon(t *testing.T) {
	s := newV2TestServer(t, &fakeBackend{statusResp: service.StatusResponse{Fresh: true}})
	res, err := s.handleIndexStatus(context.Background(), toolRequest(map[string]any{}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || !strings.Contains(textResultPayload(t, res), "restart") {
		t.Fatalf("nil backfill sentinel must demand a restart: %+v", res)
	}
}
