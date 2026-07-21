package mcp

import (
	"context"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/yoanbernabeu/grepai/config"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/daemonctl"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
	"github.com/yoanbernabeu/grepai/internal/enginev2/traceview"
	"github.com/yoanbernabeu/grepai/search"
	"github.com/yoanbernabeu/grepai/stats"
	"github.com/yoanbernabeu/grepai/trace"
)

// daemonBackend is the query seam between the MCP handlers and the grepaid
// daemon in engine:v2 mode. Query-only (invariant 3): the sole mutation is the
// idempotent Register that resolves the served root's worktree id. A fake
// implementation backs the handler tests.
type daemonBackend interface {
	Search(ctx context.Context, query string, limit int, pathPrefix string) (service.SearchResponse, error)
	Trace(ctx context.Context, symbol, direction string, depth int) (service.TraceResponse, error)
	Status(ctx context.Context) (service.StatusResponse, error)
}

// mcpDaemonDialTimeout bounds the lazy daemon start on a tool call.
const mcpDaemonDialTimeout = 8 * time.Second

// rpcBackend is the production daemonBackend: it dials per tool call (cheap on
// a Unix socket, and robust to the daemon restarting under a long-lived MCP
// server) and registers the served root each time (idempotent).
type rpcBackend struct {
	root string
}

func (b *rpcBackend) withClient(ctx context.Context, f func(ctx context.Context, c serviceCaller, wt core.WorktreeID) error) error {
	client, err := daemonctl.Connect(ctx, mcpDaemonDialTimeout)
	if err != nil {
		return err
	}
	defer client.Close()
	reg, err := client.Register(ctx, service.RegisterRequest{Root: b.root})
	if err != nil {
		return err
	}
	return f(ctx, client, reg.WorktreeID)
}

// serviceCaller is the slice of the rpc client the backend uses.
type serviceCaller interface {
	Search(ctx context.Context, req service.SearchRequest) (service.SearchResponse, error)
	Trace(ctx context.Context, req service.TraceRequest) (service.TraceResponse, error)
	Status(ctx context.Context, req service.StatusRequest) (service.StatusResponse, error)
}

func (b *rpcBackend) Search(ctx context.Context, query string, limit int, pathPrefix string) (service.SearchResponse, error) {
	var out service.SearchResponse
	err := b.withClient(ctx, func(ctx context.Context, c serviceCaller, wt core.WorktreeID) error {
		var cerr error
		out, cerr = c.Search(ctx, service.SearchRequest{WorktreeID: wt, Query: query, Limit: limit, PathPrefix: pathPrefix})
		return cerr
	})
	return out, err
}

func (b *rpcBackend) Trace(ctx context.Context, symbol, direction string, depth int) (service.TraceResponse, error) {
	var out service.TraceResponse
	err := b.withClient(ctx, func(ctx context.Context, c serviceCaller, wt core.WorktreeID) error {
		var cerr error
		out, cerr = c.Trace(ctx, service.TraceRequest{WorktreeID: wt, Symbol: symbol, Direction: direction, Depth: depth})
		return cerr
	})
	return out, err
}

func (b *rpcBackend) Status(ctx context.Context) (service.StatusResponse, error) {
	var out service.StatusResponse
	err := b.withClient(ctx, func(ctx context.Context, c serviceCaller, wt core.WorktreeID) error {
		var cerr error
		out, cerr = c.Status(ctx, service.StatusRequest{WorktreeID: wt})
		return cerr
	})
	return out, err
}

// V2IndexStatus is grepai_index_status's shape under engine:v2. File/chunk
// counts are a #11 (stats) follow-up; freshness and backlog are what agents
// act on today.
type V2IndexStatus struct {
	Engine           string `json:"engine"`
	ActiveGeneration int64  `json:"active_generation"`
	Fresh            bool   `json:"fresh"`
	PendingJobs      int    `json:"pending_jobs"`
	DeadLetters      int    `json:"dead_letters"`
}

// errV2ToolResult is the loud per-call rejection for v1-only features under
// engine:v2 (refs/RPG read retired v1 stores; workspace serving is v1-based).
func errV2ToolResult(feature, alternative string) *mcp.CallToolResult {
	msg := fmt.Sprintf("%s is not available under engine: v2 (the v1 stores it reads are retired on this repo)", feature)
	if alternative != "" {
		msg += "; " + alternative
	}
	return mcp.NewToolResultError(msg)
}

// handleSearchDaemon serves grepai_search from the daemon (engine:v2).
func (s *Server) handleSearchDaemon(ctx context.Context, query string, limit int, compact bool, format, path, workspace, projects string) (*mcp.CallToolResult, error) {
	if workspace != "" || projects != "" {
		return errV2ToolResult("workspace/project search", "use `grepai search-all` (CLI) for cross-repo search"), nil
	}
	// Normalize --path exactly as the CLI does (absolute paths become
	// root-relative prefixes); the daemon matches repo-relative paths.
	pathPrefix, err := search.NormalizeProjectPathPrefix(path, s.projectRoot)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid path parameter: %v", err)), nil
	}
	resp, err := s.daemon.Search(ctx, query, limit, pathPrefix)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("daemon search failed: %v", err)), nil
	}

	var data any
	if compact {
		out := make([]SearchResultCompact, 0, len(resp.Results))
		for _, h := range resp.Results {
			out = append(out, SearchResultCompact{FilePath: h.Path, StartLine: h.StartLine, EndLine: h.EndLine, Score: h.Score})
		}
		data = out
	} else {
		out := make([]SearchResult, 0, len(resp.Results))
		for _, h := range resp.Results {
			out = append(out, SearchResult{FilePath: h.Path, StartLine: h.StartLine, EndLine: h.EndLine, Score: h.Score, Content: h.Content})
		}
		data = out
	}
	output, err := encodeOutput(data, format)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to encode results: %v", err)), nil
	}
	s.recordMCPStats(stats.Search, mcpOutputMode(compact, format), len(resp.Results), output)
	return mcp.NewToolResultText(output), nil
}

// handleTraceDaemon serves the three trace tools from the daemon (engine:v2),
// rendering the shared v1-parity assembly (traceview) in the same shapes the
// v1 MCP handlers emit.
func (s *Server) handleTraceDaemon(ctx context.Context, symbol, direction string, depth int, compact bool, format, workspace string) (*mcp.CallToolResult, error) {
	if workspace != "" {
		return errV2ToolResult("workspace/project trace", ""), nil
	}
	resp, err := s.daemon.Trace(ctx, symbol, direction, depth)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("daemon trace failed: %v", err)), nil
	}
	if perr := traceview.CheckProtocol(resp); perr != nil {
		return mcp.NewToolResultError(perr.Error()), nil
	}
	result := traceview.Assemble(symbol, direction, depth, "fast", resp)

	var data any = result
	var statType string
	var count int
	switch direction {
	case service.TraceCallees:
		statType, count = stats.TraceCallees, len(result.Callees)
		if compact {
			out := struct {
				Query   string              `json:"query"`
				Mode    string              `json:"mode"`
				Symbol  *trace.Symbol       `json:"symbol,omitempty"`
				Callees []CalleeInfoCompact `json:"callees,omitempty"`
			}{Query: result.Query, Mode: result.Mode, Symbol: result.Symbol}
			for _, c := range result.Callees {
				out.Callees = append(out.Callees, CalleeInfoCompact{Symbol: c.Symbol, CallSite: CallSiteCompact{File: c.CallSite.File, Line: c.CallSite.Line}})
			}
			data = out
		}
	case service.TraceGraph:
		statType = stats.TraceGraph
		if result.Graph != nil {
			count = len(result.Graph.Nodes)
		}
	default:
		statType, count = stats.TraceCallers, len(result.Callers)
		if compact {
			out := struct {
				Query   string              `json:"query"`
				Mode    string              `json:"mode"`
				Symbol  *trace.Symbol       `json:"symbol,omitempty"`
				Callers []CallerInfoCompact `json:"callers,omitempty"`
			}{Query: result.Query, Mode: result.Mode, Symbol: result.Symbol}
			for _, c := range result.Callers {
				out.Callers = append(out.Callers, CallerInfoCompact{Symbol: c.Symbol, CallSite: CallSiteCompact{File: c.CallSite.File, Line: c.CallSite.Line}})
			}
			data = out
		}
	}
	output, err := encodeOutput(data, format)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to encode results: %v", err)), nil
	}
	s.recordMCPStats(statType, mcpOutputMode(compact, format), count, output)
	return mcp.NewToolResultText(output), nil
}

// handleIndexStatusDaemon serves grepai_index_status from the daemon.
func (s *Server) handleIndexStatusDaemon(ctx context.Context, format, workspace string) (*mcp.CallToolResult, error) {
	if workspace != "" {
		return errV2ToolResult("workspace index status", ""), nil
	}
	st, err := s.daemon.Status(ctx)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("daemon status failed: %v", err)), nil
	}
	status := V2IndexStatus{
		Engine:           "v2",
		ActiveGeneration: int64(st.ActiveGeneration),
		Fresh:            st.Fresh,
		PendingJobs:      st.Pending,
		DeadLetters:      st.DeadLetters,
	}
	output, err := encodeOutput(status, format)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to encode status: %v", err)), nil
	}
	return mcp.NewToolResultText(output), nil
}

// rejectV2WorkspaceMembers closes the unscoped-mode gap the #8 review flagged:
// tool calls can select a workspace at runtime, bypassing the startup gate. A
// dynamically selected workspace containing engine:v2 members must reject as
// loudly as startup would — its members' v1 stores are retired and would serve
// empty results silently. Returns nil when the workspace is safe (or unknown —
// the store-load path reports that better).
func (s *Server) rejectV2WorkspaceMembers(workspaceName string) *mcp.CallToolResult {
	wsCfg, err := config.LoadWorkspaceConfig()
	if err != nil || wsCfg == nil {
		return nil
	}
	ws, err := wsCfg.GetWorkspace(workspaceName)
	if err != nil {
		return nil
	}
	var v2Members []string
	for _, p := range ws.Projects {
		if cfg, lerr := config.Load(p.Path); lerr == nil && cfg.EngineV2() {
			v2Members = append(v2Members, p.Name)
		}
	}
	if len(v2Members) > 0 {
		return mcp.NewToolResultError(fmt.Sprintf("workspace %q includes engine: v2 project(s) %v whose v1 indexes are retired — serving them would return empty results silently; use `grepai search-all` (CLI) or remove those projects from the workspace (ibreakshit/grepai#10)", workspaceName, v2Members))
	}
	return nil
}
