package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/yoanbernabeu/grepai/config"
	"github.com/yoanbernabeu/grepai/mcp"
)

var mcpServeCmd = &cobra.Command{
	Use:   "mcp-serve [project-path]",
	Short: "Start grepai as an MCP server",
	Long: `Start grepai as an MCP (Model Context Protocol) server.

This allows AI agents to use grepai as a native tool through the MCP protocol.
The server communicates via stdio and exposes the following tools:

  - grepai_search: Semantic code search with natural language (includes RPG context when enabled)
  - grepai_trace_callers: Find all functions that call a symbol
  - grepai_trace_callees: Find all functions called by a symbol
  - grepai_trace_graph: Build a call graph around a symbol
  - grepai_refs_readers: Find property/state readers for a symbol name
  - grepai_refs_writers: Find property/state writers for a symbol name
  - grepai_refs_graph: Build a property usage graph (readers + writers)
  - grepai_index_status: Check index health and statistics (includes RPG stats when enabled)
  - grepai_rpg_search: Search RPG graph nodes by feature semantics
  - grepai_rpg_fetch: Fetch hierarchy and edge context for a specific RPG node
  - grepai_rpg_explore: Traverse RPG graph neighborhoods with direction/depth filters

Arguments:
  project-path  Optional path to the grepai project directory.
                If not provided, searches for .grepai from current directory.

Flags:
  --workspace   Workspace name. When set, serves using workspace config from
                ~/.grepai/workspace.yaml without requiring local .grepai/.

Configuration for Claude Code:
  claude mcp add grepai -- grepai mcp-serve
  claude mcp add grepai -- grepai mcp-serve --workspace myworkspace

Configuration for Cursor (.cursor/mcp.json):
  {
    "mcpServers": {
      "grepai": {
        "command": "grepai",
        "args": ["mcp-serve"]
      }
    }
  }

Configuration for Cursor with explicit path (recommended for Windows):
  {
    "mcpServers": {
      "grepai": {
        "command": "grepai",
        "args": ["mcp-serve", "/path/to/your/project"]
      }
    }
  }`,
	Args: cobra.MaximumNArgs(1),
	RunE: runMCPServe,
}

func init() {
	mcpServeCmd.Flags().String("workspace", "", "Workspace name for workspace-only mode (no local .grepai/ required)")
	rootCmd.AddCommand(mcpServeCmd)
}

// resolveMCPTarget determines the project root and/or workspace for the MCP server.
// Returns (projectRoot, workspaceName, error).
// projectRoot may be empty when in workspace-only mode.
func resolveMCPTarget(explicitPath, workspaceName string) (string, string, error) {
	// Priority 1: Explicit --workspace flag
	if workspaceName != "" {
		cfg, err := config.LoadWorkspaceConfig()
		if err != nil {
			return "", "", fmt.Errorf("failed to load workspace config: %w", err)
		}
		if cfg == nil {
			return "", "", fmt.Errorf("no workspace config found at ~/.grepai/workspace.yaml")
		}
		if _, err := cfg.GetWorkspace(workspaceName); err != nil {
			return "", "", fmt.Errorf("workspace %q not found", workspaceName)
		}

		// Check if cwd has local config (optional, for trace tools)
		projectRoot := ""
		if pr, err := config.FindProjectRoot(); err == nil {
			projectRoot = pr
		}

		return projectRoot, workspaceName, nil
	}

	// Priority 2: Explicit project path argument
	if explicitPath != "" {
		if !filepath.IsAbs(explicitPath) {
			abs, err := filepath.Abs(explicitPath)
			if err != nil {
				return "", "", fmt.Errorf("failed to resolve path: %w", err)
			}
			explicitPath = abs
		}
		if !config.Exists(explicitPath) {
			return "", "", fmt.Errorf("no grepai project found at %s (run 'grepai init' first)", explicitPath)
		}
		return explicitPath, "", nil
	}

	// Priority 3: FindProjectRoot (walk upward from cwd)
	projectRoot, err := config.FindProjectRoot()
	if err == nil {
		return projectRoot, "", nil
	}

	// Priority 4: Auto-detect workspace from cwd
	cwd, cwdErr := os.Getwd()
	if cwdErr != nil {
		return "", "", fmt.Errorf("failed to find project root: %w", err)
	}

	wsName, ws, wsErr := config.FindWorkspaceForPath(cwd)
	if wsErr != nil {
		// If workspace config exists with at least one workspace, allow starting
		// unscoped MCP server and let tools accept workspace at runtime.
		cfg, cfgErr := config.LoadWorkspaceConfig()
		if cfgErr == nil && cfg != nil && len(cfg.Workspaces) > 0 {
			return "", "", nil
		}
		return "", "", fmt.Errorf("no grepai project or workspace found (run 'grepai init' or use --workspace)")
	}
	if ws != nil {
		return "", wsName, nil
	}

	// No containing workspace for cwd, but still allow startup if global
	// workspace config has entries (runtime workspace argument can be used).
	cfg, cfgErr := config.LoadWorkspaceConfig()
	if cfgErr == nil && cfg != nil && len(cfg.Workspaces) > 0 {
		return "", "", nil
	}

	return "", "", fmt.Errorf("no grepai project or workspace found (run 'grepai init' or use --workspace)")
}

// rejectEngineV2ForMCP enforces the loud-failure contract for MCP workspace
// mode: workspace serving reads v1 stores, and an engine:v2 member's v1 index
// is retired — serving it would return empty results with no error (issue #8).
// A LOCAL engine:v2 project is fine (it gets the daemon-served MCP server,
// issue #10); only workspaces containing v2 members refuse to start. A project
// whose config cannot be loaded is left to fail downstream exactly as before.
func rejectEngineV2ForMCP(projectRoot, wsName string) error {
	if wsName != "" {
		wcfg, err := config.LoadWorkspaceConfig()
		if err != nil || wcfg == nil {
			return nil // workspace resolution already validated upstream
		}
		ws, err := wcfg.GetWorkspace(wsName)
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
			return fmt.Errorf("mcp-serve is not supported for workspace %q: member project(s) %v use engine: v2 and their v1 indexes are retired — serving them would return empty results silently. Use `grepai search-all`, or remove those projects from the workspace (ibreakshit/grepai#10)", wsName, v2Members)
		}
	}
	return nil
}

func runMCPServe(cmd *cobra.Command, args []string) error {
	workspaceFlag, _ := cmd.Flags().GetString("workspace")

	var explicitPath string
	if len(args) > 0 {
		explicitPath = args[0]
	}

	projectRoot, wsName, err := resolveMCPTarget(explicitPath, workspaceFlag)
	if err != nil {
		return err
	}
	if err := rejectEngineV2ForMCP(projectRoot, wsName); err != nil {
		return err
	}

	var srv *mcp.Server
	switch {
	case wsName != "":
		srv, err = mcp.NewServerWithWorkspace(projectRoot, wsName)
	case projectRootIsEngineV2(projectRoot):
		// engine:v2 — query tools served from the grepaid daemon (issue #10).
		srv, err = mcp.NewServerV2(projectRoot)
	default:
		srv, err = mcp.NewServer(projectRoot)
	}
	if err != nil {
		return fmt.Errorf("failed to create MCP server: %w", err)
	}

	return srv.Serve()
}

// projectRootIsEngineV2 reports whether the local project is on the v2 engine.
// A missing/broken config reads as v1 (the classic server surfaces the error
// exactly as before).
func projectRootIsEngineV2(projectRoot string) bool {
	if projectRoot == "" {
		return false
	}
	cfg, err := config.Load(projectRoot)
	return err == nil && cfg.EngineV2()
}
