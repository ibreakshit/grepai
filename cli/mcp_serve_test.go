package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yoanbernabeu/grepai/config"
)

func TestResolveMCPWorkspace(t *testing.T) {
	t.Run("explicit_workspace_flag", func(t *testing.T) {
		tmpDir, _ := os.MkdirTemp("", "grepai-mcp-test")
		defer os.RemoveAll(tmpDir)
		cleanup := setTestHomeDirCLI(t, tmpDir)
		defer cleanup()

		cfg := config.DefaultWorkspaceConfig()
		cfg.AddWorkspace(config.Workspace{
			Name:  "test",
			Store: config.StoreConfig{Backend: "qdrant"},
			Embedder: config.EmbedderConfig{
				Provider: "ollama",
				Model:    "nomic-embed-text",
			},
			Projects: []config.ProjectEntry{
				{Name: "pipeline", Path: filepath.Join(tmpDir, "pipeline")},
			},
		})
		config.SaveWorkspaceConfig(cfg)

		projectRoot, wsName, err := resolveMCPTarget("", "test")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if wsName != "test" {
			t.Errorf("expected workspace test, got %s", wsName)
		}
		_ = projectRoot
	})

	t.Run("explicit_workspace_not_found", func(t *testing.T) {
		tmpDir, _ := os.MkdirTemp("", "grepai-mcp-test")
		defer os.RemoveAll(tmpDir)
		cleanup := setTestHomeDirCLI(t, tmpDir)
		defer cleanup()

		_, _, err := resolveMCPTarget("", "nonexistent")
		if err == nil {
			t.Error("expected error for nonexistent workspace")
		}
	})

	t.Run("explicit_project_path", func(t *testing.T) {
		tmpDir, _ := os.MkdirTemp("", "grepai-mcp-test")
		defer os.RemoveAll(tmpDir)

		grepaiDir := filepath.Join(tmpDir, ".grepai")
		os.MkdirAll(grepaiDir, 0755)
		os.WriteFile(filepath.Join(grepaiDir, "config.yaml"), []byte("version: 1\n"), 0644)

		projectRoot, wsName, err := resolveMCPTarget(tmpDir, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if wsName != "" {
			t.Errorf("expected empty workspace name, got %s", wsName)
		}
		if projectRoot != tmpDir {
			t.Errorf("expected projectRoot %s, got %s", tmpDir, projectRoot)
		}
	})

	t.Run("explicit_project_path_no_config", func(t *testing.T) {
		tmpDir, _ := os.MkdirTemp("", "grepai-mcp-test")
		defer os.RemoveAll(tmpDir)

		_, _, err := resolveMCPTarget(tmpDir, "")
		if err == nil {
			t.Error("expected error when no .grepai/ at path")
		}
	})

	t.Run("no_local_project_uses_runtime_workspace_when_configured", func(t *testing.T) {
		tmpDir, _ := os.MkdirTemp("", "grepai-mcp-test")
		defer os.RemoveAll(tmpDir)
		cleanup := setTestHomeDirCLI(t, tmpDir)
		defer cleanup()

		cfg := config.DefaultWorkspaceConfig()
		cfg.AddWorkspace(config.Workspace{
			Name:  "runtime-only",
			Store: config.StoreConfig{Backend: "qdrant"},
			Embedder: config.EmbedderConfig{
				Provider: "ollama",
				Model:    "nomic-embed-text",
			},
			Projects: []config.ProjectEntry{
				{Name: "service", Path: filepath.Join(tmpDir, "service")},
			},
		})
		if err := config.SaveWorkspaceConfig(cfg); err != nil {
			t.Fatalf("failed to save workspace config: %v", err)
		}

		wd, err := os.Getwd()
		if err != nil {
			t.Fatalf("failed to get cwd: %v", err)
		}
		defer func() { _ = os.Chdir(wd) }()

		emptyDir := filepath.Join(tmpDir, "empty")
		if err := os.MkdirAll(emptyDir, 0o755); err != nil {
			t.Fatalf("failed to create empty dir: %v", err)
		}
		if err := os.Chdir(emptyDir); err != nil {
			t.Fatalf("failed to chdir: %v", err)
		}

		projectRoot, wsName, err := resolveMCPTarget("", "")
		if err != nil {
			t.Fatalf("expected fallback startup, got error: %v", err)
		}
		if projectRoot != "" || wsName != "" {
			t.Fatalf("expected unscoped startup (\"\", \"\"), got (%q, %q)", projectRoot, wsName)
		}
	})

	t.Run("no_local_project_and_no_workspace_config_still_errors", func(t *testing.T) {
		tmpDir, _ := os.MkdirTemp("", "grepai-mcp-test")
		defer os.RemoveAll(tmpDir)
		cleanup := setTestHomeDirCLI(t, tmpDir)
		defer cleanup()

		wd, err := os.Getwd()
		if err != nil {
			t.Fatalf("failed to get cwd: %v", err)
		}
		defer func() { _ = os.Chdir(wd) }()

		emptyDir := filepath.Join(tmpDir, "empty")
		if err := os.MkdirAll(emptyDir, 0o755); err != nil {
			t.Fatalf("failed to create empty dir: %v", err)
		}
		if err := os.Chdir(emptyDir); err != nil {
			t.Fatalf("failed to chdir: %v", err)
		}

		_, _, err = resolveMCPTarget("", "")
		if err == nil {
			t.Fatal("expected error when no local project and no workspace config")
		}
		if !strings.Contains(err.Error(), "no grepai project or workspace found") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func writeEngineRepo(t *testing.T, engine string) string {
	t.Helper()
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Engine = engine
	if err := cfg.Save(dir); err != nil {
		t.Fatal(err)
	}
	return dir
}

// A local engine:v2 project is no longer rejected — it gets the daemon-served
// MCP server (issue #10). The gate only guards workspace mode now, and the
// selector must route v2 repos to the daemon-backed constructor.
func TestMCPGateAllowsLocalEngineV2Project(t *testing.T) {
	dir := writeEngineRepo(t, "v2")
	if err := rejectEngineV2ForMCP(dir, ""); err != nil {
		t.Fatalf("local engine:v2 project must pass the startup gate (daemon-served): %v", err)
	}
	if !projectRootIsEngineV2(dir) {
		t.Fatal("selector must detect engine:v2 and route to the daemon-backed server")
	}
	if projectRootIsEngineV2(writeEngineRepo(t, "v1")) || projectRootIsEngineV2("") {
		t.Fatal("v1/empty roots must not route to the daemon-backed server")
	}
}

func TestMCPGateAllowsV1Project(t *testing.T) {
	for _, engine := range []string{"", "v1"} {
		dir := writeEngineRepo(t, engine)
		if err := rejectEngineV2ForMCP(dir, ""); err != nil {
			t.Fatalf("engine %q must pass the gate, got: %v", engine, err)
		}
	}
}

func TestMCPGateRejectsWorkspaceWithV2Member(t *testing.T) {
	v2dir := writeEngineRepo(t, "v2")
	v1dir := writeEngineRepo(t, "v1")
	tmpHome, _ := os.MkdirTemp("", "grepai-mcp-gate")
	defer os.RemoveAll(tmpHome)
	cleanup := setTestHomeDirCLI(t, tmpHome)
	defer cleanup()

	cfg := config.DefaultWorkspaceConfig()
	cfg.AddWorkspace(config.Workspace{
		Name:     "mixed",
		Store:    config.StoreConfig{Backend: "gob"},
		Embedder: config.EmbedderConfig{Provider: "ollama", Model: "m"},
		Projects: []config.ProjectEntry{
			{Name: "clean", Path: v1dir},
			{Name: "modern", Path: v2dir},
		},
	})
	if err := config.SaveWorkspaceConfig(cfg); err != nil {
		t.Fatal(err)
	}
	err := rejectEngineV2ForMCP("", "mixed")
	if err == nil {
		t.Fatal("workspace containing an engine:v2 member must be rejected")
	}
	if !strings.Contains(err.Error(), "modern") {
		t.Fatalf("error should name the v2 member, got: %v", err)
	}
}

// A --workspace start from inside an engine:v2 repo must not hand the v2 root
// to the v1 workspace server (its RPG/local stores are retired) — codex #10
// merge-gate finding 1.
func TestMCPWorkspaceModeDropsV2LocalRoot(t *testing.T) {
	if got := mcpWorkspaceLocalRoot(writeEngineRepo(t, "v2")); got != "" {
		t.Fatalf("v2 local root must be dropped in workspace mode, got %q", got)
	}
	v1dir := writeEngineRepo(t, "v1")
	if got := mcpWorkspaceLocalRoot(v1dir); got != v1dir {
		t.Fatalf("v1 local root must be kept, got %q", got)
	}
}
