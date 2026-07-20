package cli

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/yoanbernabeu/grepai/config"
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
