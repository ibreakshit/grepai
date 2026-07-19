package cli

import (
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
	if _, v2 := repoEngineV2(); !v2 {
		t.Fatal("engine:v2 config must route to the daemon path")
	}

	// engine: v1 -> v1 path (no daemon).
	cfg.Engine = "v1"
	if err := cfg.Save(dir); err != nil {
		t.Fatalf("save v1: %v", err)
	}
	if _, v2 := repoEngineV2(); v2 {
		t.Fatal("engine:v1 must NOT route to the daemon path")
	}

	// unset engine -> v1 default (no daemon).
	cfg.Engine = ""
	if err := cfg.Save(dir); err != nil {
		t.Fatalf("save default: %v", err)
	}
	if _, v2 := repoEngineV2(); v2 {
		t.Fatal("unset engine must default to v1 (no daemon)")
	}
}
