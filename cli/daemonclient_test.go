package cli

import (
	"testing"

	"github.com/yoanbernabeu/grepai/config"
)

func TestDaemonSocketHonorsOverride(t *testing.T) {
	cfg := &config.Config{Daemon: config.DaemonConfig{Socket: "/tmp/custom.sock"}}
	got, err := daemonSocket(cfg)
	if err != nil {
		t.Fatalf("daemonSocket: %v", err)
	}
	if got != "/tmp/custom.sock" {
		t.Fatalf("override ignored: got %q", got)
	}
}

func TestDaemonSocketFallsBackToHostDefault(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "/xdg/state")
	t.Setenv("GREPAID_SOCKET", "")
	t.Setenv("XDG_RUNTIME_DIR", "")
	got, err := daemonSocket(&config.Config{})
	if err != nil {
		t.Fatalf("daemonSocket: %v", err)
	}
	if got != "/xdg/state/grepai/grepaid.sock" {
		t.Fatalf("host default wrong: got %q", got)
	}
}
