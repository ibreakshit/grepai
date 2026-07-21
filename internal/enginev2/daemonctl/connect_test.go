package daemonctl

import (
	"path/filepath"
	"testing"
)

func TestDaemonSocketPrecedence(t *testing.T) {
	// GREPAID_SOCKET wins over everything.
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_RUNTIME_DIR", "")
	t.Setenv("GREPAID_SOCKET", "/tmp/env.sock")
	got, err := Socket()
	if err != nil {
		t.Fatalf("Socket: %v", err)
	}
	if got != "/tmp/env.sock" {
		t.Fatalf("env should win: got %q", got)
	}
}

func TestDaemonSocketFallsBackToHostDefault(t *testing.T) {
	state := t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	t.Setenv("GREPAID_SOCKET", "")
	t.Setenv("XDG_RUNTIME_DIR", "")
	got, err := Socket()
	if err != nil {
		t.Fatalf("Socket: %v", err)
	}
	want := filepath.Join(state, "grepai", "grepaid.sock")
	if got != want {
		t.Fatalf("host default wrong: got %q want %q", got, want)
	}
}
