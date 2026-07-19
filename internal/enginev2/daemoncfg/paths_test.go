package daemoncfg

import (
	"strings"
	"testing"
)

func TestResolvePathsHonorsXDGStateHome(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "/xdg/state")
	t.Setenv("XDG_RUNTIME_DIR", "")
	t.Setenv("GREPAID_SOCKET", "")
	p, err := ResolvePaths()
	if err != nil {
		t.Fatalf("ResolvePaths: %v", err)
	}
	if p.StateDir != "/xdg/state/grepai" {
		t.Fatalf("StateDir = %q", p.StateDir)
	}
	if p.Registry != "/xdg/state/grepai/registry.json" || p.Config != "/xdg/state/grepai/daemon.json" {
		t.Fatalf("bad derived paths: %+v", p)
	}
	if p.Lock != "/xdg/state/grepai/grepaid.lock" {
		t.Fatalf("Lock = %q", p.Lock)
	}
	if !strings.HasSuffix(p.Socket, "grepaid.sock") {
		t.Fatalf("Socket = %q", p.Socket)
	}
}

func TestResolvePathsHonorsRuntimeDir(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "/xdg/state")
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1000")
	t.Setenv("GREPAID_SOCKET", "")
	p, err := ResolvePaths()
	if err != nil {
		t.Fatalf("ResolvePaths: %v", err)
	}
	if p.Socket != "/run/user/1000/grepai/grepaid.sock" {
		t.Fatalf("Socket should be under XDG_RUNTIME_DIR, got %q", p.Socket)
	}
}

func TestSocketEnvOverride(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "/xdg/state")
	t.Setenv("GREPAID_SOCKET", "/tmp/custom.sock")
	p, err := ResolvePaths()
	if err != nil {
		t.Fatalf("ResolvePaths: %v", err)
	}
	if p.Socket != "/tmp/custom.sock" {
		t.Fatalf("env override ignored: Socket = %q", p.Socket)
	}
}
