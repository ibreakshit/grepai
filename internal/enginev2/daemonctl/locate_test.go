package daemonctl

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLocateBinaryOnPath(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "grepaid")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o755); err != nil { //nolint:gosec // test fixture executable
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	got, err := LocateBinary()
	if err != nil {
		t.Fatalf("LocateBinary: %v", err)
	}
	if got != bin {
		t.Fatalf("LocateBinary = %q, want %q", got, bin)
	}
}

func TestLocateBinaryMissing(t *testing.T) {
	t.Setenv("PATH", t.TempDir()) // empty dir, no grepaid
	if _, err := LocateBinary(); err == nil {
		t.Fatal("expected error when grepaid is not found")
	}
}
