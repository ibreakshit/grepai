// git/truth_test.go
package git

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func mkRepo(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	dir := t.TempDir()
	run := func(args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	run("init", "-q", "-b", "main")
	run("config", "user.email", "t@e.com")
	run("config", "user.name", "t")
	return dir
}

func gitIn(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func write(t *testing.T, dir, rel, content string) {
	t.Helper()
	p := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestWorktreeTruthCleanTracked(t *testing.T) {
	dir := mkRepo(t)
	write(t, dir, "a.go", "package a\n")
	write(t, dir, "sub/b.go", "package b\n")
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "-m", "c1")

	truth, err := WorktreeTruth(context.Background(), dir)
	if err != nil {
		t.Fatalf("truth: %v", err)
	}
	if len(truth) != 2 {
		t.Fatalf("want 2 files, got %d: %v", len(truth), truth)
	}
	if truth["a.go"] == "" || truth["sub/b.go"] == "" {
		t.Fatalf("missing expected paths: %v", truth)
	}
	// Clean tracked sourceHash is the git blob OID (40 hex chars).
	if len(truth["a.go"]) != 40 {
		t.Fatalf("clean tracked hash should be a 40-char blob OID, got %q", truth["a.go"])
	}
	// Reconciling again yields identical hashes (stable → idle means idle).
	truth2, _ := WorktreeTruth(context.Background(), dir)
	if truth2["a.go"] != truth["a.go"] {
		t.Fatal("clean tracked hash not stable across calls")
	}
}

func TestWorktreeTruthDirtyUntrackedDeleted(t *testing.T) {
	dir := mkRepo(t)
	write(t, dir, "keep.go", "package a\n")
	write(t, dir, "mod.go", "package a\n")
	write(t, dir, "del.go", "package a\n")
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "-m", "c1")
	cleanOID := func() string { m, _ := WorktreeTruth(context.Background(), dir); return m["mod.go"] }()

	// Modify mod.go (dirty), add untracked new.go, delete del.go from working tree.
	write(t, dir, "mod.go", "package a\n// changed\n")
	write(t, dir, "new.go", "package new\n")
	if err := os.Remove(filepath.Join(dir, "del.go")); err != nil {
		t.Fatal(err)
	}

	truth, err := WorktreeTruth(context.Background(), dir)
	if err != nil {
		t.Fatalf("truth: %v", err)
	}
	if _, ok := truth["del.go"]; ok {
		t.Fatal("working-tree-deleted file must be excluded from truth")
	}
	if truth["keep.go"] == "" {
		t.Fatal("clean file missing")
	}
	if truth["new.go"] == "" {
		t.Fatal("untracked non-ignored file must be included")
	}
	if truth["mod.go"] == "" || truth["mod.go"] == cleanOID {
		t.Fatalf("dirty file hash must reflect changed content, got %q (clean was %q)", truth["mod.go"], cleanOID)
	}
}

func TestWorktreeTruthRespectsGitignore(t *testing.T) {
	dir := mkRepo(t)
	write(t, dir, ".gitignore", "ignored/\n*.log\n")
	write(t, dir, "a.go", "package a\n")
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "-m", "c1")
	write(t, dir, "ignored/x.go", "package x\n")
	write(t, dir, "debug.log", "noise\n")

	truth, err := WorktreeTruth(context.Background(), dir)
	if err != nil {
		t.Fatalf("truth: %v", err)
	}
	if _, ok := truth["ignored/x.go"]; ok {
		t.Fatal("ignored dir must be excluded")
	}
	if _, ok := truth["debug.log"]; ok {
		t.Fatal("ignored glob must be excluded")
	}
}
