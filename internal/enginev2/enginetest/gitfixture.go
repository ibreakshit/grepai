// internal/enginev2/enginetest/gitfixture.go
package enginetest

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// GitFixture builds a throwaway Git repository with multiple linked worktrees
// for reconciliation and isolation tests. It cleans up via t.TempDir.
type GitFixture struct {
	t    *testing.T
	root string
}

// NewGitFixture initializes a Git repo in a temp dir. It skips the test if the
// git binary is unavailable.
func NewGitFixture(t *testing.T) *GitFixture {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	root := t.TempDir()
	f := &GitFixture{t: t, root: root}
	f.git(root, "init", "-q", "-b", "main")
	f.git(root, "config", "user.email", "test@example.com")
	f.git(root, "config", "user.name", "test")
	return f
}

// Root returns the main worktree path.
func (f *GitFixture) Root() string { return f.root }

// WriteFile writes content to relPath within the main worktree.
func (f *GitFixture) WriteFile(relPath, content string) {
	f.t.Helper()
	full := filepath.Join(f.root, relPath)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		f.t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		f.t.Fatalf("write: %v", err)
	}
}

// Commit stages all changes in the main worktree and commits them.
func (f *GitFixture) Commit(msg string) {
	f.t.Helper()
	f.git(f.root, "add", "-A")
	f.git(f.root, "commit", "-q", "-m", msg)
}

// AddWorktree creates a linked worktree on a new branch and returns its path.
func (f *GitFixture) AddWorktree(name, branch string) string {
	f.t.Helper()
	path := filepath.Join(f.t.TempDir(), name)
	f.git(f.root, "worktree", "add", "-q", "-b", branch, path)
	return path
}

func (f *GitFixture) git(dir string, args ...string) {
	f.t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		f.t.Fatalf("git %v failed: %v\n%s", args, err, out)
	}
}
