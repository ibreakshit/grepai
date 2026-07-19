// internal/enginev2/enginetest/gitfixture_test.go
package enginetest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGitFixtureCreatesWorktrees(t *testing.T) {
	f := NewGitFixture(t)
	f.WriteFile("shared.go", "package a\n")
	f.Commit("initial")

	wtA := f.AddWorktree("feature-a", "feat-a")
	wtB := f.AddWorktree("feature-b", "feat-b")

	for _, wt := range []string{wtA, wtB} {
		if _, err := os.Stat(filepath.Join(wt, "shared.go")); err != nil {
			t.Fatalf("worktree %s missing shared.go: %v", wt, err)
		}
	}
	if wtA == wtB {
		t.Fatal("worktrees must have distinct paths")
	}
}

func TestGitFixtureIsolatesWorktreeEdits(t *testing.T) {
	f := NewGitFixture(t)
	f.WriteFile("shared.go", "package a\n")
	f.Commit("initial")
	wtA := f.AddWorktree("feature-a", "feat-a")

	// Edit only in the main worktree.
	f.WriteFile("shared.go", "package a\n// edited in main\n")
	f.Commit("edit main")

	got, err := os.ReadFile(filepath.Join(wtA, "shared.go"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "package a\n" {
		t.Fatalf("worktree A must not see main's later edit, got %q", string(got))
	}
}
