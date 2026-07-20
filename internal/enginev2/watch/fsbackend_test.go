package watch

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fsHarness sets up a real fsBackend over a temp tree.
func fsHarness(t *testing.T, prepare func(root string)) (string, Backend) {
	t.Helper()
	root := t.TempDir()
	// A gitignore ignoring vendor/, plus the .grepai dir the daemon creates.
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("vendor/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{".grepai", ".git", "src", "vendor"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	if prepare != nil {
		prepare(root)
	}
	b, err := NewFSBackend(root)
	if err != nil {
		t.Fatalf("newFSBackend: %v", err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return root, b
}

// expectHint waits (bounded) for at least one hint.
func expectHint(t *testing.T, b Backend, what string) {
	t.Helper()
	select {
	case <-b.Hints():
	case <-time.After(3 * time.Second):
		t.Fatalf("no hint for %s", what)
	}
}

// expectQuiet asserts NO hint arrives within the window.
func expectQuiet(t *testing.T, b Backend, what string) {
	t.Helper()
	select {
	case <-b.Hints():
		t.Fatalf("unexpected hint from %s", what)
	case <-time.After(250 * time.Millisecond):
	}
}

func drain(b Backend) {
	for {
		select {
		case <-b.Hints():
		default:
			return
		}
	}
}

func TestFSBackendEditSignals(t *testing.T) {
	root, b := fsHarness(t, nil)

	// Create.
	if err := os.WriteFile(filepath.Join(root, "src", "a.go"), []byte("package a"), 0o600); err != nil {
		t.Fatal(err)
	}
	expectHint(t, b, "create")
	drain(b)

	// In-place write.
	if err := os.WriteFile(filepath.Join(root, "src", "a.go"), []byte("package a // edited"), 0o600); err != nil {
		t.Fatal(err)
	}
	expectHint(t, b, "write")
	drain(b)

	// Atomic-rename save (vim/VS Code/gofmt pattern).
	tmp := filepath.Join(root, "src", ".a.go.tmp")
	if err := os.WriteFile(tmp, []byte("package a // renamed in"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, filepath.Join(root, "src", "a.go")); err != nil {
		t.Fatal(err)
	}
	expectHint(t, b, "atomic-rename save")
	drain(b)

	// Delete.
	if err := os.Remove(filepath.Join(root, "src", "a.go")); err != nil {
		t.Fatal(err)
	}
	expectHint(t, b, "delete")
}

func TestFSBackendNewNestedDirIsPickedUp(t *testing.T) {
	root, b := fsHarness(t, nil)
	if err := os.MkdirAll(filepath.Join(root, "src", "deep", "deeper"), 0o750); err != nil {
		t.Fatal(err)
	}
	expectHint(t, b, "mkdir")
	drain(b)
	// Give the recursive add a moment, then write inside the new subtree.
	time.Sleep(150 * time.Millisecond)
	drain(b)
	if err := os.WriteFile(filepath.Join(root, "src", "deep", "deeper", "n.go"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	expectHint(t, b, "write in new nested dir")
}

func TestFSBackendIgnoresCatalogGitAndGitignored(t *testing.T) {
	root, b := fsHarness(t, nil)
	// .grepai churn (the catalog WAL) must NOT hint — else reconcile loops.
	if err := os.WriteFile(filepath.Join(root, ".grepai", "catalog_v2.db-wal"), []byte("wal"), 0o600); err != nil {
		t.Fatal(err)
	}
	expectQuiet(t, b, ".grepai WAL churn")
	// .git internal churn must not hint.
	if err := os.WriteFile(filepath.Join(root, ".git", "index.lock"), []byte(""), 0o600); err != nil {
		t.Fatal(err)
	}
	expectQuiet(t, b, ".git churn")
	// Gitignored dir isn't watched.
	if err := os.WriteFile(filepath.Join(root, "vendor", "dep.go"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	expectQuiet(t, b, "gitignored vendor/")
}

func TestFSBackendDotfilesDoHint(t *testing.T) {
	root, b := fsHarness(t, func(root string) {
		_ = os.MkdirAll(filepath.Join(root, ".github", "workflows"), 0o750)
	})
	// Unlike v1, tracked dotfiles matter (.github/workflows).
	if err := os.WriteFile(filepath.Join(root, ".github", "workflows", "ci.yml"), []byte("on: push"), 0o600); err != nil {
		t.Fatal(err)
	}
	expectHint(t, b, "dotdir workflow file")
}

func TestFSBackendChmodIsQuiet(t *testing.T) {
	root, b := fsHarness(t, func(root string) {
		_ = os.WriteFile(filepath.Join(root, "src", "c.go"), []byte("x"), 0o600)
	})
	drain(b)
	if err := os.Chmod(filepath.Join(root, "src", "c.go"), 0o644); err != nil {
		t.Fatal(err)
	}
	expectQuiet(t, b, "chmod")
}

func TestFSBackendRootGone(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "repo")
	if err := os.MkdirAll(filepath.Join(sub, "src"), 0o750); err != nil {
		t.Fatal(err)
	}
	b, err := NewFSBackend(sub)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := os.RemoveAll(sub); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(3 * time.Second)
	for {
		select {
		case err := <-b.Errors():
			if err == ErrRootGone {
				return
			}
		case <-b.Hints(): // deletes inside may hint first; keep waiting
		case <-deadline:
			t.Fatal("ErrRootGone never reported after root deletion")
		}
	}
}

func TestFSBackendStartOnMissingRootIsRootGone(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "never-existed")
	b, err := NewFSBackend(missing)
	if err != nil {
		// Constructing may legitimately fail instead; either surface is fine as
		// long as it is loud. But if construction succeeded, Start MUST report
		// ErrRootGone — a silent zero-watch backend retries a vanished repo
		// forever.
		t.Skipf("construction failed loudly instead: %v", err)
	}
	defer b.Close()
	if serr := b.Start(); !errorsIs(serr, ErrRootGone) {
		t.Fatalf("Start on missing root = %v; want ErrRootGone", serr)
	}
}

func errorsIs(err, target error) bool { return errors.Is(err, target) }
