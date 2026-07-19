package runtime_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/runtime"
)

func TestNewDiskLoaderRejectsChangedContent(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f.txt"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	l := runtime.NewDiskLoader()
	// A wrong desiredHash must be rejected (content changed since reconciliation).
	if _, err := l.Load(context.Background(), core.RepositoryID(dir), dir, "f.txt", "deadbeef"); err == nil {
		t.Fatal("expected rejection for mismatched desiredHash")
	}
	// An empty desiredHash skips verification (dirty/non-git path), so it loads.
	if b, err := l.Load(context.Background(), core.RepositoryID(dir), dir, "f.txt", ""); err != nil || string(b) != "hello" {
		t.Fatalf("empty-hash load = %q, %v; want hello, nil", b, err)
	}
}
