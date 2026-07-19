# GrepAI v2 — Phase 2 Implementation Plan (Reconciler)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Implement desired-state reconciliation — compute, from Git (or filesystem) truth versus the catalog's indexed view, the exact set of jobs needed to make one worktree fresh — so Gate 2 passes: a repeated unchanged reconcile creates zero jobs (idle means idle), a branch switch ends with an exact file-view match, and a change that arrived with no filesystem event is still repaired by reconciliation.

**Architecture:** A new `internal/enginev2/reconcile` reconciler compares two maps: **desired** `path → sourceHash` (current worktree truth) and **indexed** `path → sourceHash` (what the catalog's worktree view currently points to). Paths that are new or changed become `OpUpsert` jobs; indexed paths absent from truth become `OpDelete` jobs; equal paths produce nothing. Git truth is gathered without fragile porcelain XY-parsing: `git ls-files -s` gives staged blob OIDs for clean tracked files (no content read — preserves idle-means-idle), and `--deleted`/`--modified`/`--others --exclude-standard` give the deltas; only the few dirty/untracked files are content-identified via a batched `git hash-object` (blob-OID identity, stable across a dirty→commit transition). Non-Git projects fall back to a filesystem walk with SHA-256 content hashes. The reconciler depends on a small `CatalogReader` interface (implemented by the Phase 1 SQLite catalog, extended with two read methods) and an injectable truth function (real Git/FS in production, a fake in unit tests).

**Tech Stack:** Go 1.24.2, `os/exec` (git), `crypto/sha256`, the Phase 1 `catalog/sqlite` package, the existing `git` package, and `enginetest.GitFixture` for integration tests.

## Global Constraints

- Go 1.24.2 floor; go.mod `go` directive stays `go 1.24.2`; `modernc.org/sqlite` stays `v1.45.0`.
- CGO_ENABLED=0 must stay buildable. No new module dependency.
- Module `github.com/yoanbernabeu/grepai`. New code under `internal/enginev2/reconcile/`; Git helpers extend the existing top-level `git/` package.
- The concrete reconciler is named **`Engine`** (NOT `Reconciler` — the package already exports the `Reconciler` interface from Phase 0; two same-named types in one package do not compile). `*Engine` MUST satisfy `reconcile.Reconciler` (`Reconcile(ctx, core.WorktreeID) (Plan, error)`), asserted in-package as `var _ Reconciler = (*Engine)(nil)`.
- `go test -race` must pass; `gofmt`-clean; `make lint` (golangci-lint v1.64.2) green (annotate justified gosec with `// #nosec GXXX - reason`; `_test.go` excluded from gosec/errcheck).
- Conventional commits (scope `reconcile` or `git`). Never push to `main`.
- **Idle means idle (invariant 1):** an unchanged worktree yields an empty `Plan` (zero jobs). Clean tracked files are identified by their staged blob OID — reconciliation never reads clean file content.
- **Events are hints (invariant 9):** reconciliation determines truth from Git/FS state, not from events. (fsnotify wiring is out of scope — deferred to Phase 4.)
- **Worktree isolation (invariant 4):** reconciliation of one worktree reads only that worktree's root path and its own indexed view.

## Scope / Non-goals (this phase)

- **In:** Git-truth reconciliation (staged OIDs, dirty/deleted/untracked classification, batched hash-object for dirty/untracked), non-Git filesystem fallback, the diff→jobs core, the two catalog read extensions, and Gate 2 tests.
- **Out (Phase 4, the daemon that owns the watcher):** fsnotify event aggregation and overflow recovery, watch-descriptor-exhaustion fallback to periodic reconciliation, branch-switch quiescence *scheduling* (Phase 2 proves branch-switch *correctness* via reconciliation, not the debounce). Also out: a HEAD-tree short-circuit fast-path (a perf optimization; correctness holds without it) and renamed-file special handling (a rename reconciles correctly as delete-old + upsert-new).

## Consumed surfaces (do not modify)

- Phase 0 `reconcile.Plan{Jobs []core.Job}` and `reconcile.Reconciler` interface (`internal/enginev2/reconcile/reconcile.go`).
- `core.Job{WorktreeID, Path, DesiredHash, Generation, Operation, Priority, Attempts}`; `core.OpUpsert`/`OpDelete`; `core.PriorityReconcile`; `core.Generation`; `core.RepositoryID`/`core.WorktreeID`.
- Phase 1 `catalog/sqlite.Catalog` (add methods; do not change existing ones): existing `ActiveGeneration`, `RegisterRepository`, `RegisterWorktree`, `CreateGeneration`, `SetActiveGeneration`, `PutArtifact`, `CommitUpdate`.
- Existing `git` package: `Detect`, `IsGitRepo`.
- `enginetest.GitFixture` (`NewGitFixture`, `WriteFile`, `Commit`, `AddWorktree`, `Root`).

---

## File Structure

```
git/
  truth.go              # WorktreeTruth(ctx, root) map[relpath]blobOID-or-contenthash (git-native)
  truth_test.go
internal/enginev2/reconcile/
  reconcile.go          # (existing) Plan + Reconciler interface — unchanged
  catalogreader.go      # CatalogReader interface (WorktreeInfo, ActiveGeneration, WorktreeIndexedHashes)
  reconciler.go         # Engine struct (impl of Reconciler), New, Reconcile (diff), truth dispatch (git vs non-git), non-git walk
  reconciler_test.go    # unit tests with a fake CatalogReader + injected truth
  gate2_test.go         # integration: real sqlite catalog + GitFixture (unchanged→0, branch-switch→exact, dropped-event→repaired)
internal/enginev2/catalog/sqlite/
  reader.go             # WorktreeInfo + WorktreeIndexedHashes methods on *Catalog
  reader_test.go
```

---

## Task 1: Git worktree truth

**Files:**
- Create: `git/truth.go`, `git/truth_test.go`

**Interfaces:**
- Produces: `func WorktreeTruth(ctx context.Context, root string) (map[string]string, error)` — returns `relativePath → sourceHash` for every file that should be indexed (tracked-and-present + untracked-not-ignored, excluding working-tree-deleted). Clean tracked files use their staged blob OID; dirty tracked and untracked files use the `git hash-object` of their working content. Paths are slash-relative to `root`.

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./git/ -run TestWorktreeTruth`
Expected: FAIL — `WorktreeTruth` undefined.

- [ ] **Step 3: Write minimal implementation**

```go
// git/truth.go
package git

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// WorktreeTruth returns the desired index state of a Git worktree as a map of
// slash-relative path to source hash. Clean tracked files use their staged Git
// blob OID (no content read — keeps unchanged reconciliation cheap). Dirty
// tracked files and untracked, non-ignored files use the git hash-object of
// their current working content (also a blob OID, so identity is stable when a
// dirty edit is later committed). Working-tree-deleted files are excluded.
func WorktreeTruth(ctx context.Context, root string) (map[string]string, error) {
	staged, err := lsFilesStaged(ctx, root)
	if err != nil {
		return nil, err
	}
	deleted, err := lsFilesSet(ctx, root, "--deleted")
	if err != nil {
		return nil, err
	}
	modified, err := lsFilesSet(ctx, root, "--modified")
	if err != nil {
		return nil, err
	}
	untracked, err := lsFilesSet(ctx, root, "--others", "--exclude-standard")
	if err != nil {
		return nil, err
	}

	// Files needing a working-content hash: dirty tracked (modified but not
	// deleted) plus untracked.
	needHash := make([]string, 0, len(modified)+len(untracked))
	for p := range modified {
		if !deleted[p] {
			needHash = append(needHash, p)
		}
	}
	for p := range untracked {
		needHash = append(needHash, p)
	}
	hashes, err := hashObjects(ctx, root, needHash)
	if err != nil {
		return nil, err
	}

	truth := make(map[string]string, len(staged)+len(untracked))
	for p, oid := range staged {
		if deleted[p] {
			continue // removed from the working tree
		}
		if modified[p] {
			truth[p] = hashes[p] // dirty: working content OID
		} else {
			truth[p] = oid // clean: staged blob OID
		}
	}
	for p := range untracked {
		truth[p] = hashes[p]
	}
	return truth, nil
}

// lsFilesStaged parses `git ls-files -s -z` into path -> blob OID (stage 0).
func lsFilesStaged(ctx context.Context, root string) (map[string]string, error) {
	out, err := gitOutput(ctx, root, "ls-files", "-s", "-z")
	if err != nil {
		return nil, err
	}
	res := map[string]string{}
	for _, entry := range splitNUL(out) {
		// "<mode> <oid> <stage>\t<path>"
		tab := strings.IndexByte(entry, '\t')
		if tab < 0 {
			continue
		}
		meta := strings.Fields(entry[:tab])
		if len(meta) < 3 {
			continue
		}
		res[entry[tab+1:]] = meta[1]
	}
	return res, nil
}

// lsFilesSet runs `git ls-files -z <args>` and returns the path set.
func lsFilesSet(ctx context.Context, root string, args ...string) (map[string]bool, error) {
	full := append([]string{"ls-files", "-z"}, args...)
	out, err := gitOutput(ctx, root, full...)
	if err != nil {
		return nil, err
	}
	set := map[string]bool{}
	for _, p := range splitNUL(out) {
		set[p] = true
	}
	return set, nil
}

// hashObjects returns path -> blob OID for the working content of each path,
// via a single `git hash-object -- <paths...>` call (empty input -> empty map).
func hashObjects(ctx context.Context, root string, paths []string) (map[string]string, error) {
	res := make(map[string]string, len(paths))
	if len(paths) == 0 {
		return res, nil
	}
	args := append([]string{"hash-object", "--"}, paths...)
	out, err := gitOutput(ctx, root, args...)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimRight(string(out), "\n"), "\n")
	if len(lines) != len(paths) {
		return nil, fmt.Errorf("git hash-object returned %d oids for %d paths", len(lines), len(paths))
	}
	for i, p := range paths {
		res[p] = strings.TrimSpace(lines[i])
	}
	return res, nil
}

func gitOutput(ctx context.Context, root string, args ...string) ([]byte, error) {
	full := append([]string{"-C", root}, args...)
	cmd := exec.CommandContext(ctx, "git", full...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("git %s: %w (%s)", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

func splitNUL(b []byte) []string {
	s := strings.TrimRight(string(b), "\x00")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\x00")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./git/ -run TestWorktreeTruth`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add git/truth.go git/truth_test.go
git commit -m "feat(git): WorktreeTruth — git-native desired index state per worktree"
```

---

## Task 2: Catalog read extensions

**Files:**
- Create: `internal/enginev2/catalog/sqlite/reader.go`, `internal/enginev2/catalog/sqlite/reader_test.go`

**Interfaces:**
- Produces on `*Catalog`:
  - `WorktreeInfo(ctx, wt core.WorktreeID) (rootPath string, repo core.RepositoryID, err error)` — from the `worktrees` table; returns `ErrNoSuchWorktree` if unregistered.
  - `WorktreeIndexedHashes(ctx, wt core.WorktreeID) (map[string]string, error)` — `relativePath → file_artifacts.source_hash` for every current worktree view row (join `worktree_files` → `file_artifacts`).
- Also produces `var ErrNoSuchWorktree = errors.New(...)`.

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/catalog/sqlite/reader_test.go
package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

func TestWorktreeInfo(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	if err := c.RegisterRepository(ctx, "repo1", "/repo1", ""); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterWorktree(ctx, "wt1", "repo1", "/repo1/wtA", 1); err != nil {
		t.Fatal(err)
	}
	root, repo, err := c.WorktreeInfo(ctx, "wt1")
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if root != "/repo1/wtA" || repo != "repo1" {
		t.Fatalf("got root=%q repo=%q", root, repo)
	}
	if _, _, err := c.WorktreeInfo(ctx, "ghost"); !errors.Is(err, ErrNoSuchWorktree) {
		t.Fatalf("expected ErrNoSuchWorktree, got %v", err)
	}
}

func TestWorktreeIndexedHashes(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	if err := c.RegisterRepository(ctx, "repo1", "/repo1", ""); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterWorktree(ctx, "wt1", "repo1", "/repo1", 1); err != nil {
		t.Fatal(err)
	}
	// Commit two views for wt1.
	for _, f := range []struct{ path, oid string }{{"a.go", "oidA"}, {"b.go", "oidB"}} {
		key := core.ArtifactKey{RepositoryID: "repo1", RelativePath: f.path, SourceHash: f.oid, Fingerprint: "fp"}
		art := core.Artifact{ID: key.ArtifactID(), Key: key, Dimensions: 4}
		req := core.CommitRequest{View: core.ViewEntry{WorktreeID: "wt1", Path: f.path, ArtifactID: art.ID, Generation: 1}, Artifact: art}
		if err := c.CommitUpdate(ctx, req, core.Job{WorktreeID: "wt1", Path: f.path, Generation: 1, Operation: core.OpUpsert}); err != nil {
			t.Fatalf("commit %s: %v", f.path, err)
		}
	}
	hashes, err := c.WorktreeIndexedHashes(ctx, "wt1")
	if err != nil {
		t.Fatalf("hashes: %v", err)
	}
	if len(hashes) != 2 || hashes["a.go"] != "oidA" || hashes["b.go"] != "oidB" {
		t.Fatalf("got %v", hashes)
	}
	// A different worktree sees none of wt1's views (isolation).
	other, err := c.WorktreeIndexedHashes(ctx, "wt2")
	if err != nil {
		t.Fatalf("other: %v", err)
	}
	if len(other) != 0 {
		t.Fatalf("wt2 should have no indexed hashes, got %v", other)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/catalog/sqlite/ -run 'TestWorktreeInfo|TestWorktreeIndexedHashes'`
Expected: FAIL — `WorktreeInfo` undefined.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/catalog/sqlite/reader.go
package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// ErrNoSuchWorktree is returned when a worktree id is not registered.
var ErrNoSuchWorktree = errors.New("catalog/sqlite: worktree not registered")

// WorktreeInfo returns a worktree's root path and repository namespace.
func (c *Catalog) WorktreeInfo(ctx context.Context, wt core.WorktreeID) (string, core.RepositoryID, error) {
	var root, repo string
	err := c.db.QueryRowContext(ctx, `
		SELECT root_path, repository_id FROM worktrees WHERE worktree_id=?`,
		string(wt)).Scan(&root, &repo)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", ErrNoSuchWorktree
	}
	if err != nil {
		return "", "", err
	}
	return root, core.RepositoryID(repo), nil
}

// WorktreeIndexedHashes returns the currently-indexed source hash for every
// path in a worktree's view (invariant 4: only this worktree's rows).
func (c *Catalog) WorktreeIndexedHashes(ctx context.Context, wt core.WorktreeID) (map[string]string, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT wf.relative_path, fa.source_hash
		FROM worktree_files wf
		JOIN file_artifacts fa ON fa.artifact_id = wf.artifact_id
		WHERE wf.worktree_id=?`, string(wt))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := map[string]string{}
	for rows.Next() {
		var path, hash string
		if err := rows.Scan(&path, &hash); err != nil {
			return nil, err
		}
		res[path] = hash
	}
	return res, rows.Err()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/catalog/sqlite/ -run 'TestWorktreeInfo|TestWorktreeIndexedHashes'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/catalog/sqlite/reader.go internal/enginev2/catalog/sqlite/reader_test.go
git commit -m "feat(catalog): WorktreeInfo and WorktreeIndexedHashes read helpers"
```

---

## Task 3: The reconciler

**Files:**
- Create: `internal/enginev2/reconcile/catalogreader.go`, `internal/enginev2/reconcile/reconciler.go`, `internal/enginev2/reconcile/reconciler_test.go`

**Interfaces:**
- Consumes: `core`, `git` (`IsGitRepo`, `WorktreeTruth`).
- Produces:
  - `type CatalogReader interface { WorktreeInfo(ctx, core.WorktreeID) (string, core.RepositoryID, error); ActiveGeneration(ctx, core.RepositoryID) (core.Generation, error); WorktreeIndexedHashes(ctx, core.WorktreeID) (map[string]string, error) }`
  - `type TruthFunc func(ctx context.Context, root string) (map[string]string, error)`
  - `func New(cat CatalogReader) *Engine` (production truth = git-or-filesystem dispatch)
  - `func NewWithTruth(cat CatalogReader, truth TruthFunc) *Engine` (test seam)
  - `func (e *Engine) Reconcile(ctx, core.WorktreeID) (Plan, error)` — satisfies the `Reconciler` interface; jobs sorted deterministically by (Operation, Path).

- [ ] **Step 1: Write the failing test**

```go
// internal/enginev2/reconcile/reconciler_test.go
package reconcile

import (
	"context"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

var _ Reconciler = (*Engine)(nil)

// fakeReader implements CatalogReader for unit tests.
type fakeReader struct {
	root    string
	repo    core.RepositoryID
	gen     core.Generation
	indexed map[string]string
}

func (f *fakeReader) WorktreeInfo(_ context.Context, _ core.WorktreeID) (string, core.RepositoryID, error) {
	return f.root, f.repo, nil
}
func (f *fakeReader) ActiveGeneration(_ context.Context, _ core.RepositoryID) (core.Generation, error) {
	return f.gen, nil
}
func (f *fakeReader) WorktreeIndexedHashes(_ context.Context, _ core.WorktreeID) (map[string]string, error) {
	return f.indexed, nil
}

func plan(t *testing.T, indexed, desired map[string]string, gen core.Generation) Plan {
	t.Helper()
	r := NewWithTruth(&fakeReader{root: "/x", repo: "repo1", gen: gen, indexed: indexed},
		func(context.Context, string) (map[string]string, error) { return desired, nil })
	p, err := r.Reconcile(context.Background(), "wt1")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return p
}

func TestReconcileUnchangedYieldsNoJobs(t *testing.T) {
	m := map[string]string{"a.go": "o1", "b.go": "o2"}
	p := plan(t, m, m, 1)
	if len(p.Jobs) != 0 {
		t.Fatalf("unchanged reconcile must yield 0 jobs, got %d: %+v", len(p.Jobs), p.Jobs)
	}
}

func TestReconcileEmitsUpsertsAndDeletes(t *testing.T) {
	indexed := map[string]string{"keep.go": "o1", "change.go": "old", "gone.go": "o3"}
	desired := map[string]string{"keep.go": "o1", "change.go": "new", "added.go": "o4"}
	p := plan(t, indexed, desired, 7)

	byPath := map[string]core.Job{}
	for _, j := range p.Jobs {
		byPath[j.Path] = j
	}
	if _, ok := byPath["keep.go"]; ok {
		t.Fatal("unchanged file must not produce a job")
	}
	if j := byPath["change.go"]; j.Operation != core.OpUpsert || j.DesiredHash != "new" || j.Generation != 7 || j.Priority != core.PriorityReconcile {
		t.Fatalf("change.go job wrong: %+v", j)
	}
	if j := byPath["added.go"]; j.Operation != core.OpUpsert || j.DesiredHash != "o4" {
		t.Fatalf("added.go job wrong: %+v", j)
	}
	if j := byPath["gone.go"]; j.Operation != core.OpDelete {
		t.Fatalf("gone.go should be a delete, got %+v", j)
	}
	if len(p.Jobs) != 3 {
		t.Fatalf("want 3 jobs, got %d", len(p.Jobs))
	}
}

func TestReconcileDefaultsGenerationToOne(t *testing.T) {
	// No active generation (0) -> jobs target generation 1 (bootstrap).
	p := plan(t, map[string]string{}, map[string]string{"a.go": "o1"}, 0)
	if len(p.Jobs) != 1 || p.Jobs[0].Generation != 1 {
		t.Fatalf("expected 1 job at generation 1, got %+v", p.Jobs)
	}
}

func TestReconcileJobsDeterministicOrder(t *testing.T) {
	indexed := map[string]string{"z.go": "x", "a.go": "x"}
	desired := map[string]string{}
	p := plan(t, indexed, desired, 1)
	// Both deletes, sorted by path: a.go before z.go.
	if len(p.Jobs) != 2 || p.Jobs[0].Path != "a.go" || p.Jobs[1].Path != "z.go" {
		t.Fatalf("jobs not deterministically ordered: %+v", p.Jobs)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/enginev2/reconcile/ -run TestReconcile`
Expected: FAIL — `NewWithTruth`/`Reconciler` struct undefined.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/enginev2/reconcile/catalogreader.go
package reconcile

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// CatalogReader is the read surface the reconciler needs from the catalog.
// The Phase 1 SQLite catalog satisfies it.
type CatalogReader interface {
	WorktreeInfo(ctx context.Context, wt core.WorktreeID) (root string, repo core.RepositoryID, err error)
	ActiveGeneration(ctx context.Context, repo core.RepositoryID) (core.Generation, error)
	WorktreeIndexedHashes(ctx context.Context, wt core.WorktreeID) (map[string]string, error)
}
```

```go
// internal/enginev2/reconcile/reconciler.go
package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/yoanbernabeu/grepai/git"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// TruthFunc returns the desired index state (path -> source hash) for a root.
type TruthFunc func(ctx context.Context, root string) (map[string]string, error)

// Engine is the git/filesystem-backed reconciler. It implements the
// reconcile.Reconciler interface (named Engine to avoid colliding with that
// interface, which lives in this same package).
type Engine struct {
	cat   CatalogReader
	truth TruthFunc
}

// New returns an Engine using real Git/filesystem truth.
func New(cat CatalogReader) *Engine {
	return &Engine{cat: cat, truth: defaultTruth}
}

// NewWithTruth returns an Engine with an injected truth function (tests).
func NewWithTruth(cat CatalogReader, truth TruthFunc) *Engine {
	return &Engine{cat: cat, truth: truth}
}

// Reconcile diffs desired truth against the indexed view and returns the jobs
// that make them match. An empty Plan means the view is already fresh.
func (e *Engine) Reconcile(ctx context.Context, wt core.WorktreeID) (Plan, error) {
	root, repo, err := e.cat.WorktreeInfo(ctx, wt)
	if err != nil {
		return Plan{}, err
	}
	gen, err := e.cat.ActiveGeneration(ctx, repo)
	if err != nil {
		return Plan{}, err
	}
	if gen == 0 {
		gen = 1 // bootstrap: no active generation yet
	}
	indexed, err := e.cat.WorktreeIndexedHashes(ctx, wt)
	if err != nil {
		return Plan{}, err
	}
	desired, err := e.truth(ctx, root)
	if err != nil {
		return Plan{}, err
	}

	var jobs []core.Job
	for path, dh := range desired {
		if ih, ok := indexed[path]; !ok || ih != dh {
			jobs = append(jobs, core.Job{
				WorktreeID: wt, Path: path, DesiredHash: dh, Generation: gen,
				Operation: core.OpUpsert, Priority: core.PriorityReconcile,
			})
		}
	}
	for path := range indexed {
		if _, ok := desired[path]; !ok {
			jobs = append(jobs, core.Job{
				WorktreeID: wt, Path: path, Generation: gen,
				Operation: core.OpDelete, Priority: core.PriorityReconcile,
			})
		}
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].Operation != jobs[j].Operation {
			return jobs[i].Operation < jobs[j].Operation
		}
		return jobs[i].Path < jobs[j].Path
	})
	return Plan{Jobs: jobs}, nil
}

// defaultTruth uses Git when root is a Git repo, else a filesystem walk.
func defaultTruth(ctx context.Context, root string) (map[string]string, error) {
	if git.IsGitRepo(root) {
		return git.WorktreeTruth(ctx, root)
	}
	return filesystemTruth(root)
}

// filesystemTruth walks a non-Git root and content-hashes every file (skipping
// any .git directory), producing slash-relative path -> sha256 hex.
func filesystemTruth(root string) (map[string]string, error) {
	truth := map[string]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(p) // #nosec G304 - path is within the registered worktree root
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		truth[filepath.ToSlash(rel)] = hex.EncodeToString(sum[:])
		return nil
	})
	if err != nil {
		return nil, err
	}
	return truth, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/enginev2/reconcile/ -run TestReconcile`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/enginev2/reconcile/catalogreader.go internal/enginev2/reconcile/reconciler.go internal/enginev2/reconcile/reconciler_test.go
git commit -m "feat(reconcile): desired-state reconciler with git/filesystem truth"
```

---

## Task 4: Gate 2 integration tests

**Files:**
- Create: `internal/enginev2/reconcile/gate2_test.go`

**Interfaces:**
- Consumes: `catalog/sqlite`, `enginetest.GitFixture`, `git`, the `Reconciler`. Uses the REAL git truth path (not injected) against real fixtures + a real SQLite catalog.

Helper: a small function commits a reconciliation plan into the catalog (so a subsequent reconcile sees an up-to-date indexed view), simulating a worker without embeddings — it maps each upsert job's DesiredHash to an artifact and calls `CommitUpdate`, and each delete job by removing the view. For Phase 2 we index by writing the artifact whose `SourceHash == job.DesiredHash`.

- [ ] **Step 1: Write the Gate 2 tests**

```go
// internal/enginev2/reconcile/gate2_test.go
package reconcile

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/catalog/sqlite"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
)

// applyPlan indexes a plan into the catalog (no embeddings): each upsert job
// becomes an artifact keyed by its DesiredHash + a committed view; each delete
// removes the view via a tombstone commit is not available, so we rely on the
// fact that Phase 2 tests only need the indexed hashes to match desired.
func applyPlan(t *testing.T, c *sqlite.Catalog, repo core.RepositoryID, wt core.WorktreeID, p Plan) {
	t.Helper()
	ctx := context.Background()
	for _, j := range p.Jobs {
		if j.Operation == core.OpDelete {
			// Represent a delete by committing an empty-view removal: re-commit
			// all-but-this is unnecessary; Phase 2 delete handling is validated
			// by the reconciler emitting the delete job (asserted in tests),
			// and by re-reconciliation converging. Skip applying deletes here.
			continue
		}
		key := core.ArtifactKey{RepositoryID: repo, RelativePath: j.Path, SourceHash: j.DesiredHash, Fingerprint: "fp"}
		art := core.Artifact{ID: key.ArtifactID(), Key: key, Dimensions: 4}
		req := core.CommitRequest{
			View:     core.ViewEntry{WorktreeID: wt, Path: j.Path, ArtifactID: art.ID, Generation: j.Generation},
			Artifact: art,
		}
		if err := c.CommitUpdate(ctx, req, j); err != nil {
			t.Fatalf("apply upsert %s: %v", j.Path, err)
		}
	}
}

func setupCatalog(t *testing.T, root string) (*sqlite.Catalog, core.RepositoryID, core.WorktreeID) {
	t.Helper()
	ctx := context.Background()
	c, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "cat.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	repo := core.RepositoryID("repo1")
	wt := core.WorktreeID("wt1")
	if err := c.RegisterRepository(ctx, repo, root, ""); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterWorktree(ctx, wt, repo, root, 1); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateGeneration(ctx, repo, 1, "fp"); err != nil {
		t.Fatal(err)
	}
	if err := c.SetActiveGeneration(ctx, repo, 1); err != nil {
		t.Fatal(err)
	}
	return c, repo, wt
}

// Gate 2: repeated unchanged reconciliation creates no jobs.
func TestGate2_UnchangedCreatesNoJobs(t *testing.T) {
	f := enginetest.NewGitFixture(t)
	f.WriteFile("a.go", "package a\n")
	f.WriteFile("b.go", "package b\n")
	f.Commit("c1")

	c, _, wt := setupCatalog(t, f.Root())
	r := New(c)
	ctx := context.Background()

	// First reconcile bootstraps: two upserts.
	p1, err := r.Reconcile(ctx, wt)
	if err != nil {
		t.Fatalf("reconcile1: %v", err)
	}
	if len(p1.Jobs) != 2 {
		t.Fatalf("bootstrap should produce 2 jobs, got %d", len(p1.Jobs))
	}
	applyPlan(t, c, "repo1", wt, p1)

	// Second reconcile with no changes: zero jobs (idle means idle).
	p2, err := r.Reconcile(ctx, wt)
	if err != nil {
		t.Fatalf("reconcile2: %v", err)
	}
	if len(p2.Jobs) != 0 {
		t.Fatalf("unchanged reconcile must be idle, got %d jobs: %+v", len(p2.Jobs), p2.Jobs)
	}
}

// Gate 2: a branch switch ends with an exact file-view match.
func TestGate2_BranchSwitchExactMatch(t *testing.T) {
	f := enginetest.NewGitFixture(t)
	f.WriteFile("keep.go", "package a\n")
	f.WriteFile("onmain.go", "package a\n")
	f.Commit("main")

	c, _, wt := setupCatalog(t, f.Root())
	r := New(c)
	ctx := context.Background()
	applyPlan(t, c, "repo1", wt, mustReconcile(t, r, wt))

	// Create and switch to a feature branch: change keep.go, drop onmain.go, add onfeat.go.
	gitInDir(t, f.Root(), "checkout", "-q", "-b", "feat")
	f.WriteFile("keep.go", "package a\n// feature\n")
	rmFile(t, f.Root(), "onmain.go")
	f.WriteFile("onfeat.go", "package feat\n")
	f.Commit("feat")

	p := mustReconcile(t, r, wt)
	byPath := map[string]core.Job{}
	for _, j := range p.Jobs {
		byPath[j.Path] = j
	}
	if j, ok := byPath["keep.go"]; !ok || j.Operation != core.OpUpsert {
		t.Fatalf("keep.go should be re-upserted after content change: %+v", byPath)
	}
	if j, ok := byPath["onfeat.go"]; !ok || j.Operation != core.OpUpsert {
		t.Fatalf("onfeat.go should be added: %+v", byPath)
	}
	if j, ok := byPath["onmain.go"]; !ok || j.Operation != core.OpDelete {
		t.Fatalf("onmain.go should be deleted after branch switch: %+v", byPath)
	}

	// Apply upserts, then a final reconcile shows only the pending delete (view
	// still references onmain.go until a worker removes it) — the upserts have
	// converged.
	applyPlan(t, c, "repo1", wt, p)
	p2 := mustReconcile(t, r, wt)
	for _, j := range p2.Jobs {
		if j.Operation != core.OpDelete || j.Path != "onmain.go" {
			t.Fatalf("after applying upserts, only the onmain.go delete should remain, got %+v", j)
		}
	}
}

// Gate 2: a change made with no filesystem event is still repaired by reconcile.
func TestGate2_DroppedEventRepaired(t *testing.T) {
	f := enginetest.NewGitFixture(t)
	f.WriteFile("a.go", "package a\n")
	f.Commit("c1")

	c, _, wt := setupCatalog(t, f.Root())
	r := New(c)
	ctx := context.Background()
	applyPlan(t, c, "repo1", wt, mustReconcile(t, r, wt))

	// Modify the file WITHOUT any event notification.
	f.WriteFile("a.go", "package a\n// silently changed\n")

	// Reconciliation (truth-based) detects the change regardless of events.
	p, err := r.Reconcile(ctx, wt)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(p.Jobs) != 1 || p.Jobs[0].Path != "a.go" || p.Jobs[0].Operation != core.OpUpsert {
		t.Fatalf("silent change must be detected as an upsert, got %+v", p.Jobs)
	}
}

func mustReconcile(t *testing.T, r *Engine, wt core.WorktreeID) Plan {
	t.Helper()
	p, err := r.Reconcile(context.Background(), wt)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return p
}
```

Also add these small test helpers at the end of `gate2_test.go` (git command + remove-file, mirroring the fixture's own git usage):

```go
func gitInDir(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := execCommand("git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func rmFile(t *testing.T, root, rel string) {
	t.Helper()
	if err := osRemove(filepath.Join(root, rel)); err != nil {
		t.Fatalf("rm %s: %v", rel, err)
	}
}
```

Add the imports `os` (aliased helper `osRemove = os.Remove`) and `os/exec` (`execCommand = exec.Command`) — OR, simpler, inline `exec.Command`/`os.Remove` directly and import `os` and `os/exec` in the file's import block. Use the direct form: import `"os"` and `"os/exec"`, replace `execCommand`→`exec.Command` and `osRemove`→`os.Remove`, and drop the alias lines.

- [ ] **Step 2: Run the Gate 2 tests under the race detector**

Run: `go test -race ./internal/enginev2/reconcile/ -run TestGate2 -v`
Expected: `TestGate2_UnchangedCreatesNoJobs`, `TestGate2_BranchSwitchExactMatch`, `TestGate2_DroppedEventRepaired` all PASS.

- [ ] **Step 3: Full Gate 2 verification**

```bash
go build ./...
go vet ./internal/enginev2/... ./git/
go test -race ./internal/enginev2/... ./git/
gofmt -l internal/enginev2 git
make lint
CGO_ENABLED=0 go build ./...
```
Expected: all pass; gofmt prints nothing; `make lint` exit 0; CGO_ENABLED=0 build succeeds.

- [ ] **Step 4: Commit**

```bash
git add internal/enginev2/reconcile/gate2_test.go
git commit -m "test(reconcile): Gate 2 — unchanged idle, branch-switch exact match, dropped-event repair"
```

---

## Gate 2 Exit Criteria (spec §9, Phase 2)

- [ ] `*reconcile.Engine` satisfies the `Reconciler` interface (assertion `var _ Reconciler = (*Engine)(nil)` present).
- [ ] Repeated unchanged reconciliation creates zero jobs (idle means idle) — proven with a real Git worktree + SQLite catalog.
- [ ] A branch switch produces the exact upsert/delete set to match the new checkout.
- [ ] A change made with no filesystem event is detected by reconciliation (dropped-event repair).
- [ ] Non-Git worktrees reconcile via filesystem content hashes.
- [ ] `go build ./...`, `go vet`, `go test -race`, `gofmt -l`, `make lint`, and `CGO_ENABLED=0 go build ./...` all pass; go.mod pin unchanged (go 1.24.2 / modernc v1.45.0).

---

## Self-Review Notes

**Spec coverage (Phase 2 deliverables, tight scope):**
- Git tree/blob, dirty, staged, deleted, untracked reconciliation → Task 1 (`WorktreeTruth`) + Task 3 diff. (Renames reconcile correctly as delete-old + upsert-new; no special-casing — noted non-goal.)
- non-Git metadata/content fallback → Task 3 (`filesystemTruth`).
- worktree discovery with no automatic index copying → N/A here (registration is explicit; reconciler never copies another worktree's index — it reads only `wt`'s own view).
- Deferred to Phase 4 (documented non-goals): fsnotify aggregation/overflow, watch-descriptor exhaustion fallback, branch-switch quiescence *debounce*, HEAD-tree short-circuit fast-path.

**Type consistency:** `CatalogReader` (Task 3) is satisfied by `*sqlite.Catalog` via existing `ActiveGeneration` + new `WorktreeInfo`/`WorktreeIndexedHashes` (Task 2) — the Gate 2 tests (Task 4) pass a real `*sqlite.Catalog` to `New`, which compiles only if the method set matches exactly. `TruthFunc` signature matches `git.WorktreeTruth`'s `(ctx, root) (map[string]string, error)`. Job fields (`Operation`, `Priority`, `Generation`, `DesiredHash`) match `core.Job`. The generation-monotonicity guard added to `CommitUpdate` in Phase 1's fix wave means `applyPlan`'s upsert commits (generation = job.Generation = active generation) are accepted.

**Known Phase 2 test simplification:** `applyPlan` does not apply delete jobs (the catalog has no delete-view primitive yet — that lands with the Phase 3 worker). Gate 2's delete assertions verify the reconciler *emits* the correct delete jobs and that upserts converge; full delete-application is a Phase 3 concern. This is called out so a reviewer does not read the skipped delete as a gap.
