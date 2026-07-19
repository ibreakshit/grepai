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
