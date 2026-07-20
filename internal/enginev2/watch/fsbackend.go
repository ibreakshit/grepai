package watch

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/fsnotify/fsnotify"
	"github.com/yoanbernabeu/grepai/indexer"
)

// fsBackend is the production Backend: fsnotify (inotify on Linux) with a
// recursive, gitignore-aware watch over the worktree root.
//
// Unlike the v1 watcher there is NO extension filter and NO dotfile skip —
// v2 indexes every git-tracked file (.github/workflows included), so any
// Create/Write/Rename/Remove under a watched directory is a hint. Two trees
// are hard-excluded independent of any ignore file: .grepai (the catalog's
// own WAL churns on every commit — watching it would self-trigger reconcile
// loops) and .git (internal churn; a checkout fires events on the working
// files themselves, which is the signal we want).
type fsBackend struct {
	root   string
	fsw    *fsnotify.Watcher
	ignore *indexer.IgnoreMatcher
	hints  chan struct{}
	errs   chan error
	done   chan struct{}
}

// NewFSBackend is the BackendFactory for production use.
func NewFSBackend(root string) (Backend, error) {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	// Gitignore hierarchy keeps giant ignored trees (node_modules) out of the
	// kernel watch table; nil extras — the daemon does not load per-repo config.
	ign, err := indexer.NewIgnoreMatcher(root, nil, "")
	if err != nil {
		_ = fsw.Close()
		return nil, err
	}
	return &fsBackend{
		root:   root,
		fsw:    fsw,
		ignore: ign,
		hints:  make(chan struct{}, 1), // coalescing: one pending hint is enough
		errs:   make(chan error, 4),
		done:   make(chan struct{}),
	}, nil
}

var _ BackendFactory = NewFSBackend

var _ Backend = (*fsBackend)(nil)

// Start adds watches over the tree and begins translating events into hints.
func (b *fsBackend) Start() error {
	if err := b.addTree(b.root); err != nil {
		return err
	}
	go b.run()
	return nil
}

func (b *fsBackend) Hints() <-chan struct{} { return b.hints }
func (b *fsBackend) Errors() <-chan error   { return b.errs }

func (b *fsBackend) Close() error {
	close(b.done)
	return b.fsw.Close()
}

// hardSkip reports whether rel names (or is inside) a tree we never watch.
func hardSkip(rel string) bool {
	return rel == ".git" || strings.HasPrefix(rel, ".git"+string(filepath.Separator)) ||
		rel == ".grepai" || strings.HasPrefix(rel, ".grepai"+string(filepath.Separator))
}

// addTree registers a watch on every non-ignored directory under dir.
// ENOSPC (inotify watch limit) is translated to ErrExhausted so the manager
// degrades this repo to polling.
func (b *fsBackend) addTree(dir string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// The ROOT itself failing to stat is not a best-effort case: it means
			// the worktree is gone and the manager must deregister it — otherwise
			// Start would succeed with ZERO watches and the safety-net reconcile
			// would retry a vanished repo forever.
			if path == b.root {
				return fmt.Errorf("%w: %v", ErrRootGone, err)
			}
			return nil // unreadable subtree: skip, reconcile still covers it
		}
		if !info.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(b.root, path)
		if rerr != nil {
			return nil
		}
		if rel != "." {
			if hardSkip(rel) {
				return filepath.SkipDir
			}
			if b.ignore.ShouldSkipDir(rel) {
				return filepath.SkipDir
			}
		}
		if aerr := b.fsw.Add(path); aerr != nil {
			if errors.Is(aerr, syscall.ENOSPC) {
				return fmt.Errorf("%w: %v", ErrExhausted, aerr)
			}
			// Other per-dir failures (permissions, races): skip; the safety-net
			// reconcile covers whatever happens inside.
			return nil
		}
		return nil
	})
}

// run translates raw fsnotify events into coalesced hints.
func (b *fsBackend) run() {
	for {
		select {
		case <-b.done:
			return

		case ev, ok := <-b.fsw.Events:
			if !ok {
				return
			}
			rel, err := filepath.Rel(b.root, ev.Name)
			if err != nil || hardSkip(rel) {
				continue
			}
			if rel != "." && b.ignore.ShouldIgnore(rel) {
				continue
			}
			// Chmod alone is not a content change.
			if !ev.Op.Has(fsnotify.Create) && !ev.Op.Has(fsnotify.Write) &&
				!ev.Op.Has(fsnotify.Rename) && !ev.Op.Has(fsnotify.Remove) {
				continue
			}
			// The root vanishing ends this watch (deleted worktree).
			if rel == "." && (ev.Op.Has(fsnotify.Remove) || ev.Op.Has(fsnotify.Rename)) {
				b.sendErr(ErrRootGone)
				return
			}
			// A new directory needs watches on its whole subtree — and files may
			// have landed inside before the watch attached, hence also a hint.
			if ev.Op.Has(fsnotify.Create) {
				if fi, serr := os.Stat(ev.Name); serr == nil && fi.IsDir() {
					if aerr := b.addTree(ev.Name); aerr != nil && errors.Is(aerr, ErrExhausted) {
						b.sendErr(ErrExhausted)
						return
					}
				}
			}
			b.hint()

		case err, ok := <-b.fsw.Errors:
			if !ok {
				return
			}
			if errors.Is(err, fsnotify.ErrEventOverflow) {
				b.sendErr(ErrOverflow)
				continue
			}
			if _, serr := os.Stat(b.root); serr != nil {
				b.sendErr(ErrRootGone)
				return
			}
			b.sendErr(err)
		}
	}
}

// hint delivers a coalesced dirty signal (never blocks: one pending suffices).
func (b *fsBackend) hint() {
	select {
	case b.hints <- struct{}{}:
	default:
	}
}

func (b *fsBackend) sendErr(err error) {
	select {
	case b.errs <- err:
	default:
	}
}
