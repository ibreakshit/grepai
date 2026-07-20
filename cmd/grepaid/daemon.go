package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yoanbernabeu/grepai/embedder"
	"github.com/yoanbernabeu/grepai/indexer"
	"github.com/yoanbernabeu/grepai/internal/enginev2/artifacts"
	"github.com/yoanbernabeu/grepai/internal/enginev2/catalogset"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/daemoncfg"
	v2embedder "github.com/yoanbernabeu/grepai/internal/enginev2/embedder"
	"github.com/yoanbernabeu/grepai/internal/enginev2/reconcile"
	"github.com/yoanbernabeu/grepai/internal/enginev2/registry"
	"github.com/yoanbernabeu/grepai/internal/enginev2/rpc"
	"github.com/yoanbernabeu/grepai/internal/enginev2/runtime"
	"github.com/yoanbernabeu/grepai/internal/enginev2/scheduler"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
	"github.com/yoanbernabeu/grepai/internal/enginev2/worker"
)

// run assembles the daemon over the host paths + config and serves until ctx is
// canceled. It is separate from main (no signal handling / os.Exit) so tests
// can drive it with a cancelable context.
func run(ctx context.Context, p daemoncfg.Paths, cfg *daemoncfg.Config, logw io.Writer) error {
	emb, err := embedder.NewFromConfig(cfg.ToConfig())
	if err != nil {
		return fmt.Errorf("build embedder: %w", err)
	}
	return runWithEmbedder(ctx, p, cfg, emb, logw)
}

// runWithEmbedder is run with the embedder injected, so tests drive it with a
// deterministic FakeEmbedder.
func runWithEmbedder(ctx context.Context, p daemoncfg.Paths, cfg *daemoncfg.Config, emb v2embedder.Embedder, logw io.Writer) error {
	lg := log.New(logw, "grepaid ", log.LstdFlags)

	cc := cfg.ToConfig()
	fp := runtime.Fingerprint(cc)
	size, overlap := effectiveChunkParams(cfg)

	set := catalogset.New()
	defer set.Close()
	set.OnAggregateError(func(repo core.RepositoryID, err error) {
		lg.Printf("aggregate: skipping catalog %s: %v", repo, err)
	})
	br := catalogset.NewBuilderRouter()

	reg, err := registry.Load(p.Registry)
	if err != nil {
		return fmt.Errorf("load registry: %w", err)
	}
	rm := &registryManager{path: p.Registry, reg: reg}

	rec := reconcile.New(set)
	inner := service.New(set, rec, emb, fp, cfg.SearchLimit)
	regsvc := &registeringService{
		Server:  inner,
		set:     set,
		br:      br,
		emb:     emb,
		fp:      fp,
		reg:     rm,
		lg:      lg,
		size:    size,
		overlap: overlap,
	}

	// Rehydrate every known repo through the same register path (opens the
	// catalog, builds its per-repo builder, rolls a fingerprint mismatch, and
	// refreshes the registry). A repo that fails (schema too new, open error) is
	// logged and skipped, never wedging the daemon. A repo whose directory is
	// gone is skipped BEFORE Register, which would otherwise recreate the
	// deleted .grepai directory (spec: "repo dir gone → skip + log").
	for _, e := range reg.Entries {
		if _, statErr := os.Stat(e.Root); statErr != nil {
			lg.Printf("startup: skipping repo %s: root missing (%v)", e.RepositoryID, statErr)
			continue
		}
		// A root whose .grepai directory is gone was deliberately uninitialized:
		// skip it rather than recreating state the operator deleted. (This is a
		// best-effort check — an operator deleting .grepai mid-startup races it,
		// which is inherent to any filesystem check.) A missing CATALOG under an
		// intact .grepai needs no special-casing: Register's empty-view
		// predicate recreates and reconciles it.
		if _, statErr := os.Stat(filepath.Join(e.Root, ".grepai")); statErr != nil {
			lg.Printf("startup: skipping repo %s: .grepai removed (uninitialized); delete it from %s to silence", e.RepositoryID, p.Registry)
			continue
		}
		if _, err := regsvc.Register(ctx, service.RegisterRequest{Root: e.Root}); err != nil {
			lg.Printf("startup: skipping repo %s: %v", e.RepositoryID, err)
		}
	}

	sc := cfg.SchedulerConfigOrDefault()
	wk := worker.New(set, br, runtime.NewDiskLoader(), worker.NoCrash, sc.MaxJobAttempts)
	if n, err := wk.Recover(ctx); err != nil {
		lg.Printf("startup: worker recover failed: %v", err)
	} else if n > 0 {
		lg.Printf("startup: requeued %d claimed jobs", n)
	}

	sch, err := scheduler.New(sc, set, wk, scheduler.SystemClock{}, 1)
	if err != nil {
		return fmt.Errorf("build scheduler: %w", err)
	}

	// Listen. The flock (held by main) makes unlinking a stale socket safe —
	// but only ever unlink an actual socket: a mistyped GREPAID_SOCKET pointing
	// at a regular file must not delete it.
	if err := os.MkdirAll(filepath.Dir(p.Socket), 0o700); err != nil {
		return fmt.Errorf("create socket dir: %w", err)
	}
	if fi, statErr := os.Lstat(p.Socket); statErr == nil {
		if fi.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("refusing to remove %s: exists and is not a socket", p.Socket)
		}
		_ = os.Remove(p.Socket)
	}
	l, err := net.Listen("unix", p.Socket)
	if err != nil {
		return fmt.Errorf("listen %s: %w", p.Socket, err)
	}
	// The RPC surface is unauthenticated; the socket mode is the access control.
	// A chmod failure is fatal, not a log line.
	if err := os.Chmod(p.Socket, 0o600); err != nil {
		_ = l.Close()
		_ = os.Remove(p.Socket)
		return fmt.Errorf("chmod socket: %w", err)
	}

	// The scheduler runs under a derived context so a serve failure also stops it.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	schDone := make(chan struct{})
	go func() { _ = sch.Run(runCtx); close(schDone) }()
	serveErr := make(chan error, 1)
	go func() { serveErr <- rpc.Serve(runCtx, l, regsvc) }()
	lg.Printf("listening on %s (%d repos registered)", p.Socket, len(reg.Entries))

	var retErr error
	serving, scheduling := true, true
	select {
	case <-ctx.Done():
	case err := <-serveErr:
		serving = false // Serve already returned; don't wait on it again below
		if err != nil {
			retErr = fmt.Errorf("rpc serve: %w", err)
		}
	}

	// Graceful shutdown ordering. cancel() makes rpc.Serve close the listener
	// AND every open connection (handler goroutines — including mutating
	// Register/Reconcile calls — see canceled contexts and unblock), and stops
	// the scheduler. We then WAIT for both loops before the deferred set.Close()
	// tears down the catalogs; the closed Set additionally rejects any
	// theoretical straggler Register instead of opening a fresh handle.
	cancel()
	drain := time.After(30 * time.Second)
	for serving || scheduling {
		select {
		case <-serveErr:
			serving = false
		case <-schDone:
			scheduling = false
		case <-drain:
			// Last resort for an embedder call that ignores cancellation. Closing
			// the catalogs under an in-flight commit is safe for durability (the
			// commit fails, the job stays claimed, and Recover requeues it on
			// next start) — preferable to a daemon that can never shut down.
			lg.Printf("shutdown did not drain within 30s (serving=%v scheduling=%v); closing catalogs anyway (claimed jobs recover on next start)", serving, scheduling)
			serving, scheduling = false, false
		}
	}
	_ = os.Remove(p.Socket)
	lg.Printf("shutting down")
	return retErr
}

// effectiveChunkParams mirrors runtime.chunkParams so the chunker the daemon
// builds matches the size/overlap that runtime.Fingerprint encodes.
func effectiveChunkParams(cfg *daemoncfg.Config) (size, overlap int) {
	size = cfg.Chunking.Size
	if size <= 0 {
		size = indexer.DefaultChunkSize
	}
	overlap = cfg.Chunking.Overlap
	if overlap < 0 {
		overlap = indexer.DefaultChunkOverlap
	}
	return size, overlap
}

// registryManager serializes registry writes (Phase 0 review obligation:
// Upsert/Save are not internally synchronized).
type registryManager struct {
	mu   sync.Mutex
	path string
	reg  *registry.Registry
}

// upsert stores the entry, carrying forward the existing reconcile cursor when
// the caller passes zero values (a plain re-Register must not blank the cursor
// touchReconcile maintains).
func (m *registryManager) upsert(e registry.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, old := range m.reg.Entries {
		if old.RepositoryID == e.RepositoryID {
			if e.LastReconciledAt == "" {
				e.LastReconciledAt = old.LastReconciledAt
			}
			if e.PendingCount == 0 {
				e.PendingCount = old.PendingCount
			}
			break
		}
	}
	m.reg.Upsert(e)
	return m.reg.Save(m.path)
}

// touchReconcile refreshes a repo's registry cursor after a reconcile. The
// pending value is the jobs queued BY that reconcile — a point-in-time cache
// for cheap status, not a live count (the catalog stays the source of truth).
func (m *registryManager) touchReconcile(repoID string, pending int, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.reg.Entries {
		if m.reg.Entries[i].RepositoryID == repoID {
			m.reg.Entries[i].LastReconciledAt = at.UTC().Format(time.RFC3339)
			m.reg.Entries[i].PendingCount = pending
			_ = m.reg.Save(m.path) // best-effort cursor cache
			return
		}
	}
}

// registeringService decorates service.Server so a Register call also opens the
// repo's catalog into the set, builds its per-repo builder, rolls a fingerprint
// mismatch, and records it in the registry. The other seven methods are the
// embedded Server's. Register is serialized (set.Add + builder + registry).
type registeringService struct {
	*service.Server
	mu      sync.Mutex
	set     *catalogset.Set
	br      *catalogset.BuilderRouter
	emb     v2embedder.Embedder
	fp      string
	reg     *registryManager
	lg      *log.Logger
	size    int
	overlap int
}

func (s *registeringService) Register(ctx context.Context, req service.RegisterRequest) (service.RegisterResponse, error) {
	canonical := canonicalizeRoot(req.Root)
	repo := core.RepositoryID(canonical)
	catPath := filepath.Join(canonical, ".grepai", "catalog_v2.db")
	catDir := filepath.Dir(catPath)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Create the .grepai data dir and keep it out of reconciliation before
	// opening the catalog (mirrors runtime.Open).
	if err := os.MkdirAll(catDir, 0o750); err != nil {
		return service.RegisterResponse{}, fmt.Errorf("create data dir for %s: %w", repo, err)
	}
	if err := runtime.EnsureSelfIgnore(catDir, filepath.Base(catPath)); err != nil {
		return service.RegisterResponse{}, fmt.Errorf("self-ignore for %s: %w", repo, err)
	}
	if err := s.set.Add(ctx, repo, catPath); err != nil {
		return service.RegisterResponse{}, fmt.Errorf("open catalog for %s: %w", repo, err)
	}
	cache, err := s.set.ChunkCache(repo)
	if err != nil {
		return service.RegisterResponse{}, err
	}
	s.br.Add(repo, artifacts.New(indexer.NewChunker(s.size, s.overlap), s.emb, cache))

	resp, err := s.Server.Register(ctx, service.RegisterRequest{Root: canonical})
	if err != nil {
		return resp, err
	}
	if err := s.rollIfMismatch(ctx, repo, resp.WorktreeID); err != nil {
		return resp, err
	}

	// Unified needs-reconcile predicate: an empty view with nothing pending
	// means this worktree is unindexed — whether because it is brand new, a
	// fingerprint roll just cleared it, a prior initial reconcile failed and is
	// being retried, or the catalog file was recreated after deletion. This is a
	// durable state check, not a fragile registered-before flag, so every one of
	// those paths converges on "reconcile now, loudly". (Invariant 3 intact:
	// Register is a mutation path; queries still never enqueue.)
	indexed, err := s.set.WorktreeIndexedHashes(ctx, resp.WorktreeID)
	if err != nil {
		return resp, err
	}
	pending, err := s.set.WorktreePendingCount(ctx, resp.WorktreeID)
	if err != nil {
		return resp, err
	}
	if len(indexed) == 0 && pending == 0 {
		rresp, rerr := s.Reconcile(ctx, service.ReconcileRequest{WorktreeID: resp.WorktreeID})
		if rerr != nil {
			// Fail Register loudly BEFORE the registry upsert: a retry sees the
			// same empty-view state and reconciles again — no false fresh-empty.
			return resp, fmt.Errorf("registered %s but its initial reconcile failed: %w", repo, rerr)
		}
		s.lg.Printf("registered %s: initial reconcile queued %d jobs", repo, rresp.JobsQueued)
	}

	active, err := s.set.ActiveGeneration(ctx, repo)
	if err != nil {
		return resp, err
	}
	if err := s.reg.upsert(registry.Entry{
		RepositoryID:     string(repo),
		Root:             canonical,
		CatalogPath:      catPath,
		ActiveGeneration: int64(active),
	}); err != nil {
		s.lg.Printf("registry update for %s failed: %v", repo, err)
	}
	return resp, nil
}

// rollIfMismatch rolls the repo to a fresh generation under the daemon's
// fingerprint when its active generation was built with a different embedder
// (a background service must not wedge on one stale repo; the old vectors are
// unrankable against the daemon's query embeddings).
//
// Because the worktree view is not generation-filtered, rolling the generation
// alone is not enough: reconciliation would still see the old hashes (queue
// nothing) and search would keep serving incompatible vectors. So the roll also
// CLEARS the worktree's view+jobs — the next reconcile re-desires every file
// under the new fingerprint, and search is transiently empty (correct) until
// reindexed.
//
// The generation advance is crash-idempotent: it scans forward from active+1,
// reusing a generation that already carries the daemon fingerprint (a prior
// crash between create and activate) and skipping any occupied by a different
// fingerprint, so a restart never fails on the unique generation key.
func (s *registeringService) rollIfMismatch(ctx context.Context, repo core.RepositoryID, wt core.WorktreeID) error {
	active, err := s.set.ActiveGeneration(ctx, repo)
	if err != nil {
		return err
	}
	activeFP, err := s.set.GenerationFingerprint(ctx, repo, active)
	if err != nil {
		return err
	}
	if activeFP == s.fp {
		return nil
	}
	next := active + 1
	for {
		existingFP, gerr := s.set.GenerationFingerprint(ctx, repo, next)
		if gerr != nil { // free slot: create it
			if cerr := s.set.CreateGeneration(ctx, repo, next, s.fp); cerr != nil {
				return cerr
			}
			break
		}
		if existingFP == s.fp { // left over from a crashed prior roll: reuse
			break
		}
		next++ // occupied by another fingerprint: keep scanning
	}
	// The clear (view + jobs) and the activation are ONE transaction
	// (RollWorktreeGeneration): a crash before it leaves the mismatch detectable
	// (restart re-rolls; the generation scan reuses the half-created
	// generation), and single-writer serialization means a concurrent worker
	// commit lands either before the roll (its rows are cleared) or after (the
	// invariant-12 guard rejects its now-retired generation) — no interleaving
	// can strand stale view rows behind a matching fingerprint.
	if err := s.set.RollWorktreeGeneration(ctx, wt, repo, next); err != nil {
		return err
	}
	s.lg.Printf("repo %s fingerprint mismatch (%s != daemon %s); cleared the view and rolled to generation %d for reindex", repo, activeFP, s.fp, next)
	return nil
}

// Reconcile delegates to the inner service, then refreshes the repo's registry
// cursor (LastReconciledAt/PendingCount) — a cheap status cache for
// `grepai daemon status` and restart.
func (s *registeringService) Reconcile(ctx context.Context, req service.ReconcileRequest) (service.ReconcileResponse, error) {
	resp, err := s.Server.Reconcile(ctx, req)
	if err != nil {
		return resp, err
	}
	if _, repo, werr := s.set.WorktreeInfo(ctx, req.WorktreeID); werr == nil {
		s.reg.touchReconcile(string(repo), resp.JobsQueued, time.Now())
	}
	return resp, nil
}

// canonicalizeRoot resolves an absolute, symlink-free root so the same physical
// repo always maps to the same id (mirrors runtime.Open).
func canonicalizeRoot(root string) string {
	abs, err := filepath.Abs(root)
	if err != nil {
		abs = root
	}
	if resolved, rerr := filepath.EvalSymlinks(abs); rerr == nil {
		return resolved
	}
	return abs
}
