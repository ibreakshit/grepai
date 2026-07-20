package worker

import (
	"context"
	"errors"

	"github.com/yoanbernabeu/grepai/internal/enginev2/artifacts"
	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// Catalog is the durable-state surface the worker needs.
type Catalog interface {
	ClaimNextJob(ctx context.Context, minPriority core.Priority) (core.Job, bool, error)
	WorktreeInfo(ctx context.Context, wt core.WorktreeID) (string, core.RepositoryID, error)
	GenerationFingerprint(ctx context.Context, repo core.RepositoryID, gen core.Generation) (string, error)
	GetArtifact(ctx context.Context, key core.ArtifactKey) (core.Artifact, bool, error)
	PutChunkVector(ctx context.Context, chunkID string, repo core.RepositoryID, fingerprint string, vec []float32, content string) error
	CommitUpdate(ctx context.Context, req core.CommitRequest, job core.Job) error
	CommitDelete(ctx context.Context, wt core.WorktreeID, relPath string, gen core.Generation, job core.Job) error
	DeadLetterJob(ctx context.Context, job core.Job, reason string) error
	FailJobAttempt(ctx context.Context, job core.Job) (int, error)
	CurrentJob(ctx context.Context, wt core.WorktreeID, relPath string) (core.Generation, string, bool, error)
	RequeueClaimedJobs(ctx context.Context) (int, error)
}

// isSuperseded reports whether the current desired intent for a path has moved
// past the job the worker claimed — a newer generation, or the same generation
// with a different desired hash (a rapid re-save). A vanished row (ok=false)
// also counts: the claimed intent no longer exists, so nothing should commit.
func isSuperseded(curGen core.Generation, curHash string, ok bool, job core.Job) bool {
	if !ok {
		return true
	}
	return curGen > job.Generation || (curGen == job.Generation && curHash != job.DesiredHash)
}

// Builder builds an artifact from content (satisfied by *artifacts.DefaultBuilder).
// The EndpointResult reports how the embedding backend was involved.
type Builder interface {
	Build(ctx context.Context, req artifacts.BuildRequest) (core.Artifact, artifacts.EndpointResult, error)
}

// SymbolExtractor extracts artifact-scoped symbol definitions and call edges
// from file content (satisfied by *symbols.Extractor). Derived data: extraction
// failures are non-fatal — the artifact commits without symbols and the marker
// stays 0 so the daemon backfill retries later.
type SymbolExtractor interface {
	Extract(ctx context.Context, relPath, content string) ([]core.SymbolDef, []core.SymbolEdge, error)
}

// CrashHook is called at durable-state boundaries; a non-nil return simulates
// a process crash at that point. Production uses NoCrash.
type CrashHook func(name string) error

// NoCrash is the production crash hook: it never crashes.
func NoCrash(string) error { return nil }

// Worker drains the durable job queue: claim → build (cache-miss-only) →
// persist chunk cache → atomic commit, classifying failures.
type Worker struct {
	cat         Catalog
	build       Builder
	load        ContentLoader
	crash       CrashHook
	maxAttempts int
	symbols     SymbolExtractor // nil: no extraction (marker stays 0)
}

// SetSymbolExtractor wires symbol extraction into the build path. Optional —
// nil keeps the pre-trace behavior (used by tests and the one-shot runtime
// until it opts in).
func (w *Worker) SetSymbolExtractor(x SymbolExtractor) { w.symbols = x }

// New constructs a Worker. A nil crash hook defaults to NoCrash; maxAttempts
// below 1 defaults to 5.
func New(cat Catalog, build Builder, load ContentLoader, crash CrashHook, maxAttempts int) *Worker {
	if crash == nil {
		crash = NoCrash
	}
	if maxAttempts < 1 {
		maxAttempts = 5
	}
	return &Worker{cat: cat, build: build, load: load, crash: crash, maxAttempts: maxAttempts}
}

// Recover requeues jobs a crashed worker left claimed. Run once at startup.
func (w *Worker) Recover(ctx context.Context) (int, error) {
	return w.cat.RequeueClaimedJobs(ctx)
}

// Outcome classifies the result of processing one claimed job. The zero value
// is reserved for "unclassified" (an injected crash or infra error where the
// job must stay claimed for Recover to requeue).
type Outcome uint8

const (
	// OutcomeCommitted: the artifact/view/job committed (or a delete applied).
	OutcomeCommitted Outcome = iota + 1
	// OutcomeSuperseded: a newer desired intent replaced this job; dropped.
	OutcomeSuperseded
	// OutcomeTransient: a retryable failure (endpoint/timeout/read error).
	OutcomeTransient
	// OutcomePermanent: a non-retryable failure (e.g. dimension mismatch).
	OutcomePermanent
)

// ProcessClaimed processes an already-claimed job and returns its classified
// outcome plus how the embedding backend was involved (so the scheduler's
// circuit breaker reacts only to real endpoint health — independent of any
// later local catalog error). It does NOT retry, dead-letter, or unclaim — the
// caller owns that. For OutcomeTransient/OutcomePermanent the returned error is
// the cause. An injected crash returns (0, ep, err) with a zero Outcome.
func (w *Worker) ProcessClaimed(ctx context.Context, job core.Job) (Outcome, artifacts.EndpointResult, error) {
	ep := artifacts.EndpointNotContacted
	if err := w.crash("after-claim"); err != nil {
		return 0, ep, err
	}
	// Supersession pre-check: a newer desired intent means this claim is stale.
	// A read error must not be mistaken for supersession (that would drop the
	// still-claimed job): classify it transient so the caller releases/retries.
	curGen, curHash, cur, err := w.cat.CurrentJob(ctx, job.WorktreeID, job.Path)
	if err != nil {
		return OutcomeTransient, ep, err
	}
	if isSuperseded(curGen, curHash, cur, job) {
		return OutcomeSuperseded, ep, nil
	}
	if job.Operation == core.OpDelete {
		if err := w.cat.CommitDelete(ctx, job.WorktreeID, job.Path, job.Generation, job); err != nil {
			return OutcomeTransient, ep, err
		}
		return OutcomeCommitted, ep, nil
	}
	root, repo, err := w.cat.WorktreeInfo(ctx, job.WorktreeID)
	if err != nil {
		return OutcomeTransient, ep, err
	}
	fp, err := w.cat.GenerationFingerprint(ctx, repo, job.Generation)
	if err != nil {
		return OutcomeTransient, ep, err
	}
	content, err := w.load.Load(ctx, repo, root, job.Path, job.DesiredHash)
	if err != nil {
		return OutcomeTransient, ep, err
	}
	key := core.ArtifactKey{RepositoryID: repo, RelativePath: job.Path, SourceHash: job.DesiredHash, Fingerprint: fp}

	var art core.Artifact
	wholeHit := false
	if existing, ok, gerr := w.cat.GetArtifact(ctx, key); gerr == nil && ok {
		// Whole-file cache hit: the artifact and its artifact_chunks mapping
		// already exist; reuse it and commit only the view switch.
		art, wholeHit = existing, true
	} else {
		art, ep, err = w.build.Build(ctx, artifacts.BuildRequest{Key: key, Content: content})
		if err != nil {
			if errors.Is(err, artifacts.ErrDimensionMismatch) {
				return OutcomePermanent, ep, err
			}
			return OutcomeTransient, ep, err
		}
	}
	// From here the endpoint result is fixed: a successful embed (ep) stays the
	// endpoint signal even if a later local write fails.
	if err := w.crash("after-build"); err != nil {
		return 0, ep, err
	}
	if !wholeHit {
		for _, ch := range art.Chunks {
			if err := w.cat.PutChunkVector(ctx, ch.ChunkID, repo, fp, ch.Vector, ch.Content); err != nil {
				return OutcomeTransient, ep, err
			}
		}
	}
	if err := w.crash("after-chunks"); err != nil {
		return 0, ep, err
	}
	// Cheap second supersession guard; the commit's generation + desired_hash
	// guards are the ultimate safety net, but this avoids a wasted commit and a
	// visible view flicker when a rapid re-save arrived during the build.
	curGen2, curHash2, cur2, err := w.cat.CurrentJob(ctx, job.WorktreeID, job.Path)
	if err != nil {
		return OutcomeTransient, ep, err
	}
	if isSuperseded(curGen2, curHash2, cur2, job) {
		return OutcomeSuperseded, ep, nil
	}
	req := core.CommitRequest{
		View:     core.ViewEntry{WorktreeID: job.WorktreeID, Path: job.Path, ArtifactID: art.ID, Generation: job.Generation},
		Artifact: art,
		Chunks:   art.Chunks, // nil for a whole-file cache hit (mapping already present)
	}
	// Symbol extraction (artifact-scoped derived data, spec §5.3). A whole-file
	// cache hit reuses an artifact whose symbols were extracted when it was
	// first committed — no work. Extraction errors are non-fatal: the artifact
	// still commits, SymbolsExtracted stays false, and the daemon backfill
	// retries later.
	if w.symbols != nil && !wholeHit {
		if defs, edges, serr := w.symbols.Extract(ctx, job.Path, string(content)); serr == nil {
			req.Symbols, req.SymbolEdges, req.SymbolsExtracted = defs, edges, true
		}
	}
	if err := w.cat.CommitUpdate(ctx, req, job); err != nil {
		return OutcomeTransient, ep, err
	}
	return OutcomeCommitted, ep, nil
}

// ProcessOne claims and fully processes the next eligible job, applying durable
// retry and dead-lettering itself (the standalone Phase 3 driver). It returns
// (true, nil) when a job was handled (committed, dead-lettered, retried, or
// dropped as superseded), (false, nil) when the queue was empty, and a non-nil
// error only for an unclassified crash — in which case the claimed job stays
// claimed for Recover to requeue. The scheduler instead claims jobs and calls
// ProcessClaimed directly, owning retry timing.
func (w *Worker) ProcessOne(ctx context.Context) (bool, error) {
	job, ok, err := w.cat.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	oc, _, cause := w.ProcessClaimed(ctx, job)
	switch oc {
	case OutcomeCommitted, OutcomeSuperseded:
		return true, nil
	case OutcomePermanent:
		return true, w.retryOrDeadLetter(ctx, job, core.FailurePermanent, cause)
	case OutcomeTransient:
		return true, w.retryOrDeadLetter(ctx, job, core.FailureTransient, cause)
	default: // 0: unclassified crash — leave claimed for Recover
		return false, cause
	}
}

// Run drains the queue until empty, then returns when ctx is canceled.
func (w *Worker) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		did, err := w.ProcessOne(ctx)
		if err != nil {
			return err
		}
		if !did {
			return nil // queue drained
		}
	}
}

func (w *Worker) retryOrDeadLetter(ctx context.Context, job core.Job, class core.FailureClass, cause error) error {
	switch class {
	case core.FailurePermanent:
		return w.cat.DeadLetterJob(ctx, job, "permanent: "+cause.Error())
	case core.FailureSuperseded:
		return nil
	default: // transient
		attempts, err := w.cat.FailJobAttempt(ctx, job)
		if err != nil {
			return err
		}
		if attempts >= w.maxAttempts {
			return w.cat.DeadLetterJob(ctx, job, "attempts exhausted: "+cause.Error())
		}
		return nil
	}
}
