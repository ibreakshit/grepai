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
	PutChunkVector(ctx context.Context, chunkID string, repo core.RepositoryID, fingerprint string, vec []float32) error
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
type Builder interface {
	Build(ctx context.Context, req artifacts.BuildRequest) (core.Artifact, error)
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
}

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

// ProcessOne claims and fully processes the next eligible job. It returns
// (true, nil) when a job was handled (committed, dead-lettered, retried, or
// dropped as superseded), (false, nil) when the queue was empty, and a non-nil
// error only for an infrastructure failure or an injected crash — in which case
// the claimed job stays claimed for Recover to requeue.
func (w *Worker) ProcessOne(ctx context.Context) (bool, error) {
	job, ok, err := w.cat.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if err := w.crash("after-claim"); err != nil {
		return false, err
	}
	// Supersession pre-check: a newer desired intent means this claim is stale
	// — abandon it; the newer job is unclaimed and will be processed.
	if curGen, curHash, ok, _ := w.cat.CurrentJob(ctx, job.WorktreeID, job.Path); isSuperseded(curGen, curHash, ok, job) {
		return true, nil
	}
	if job.Operation == core.OpDelete {
		return true, w.cat.CommitDelete(ctx, job.WorktreeID, job.Path, job.Generation, job)
	}
	root, repo, err := w.cat.WorktreeInfo(ctx, job.WorktreeID)
	if err != nil {
		return true, w.retryOrDeadLetter(ctx, job, core.FailureTransient, err)
	}
	fp, err := w.cat.GenerationFingerprint(ctx, repo, job.Generation)
	if err != nil {
		return true, w.retryOrDeadLetter(ctx, job, core.FailureTransient, err)
	}
	content, err := w.load.Load(ctx, repo, root, job.Path, job.DesiredHash)
	if err != nil {
		return true, w.retryOrDeadLetter(ctx, job, core.FailureTransient, err)
	}
	key := core.ArtifactKey{RepositoryID: repo, RelativePath: job.Path, SourceHash: job.DesiredHash, Fingerprint: fp}

	var art core.Artifact
	wholeHit := false
	if existing, ok, gerr := w.cat.GetArtifact(ctx, key); gerr == nil && ok {
		// Whole-file cache hit: the artifact and its artifact_chunks mapping
		// already exist; reuse it and commit only the view switch.
		art, wholeHit = existing, true
	} else {
		art, err = w.build.Build(ctx, artifacts.BuildRequest{Key: key, Content: content})
		if err != nil {
			class := core.FailureTransient
			if errors.Is(err, artifacts.ErrDimensionMismatch) {
				class = core.FailurePermanent
			}
			return true, w.retryOrDeadLetter(ctx, job, class, err)
		}
	}
	if err := w.crash("after-build"); err != nil {
		return false, err
	}
	if !wholeHit {
		for _, ch := range art.Chunks {
			if err := w.cat.PutChunkVector(ctx, ch.ChunkID, repo, fp, ch.Vector); err != nil {
				return true, w.retryOrDeadLetter(ctx, job, core.FailureTransient, err)
			}
		}
	}
	if err := w.crash("after-chunks"); err != nil {
		return false, err
	}
	// Cheap second supersession guard; the commit's generation + desired_hash
	// guards are the ultimate safety net, but this avoids a wasted commit and a
	// visible view flicker when a rapid re-save arrived during the build.
	if curGen, curHash, ok, _ := w.cat.CurrentJob(ctx, job.WorktreeID, job.Path); isSuperseded(curGen, curHash, ok, job) {
		return true, nil
	}
	req := core.CommitRequest{
		View:     core.ViewEntry{WorktreeID: job.WorktreeID, Path: job.Path, ArtifactID: art.ID, Generation: job.Generation},
		Artifact: art,
		Chunks:   art.Chunks, // nil for a whole-file cache hit (mapping already present)
	}
	if err := w.cat.CommitUpdate(ctx, req, job); err != nil {
		return true, w.retryOrDeadLetter(ctx, job, core.FailureTransient, err)
	}
	return true, nil
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
