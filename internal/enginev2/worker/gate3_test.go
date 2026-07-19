package worker_test

import (
	"context"
	"errors"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
	"github.com/yoanbernabeu/grepai/internal/enginev2/worker"
)

// hashLoader returns content chosen by desiredHash, so gen1/gen2 differ.
type hashLoader struct{ byHash map[string][]byte }

func (l hashLoader) Load(_ context.Context, _ core.RepositoryID, _, _, desiredHash string) ([]byte, error) {
	if b, ok := l.byHash[desiredHash]; ok {
		return b, nil
	}
	return nil, errors.New("no content for hash")
}

func TestGate3_FailedRequestPreservesOldFile(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)
	emb := enginetest.NewFakeEmbedder(4)
	load := hashLoader{byHash: map[string][]byte{"h1": []byte("func v1() {}"), "h2": []byte("func v2() {}")}}
	w := worker.New(c, realBuilder(emb, c), load, worker.NoCrash, 5)

	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatal(err)
	}
	v1, ok, _ := c.ResolveView(ctx, "w", "a.go")
	if !ok {
		t.Fatal("gen1 not committed")
	}
	// A rapid re-save (same generation, new desired hash) fails on every embed.
	emb.SetError(errors.New("endpoint down"))
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h2", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	for i := 0; i < 10; i++ { // exhaust attempts
		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatal(err)
		}
		if dlc, _ := c.DeadLetterCount(ctx); dlc > 0 {
			break
		}
	}
	if dlc, _ := c.DeadLetterCount(ctx); dlc == 0 {
		t.Fatal("the failing re-save should have dead-lettered after exhausting attempts")
	}
	v2, ok, _ := c.ResolveView(ctx, "w", "a.go")
	if !ok || v2 != v1 {
		t.Fatalf("old searchable file not preserved: v1=%s v2=%s ok=%v", v1, v2, ok)
	}
}

func TestGate3_CrashAtEveryInjectionPointRecovers(t *testing.T) {
	for _, name := range []string{"after-claim", "after-build", "after-chunks"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			c := newTestCatalog(t)
			seedRepoWorktree(t, c)
			emb := enginetest.NewFakeEmbedder(4)
			load := hashLoader{byHash: map[string][]byte{"h1": []byte("func v1() {}")}}
			reg := enginetest.NewCrashRegistry()
			w := worker.New(c, realBuilder(emb, c), load, reg.Check, 5)
			must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))

			reg.ArmAt(name)
			if _, err := w.ProcessOne(ctx); !errors.Is(err, enginetest.ErrInjectedCrash) {
				t.Fatalf("expected injected crash at %s, got %v", name, err)
			}
			if _, ok, _ := c.ResolveView(ctx, "w", "a.go"); ok {
				t.Fatal("view must not be visible after a pre-commit crash")
			}
			embeddedBefore := emb.TextsEmbedded()

			// Restart: recover + fresh worker, no crash.
			w2 := worker.New(c, realBuilder(emb, c), load, worker.NoCrash, 5)
			if _, err := w2.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			for {
				did, err := w2.ProcessOne(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if !did {
					break
				}
			}
			if _, ok, _ := c.ResolveView(ctx, "w", "a.go"); !ok {
				t.Fatalf("recovery did not commit a valid view (point=%s)", name)
			}
			if dlc, _ := c.DeadLetterCount(ctx); dlc != 0 {
				t.Fatalf("recovery dead-lettered (point=%s): %d", name, dlc)
			}
			if name == "after-chunks" && emb.TextsEmbedded() != embeddedBefore {
				t.Fatalf("after-chunks recovery re-embedded: before=%d after=%d", embeddedBefore, emb.TextsEmbedded())
			}
		})
	}
}

func TestGate3_RapidSavesCommitFinalGenerationOnly(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)
	emb := enginetest.NewFakeEmbedder(4)
	load := hashLoader{byHash: map[string][]byte{"h1": []byte("func v1() {}"), "h2": []byte("func v2() {}")}}
	w := worker.New(c, realBuilder(emb, c), load, worker.NoCrash, 5)

	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	// Rapid re-save before any processing (same generation, new desired hash).
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h2", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	id, ok, _ := c.ResolveView(ctx, "w", "a.go")
	if !ok {
		t.Fatal("no view committed")
	}
	// The committed artifact must be the final save's (SourceHash h2).
	want := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "h2", Fingerprint: "fp"}.ArtifactID()
	if id != want {
		t.Fatalf("final view is not the last save: got %s want %s", id, want)
	}
}

func TestGate3_SupersededMidClaimCommitsOnlyFinal(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	seedRepoWorktree(t, c)
	emb := enginetest.NewFakeEmbedder(4)
	load := hashLoader{byHash: map[string][]byte{"h1": []byte("func v1() {}"), "h2": []byte("func v2() {}")}}
	w := worker.New(c, realBuilder(emb, c), load, worker.NoCrash, 5)

	// Claim h1 first, THEN re-save to h2 while h1 is in flight, then finish.
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h1", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	claimed, ok, err := c.ClaimNextJob(ctx, core.PriorityBootstrap)
	if err != nil || !ok || claimed.DesiredHash != "h1" {
		t.Fatalf("claim h1: hash=%q ok=%v err=%v", claimed.DesiredHash, ok, err)
	}
	// Re-save supersedes the claimed h1 (same generation, resets claimed=0).
	must(t, c.UpsertJob(ctx, core.Job{WorktreeID: "w", Path: "a.go", DesiredHash: "h2", Generation: 1, Operation: core.OpUpsert, Priority: core.PriorityReconcile}))
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	want2 := core.ArtifactKey{RepositoryID: "r", RelativePath: "a.go", SourceHash: "h2", Fingerprint: "fp"}.ArtifactID()
	id, ok, _ := c.ResolveView(ctx, "w", "a.go")
	if !ok || id != want2 {
		t.Fatalf("final view must be the last save: got %s want %s ok=%v", id, want2, ok)
	}
	if dlc, _ := c.DeadLetterCount(ctx); dlc != 0 {
		t.Fatalf("supersession must not dead-letter: %d", dlc)
	}
}
