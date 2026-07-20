package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"github.com/yoanbernabeu/grepai/internal/enginev2/daemoncfg"
	"github.com/yoanbernabeu/grepai/internal/enginev2/daemonctl"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
	"github.com/yoanbernabeu/grepai/internal/enginev2/rpc"
	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
)

// testConfig returns a daemon config whose embedder fields are cosmetic (the
// test injects a FakeEmbedder); only chunking + search limit matter.
func testConfig() *daemoncfg.Config {
	dims := 4
	return &daemoncfg.Config{
		Embedder:    daemoncfg.EmbedderConfig{Provider: "synthetic", Model: "fake", Dimensions: &dims},
		Chunking:    daemoncfg.ChunkingConfig{Size: 512, Overlap: 64},
		SearchLimit: 10,
	}
}

func setHostEnv(t *testing.T) daemoncfg.Paths {
	t.Helper()
	state := t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	t.Setenv("XDG_RUNTIME_DIR", "")
	t.Setenv("GREPAID_SOCKET", filepath.Join(state, "grepaid.sock"))
	p, err := daemoncfg.ResolvePaths()
	if err != nil {
		t.Fatalf("ResolvePaths: %v", err)
	}
	if err := p.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs: %v", err)
	}
	return p
}

func dialWithRetry(t *testing.T, socket string, timeout time.Duration) *rpc.Client {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if c, err := rpc.Dial(socket); err == nil {
			return c
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon never became reachable at %s", socket)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitFresh(t *testing.T, c *rpc.Client, wt core.WorktreeID, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		st, err := c.Status(context.Background(), service.StatusRequest{WorktreeID: wt})
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if st.Fresh {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("worktree %s never became fresh (pending=%d)", wt, st.Pending)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestDaemonRegisterReconcileSearchIsolation(t *testing.T) {
	p := setHostEnv(t)

	fxA := enginetest.NewGitFixture(t)
	fxA.WriteFile("alpha.txt", "alpha unique content for repo A")
	fxA.Commit("a")
	fxB := enginetest.NewGitFixture(t)
	fxB.WriteFile("beta.txt", "beta unique content for repo B")
	fxB.Commit("b")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runWithEmbedder(ctx, p, testConfig(), enginetest.NewFakeEmbedder(4), os.Stderr) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("daemon did not shut down")
		}
	}()

	c := dialWithRetry(t, p.Socket, 3*time.Second)
	defer c.Close()

	regA, err := c.Register(context.Background(), service.RegisterRequest{Root: fxA.Root()})
	if err != nil {
		t.Fatalf("Register A: %v", err)
	}
	regB, err := c.Register(context.Background(), service.RegisterRequest{Root: fxB.Root()})
	if err != nil {
		t.Fatalf("Register B: %v", err)
	}
	if _, err := c.Reconcile(context.Background(), service.ReconcileRequest{WorktreeID: regA.WorktreeID}); err != nil {
		t.Fatalf("Reconcile A: %v", err)
	}
	if _, err := c.Reconcile(context.Background(), service.ReconcileRequest{WorktreeID: regB.WorktreeID}); err != nil {
		t.Fatalf("Reconcile B: %v", err)
	}
	waitFresh(t, c, regA.WorktreeID, 5*time.Second)
	waitFresh(t, c, regB.WorktreeID, 5*time.Second)

	// Search in A returns A's file and never B's (structural isolation).
	resA, err := c.Search(context.Background(), service.SearchRequest{WorktreeID: regA.WorktreeID, Query: "content"})
	if err != nil {
		t.Fatalf("Search A: %v", err)
	}
	if len(resA.Results) == 0 {
		t.Fatal("search A returned no results")
	}
	for _, h := range resA.Results {
		if strings.Contains(h.Path, "beta") {
			t.Fatalf("ISOLATION BREACH: repo A search returned repo B file %q", h.Path)
		}
		if !strings.Contains(h.Path, "alpha") {
			t.Fatalf("repo A search returned unexpected path %q", h.Path)
		}
	}

	resB, err := c.Search(context.Background(), service.SearchRequest{WorktreeID: regB.WorktreeID, Query: "content"})
	if err != nil {
		t.Fatalf("Search B: %v", err)
	}
	if len(resB.Results) == 0 {
		t.Fatal("search B returned no results")
	}
	for _, h := range resB.Results {
		if strings.Contains(h.Path, "alpha") {
			t.Fatalf("ISOLATION BREACH: repo B search returned repo A file %q", h.Path)
		}
	}
}

// TestFingerprintRolloverOnRestart indexes a repo under one embedder, then
// restarts the daemon with a different embedder (different fingerprint) and
// asserts the repo rolls to a fresh generation and re-indexes — never wedges.
func TestFingerprintRolloverOnRestart(t *testing.T) {
	p := setHostEnv(t)
	fx := enginetest.NewGitFixture(t)
	fx.WriteFile("f.txt", "some content to index")
	fx.Commit("c")

	// First run: index with a 4-dim embedder.
	cfg1 := testConfig() // provider "synthetic", cosmetic
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- runWithEmbedder(ctx1, p, cfg1, enginetest.NewFakeEmbedder(4), os.Stderr) }()
	c1 := dialWithRetry(t, p.Socket, 3*time.Second)
	reg, err := c1.Register(context.Background(), service.RegisterRequest{Root: fx.Root()})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := c1.Reconcile(context.Background(), service.ReconcileRequest{WorktreeID: reg.WorktreeID}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	waitFresh(t, c1, reg.WorktreeID, 5*time.Second)
	st1, _ := c1.Status(context.Background(), service.StatusRequest{WorktreeID: reg.WorktreeID})
	c1.Close()
	cancel1()
	<-done1

	// Second run: a DIFFERENT-DIMENSION embedder (4 -> 8) — the harshest
	// mismatch: the fingerprint changes AND every stored vector is dimensionally
	// incompatible with new query embeddings. Without the rollover clearing the
	// worktree view, reconcile would see the old hashes and queue NOTHING, and
	// search would be permanently empty while reporting fresh (the exact failure
	// the merge-gate review demonstrated against the naive roll).
	cfg2 := testConfig()
	dims8 := 8
	cfg2.Embedder.Dimensions = &dims8
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- runWithEmbedder(ctx2, p, cfg2, enginetest.NewFakeEmbedder(8), os.Stderr) }()
	defer func() { cancel2(); <-done2 }()
	c2 := dialWithRetry(t, p.Socket, 3*time.Second)
	defer c2.Close()
	st2, err := c2.Status(context.Background(), service.StatusRequest{WorktreeID: reg.WorktreeID})
	if err != nil {
		t.Fatalf("Status after restart: %v", err)
	}
	if st2.ActiveGeneration <= st1.ActiveGeneration {
		t.Fatalf("expected generation to roll forward from %d, got %d", st1.ActiveGeneration, st2.ActiveGeneration)
	}
	// The roll must have cleared the view: reconcile re-desires EVERY file (a
	// naive roll would queue zero because the old hashes still look indexed).
	rec, err := c2.Reconcile(context.Background(), service.ReconcileRequest{WorktreeID: reg.WorktreeID})
	if err != nil {
		t.Fatalf("Reconcile after roll: %v", err)
	}
	if rec.JobsQueued == 0 {
		t.Fatal("rollover did not force a reindex: reconcile after fingerprint roll queued 0 jobs")
	}
	waitFresh(t, c2, reg.WorktreeID, 5*time.Second)
	res, err := c2.Search(context.Background(), service.SearchRequest{WorktreeID: reg.WorktreeID, Query: "content"})
	if err != nil {
		t.Fatalf("Search after roll: %v", err)
	}
	if len(res.Results) == 0 {
		t.Fatal("search returned nothing after fingerprint roll + reindex (8-dim query against a properly reindexed view must hit)")
	}
}

// TestCleanShutdownAfterReconcile cancels the daemon right after enqueuing work
// and asserts run() returns cleanly (the scheduler drains before catalogs close).
func TestCleanShutdownAfterReconcile(t *testing.T) {
	p := setHostEnv(t)
	fx := enginetest.NewGitFixture(t)
	fx.WriteFile("f.txt", "content")
	fx.Commit("c")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runWithEmbedder(ctx, p, testConfig(), enginetest.NewFakeEmbedder(4), os.Stderr) }()
	c := dialWithRetry(t, p.Socket, 3*time.Second)
	reg, err := c.Register(context.Background(), service.RegisterRequest{Root: fx.Root()})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := c.Reconcile(context.Background(), service.ReconcileRequest{WorktreeID: reg.WorktreeID}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	c.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned error on shutdown: %v", err)
		}
	case <-time.After(12 * time.Second):
		t.Fatal("run did not return within 12s of cancel (shutdown hang)")
	}
	// Socket cleaned up.
	if _, err := os.Stat(p.Socket); !os.IsNotExist(err) {
		t.Fatalf("socket not removed on shutdown: %v", err)
	}
}

func TestSingletonRejectsSecondDaemon(t *testing.T) {
	p := setHostEnv(t)
	l1, err := daemonctl.Acquire(p.Lock)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	defer l1.Release()
	if _, err := daemonctl.Acquire(p.Lock); !errors.Is(err, daemonctl.ErrAlreadyRunning) {
		t.Fatalf("second Acquire = %v; want ErrAlreadyRunning", err)
	}
}

func TestLazyStartSpawnsAndRespawns(t *testing.T) {
	p := setHostEnv(t)

	// Pre-write a daemon.json with a keyless embedder (ollama constructs without
	// a key or network; the test never embeds, only starts + connects).
	dims := 4
	cfg := &daemoncfg.Config{
		Embedder: daemoncfg.EmbedderConfig{Provider: "ollama", Endpoint: "http://127.0.0.1:11434", Model: "nomic-embed-text", Dimensions: &dims},
		Chunking: daemoncfg.ChunkingConfig{Size: 512, Overlap: 64},
	}
	if err := cfg.Save(p.Config); err != nil {
		t.Fatalf("write daemon.json: %v", err)
	}

	// Build the grepaid binary once.
	bin := filepath.Join(t.TempDir(), "grepaid")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("go build grepaid: %v", err)
	}

	// No daemon yet: EnsureDaemon spawns one and connects.
	c1, err := daemonctl.EnsureDaemon(context.Background(), p.Socket, bin, 8*time.Second)
	if err != nil {
		t.Fatalf("EnsureDaemon (cold): %v", err)
	}
	// Status on an unknown worktree errors, but the connection itself proves the
	// daemon is up.
	if _, err := c1.Status(context.Background(), service.StatusRequest{WorktreeID: "/does/not/exist"}); err == nil {
		t.Log("status on unknown worktree unexpectedly succeeded (acceptable)")
	}
	c1.Close()

	// A second EnsureDaemon connects to the same (already-running) daemon.
	c2, err := daemonctl.EnsureDaemon(context.Background(), p.Socket, bin, 8*time.Second)
	if err != nil {
		t.Fatalf("EnsureDaemon (warm): %v", err)
	}
	c2.Close()

	// Shut it down via SIGTERM (found through the lock-file pid) and confirm a
	// fresh EnsureDaemon respawns it.
	if err := daemonctl.StopDaemon(p.Lock, 5*time.Second); err != nil {
		t.Fatalf("StopDaemon: %v", err)
	}
	c3, err := daemonctl.EnsureDaemon(context.Background(), p.Socket, bin, 8*time.Second)
	if err != nil {
		t.Fatalf("EnsureDaemon (respawn): %v", err)
	}
	c3.Close()
	if err := daemonctl.StopDaemon(p.Lock, 5*time.Second); err != nil {
		t.Fatalf("final StopDaemon: %v", err)
	}
}
