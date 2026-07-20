package main

import (
	"context"
	"database/sql"
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
	_ "modernc.org/sqlite"
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
	// The roll cleared the view, and Register's empty-view predicate already
	// kicked off the reindex during rehydrate (no manual reconcile needed).
	// Wait for it, then prove the view was genuinely REBUILT under the new
	// embedder: an 8-dim query only hits 8-dim vectors — had the naive roll left
	// the old 4-dim view in place, reconcile would have queued nothing and this
	// search would come back empty on a "fresh" index.
	waitFresh(t, c2, reg.WorktreeID, 5*time.Second)
	res, err := c2.Search(context.Background(), service.SearchRequest{WorktreeID: reg.WorktreeID, Query: "content"})
	if err != nil {
		t.Fatalf("Search after roll: %v", err)
	}
	if len(res.Results) == 0 {
		t.Fatal("rollover did not force a reindex: 8-dim search returned nothing on a fresh index (stale or empty view)")
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

// TestWatcherAutoFreshness is the watcher's end-to-end proof: with the daemon
// running, editing a file makes the index stale and then fresh again — and the
// new content becomes searchable — WITHOUT any reconcile command being issued.
func TestWatcherAutoFreshness(t *testing.T) {
	p := setHostEnv(t)
	fx := enginetest.NewGitFixture(t)
	fx.WriteFile("main.go", "package main // alpha seed content")
	fx.Commit("seed")

	cfg := testConfig()
	cfg.Watch.QuietMS = 100 // fast debounce for the test
	cfg.Watch.MaxLatencyMS = 1000

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runWithEmbedder(ctx, p, cfg, enginetest.NewFakeEmbedder(4), os.Stderr) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("daemon did not shut down")
		}
	}()

	c := dialWithRetry(t, p.Socket, 3*time.Second)
	defer c.Close()
	reg, err := c.Register(context.Background(), service.RegisterRequest{Root: fx.Root()})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	waitFresh(t, c, reg.WorktreeID, 5*time.Second)

	// THE EDIT: write a brand-new file. No Reconcile/watch command follows.
	newPath := filepath.Join(fx.Root(), "zeta_widget.go")
	if err := os.WriteFile(newPath, []byte("package main // zeta widget marker"), 0o600); err != nil {
		t.Fatal(err)
	}

	// The watcher must notice, reconcile, and index it on its own.
	deadline := time.Now().Add(10 * time.Second)
	for {
		res, serr := c.Search(context.Background(), service.SearchRequest{WorktreeID: reg.WorktreeID, Query: "zeta widget"})
		if serr == nil {
			found := false
			for _, h := range res.Results {
				if h.Path == "zeta_widget.go" {
					found = true
					break
				}
			}
			if found && res.Fresh {
				break // auto-indexed, no command issued
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("edited file never became searchable without a reconcile command")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Negative control: catalog WAL churn (constant during the test) must not
	// have wedged the repo in a dirty loop — it settles fresh and stays fresh.
	time.Sleep(500 * time.Millisecond)
	st, err := c.Status(context.Background(), service.StatusRequest{WorktreeID: reg.WorktreeID})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if !st.Fresh {
		t.Fatalf("repo did not settle fresh after watcher indexing (pending=%d) — possible self-trigger loop", st.Pending)
	}
}

// TestSearchAllAcrossRepos proves the cross-repo fan-out end-to-end: two repos
// registered with the daemon, one query returns tagged hits from BOTH — while a
// plain per-repo Search still sees only its own repo (isolation preserved).
func TestSearchAllAcrossRepos(t *testing.T) {
	p := setHostEnv(t)
	fxA := enginetest.NewGitFixture(t)
	fxA.WriteFile("alpha.go", "package a // shared metric harvest logic")
	fxA.Commit("a")
	fxB := enginetest.NewGitFixture(t)
	fxB.WriteFile("beta.go", "package b // shared metric harvest logic too")
	fxB.Commit("b")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runWithEmbedder(ctx, p, testConfig(), enginetest.NewFakeEmbedder(4), os.Stderr) }()
	defer func() { cancel(); <-done }()

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
	waitFresh(t, c, regA.WorktreeID, 5*time.Second)
	waitFresh(t, c, regB.WorktreeID, 5*time.Second)

	resp, err := c.SearchAll(context.Background(), service.SearchAllRequest{Query: "shared metric harvest", Limit: 10})
	if err != nil {
		t.Fatalf("SearchAll: %v", err)
	}
	got := map[core.WorktreeID]string{}
	for _, r := range resp.Results {
		got[r.Worktree] = r.Hit.Path
	}
	if got[regA.WorktreeID] != "alpha.go" || got[regB.WorktreeID] != "beta.go" {
		t.Fatalf("SearchAll should return tagged hits from BOTH repos, got %+v", got)
	}
	if len(resp.Skipped) != 0 {
		t.Fatalf("no repo should be skipped, got %+v", resp.Skipped)
	}

	// Isolation control: plain Search in A still only sees A.
	single, err := c.Search(context.Background(), service.SearchRequest{WorktreeID: regA.WorktreeID, Query: "shared metric harvest"})
	if err != nil {
		t.Fatalf("Search A: %v", err)
	}
	for _, h := range single.Results {
		if h.Path == "beta.go" {
			t.Fatal("ISOLATION BREACH: single-repo search returned another repo's file")
		}
	}
}

// traceFixture writes a Go call chain the regex extractor understands.
func traceFixture(fx *enginetest.GitFixture) {
	fx.WriteFile("handler.go", "package m\n\nfunc HandleReq(x int) {\n\tif Validate(x) {\n\t\trecordMetric(x)\n\t}\n}\n")
	fx.WriteFile("validate.go", "package m\n\nfunc Validate(x int) bool {\n\treturn x > 0\n}\n\nfunc recordMetric(x int) {}\n")
	fx.Commit("trace fixture")
}

// TestTraceEndToEnd: real files, real (regex) extraction in the daemon build
// path, trace served over RPC through the active view.
func TestTraceEndToEnd(t *testing.T) {
	p := setHostEnv(t)
	fx := enginetest.NewGitFixture(t)
	traceFixture(fx)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runWithEmbedder(ctx, p, testConfig(), enginetest.NewFakeEmbedder(4), os.Stderr) }()
	defer func() { cancel(); <-done }()
	c := dialWithRetry(t, p.Socket, 3*time.Second)
	defer c.Close()
	reg, err := c.Register(context.Background(), service.RegisterRequest{Root: fx.Root()})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	waitFresh(t, c, reg.WorktreeID, 5*time.Second)

	resp, err := c.Trace(context.Background(), service.TraceRequest{WorktreeID: reg.WorktreeID, Symbol: "Validate", Direction: service.TraceCallers})
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	foundDef, foundCaller := false, false
	for _, d := range resp.Definitions {
		if d.Path == "validate.go" && d.Name == "Validate" {
			foundDef = true
		}
	}
	for _, e := range resp.Edges {
		if e.Caller == "HandleReq" && e.Callee == "Validate" && e.Path == "handler.go" {
			foundCaller = true
		}
	}
	if !foundDef || !foundCaller {
		t.Fatalf("trace incomplete: defs=%+v edges=%+v", resp.Definitions, resp.Edges)
	}
	if resp.BackfillPending != 0 {
		t.Fatalf("fresh daemon-built index should have no backfill pending, got %d", resp.BackfillPending)
	}
}

// TestSymbolBackfillOnRestart simulates the real fleet upgrade: artifacts exist
// WITHOUT symbols (as committed by a pre-trace binary), and a daemon restart
// backfills them without re-embedding anything.
func TestSymbolBackfillOnRestart(t *testing.T) {
	p := setHostEnv(t)
	fx := enginetest.NewGitFixture(t)
	traceFixture(fx)

	// Run 1: index normally (symbols get extracted).
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- runWithEmbedder(ctx1, p, testConfig(), enginetest.NewFakeEmbedder(4), os.Stderr) }()
	c1 := dialWithRetry(t, p.Socket, 3*time.Second)
	reg, err := c1.Register(context.Background(), service.RegisterRequest{Root: fx.Root()})
	if err != nil {
		t.Fatal(err)
	}
	waitFresh(t, c1, reg.WorktreeID, 5*time.Second)
	c1.Close()
	cancel1()
	<-done1

	// Strip symbols to simulate a catalog written by a pre-trace binary.
	db, err := sql.Open("sqlite", "file:"+filepath.Join(fx.Root(), ".grepai", "catalog_v2.db"))
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{"DELETE FROM symbols", "DELETE FROM symbol_edges", "UPDATE file_artifacts SET symbols_version=0"} {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	db.Close()

	// Run 2: rehydrate triggers the backfill (CPU-only; FakeEmbedder counts
	// stay untouched because nothing re-embeds).
	emb2 := enginetest.NewFakeEmbedder(4)
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- runWithEmbedder(ctx2, p, testConfig(), emb2, os.Stderr) }()
	defer func() { cancel2(); <-done2 }()
	c2 := dialWithRetry(t, p.Socket, 3*time.Second)
	defer c2.Close()

	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, terr := c2.Trace(context.Background(), service.TraceRequest{WorktreeID: reg.WorktreeID, Symbol: "Validate", Direction: service.TraceCallers})
		if terr == nil && resp.BackfillPending == 0 && len(resp.Edges) > 0 {
			break // backfilled and trace-able
		}
		if time.Now().After(deadline) {
			t.Fatalf("backfill never completed: %+v err=%v", resp, terr)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if emb2.TextsEmbedded() != 0 {
		t.Fatalf("backfill must not re-embed anything; embedded %d texts", emb2.TextsEmbedded())
	}
}
