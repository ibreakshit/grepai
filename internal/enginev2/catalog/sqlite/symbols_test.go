package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

// openTestCatalog opens a fresh catalog in a temp dir, closed via t.Cleanup.
func openTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	c, err := Open(context.Background(), filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// seedSymbolWorld registers repo+wt, commits one artifact for path with the
// given symbols/edges (extracted=true) and returns the artifact id.
func seedSymbolWorld(t *testing.T, c *Catalog, repo core.RepositoryID, wt core.WorktreeID, path string, defs []core.SymbolDef, edges []core.SymbolEdge, extracted bool) core.ArtifactID {
	t.Helper()
	ctx := context.Background()
	_ = c.RegisterRepository(ctx, repo, "/"+string(repo), "")
	_ = c.RegisterWorktree(ctx, wt, repo, "/"+string(wt), 1)
	_ = c.CreateGeneration(ctx, repo, 1, "fp")
	_ = c.SetActiveGeneration(ctx, repo, 1)
	key := core.ArtifactKey{RepositoryID: repo, RelativePath: path, SourceHash: "h-" + path, Fingerprint: "fp"}
	art := core.Artifact{ID: key.ArtifactID(), Key: key, Dimensions: 4}
	req := core.CommitRequest{
		View:             core.ViewEntry{WorktreeID: wt, Path: path, ArtifactID: art.ID, Generation: 1},
		Artifact:         art,
		Symbols:          defs,
		SymbolEdges:      edges,
		SymbolsExtracted: extracted,
	}
	if err := c.CommitUpdate(ctx, req, core.Job{WorktreeID: wt, Path: path, DesiredHash: "h-" + path, Generation: 1, Operation: core.OpUpsert}); err != nil {
		t.Fatal(err)
	}
	return art.ID
}

func TestSymbolCommitAndTraceReads(t *testing.T) {
	ctx := context.Background()
	c, err := Open(ctx, filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	seedSymbolWorld(t, c, "r", "w", "a.go",
		[]core.SymbolDef{{Name: "HandleReq", Kind: "function", Line: 10, EndLine: 20, Signature: "func HandleReq()"}},
		[]core.SymbolEdge{{Caller: "HandleReq", Callee: "Validate", Line: 12}}, true)
	seedSymbolWorld(t, c, "r", "w", "b.go",
		[]core.SymbolDef{{Name: "Validate", Kind: "function", Line: 5, EndLine: 9}},
		nil, true)

	defs, err := c.SymbolDefinitions(ctx, "w", "Validate")
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 1 || defs[0].Path != "b.go" || defs[0].Line != 5 {
		t.Fatalf("Validate definition wrong: %+v", defs)
	}
	callers, err := c.SymbolEdges(ctx, "w", "Validate", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(callers) != 1 || callers[0].Caller != "HandleReq" || callers[0].Path != "a.go" || callers[0].Line != 12 {
		t.Fatalf("callers of Validate wrong: %+v", callers)
	}
	callees, err := c.SymbolEdges(ctx, "w", "HandleReq", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(callees) != 1 || callees[0].Callee != "Validate" {
		t.Fatalf("callees of HandleReq wrong: %+v", callees)
	}
}

func TestSymbolReadsAreWorktreeIsolated(t *testing.T) {
	ctx := context.Background()
	c, err := Open(ctx, filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// Same symbol name in two different worktrees.
	seedSymbolWorld(t, c, "ra", "wa", "a.go", []core.SymbolDef{{Name: "Dup", Kind: "function", Line: 1}}, nil, true)
	seedSymbolWorld(t, c, "rb", "wb", "b.go", []core.SymbolDef{{Name: "Dup", Kind: "function", Line: 2}}, nil, true)
	defs, err := c.SymbolDefinitions(ctx, "wa", "Dup")
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 1 || defs[0].Path != "a.go" {
		t.Fatalf("wa must only see its own Dup: %+v", defs)
	}
}

func TestArtifactsMissingSymbolsAndBackfillWrite(t *testing.T) {
	ctx := context.Background()
	c, err := Open(ctx, filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// One artifact committed WITHOUT extraction (pre-upgrade fleet state), one with.
	missingID := seedSymbolWorld(t, c, "r", "w", "old.go", nil, nil, false)
	seedSymbolWorld(t, c, "r", "w", "new.go", []core.SymbolDef{{Name: "F", Kind: "function"}}, nil, true)

	miss, err := c.ArtifactsMissingSymbols(ctx, "w")
	if err != nil {
		t.Fatal(err)
	}
	if len(miss) != 1 || miss[0].Path != "old.go" || miss[0].ArtifactID != missingID || miss[0].SourceHash != "h-old.go" {
		t.Fatalf("missing list wrong: %+v", miss)
	}
	// Backfill write clears it — including for a zero-symbol file.
	if err := c.PutArtifactSymbols(ctx, missingID, nil, nil); err != nil {
		t.Fatal(err)
	}
	miss, err = c.ArtifactsMissingSymbols(ctx, "w")
	if err != nil {
		t.Fatal(err)
	}
	if len(miss) != 0 {
		t.Fatalf("backfilled artifact still listed missing: %+v", miss)
	}
}

// TestSameNameSymbolsAndRepeatedCallSitesPersist guards migration 0003's
// location-aware keys: Go methods on different receivers share (name, kind),
// and one caller can call the same callee at several lines — every row must
// survive, not collapse under INSERT OR IGNORE.
func TestSameNameSymbolsAndRepeatedCallSitesPersist(t *testing.T) {
	ctx := context.Background()
	c := openTestCatalog(t)
	seedSymbolWorld(t, c, "r", "w", "a.go",
		[]core.SymbolDef{
			{Name: "Get", Kind: "method", Line: 10, Signature: "func (a A) Get()"},
			{Name: "Get", Kind: "method", Line: 50, Signature: "func (b B) Get()"},
		},
		[]core.SymbolEdge{
			{Caller: "Run", Callee: "Get", Line: 3},
			{Caller: "Run", Callee: "Get", Line: 7},
		}, true)
	defs, err := c.SymbolDefinitions(ctx, "w", "Get")
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 2 {
		t.Fatalf("both same-name definitions must persist, got %d: %+v", len(defs), defs)
	}
	edges, err := c.SymbolEdges(ctx, "w", "Get", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) != 2 {
		t.Fatalf("both call sites must persist, got %d: %+v", len(edges), edges)
	}
}

// TestPutArtifactSymbolsReplaces guards the extractor-upgrade path: a re-put
// for the same artifact must replace prior rows, not merge with them.
func TestPutArtifactSymbolsReplaces(t *testing.T) {
	ctx := context.Background()
	c := openTestCatalog(t)
	id := seedSymbolWorld(t, c, "r", "w", "a.go",
		[]core.SymbolDef{{Name: "Old", Kind: "function", Line: 1}},
		[]core.SymbolEdge{{Caller: "Old", Callee: "X", Line: 2}}, true)
	if err := c.PutArtifactSymbols(ctx, id,
		[]core.SymbolDef{{Name: "New", Kind: "function", Line: 5}},
		[]core.SymbolEdge{{Caller: "New", Callee: "Y", Line: 6}}); err != nil {
		t.Fatal(err)
	}
	if defs, _ := c.SymbolDefinitions(ctx, "w", "Old"); len(defs) != 0 {
		t.Fatalf("stale definition survived replace: %+v", defs)
	}
	if defs, _ := c.SymbolDefinitions(ctx, "w", "New"); len(defs) != 1 {
		t.Fatalf("replacement definition missing: %+v", defs)
	}
	if edges, _ := c.SymbolEdges(ctx, "w", "X", true); len(edges) != 0 {
		t.Fatalf("stale edge survived replace: %+v", edges)
	}
}

// TestV1ParityFieldsRoundTrip guards migration 0004: the extractor detail
// fields (receiver/package/exported/language/docstring, edge context) must
// survive commit → view read intact.
func TestV1ParityFieldsRoundTrip(t *testing.T) {
	ctx := context.Background()
	c := openTestCatalog(t)
	seedSymbolWorld(t, c, "r", "w", "a.go",
		[]core.SymbolDef{{
			Name: "Get", Kind: "method", Line: 10, EndLine: 20,
			Signature: "func (s *Store) Get(k string) (string, error)",
			Receiver:  "*Store", Package: "store", Exported: true,
			Language: "go", Docstring: "Get returns the value for k.",
		}},
		[]core.SymbolEdge{{Caller: "Get", Callee: "lookup", Line: 12, Context: "\tv, err := lookup(k)"}}, true)

	defs, err := c.SymbolDefinitions(ctx, "w", "Get")
	if err != nil || len(defs) != 1 {
		t.Fatalf("defs: %+v err=%v", defs, err)
	}
	d := defs[0]
	if d.Receiver != "*Store" || d.Package != "store" || !d.Exported || d.Language != "go" ||
		d.Docstring != "Get returns the value for k." {
		t.Fatalf("v1-parity symbol fields lost: %+v", d)
	}
	edges, err := c.SymbolEdges(ctx, "w", "lookup", true)
	if err != nil || len(edges) != 1 {
		t.Fatalf("edges: %+v err=%v", edges, err)
	}
	if edges[0].Context != "\tv, err := lookup(k)" {
		t.Fatalf("edge context lost: %+v", edges[0])
	}
}
