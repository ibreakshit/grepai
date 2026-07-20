package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

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
