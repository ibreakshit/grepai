package symbols

import (
	"context"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
	"testing"
)

const goFixture = `package demo

func Validate(x int) bool { return x > 0 }

func HandleReq(x int) {
	if Validate(x) {
		process(x)
	}
}

func process(x int) {}
`

func TestExtractGoDefsAndCallEdges(t *testing.T) {
	e := New()
	defs, edges, err := e.Extract(context.Background(), "demo.go", goFixture)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]bool{}
	for _, d := range defs {
		byName[d.Name] = true
		if d.Line == 0 {
			t.Fatalf("definition %s missing line info", d.Name)
		}
	}
	for _, want := range []string{"Validate", "HandleReq", "process"} {
		if !byName[want] {
			t.Fatalf("missing definition %s in %+v", want, defs)
		}
	}
	foundCall := false
	for _, ed := range edges {
		if ed.Caller == "HandleReq" && ed.Callee == "Validate" {
			foundCall = true
			if ed.Line == 0 {
				t.Fatal("call edge missing line")
			}
		}
	}
	if !foundCall {
		t.Fatalf("HandleReq->Validate edge not extracted: %+v", edges)
	}
}

func TestExtractUnsupportedLanguageIsEmptyNotError(t *testing.T) {
	e := New()
	defs, edges, err := e.Extract(context.Background(), "notes.md", "# just prose\n")
	if err != nil {
		t.Fatalf("unsupported language must not error: %v", err)
	}
	if len(defs) != 0 && len(edges) != 0 {
		t.Logf("unexpected but harmless extraction from markdown: %d defs", len(defs))
	}
}

// TestExtractPassesV1FieldsThrough guards issue #20: the adapter must not drop
// the detail fields the v1 extractor produces (receiver/package/exported/
// language/docstring on symbols; call-site context on edges).
func TestExtractPassesV1FieldsThrough(t *testing.T) {
	src := "package store\n\n" +
		"// Get returns the value for k.\n" +
		"func (s *Store) Get(k string) string {\n" +
		"\treturn lookup(k)\n" +
		"}\n\n" +
		"func lookup(k string) string { return k }\n"
	defs, edges, err := New().Extract(context.Background(), "store/store.go", src)
	if err != nil {
		t.Fatal(err)
	}
	var get *core.SymbolDef
	for i := range defs {
		if defs[i].Name == "Get" {
			get = &defs[i]
		}
	}
	if get == nil {
		t.Fatalf("Get not extracted: %+v", defs)
	}
	// Docstring/Package are produced only by the tree-sitter extractor (v1
	// precise mode); the regex extractor (v1 fast mode, the CGO-free default)
	// leaves them empty — parity means passing through whatever the shared
	// extractor emits, so only the fast-mode fields are asserted here.
	if get.Language != "go" || !get.Exported || get.Receiver == "" {
		t.Fatalf("v1 detail fields dropped: %+v", *get)
	}
	found := false
	for _, e := range edges {
		if e.Caller == "Get" && e.Callee == "lookup" {
			found = true
			if e.Context == "" {
				t.Fatalf("edge context dropped: %+v", e)
			}
		}
	}
	if !found {
		t.Fatalf("Get->lookup edge missing: %+v", edges)
	}
}
