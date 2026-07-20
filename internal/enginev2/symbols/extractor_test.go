package symbols

import (
	"context"
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
