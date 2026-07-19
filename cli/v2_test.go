package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/core"
)

func TestV2CommandsRegistered(t *testing.T) {
	root := GetRootCmd()
	for _, path := range [][]string{{"v2"}, {"v2", "index"}, {"v2", "search"}} {
		c, _, err := root.Find(path)
		if err != nil || c.Name() != path[len(path)-1] {
			t.Fatalf("command %v not registered (got %v, err %v)", path, c.Name(), err)
		}
	}
	search, _, _ := root.Find([]string{"v2", "search"})
	if search.Flags().Lookup("json") == nil || search.Flags().Lookup("limit") == nil {
		t.Fatal("v2 search must expose --json and --limit")
	}
}

func TestWriteV2Text(t *testing.T) {
	var buf bytes.Buffer
	writeV2Text(&buf, []core.SearchHit{
		{Path: "auth.go", Score: 0.912, StartLine: 3, EndLine: 5, Content: "func Login() {}\nreturn nil"},
	})
	out := buf.String()
	if !strings.Contains(out, "auth.go:3-5") {
		t.Fatalf("missing path:lines header: %q", out)
	}
	if !strings.Contains(out, "0.912") {
		t.Fatalf("missing score: %q", out)
	}
	if !strings.Contains(out, "func Login()") {
		t.Fatalf("missing snippet: %q", out)
	}
}

func TestWriteV2TextEmpty(t *testing.T) {
	var buf bytes.Buffer
	writeV2Text(&buf, nil)
	if !strings.Contains(buf.String(), "no results") {
		t.Fatalf("empty search should say so: %q", buf.String())
	}
}

func TestWriteV2JSON(t *testing.T) {
	var buf bytes.Buffer
	if err := writeV2JSON(&buf, []core.SearchHit{{Path: "a.go", Score: 0.5, StartLine: 2, EndLine: 4, Content: "x"}}, 3, true); err != nil {
		t.Fatal(err)
	}
	var got struct {
		ActiveGeneration int  `json:"activeGeneration"`
		Fresh            bool `json:"fresh"`
		Results          []struct {
			Path      string `json:"path"`
			StartLine int    `json:"startLine"`
			Content   string `json:"content"`
		} `json:"results"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("json: %v", err)
	}
	if got.ActiveGeneration != 3 || !got.Fresh || len(got.Results) != 1 || got.Results[0].Path != "a.go" || got.Results[0].StartLine != 2 || got.Results[0].Content != "x" {
		t.Fatalf("json shape wrong: %+v", got)
	}
}

func TestSnippetLinesTruncates(t *testing.T) {
	got := snippetLines("l1\nl2\nl3\nl4\nl5\nl6", 3)
	if len(got) != 4 || got[3] != "    …" {
		t.Fatalf("expected 3 lines + ellipsis, got %v", got)
	}
}
