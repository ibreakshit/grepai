package daemoncfg

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadMissingReturnsDefaults(t *testing.T) {
	c, existed, err := Load(filepath.Join(t.TempDir(), "daemon.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if existed {
		t.Fatal("missing file should report existed=false")
	}
	if c.Embedder.Provider == "" {
		t.Fatalf("defaults missing provider: %+v", c)
	}
	if c.ToConfig().Embedder.Provider != c.Embedder.Provider {
		t.Fatal("ToConfig did not carry the provider")
	}
}

func TestLoadRoundTripAndToConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.json")
	body := `{"embedder":{"provider":"openai","model":"m","endpoint":"http://x","dimensions":128},"chunking":{"size":100,"overlap":10},"search_limit":5}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	c, existed, err := Load(path)
	if err != nil || !existed {
		t.Fatalf("Load: existed=%v err=%v", existed, err)
	}
	cc := c.ToConfig()
	if cc.Embedder.Model != "m" || cc.Embedder.Endpoint != "http://x" {
		t.Fatalf("ToConfig embedder mapping wrong: %+v", cc.Embedder)
	}
	if cc.Embedder.Dimensions == nil || *cc.Embedder.Dimensions != 128 {
		t.Fatalf("ToConfig dimensions wrong: %+v", cc.Embedder.Dimensions)
	}
	if cc.Chunking.Size != 100 || cc.Chunking.Overlap != 10 {
		t.Fatalf("ToConfig chunking wrong: %+v", cc.Chunking)
	}
	if c.SearchLimit != 5 {
		t.Fatalf("SearchLimit = %d", c.SearchLimit)
	}
}

func TestSchedulerConfigOrDefaultAppliesOverrides(t *testing.T) {
	c := Default()
	base := c.SchedulerConfigOrDefault()
	if base.MaxIndexInflight < 1 {
		t.Fatalf("default MaxIndexInflight invalid: %d", base.MaxIndexInflight)
	}
	c.Scheduler = &SchedulerConfig{MaxIndexInflight: 9}
	got := c.SchedulerConfigOrDefault()
	if got.MaxIndexInflight != 9 {
		t.Fatalf("override not applied: MaxIndexInflight = %d", got.MaxIndexInflight)
	}
	if got.MaxJobAttempts != base.MaxJobAttempts {
		t.Fatalf("unset override should keep default MaxJobAttempts, got %d", got.MaxJobAttempts)
	}
}
