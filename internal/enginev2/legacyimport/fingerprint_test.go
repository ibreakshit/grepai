package legacyimport_test

import (
	"testing"

	"github.com/yoanbernabeu/grepai/config"
	"github.com/yoanbernabeu/grepai/internal/enginev2/legacyimport"
)

func baseConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Embedder.Provider = "openai"
	cfg.Embedder.Model = "qwen3-embedding-8b"
	cfg.Chunking.Size = 512
	cfg.Chunking.Overlap = 50
	return cfg
}

func TestDeriveFingerprintStableAndSensitive(t *testing.T) {
	a := legacyimport.DeriveFingerprint(baseConfig())
	b := legacyimport.DeriveFingerprint(baseConfig())
	if a == "" || a != b {
		t.Fatalf("not stable: %q %q", a, b)
	}

	sizeChanged := baseConfig()
	sizeChanged.Chunking.Size = 256
	if legacyimport.DeriveFingerprint(sizeChanged) == a {
		t.Fatal("fingerprint must change when chunk size changes")
	}

	modelChanged := baseConfig()
	modelChanged.Embedder.Model = "nomic-embed-text"
	if legacyimport.DeriveFingerprint(modelChanged) == a {
		t.Fatal("fingerprint must change when model changes")
	}

	dims := 4096
	dimsSet := baseConfig()
	dimsSet.Embedder.Dimensions = &dims
	if legacyimport.DeriveFingerprint(dimsSet) == a {
		t.Fatal("fingerprint must change when dimensions are set")
	}
}
