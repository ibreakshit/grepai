package core

import (
	"encoding/binary"
	"testing"
)

func sampleFingerprint() IndexingFingerprint {
	return IndexingFingerprint{
		EmbedderProvider:          "openai",                 // 6
		EmbedderModel:             "text-embedding-3-large", // 22
		Dimensions:                3072,
		ChunkerImplementation:     "v1", // 2
		ChunkerSettings:           map[string]string{"overlap": "64", "size": "512"},
		FrameworkTransformVersion: "tf1", // 3
		EmbeddingInputFormat:      "raw", // 3
	}
}

func TestCanonicalLengthAndVersionPrefix(t *testing.T) {
	c := sampleFingerprint().Canonical()
	// 4 (ver) +10 provider +26 model +8 dims +6 chunker +4 count
	// +17 (overlap,64) +15 (size,512) +7 transform +7 input = 104
	if len(c) != 104 {
		t.Fatalf("canonical length = %d, want 104", len(c))
	}
	if got := binary.BigEndian.Uint32(c[:4]); got != FingerprintEncodingVersion {
		t.Fatalf("version prefix = %d, want %d", got, FingerprintEncodingVersion)
	}
}

func TestHashDeterministicAndMapOrderIndependent(t *testing.T) {
	a := sampleFingerprint()
	b := sampleFingerprint()
	// Insert settings in a different order to prove map order does not matter.
	b.ChunkerSettings = map[string]string{}
	b.ChunkerSettings["size"] = "512"
	b.ChunkerSettings["overlap"] = "64"
	if a.Hash() != b.Hash() {
		t.Fatalf("hash not order-independent: %s != %s", a.Hash(), b.Hash())
	}
	h1, h2 := a.Hash(), a.Hash()
	if h1 != h2 {
		t.Fatal("hash not deterministic across calls")
	}
	if len(a.Hash()) != 64 {
		t.Fatalf("hash length = %d, want 64 hex chars", len(a.Hash()))
	}
}

func TestHashDistinctPerField(t *testing.T) {
	base := sampleFingerprint()
	mutations := map[string]func(*IndexingFingerprint){
		"provider":  func(f *IndexingFingerprint) { f.EmbedderProvider = "ollama" },
		"model":     func(f *IndexingFingerprint) { f.EmbedderModel = "nomic" },
		"dims":      func(f *IndexingFingerprint) { f.Dimensions = 768 },
		"chunker":   func(f *IndexingFingerprint) { f.ChunkerImplementation = "v2" },
		"settings":  func(f *IndexingFingerprint) { f.ChunkerSettings = map[string]string{"size": "256"} },
		"transform": func(f *IndexingFingerprint) { f.FrameworkTransformVersion = "tf2" },
		"input":     func(f *IndexingFingerprint) { f.EmbeddingInputFormat = "annotated" },
	}
	seen := map[string]string{"base": base.Hash()}
	for name, mut := range mutations {
		f := sampleFingerprint()
		mut(&f)
		h := f.Hash()
		for other, oh := range seen {
			if h == oh {
				t.Fatalf("mutation %q collides with %q", name, other)
			}
		}
		seen[name] = h
	}
}
