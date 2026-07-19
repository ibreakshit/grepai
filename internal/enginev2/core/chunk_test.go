package core

import "testing"

func TestChunkIDStableAndFingerprintScoped(t *testing.T) {
	a := ChunkID("fp-1", "func main() {}")
	b := ChunkID("fp-1", "func main() {}")
	if a != b {
		t.Fatalf("ChunkID not deterministic: %s != %s", a, b)
	}
	if len(a) != 64 {
		t.Fatalf("ChunkID want 64 hex chars, got %d", len(a))
	}
	// Different fingerprint => different id (invariant 10: no cross-fingerprint reuse).
	if ChunkID("fp-2", "func main() {}") == a {
		t.Fatal("ChunkID collided across fingerprints")
	}
	// Boundary confusion guard: ("ab","c") must not equal ("a","bc").
	if ChunkID("ab", "c") == ChunkID("a", "bc") {
		t.Fatal("ChunkID boundary confusion")
	}
}
