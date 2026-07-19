package runtime

import "testing"

// gitBlobOID must match git's object id exactly. e69de29… is git's canonical
// empty-blob object id, so an empty content must hash to it.
func TestGitBlobOIDMatchesGit(t *testing.T) {
	if got := gitBlobOID(nil); got != "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391" {
		t.Fatalf("empty blob oid = %s, want git's canonical empty blob", got)
	}
	// Determinism.
	if gitBlobOID([]byte("abc")) != gitBlobOID([]byte("abc")) {
		t.Fatal("gitBlobOID not deterministic")
	}
	if gitBlobOID([]byte("abc")) == gitBlobOID([]byte("abd")) {
		t.Fatal("gitBlobOID collided on different content")
	}
}
