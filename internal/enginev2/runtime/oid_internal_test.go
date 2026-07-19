package runtime

import "testing"

// gitBlobOID must match git's object id exactly. e69de29… is git's canonical
// empty-blob object id, so an empty content must hash to it.
func TestGitBlobOIDMatchesGit(t *testing.T) {
	if got := gitBlobOID(nil); got != "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391" {
		t.Fatalf("empty blob oid = %s, want git's canonical empty blob", got)
	}
	// git hash-object for "hello\n" is a well-known value.
	if got := gitBlobOID([]byte("hello\n")); got != "ce013625030ba8dba906f756967f9e9ca394464a" {
		t.Fatalf("blob oid for \"hello\\n\" = %s", got)
	}
	if gitBlobOID([]byte("abc")) == gitBlobOID([]byte("abd")) {
		t.Fatal("gitBlobOID collided on different content")
	}
}
