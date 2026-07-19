package core

import "testing"

func TestArtifactIDDerivation(t *testing.T) {
	fp := sampleFingerprint().Hash()
	k1 := ArtifactKey{RepositoryID: "repo", RelativePath: "a.go", SourceHash: "oid1", Fingerprint: fp}
	k2 := ArtifactKey{RepositoryID: "repo", RelativePath: "a.go", SourceHash: "oid1", Fingerprint: fp}
	if k1.ArtifactID() != k2.ArtifactID() {
		t.Fatal("identical keys must derive identical ArtifactID")
	}
	if err := k1.ArtifactID().Validate(); err != nil {
		t.Fatalf("derived ArtifactID must be non-empty: %v", err)
	}
}

func TestArtifactIDDistinctPerComponent(t *testing.T) {
	fp := sampleFingerprint().Hash()
	base := ArtifactKey{RepositoryID: "repo", RelativePath: "a.go", SourceHash: "oid1", Fingerprint: fp}
	variants := []ArtifactKey{
		{RepositoryID: "other", RelativePath: "a.go", SourceHash: "oid1", Fingerprint: fp},
		{RepositoryID: "repo", RelativePath: "b.go", SourceHash: "oid1", Fingerprint: fp},
		{RepositoryID: "repo", RelativePath: "a.go", SourceHash: "oid2", Fingerprint: fp},
		{RepositoryID: "repo", RelativePath: "a.go", SourceHash: "oid1", Fingerprint: "different"},
	}
	baseID := base.ArtifactID()
	for i, v := range variants {
		if v.ArtifactID() == baseID {
			t.Fatalf("variant %d must not collide with base", i)
		}
	}
}
