package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
)

// ArtifactKey is the immutable identity of one indexed file version. Identical
// keys in the same repository are parsed and embedded once, then referenced by
// many worktree views (invariant 5: shared immutable work).
type ArtifactKey struct {
	RepositoryID RepositoryID
	RelativePath string
	SourceHash   string // Git blob OID for clean tracked content, else content hash
	Fingerprint  string // IndexingFingerprint.Hash()
}

// ArtifactID derives a stable identifier from the full key using the same
// length-prefixed canonical discipline as the fingerprint, so no component
// boundary can be confused with another.
func (k ArtifactKey) ArtifactID() ArtifactID {
	var buf bytes.Buffer
	writeCanonicalString(&buf, string(k.RepositoryID))
	writeCanonicalString(&buf, k.RelativePath)
	writeCanonicalString(&buf, k.SourceHash)
	writeCanonicalString(&buf, k.Fingerprint)
	sum := sha256.Sum256(buf.Bytes())
	return ArtifactID(hex.EncodeToString(sum[:]))
}

// Artifact is an immutable indexed file version stored in the catalog.
type Artifact struct {
	ID         ArtifactID
	Key        ArtifactKey
	Dimensions int
	// Chunks is the ordered chunk composition (identity + vector). Empty for a
	// whole-file cache hit that reuses an already-stored artifact.
	Chunks []ArtifactChunk
}

// ViewEntry maps a worktree path to the artifact it currently resolves to,
// under a specific generation (invariant 4: worktree isolation).
type ViewEntry struct {
	WorktreeID WorktreeID
	Path       string
	ArtifactID ArtifactID
	Generation Generation
}

// CommitRequest bundles the immutable artifact and the worktree view switch
// that must be applied atomically with job completion (invariant 6: atomic
// visibility; invariant 7: durable progress).
type CommitRequest struct {
	View     ViewEntry
	Artifact Artifact
	// Chunks is the ordered chunk composition to persist atomically with the
	// artifact and view switch (invariant 6). Ordinals must be 0..len-1.
	Chunks []ArtifactChunk
}
