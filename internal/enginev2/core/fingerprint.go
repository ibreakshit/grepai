package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
)

// FingerprintEncodingVersion is the schema version of the canonical
// fingerprint encoding. Bump it only for a deliberate, repository-wide cache
// invalidation; a bump changes every fingerprint hash.
const FingerprintEncodingVersion uint32 = 1

// IndexingFingerprint captures every input that can make a stored vector
// incompatible with a freshly computed one (invariant 10: fingerprint
// correctness). Vectors are never reused across differing fingerprints.
type IndexingFingerprint struct {
	EmbedderProvider          string
	EmbedderModel             string
	Dimensions                int
	ChunkerImplementation     string
	ChunkerSettings           map[string]string
	FrameworkTransformVersion string
	EmbeddingInputFormat      string
}

// Canonical returns the deterministic byte encoding hashed by Hash. It is
// length-prefixed and field-ordered so serialization details (map iteration
// order, whitespace, float formatting) can never change the result.
func (f IndexingFingerprint) Canonical() []byte {
	var buf bytes.Buffer
	var scratch [8]byte

	binary.BigEndian.PutUint32(scratch[:4], FingerprintEncodingVersion)
	buf.Write(scratch[:4])

	writeCanonicalString(&buf, f.EmbedderProvider)
	writeCanonicalString(&buf, f.EmbedderModel)

	binary.BigEndian.PutUint64(scratch[:8], uint64(f.Dimensions)) // #nosec G115 - embedding dimension is a small non-negative config value
	buf.Write(scratch[:8])

	writeCanonicalString(&buf, f.ChunkerImplementation)

	keys := make([]string, 0, len(f.ChunkerSettings))
	for k := range f.ChunkerSettings {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	binary.BigEndian.PutUint32(scratch[:4], uint32(len(keys))) // #nosec G115 - map length is non-negative
	buf.Write(scratch[:4])
	for _, k := range keys {
		writeCanonicalString(&buf, k)
		writeCanonicalString(&buf, f.ChunkerSettings[k])
	}

	writeCanonicalString(&buf, f.FrameworkTransformVersion)
	writeCanonicalString(&buf, f.EmbeddingInputFormat)

	return buf.Bytes()
}

// Hash returns the hex-encoded SHA-256 of the canonical encoding.
func (f IndexingFingerprint) Hash() string {
	sum := sha256.Sum256(f.Canonical())
	return hex.EncodeToString(sum[:])
}

func writeCanonicalString(buf *bytes.Buffer, s string) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(s))) // #nosec G115 - string length is non-negative
	buf.Write(lenBuf[:])
	buf.WriteString(s)
}
