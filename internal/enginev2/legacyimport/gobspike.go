// Package legacyimport is a READ-ONLY Phase 3 spike proving v2 can decode the
// legacy GOB index format ahead of the Phase 6 migration. It never writes to
// the v2 catalog and never mutates a legacy store.
package legacyimport

import (
	"encoding/gob"
	"fmt"
	"os"
)

// mirror mirrors the shape of the legacy store's gobData so gob can decode it
// structurally, without importing the legacy store package. gob matches struct
// fields by name and skips any source field absent here, so only the fields the
// spike reads need to be declared (no time.Time fields required).
type mirror struct {
	Chunks map[string]struct {
		ID          string
		FilePath    string
		Vector      []float32
		ContentHash string
	}
	Documents map[string]struct {
		Path     string
		ChunkIDs []string
	}
}

// Summary is a read-only description of a legacy GOB index.
type Summary struct {
	ChunkCount        int
	DocumentCount     int
	Dimensions        int
	SampleContentHash string
}

// InspectGOB decodes a legacy GOB index read-only and returns a summary. It
// proves the on-disk format (chunk vectors, counts) is recoverable by v2.
func InspectGOB(path string) (Summary, error) {
	f, err := os.Open(path) // #nosec G304 - operator-supplied legacy index path (read-only spike)
	if err != nil {
		return Summary{}, err
	}
	defer func() { _ = f.Close() }()

	var m mirror
	if err := gob.NewDecoder(f).Decode(&m); err != nil {
		return Summary{}, fmt.Errorf("decode legacy gob: %w", err)
	}
	s := Summary{ChunkCount: len(m.Chunks), DocumentCount: len(m.Documents)}
	for _, ch := range m.Chunks {
		s.Dimensions = len(ch.Vector)
		s.SampleContentHash = ch.ContentHash
		break
	}
	return s, nil
}
