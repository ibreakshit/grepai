// Package registry persists the host daemon's set of registered repositories to
// a single JSON file (registry.json) so the daemon re-opens their catalogs on
// restart without opening every catalog to discover them.
package registry

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// Entry records one registered repository. CatalogPath is the absolute path to
// that repo's .grepai/catalog_v2.db. The generation/cursor fields are a cheap
// status cache (kept fresh on register/reconcile); the catalog remains the
// source of truth.
type Entry struct {
	RepositoryID     string `json:"repository_id"`
	Root             string `json:"root"`
	CatalogPath      string `json:"catalog_path"`
	ActiveGeneration int64  `json:"active_generation"`
	LastReconciledAt string `json:"last_reconciled_at,omitempty"`
	PendingCount     int    `json:"pending_count"`
}

// Registry is the full set of registered repositories.
type Registry struct {
	Entries []Entry `json:"entries"`
}

// Load reads the registry file. A missing file is not an error: it returns an
// empty registry (first run).
func Load(path string) (*Registry, error) {
	b, err := os.ReadFile(path) // #nosec G304 - operator's own state file
	if errors.Is(err, os.ErrNotExist) {
		return &Registry{}, nil
	}
	if err != nil {
		return nil, err
	}
	var r Registry
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// Upsert replaces the entry with the same RepositoryID, or appends it.
func (r *Registry) Upsert(e Entry) {
	for i := range r.Entries {
		if r.Entries[i].RepositoryID == e.RepositoryID {
			r.Entries[i] = e
			return
		}
	}
	r.Entries = append(r.Entries, e)
}

// Save writes the registry atomically (temp file in the same dir + rename), so a
// crash mid-write never truncates the live registry.
func (r *Registry) Save(path string) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".registry-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil { // flush temp contents before the rename
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	// fsync the parent directory so the rename itself survives power loss.
	dir, err := os.Open(filepath.Dir(path)) // #nosec G304 - operator's own state dir
	if err != nil {
		return nil // rename succeeded; dir-sync is best-effort durability
	}
	_ = dir.Sync()
	return dir.Close()
}
