package legacyimport

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/yoanbernabeu/grepai/config"
)

// DeriveFingerprint produces a stable indexing fingerprint for a v1 index from
// its config's embedder + chunking parameters. The trailing "framework:v1"
// marker guarantees it can never collide with a v2 native fingerprint
// (runtime.Fingerprint): v1 embedded framework-transformed content that the v2
// builder does not replicate, so a migrated generation and a v2 re-index are
// deliberately distinct. Deterministic and sensitive to every input field.
func DeriveFingerprint(cfg *config.Config) string {
	dims := 0
	if cfg.Embedder.Dimensions != nil {
		dims = *cfg.Embedder.Dimensions
	}
	seed := fmt.Sprintf("%s|%s|%d|%d|%d|framework:v1",
		cfg.Embedder.Provider, cfg.Embedder.Model, dims, cfg.Chunking.Size, cfg.Chunking.Overlap)
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])
}
