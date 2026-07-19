package legacyimport

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/yoanbernabeu/grepai/config"
)

// DeriveFingerprint produces a stable indexing fingerprint for a v1 index from
// the inputs that determine its stored vectors: embedder provider/model/
// dimensions, chunk size/overlap, and the framework-processing settings that
// transform embed content. The leading "grepai-v1|" domain guarantees it can
// never collide with a v2 native fingerprint (runtime.Fingerprint): v1 embedded
// framework-transformed content the v2 builder does not replicate, so a migrated
// generation and a v2 re-index are deliberately distinct. Deterministic and
// sensitive to every input field, including framework enablement/mode — two
// indexes that differ only in framework processing produce different vectors and
// must not share a fingerprint.
func DeriveFingerprint(cfg *config.Config) string {
	dims := 0
	if cfg.Embedder.Dimensions != nil {
		dims = *cfg.Embedder.Dimensions
	}
	seed := fmt.Sprintf("grepai-v1|%s|%s|%d|%d|%d|framework=%t|mode=%s",
		cfg.Embedder.Provider, cfg.Embedder.Model, dims,
		cfg.Chunking.Size, cfg.Chunking.Overlap,
		cfg.Framework.Enabled, cfg.Framework.Mode)
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])
}
