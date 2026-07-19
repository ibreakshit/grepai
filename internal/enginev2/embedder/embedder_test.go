package embedder_test

import (
	"github.com/yoanbernabeu/grepai/internal/enginev2/embedder"
	"github.com/yoanbernabeu/grepai/internal/enginev2/enginetest"
)

// Compile-time proof the shared test double satisfies the v2 port.
var _ embedder.Embedder = enginetest.NewFakeEmbedder(4)
