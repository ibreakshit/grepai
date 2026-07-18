// internal/enginev2/enginetest/embedder.go
package enginetest

import (
	"context"
	"hash/fnv"
	"sync"
)

// FakeEmbedder implements embedder.Embedder deterministically while counting
// calls and optionally injecting faults. Vectors are a stable function of the
// input text, so cache-reuse behavior is observable.
type FakeEmbedder struct {
	dims int

	mu            sync.Mutex
	embedCalls    int
	textsEmbedded int
	failRemaining int
	failErr       error
	stickyErr     error
}

// NewFakeEmbedder returns a FakeEmbedder producing dims-length vectors.
func NewFakeEmbedder(dims int) *FakeEmbedder {
	return &FakeEmbedder{dims: dims}
}

// Dimensions returns the configured vector dimension.
func (e *FakeEmbedder) Dimensions() int { return e.dims }

// Close is a no-op.
func (e *FakeEmbedder) Close() error { return nil }

// SetError makes every subsequent call return err until cleared with nil.
func (e *FakeEmbedder) SetError(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stickyErr = err
}

// FailNext makes the next n calls return err, then resume succeeding.
func (e *FakeEmbedder) FailNext(n int, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.failRemaining = n
	e.failErr = err
}

// EmbedCalls returns the number of Embed + EmbedBatch calls made.
func (e *FakeEmbedder) EmbedCalls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.embedCalls
}

// TextsEmbedded returns the total number of texts embedded.
func (e *FakeEmbedder) TextsEmbedded() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.textsEmbedded
}

func (e *FakeEmbedder) nextErrLocked() error {
	if e.stickyErr != nil {
		return e.stickyErr
	}
	if e.failRemaining > 0 {
		e.failRemaining--
		return e.failErr
	}
	return nil
}

func (e *FakeEmbedder) vector(text string) []float32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(text))
	seed := h.Sum32()
	v := make([]float32, e.dims)
	for i := range v {
		seed = seed*1664525 + 1013904223
		v[i] = float32(seed%1000) / 1000.0
	}
	return v
}

// Embed returns a deterministic vector for text, honoring injected faults.
func (e *FakeEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.mu.Lock()
	e.embedCalls++
	if err := e.nextErrLocked(); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	e.textsEmbedded++
	e.mu.Unlock()
	return e.vector(text), nil
}

// EmbedBatch returns deterministic vectors for texts, honoring injected faults.
func (e *FakeEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	e.mu.Lock()
	e.embedCalls++
	if err := e.nextErrLocked(); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	e.textsEmbedded += len(texts)
	e.mu.Unlock()
	out := make([][]float32, len(texts))
	for i, t := range texts {
		out[i] = e.vector(t)
	}
	return out, nil
}
