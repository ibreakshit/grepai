// internal/enginev2/enginetest/embedder_test.go
package enginetest

import (
	"context"
	"errors"
	"testing"

	"github.com/yoanbernabeu/grepai/embedder"
)

var _ embedder.Embedder = (*FakeEmbedder)(nil)

func TestFakeEmbedderCounts(t *testing.T) {
	e := NewFakeEmbedder(8)
	if e.Dimensions() != 8 {
		t.Fatalf("dims = %d, want 8", e.Dimensions())
	}
	v, err := e.Embed(context.Background(), "hello")
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	if len(v) != 8 {
		t.Fatalf("vector len = %d, want 8", len(v))
	}
	if _, err := e.EmbedBatch(context.Background(), []string{"a", "b", "c"}); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if e.EmbedCalls() != 2 {
		t.Fatalf("EmbedCalls = %d, want 2", e.EmbedCalls())
	}
	if e.TextsEmbedded() != 4 {
		t.Fatalf("TextsEmbedded = %d, want 4", e.TextsEmbedded())
	}
}

func TestFakeEmbedderFaultInjection(t *testing.T) {
	e := NewFakeEmbedder(4)
	boom := errors.New("boom")
	e.FailNext(1, boom)
	if _, err := e.Embed(context.Background(), "x"); !errors.Is(err, boom) {
		t.Fatalf("expected injected error, got %v", err)
	}
	if _, err := e.Embed(context.Background(), "y"); err != nil {
		t.Fatalf("second call should succeed, got %v", err)
	}
}

func TestFakeEmbedderDeterministicVectors(t *testing.T) {
	e := NewFakeEmbedder(4)
	a, _ := e.Embed(context.Background(), "same")
	b, _ := e.Embed(context.Background(), "same")
	for i := range a {
		if a[i] != b[i] {
			t.Fatal("same text must embed to the same vector")
		}
	}
}
