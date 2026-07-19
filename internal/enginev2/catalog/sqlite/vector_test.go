package sqlite

import (
	"errors"
	"testing"
)

func TestVectorRoundTrip(t *testing.T) {
	in := []float32{0, 1, -1, 3.14159, 2.71828, 1e-9, 1e9}
	b := encodeVector(in)
	if len(b) != len(in)*4 {
		t.Fatalf("encoded length = %d, want %d", len(b), len(in)*4)
	}
	out, err := decodeVector(b, len(in))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("decoded len = %d, want %d", len(out), len(in))
	}
	for i := range in {
		if out[i] != in[i] {
			t.Fatalf("index %d: got %v want %v", i, out[i], in[i])
		}
	}
}

func TestDecodeRejectsWrongLength(t *testing.T) {
	b := encodeVector([]float32{1, 2, 3})
	if _, err := decodeVector(b, 4); !errors.Is(err, ErrVectorLength) {
		t.Fatalf("expected ErrVectorLength for dims mismatch, got %v", err)
	}
	if _, err := decodeVector(b[:len(b)-1], 3); !errors.Is(err, ErrVectorLength) {
		t.Fatalf("expected ErrVectorLength for truncated blob, got %v", err)
	}
	if _, err := decodeVector(b, -1); !errors.Is(err, ErrVectorLength) {
		t.Fatalf("expected ErrVectorLength for negative dims, got %v", err)
	}
}
