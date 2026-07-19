package sqlite

import (
	"encoding/binary"
	"errors"
	"math"
)

// ErrVectorLength indicates a vector blob whose byte length does not equal
// dimensions*4, or a negative dimension count.
var ErrVectorLength = errors.New("catalog/sqlite: vector byte length does not match dimensions")

// encodeVector serializes a float32 slice as little-endian IEEE-754 bytes.
func encodeVector(v []float32) []byte {
	b := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

// decodeVector deserializes a little-endian float32 blob, validating that its
// byte length equals dims*4 (invariant 10: never load a mis-dimensioned vector).
func decodeVector(b []byte, dims int) ([]float32, error) {
	if dims < 0 || len(b) != dims*4 {
		return nil, ErrVectorLength
	}
	v := make([]float32, dims)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return v, nil
}
