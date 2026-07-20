package rpc

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

func TestFrameRoundTripAndMultiple(t *testing.T) {
	var buf bytes.Buffer
	if err := writeFrame(&buf, []byte(`{"a":1}`)); err != nil {
		t.Fatalf("write1: %v", err)
	}
	if err := writeFrame(&buf, []byte(`{"b":2}`)); err != nil {
		t.Fatalf("write2: %v", err)
	}
	r := bufio.NewReader(&buf)
	f1, err := readFrame(r)
	if err != nil || string(f1) != `{"a":1}` {
		t.Fatalf("frame1 = %q, %v", f1, err)
	}
	f2, err := readFrame(r)
	if err != nil || string(f2) != `{"b":2}` {
		t.Fatalf("frame2 = %q, %v", f2, err)
	}
	if _, err := readFrame(r); err != io.EOF {
		t.Fatalf("third read should be io.EOF, got %v", err)
	}
}

func TestReadFramePartialReads(t *testing.T) {
	// A reader that yields the framed bytes one byte at a time must still parse.
	var buf bytes.Buffer
	_ = writeFrame(&buf, []byte(`{"hello":"world"}`))
	r := bufio.NewReader(&oneByte{buf.Bytes()})
	f, err := readFrame(r)
	if err != nil || string(f) != `{"hello":"world"}` {
		t.Fatalf("partial-read frame = %q, %v", f, err)
	}
}

// oneByte returns at most one byte per Read.
type oneByte struct{ b []byte }

func (o *oneByte) Read(p []byte) (int, error) {
	if len(o.b) == 0 {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = o.b[0]
	o.b = o.b[1:]
	return 1, nil
}
