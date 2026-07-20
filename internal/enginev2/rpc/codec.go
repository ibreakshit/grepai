package rpc

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// maxFrameBytes bounds a single message so a malformed/hostile Content-Length
// cannot force an unbounded allocation.
const maxFrameBytes = 64 << 20 // 64 MiB

// maxHeaderLine bounds a single header line so an endless unterminated header
// cannot buffer unbounded memory.
const maxHeaderLine = 4 << 10 // 4 KiB

// writeFrame writes payload with an LSP-style Content-Length header.
func writeFrame(w io.Writer, payload []byte) error {
	if _, err := fmt.Fprintf(w, "Content-Length: %d\r\n\r\n", len(payload)); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// readFrame reads one Content-Length-framed message. A clean close before any
// header byte returns io.EOF. Headers other than Content-Length are ignored.
func readFrame(r *bufio.Reader) ([]byte, error) {
	n := -1
	first := true
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if err == io.EOF && first && line == "" {
				return nil, io.EOF
			}
			return nil, err
		}
		if len(line) > maxHeaderLine {
			return nil, fmt.Errorf("rpc: header line too long (%d bytes)", len(line))
		}
		first = false
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed == "" { // blank line ends the headers
			break
		}
		if k, v, ok := strings.Cut(trimmed, ":"); ok && strings.EqualFold(strings.TrimSpace(k), "Content-Length") {
			parsed, perr := strconv.Atoi(strings.TrimSpace(v))
			if perr != nil {
				return nil, fmt.Errorf("rpc: bad Content-Length %q: %w", v, perr)
			}
			n = parsed
		}
	}
	if n < 0 {
		return nil, fmt.Errorf("rpc: missing Content-Length header")
	}
	if n > maxFrameBytes {
		return nil, fmt.Errorf("rpc: frame too large (%d bytes)", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return body, nil
}
