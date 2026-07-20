package watch

import (
	"log"
	"testing"
)

// tWriter funnels manager logs into the test log.
type tWriter struct{ t *testing.T }

func (w tWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

func testLogger(t *testing.T) *log.Logger {
	return log.New(tWriter{t}, "", 0)
}
