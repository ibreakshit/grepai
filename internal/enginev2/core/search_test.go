package core

import "testing"

func TestSearchHitFields(t *testing.T) {
	h := SearchHit{Path: "a.go", Score: 0.9}
	if h.Path != "a.go" || h.Score != 0.9 {
		t.Fatalf("unexpected: %+v", h)
	}
}
