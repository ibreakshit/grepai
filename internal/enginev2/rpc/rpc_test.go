package rpc

import (
	"encoding/json"
	"testing"
)

func TestRequestRoundTrip(t *testing.T) {
	in := Request{
		JSONRPC: Version,
		ID:      json.RawMessage(`1`),
		Method:  MethodSearch,
		Params:  json.RawMessage(`{"worktreeId":"wt-1","query":"auth flow"}`),
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Request
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.JSONRPC != Version || out.Method != MethodSearch {
		t.Fatalf("round-trip mismatch: %+v", out)
	}
}

func TestMethodNamesStable(t *testing.T) {
	want := map[string]string{
		"register":    MethodRegister,
		"reconcile":   MethodReconcile,
		"search":      MethodSearch,
		"trace":       MethodTrace,
		"status":      MethodStatus,
		"waitFresh":   MethodWaitFresh,
		"rebuild":     MethodRebuild,
		"deadLetters": MethodDeadLetters,
	}
	for short, full := range want {
		if full == "" {
			t.Fatalf("method %q is empty", short)
		}
	}
	if Version != "2.0" {
		t.Fatalf("JSON-RPC version = %q, want 2.0", Version)
	}
}
