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
	checks := []struct{ got, want string }{
		{MethodRegister, "grepai.register"},
		{MethodReconcile, "grepai.reconcile"},
		{MethodSearch, "grepai.search"},
		{MethodTrace, "grepai.trace"},
		{MethodStatus, "grepai.status"},
		{MethodWaitFresh, "grepai.waitFresh"},
		{MethodRebuild, "grepai.rebuild"},
		{MethodDeadLetters, "grepai.deadLetters"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Fatalf("method constant = %q, want %q", c.got, c.want)
		}
	}
	if Version != "2.0" {
		t.Fatalf("JSON-RPC version = %q, want 2.0", Version)
	}
}
