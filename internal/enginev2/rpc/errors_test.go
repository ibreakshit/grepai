package rpc

import "testing"

func TestErrorImplementsError(t *testing.T) {
	var err error = &Error{Code: CodeMethodNotFound, Message: "no such method"}
	if err.Error() == "" {
		t.Fatal("Error() should be non-empty")
	}
	if CodeParse != -32700 || CodeInternal != -32603 {
		t.Fatalf("JSON-RPC codes wrong: parse=%d internal=%d", CodeParse, CodeInternal)
	}
}
