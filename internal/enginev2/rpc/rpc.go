// Package rpc defines the JSON-RPC 2.0 envelope and method names for the
// grepaid Unix-socket transport (spec §7). Phase 4 implements framing
// (Content-Length) and dispatch; Phase 0 pins the wire contract.
package rpc

import "encoding/json"

// Version is the JSON-RPC protocol version string.
const Version = "2.0"

// Method names for the grepaid service surface (spec §7).
const (
	MethodRegister    = "grepai.register"
	MethodReconcile   = "grepai.reconcile"
	MethodSearch      = "grepai.search"
	MethodTrace       = "grepai.trace"
	MethodStatus      = "grepai.status"
	MethodWaitFresh   = "grepai.waitFresh"
	MethodRebuild     = "grepai.rebuild"
	MethodDeadLetters = "grepai.deadLetters"
	MethodSearchAll   = "grepai.searchAll"
)

// Request is a JSON-RPC 2.0 request object.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Response is a JSON-RPC 2.0 response object.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// Error is a JSON-RPC 2.0 error object.
type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}
