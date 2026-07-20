package rpc

import (
	"errors"
	"fmt"
)

// JSON-RPC 2.0 standard error codes.
const (
	CodeParse          = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternal       = -32603
)

// ErrDaemonDown is returned (wrapped) by Dial when no daemon is listening on the
// socket, so callers can lazily start one / fall back to v1.
var ErrDaemonDown = errors.New("rpc: daemon not reachable")

// Error implements the error interface so a decoded wire error is a Go error.
func (e *Error) Error() string {
	return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message)
}
