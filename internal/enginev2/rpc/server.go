package rpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
)

// Serve accepts connections on l and serves the JSON-RPC surface backed by h,
// one goroutine per connection. Handlers run under ctx, so canceling it aborts
// in-flight service calls; cancellation also closes the listener and every open
// connection (unblocking their reads) — no handler goroutine outlives shutdown
// to mutate state after the caller has torn it down. Returns nil when stopped
// by ctx/listener close.
func Serve(ctx context.Context, l net.Listener, h service.Service) error {
	var conns sync.Map // net.Conn -> struct{}
	go func() {
		<-ctx.Done()
		_ = l.Close()
		conns.Range(func(k, _ any) bool {
			if c, ok := k.(net.Conn); ok {
				_ = c.Close()
			}
			return true
		})
	}()
	for {
		conn, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		conns.Store(conn, struct{}{})
		go func() {
			defer conns.Delete(conn)
			_ = handleConn(ctx, conn, h)
		}()
	}
}

// handleConn reads framed requests sequentially and writes framed responses.
func handleConn(ctx context.Context, conn net.Conn, h service.Service) error {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		frame, err := readFrame(r)
		if err != nil {
			return err // io.EOF on clean close
		}
		resp, notify := dispatch(ctx, h, frame)
		if notify {
			continue // notification: no response
		}
		out, mErr := json.Marshal(resp)
		if mErr != nil {
			return mErr
		}
		if err := writeFrame(conn, out); err != nil {
			return err
		}
	}
}

var (
	errUnknownMethod = errors.New("rpc: unknown method")
	errBadParams     = errors.New("rpc: bad params")
)

// dispatch decodes one request frame, invokes the method, and returns the
// response plus whether the request was a notification (which gets no reply).
func dispatch(ctx context.Context, h service.Service, frame []byte) (resp Response, notification bool) {
	var req Request
	if err := json.Unmarshal(frame, &req); err != nil {
		return errResp(nil, CodeParse, "parse error", err), false
	}
	if req.JSONRPC != Version || req.Method == "" {
		return errResp(req.ID, CodeInvalidRequest, "invalid request", nil), req.ID == nil
	}
	// Recover a handler panic into an internal error rather than killing the
	// conn. A panicking NOTIFICATION stays silent (no id to reply to).
	defer func() {
		if p := recover(); p != nil {
			resp = errResp(req.ID, CodeInternal, "internal error", fmt.Errorf("panic: %v", p))
			notification = req.ID == nil
		}
	}()

	result, callErr := call(ctx, h, req.Method, req.Params)
	switch {
	case errors.Is(callErr, errUnknownMethod):
		return errResp(req.ID, CodeMethodNotFound, "method not found: "+req.Method, nil), req.ID == nil
	case errors.Is(callErr, errBadParams):
		return errResp(req.ID, CodeInvalidParams, "invalid params", callErr), req.ID == nil
	case callErr != nil:
		return errResp(req.ID, CodeInternal, callErr.Error(), nil), req.ID == nil
	}
	if req.ID == nil {
		return Response{}, true // notification: succeeded, no reply
	}
	rawResult, _ := json.Marshal(result)
	return Response{JSONRPC: Version, ID: req.ID, Result: rawResult}, false
}

// call decodes params for method and invokes h. The switch is the one place
// method names bind to service calls.
func call(ctx context.Context, h service.Service, method string, params json.RawMessage) (any, error) {
	switch method {
	case MethodRegister:
		var p service.RegisterRequest
		if err := decode(params, &p); err != nil {
			return nil, err
		}
		return h.Register(ctx, p)
	case MethodReconcile:
		var p service.ReconcileRequest
		if err := decode(params, &p); err != nil {
			return nil, err
		}
		return h.Reconcile(ctx, p)
	case MethodSearch:
		var p service.SearchRequest
		if err := decode(params, &p); err != nil {
			return nil, err
		}
		return h.Search(ctx, p)
	case MethodTrace:
		var p service.TraceRequest
		if err := decode(params, &p); err != nil {
			return nil, err
		}
		return h.Trace(ctx, p)
	case MethodStatus:
		var p service.StatusRequest
		if err := decode(params, &p); err != nil {
			return nil, err
		}
		return h.Status(ctx, p)
	case MethodWaitFresh:
		var p service.WaitFreshRequest
		if err := decode(params, &p); err != nil {
			return nil, err
		}
		return h.WaitFresh(ctx, p)
	case MethodRebuild:
		var p service.RebuildRequest
		if err := decode(params, &p); err != nil {
			return nil, err
		}
		return h.Rebuild(ctx, p)
	case MethodDeadLetters:
		var p service.DeadLetterRequest
		if err := decode(params, &p); err != nil {
			return nil, err
		}
		return h.DeadLetters(ctx, p)
	default:
		return nil, errUnknownMethod
	}
}

func decode(params json.RawMessage, dst any) error {
	if len(params) == 0 {
		return nil // no params -> zero-value request
	}
	if err := json.Unmarshal(params, dst); err != nil {
		return fmt.Errorf("%w: %v", errBadParams, err)
	}
	return nil
}

func errResp(id json.RawMessage, code int, msg string, data error) Response {
	e := &Error{Code: code, Message: msg}
	if data != nil {
		if b, err := json.Marshal(data.Error()); err == nil {
			e.Data = b
		}
	}
	return Response{JSONRPC: Version, ID: id, Error: e}
}
