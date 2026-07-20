package rpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
)

// Client is a JSON-RPC client over one connection. Safe for sequential use
// (Call is mutex-serialized; the daemon's per-connection server is sequential).
type Client struct {
	mu     sync.Mutex
	conn   net.Conn
	r      *bufio.Reader
	nextID uint64
}

// Dial connects to the daemon's Unix socket. A missing or unaccepting socket is
// wrapped as ErrDaemonDown so callers can lazily start the daemon.
func Dial(socketPath string) (*Client, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED) {
			return nil, fmt.Errorf("%w: %v", ErrDaemonDown, err)
		}
		return nil, err
	}
	return NewClientConn(conn), nil
}

// NewClientConn wraps an existing connection (used in tests over net.Pipe).
func NewClientConn(conn net.Conn) *Client {
	return &Client{conn: conn, r: bufio.NewReader(conn), nextID: 1}
}

// Close closes the underlying connection.
func (c *Client) Close() error { return c.conn.Close() }

// Call sends method(params) and decodes the result. A wire Error is returned as
// a *Error. The ctx deadline (if any) bounds the write+read.
func (c *Client) Call(ctx context.Context, method string, params, result any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if dl, ok := ctx.Deadline(); ok {
		_ = c.conn.SetDeadline(dl)
		defer func() { _ = c.conn.SetDeadline(time.Time{}) }()
	}

	id := c.nextID
	c.nextID++

	var rawParams json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return err
		}
		rawParams = b
	}
	reqBytes, err := json.Marshal(Request{
		JSONRPC: Version,
		ID:      json.RawMessage(strconv.FormatUint(id, 10)),
		Method:  method,
		Params:  rawParams,
	})
	if err != nil {
		return err
	}
	if err := writeFrame(c.conn, reqBytes); err != nil {
		return err
	}
	frame, err := readFrame(c.r)
	if err != nil {
		return err
	}
	var resp Response
	if err := json.Unmarshal(frame, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return resp.Error
	}
	// Verify the response id matches the request we just sent. With sequential
	// (mutex-serialized, single-outstanding) calls this is always true; a
	// mismatch means the stream desynced, which must surface loudly rather than
	// returning another call's result.
	if string(resp.ID) != strconv.FormatUint(id, 10) {
		return fmt.Errorf("rpc: response id %s does not match request id %d (stream desync)", resp.ID, id)
	}
	if result != nil && len(resp.Result) > 0 {
		return json.Unmarshal(resp.Result, result)
	}
	return nil
}

// Typed wrappers -------------------------------------------------------------

func (c *Client) Register(ctx context.Context, req service.RegisterRequest) (service.RegisterResponse, error) {
	var resp service.RegisterResponse
	err := c.Call(ctx, MethodRegister, req, &resp)
	return resp, err
}

func (c *Client) Reconcile(ctx context.Context, req service.ReconcileRequest) (service.ReconcileResponse, error) {
	var resp service.ReconcileResponse
	err := c.Call(ctx, MethodReconcile, req, &resp)
	return resp, err
}

func (c *Client) Search(ctx context.Context, req service.SearchRequest) (service.SearchResponse, error) {
	var resp service.SearchResponse
	err := c.Call(ctx, MethodSearch, req, &resp)
	return resp, err
}

func (c *Client) SearchAll(ctx context.Context, req service.SearchAllRequest) (service.SearchAllResponse, error) {
	var resp service.SearchAllResponse
	err := c.Call(ctx, MethodSearchAll, req, &resp)
	return resp, err
}

func (c *Client) Trace(ctx context.Context, req service.TraceRequest) (service.TraceResponse, error) {
	var resp service.TraceResponse
	err := c.Call(ctx, MethodTrace, req, &resp)
	return resp, err
}

func (c *Client) Status(ctx context.Context, req service.StatusRequest) (service.StatusResponse, error) {
	var resp service.StatusResponse
	err := c.Call(ctx, MethodStatus, req, &resp)
	return resp, err
}

func (c *Client) WaitFresh(ctx context.Context, req service.WaitFreshRequest) (service.WaitFreshResponse, error) {
	var resp service.WaitFreshResponse
	err := c.Call(ctx, MethodWaitFresh, req, &resp)
	return resp, err
}

func (c *Client) Rebuild(ctx context.Context, req service.RebuildRequest) (service.RebuildResponse, error) {
	var resp service.RebuildResponse
	err := c.Call(ctx, MethodRebuild, req, &resp)
	return resp, err
}

func (c *Client) DeadLetters(ctx context.Context, req service.DeadLetterRequest) (service.DeadLetterResponse, error) {
	var resp service.DeadLetterResponse
	err := c.Call(ctx, MethodDeadLetters, req, &resp)
	return resp, err
}
