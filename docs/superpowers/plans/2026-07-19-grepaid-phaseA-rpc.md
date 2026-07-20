# grepaid Phase A — RPC Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Give the existing `internal/enginev2/rpc` package a working Unix-socket JSON-RPC 2.0 transport: `Content-Length` framing, a `Server` that dispatches the 8 methods to a `service.Service`, and a `Client` that dials, calls, correlates responses, and distinguishes a down daemon from other errors.

**Architecture:** `rpc.go` already pins the wire envelope (`Request`/`Response`/`Error`) and the 8 method-name constants. Phase A adds: `codec.go` (frame read/write), `errors.go` (JSON-RPC code constants + `Error` as a Go error + `ErrDaemonDown`), `server.go` (`Serve` + a method→service dispatch table), `client.go` (`Dial`/`Call` + typed per-method wrappers). The service request/response structs are marshalled directly as the JSON-RPC `params`/`result` (client and server share the structs, so field-name JSON round-trips without tags). This is Phase A of `docs/superpowers/specs/2026-07-19-grepaid-daemon-design.md` §4.5; fully isolated — no daemon process, no CLI.

**Tech Stack:** Go 1.24.2, standard library only (`net`, `bufio`, `encoding/json`, `sync`, `syscall`).

## Global Constraints

- Module path `github.com/yoanbernabeu/grepai`; Go floor 1.24.2.
- All new code in the existing package `internal/enginev2/rpc` (`package rpc`).
- `rpc` may import `service` (the dispatch target) + `core`; `service` must NOT import `rpc` (keep it transport-independent — verify no cycle).
- Framing is LSP-style: `Content-Length: <n>\r\n\r\n` followed by exactly `<n>` bytes of UTF-8 JSON. One frame = one JSON-RPC object.
- A request with no `id` (JSON-RPC notification) receives **no** response.
- Gates (Gate A): `make build`, `make lint`, `go test ./... -race`, `go vet`, `gofmt -l` empty. Independent `codex-bg` review before Phase B.
- Commit per task, conventional commits.

## File Structure

- `internal/enginev2/rpc/errors.go` — **create**: code constants, `(*Error).Error()`, `ErrDaemonDown`.
- `internal/enginev2/rpc/codec.go` — **create**: `writeFrame`, `readFrame`.
- `internal/enginev2/rpc/server.go` — **create**: `Serve`, connection loop, dispatch.
- `internal/enginev2/rpc/client.go` — **create**: `Client`, `Dial`, `Call`, typed wrappers.
- `internal/enginev2/rpc/codec_test.go`, `server_test.go`, `client_test.go` — **create**.

## Reference: the dispatch target (`service.Service`, verbatim)

```go
Register(ctx, RegisterRequest) (RegisterResponse, error)
Reconcile(ctx, ReconcileRequest) (ReconcileResponse, error)
Search(ctx, SearchRequest) (SearchResponse, error)
Trace(ctx, TraceRequest) (TraceResponse, error)
Status(ctx, StatusRequest) (StatusResponse, error)
WaitFresh(ctx, WaitFreshRequest) (WaitFreshResponse, error)
Rebuild(ctx, RebuildRequest) (RebuildResponse, error)
DeadLetters(ctx, DeadLetterRequest) (DeadLetterResponse, error)
```

Method-name constants already in `rpc.go`: `MethodRegister`, `MethodReconcile`, `MethodSearch`, `MethodTrace`, `MethodStatus`, `MethodWaitFresh`, `MethodRebuild`, `MethodDeadLetters`.

---

## Task 1: Error codes + `Error` as a Go error + `ErrDaemonDown`

**Files:**
- Create: `internal/enginev2/rpc/errors.go`
- Test: `internal/enginev2/rpc/errors_test.go`

**Interfaces:**
- Produces: `const CodeParse=-32700, CodeInvalidRequest=-32600, CodeMethodNotFound=-32601, CodeInvalidParams=-32602, CodeInternal=-32603`; `func (e *Error) Error() string`; `var ErrDaemonDown = errors.New("rpc: daemon not reachable")`.

- [ ] **Step 1: Failing test** — create `errors_test.go`:

```go
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
```

- [ ] **Step 2: Run — FAIL** (`CodeMethodNotFound`/`Error` method undefined): `go test ./internal/enginev2/rpc/ -run TestErrorImplementsError -v`

- [ ] **Step 3: Implement** — create `errors.go`:

```go
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
```

- [ ] **Step 4: Run — PASS.**
- [ ] **Step 5: Commit** — `git add internal/enginev2/rpc/errors.go internal/enginev2/rpc/errors_test.go && git commit -m "feat(rpc): JSON-RPC error codes, Error as a Go error, ErrDaemonDown"`

---

## Task 2: Frame codec (`Content-Length`)

**Files:**
- Create: `internal/enginev2/rpc/codec.go`
- Test: `internal/enginev2/rpc/codec_test.go`

**Interfaces:**
- Produces: `func writeFrame(w io.Writer, payload []byte) error`; `func readFrame(r *bufio.Reader) ([]byte, error)` (returns `io.EOF` at a clean close).

- [ ] **Step 1: Failing test** — create `codec_test.go`:

```go
package rpc

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

func TestFrameRoundTripAndMultiple(t *testing.T) {
	var buf bytes.Buffer
	if err := writeFrame(&buf, []byte(`{"a":1}`)); err != nil {
		t.Fatalf("write1: %v", err)
	}
	if err := writeFrame(&buf, []byte(`{"b":2}`)); err != nil {
		t.Fatalf("write2: %v", err)
	}
	r := bufio.NewReader(&buf)
	f1, err := readFrame(r)
	if err != nil || string(f1) != `{"a":1}` {
		t.Fatalf("frame1 = %q, %v", f1, err)
	}
	f2, err := readFrame(r)
	if err != nil || string(f2) != `{"b":2}` {
		t.Fatalf("frame2 = %q, %v", f2, err)
	}
	if _, err := readFrame(r); err != io.EOF {
		t.Fatalf("third read should be io.EOF, got %v", err)
	}
}

func TestReadFramePartialReads(t *testing.T) {
	// A reader that yields the framed bytes one byte at a time must still parse.
	var buf bytes.Buffer
	_ = writeFrame(&buf, []byte(`{"hello":"world"}`))
	r := bufio.NewReader(iotest_oneByteReader(buf.Bytes()))
	f, err := readFrame(r)
	if err != nil || string(f) != `{"hello":"world"}` {
		t.Fatalf("partial-read frame = %q, %v", f, err)
	}
}

// iotest_oneByteReader returns a reader that returns at most one byte per Read.
func iotest_oneByteReader(b []byte) io.Reader { return &oneByte{b} }

type oneByte struct{ b []byte }

func (o *oneByte) Read(p []byte) (int, error) {
	if len(o.b) == 0 {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = o.b[0]
	o.b = o.b[1:]
	return 1, nil
}
```

- [ ] **Step 2: Run — FAIL** (`writeFrame`/`readFrame` undefined).

- [ ] **Step 3: Implement** — create `codec.go`:

```go
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
	var n int = -1
	first := true
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if err == io.EOF && first && line == "" {
				return nil, io.EOF
			}
			return nil, err
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
```

- [ ] **Step 4: Run — PASS.**
- [ ] **Step 5: Commit** — `git add internal/enginev2/rpc/codec.go internal/enginev2/rpc/codec_test.go && git commit -m "feat(rpc): Content-Length frame codec with partial-read + size bound"`

---

## Task 3: Server (`Serve` + dispatch)

**Files:**
- Create: `internal/enginev2/rpc/server.go`
- Test: `internal/enginev2/rpc/server_test.go` (uses the Client from Task 4 — write the server first with a raw-frame test, then Task 4's round-trip test exercises dispatch fully).

**Interfaces:**
- Consumes: `service.Service`, `codec`, `errors` from Tasks 1–2.
- Produces: `func Serve(l net.Listener, h service.Service) error` (returns when the listener is closed); internal `handleConn`, `dispatch`.

- [ ] **Step 1: Failing test** — create `server_test.go` (raw-frame level, no client yet):

```go
package rpc

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
)

// stubService returns canned responses; only Status is used here.
type stubService struct{ service.Service }

func (stubService) Status(_ context.Context, _ service.StatusRequest) (service.StatusResponse, error) {
	return service.StatusResponse{ActiveGeneration: 7, Pending: 0, Fresh: true, DeadLetters: 0}, nil
}

func TestServerDispatchesStatusRawFrame(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	go func() { _ = handleConn(context.Background(), a, stubService{}) }()

	// Send a Status request frame.
	params, _ := json.Marshal(service.StatusRequest{WorktreeID: "/x"})
	req, _ := json.Marshal(Request{JSONRPC: Version, ID: json.RawMessage(`1`), Method: MethodStatus, Params: params})
	if err := writeFrame(b, req); err != nil {
		t.Fatalf("write: %v", err)
	}
	resp, err := readFrame(bufio.NewReader(b))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var r Response
	if err := json.Unmarshal(resp, &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if r.Error != nil {
		t.Fatalf("unexpected error: %v", r.Error)
	}
	var sr service.StatusResponse
	if err := json.Unmarshal(r.Result, &sr); err != nil {
		t.Fatalf("result unmarshal: %v", err)
	}
	if sr.ActiveGeneration != 7 || !sr.Fresh {
		t.Fatalf("bad status result: %+v", sr)
	}
}

func TestServerUnknownMethod(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	go func() { _ = handleConn(context.Background(), a, stubService{}) }()
	req, _ := json.Marshal(Request{JSONRPC: Version, ID: json.RawMessage(`2`), Method: "grepai.nope"})
	_ = writeFrame(b, req)
	resp, _ := readFrame(bufio.NewReader(b))
	var r Response
	_ = json.Unmarshal(resp, &r)
	if r.Error == nil || r.Error.Code != CodeMethodNotFound {
		t.Fatalf("want MethodNotFound, got %+v", r.Error)
	}
}
```

> `stubService` embeds `service.Service` (nil) so it satisfies the interface with only `Status` overridden; calling any other method would panic, but these tests only call `Status`/unknown.

- [ ] **Step 2: Run — FAIL** (`handleConn` undefined).

- [ ] **Step 3: Implement** — create `server.go`:

```go
package rpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"

	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
)

// Serve accepts connections on l and serves the JSON-RPC surface backed by h,
// one goroutine per connection. It returns when l is closed.
func Serve(l net.Listener, h service.Service) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go func() { _ = handleConn(context.Background(), conn, h) }()
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

// dispatch decodes one request frame, invokes the method, and returns the
// response (and whether the request was a notification, which gets no reply).
func dispatch(ctx context.Context, h service.Service, frame []byte) (resp Response, notification bool) {
	var req Request
	if err := json.Unmarshal(frame, &req); err != nil {
		return errResp(nil, CodeParse, "parse error", err), false
	}
	if req.JSONRPC != Version || req.Method == "" {
		return errResp(req.ID, CodeInvalidRequest, "invalid request", nil), req.ID == nil
	}
	// Recover a handler panic into an internal error rather than killing the conn.
	defer func() {
		if p := recover(); p != nil {
			resp = errResp(req.ID, CodeInternal, "internal error", errorf("panic: %v", p))
			notification = false
		}
	}()

	result, callErr := call(ctx, h, req.Method, req.Params)
	if errors.Is(callErr, errUnknownMethod) {
		return errResp(req.ID, CodeMethodNotFound, "method not found: "+req.Method, nil), req.ID == nil
	}
	if errors.Is(callErr, errBadParams) {
		return errResp(req.ID, CodeInvalidParams, "invalid params", callErr), req.ID == nil
	}
	if callErr != nil {
		return errResp(req.ID, CodeInternal, callErr.Error(), nil), req.ID == nil
	}
	if req.ID == nil {
		return Response{}, true // notification
	}
	rawResult, _ := json.Marshal(result)
	return Response{JSONRPC: Version, ID: req.ID, Result: rawResult}, false
}

var (
	errUnknownMethod = errors.New("rpc: unknown method")
	errBadParams     = errors.New("rpc: bad params")
)

// call decodes params for method and invokes h. The dispatch table is the one
// place method names bind to service calls.
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
		return errorf("%w: %v", errBadParams, err)
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

// errorf is fmt.Errorf, aliased so server.go needs no direct fmt import churn.
func errorf(format string, a ...any) error { return fmtErrorf(format, a...) }
```

> Add `import "fmt"` and define `func fmtErrorf(format string, a ...any) error { return fmt.Errorf(format, a...) }`, OR simply use `fmt.Errorf` directly and drop the `errorf`/`fmtErrorf` indirection. Prefer the direct `fmt.Errorf` — replace `errorf(...)` calls with `fmt.Errorf(...)` and delete the two helper lines. (Kept explicit here so the dependency is visible.)

- [ ] **Step 4: Run — PASS** both server tests.
- [ ] **Step 5: Commit** — `git add internal/enginev2/rpc/server.go internal/enginev2/rpc/server_test.go && git commit -m "feat(rpc): Unix-socket server with framed dispatch to service.Service"`

---

## Task 4: Client (`Dial`, `Call`, typed wrappers)

**Files:**
- Create: `internal/enginev2/rpc/client.go`
- Test: `internal/enginev2/rpc/client_test.go`

**Interfaces:**
- Produces:
  - `type Client struct { … }`; `func Dial(socketPath string) (*Client, error)` (wraps `ErrDaemonDown` on ENOENT/ECONNREFUSED); `func NewClientConn(conn net.Conn) *Client` (for tests over `net.Pipe`); `func (c *Client) Close() error`.
  - `func (c *Client) Call(ctx context.Context, method string, params, result any) error`.
  - Typed wrappers: `Register`, `Reconcile`, `Search`, `Trace`, `Status`, `WaitFresh`, `Rebuild`, `DeadLetters`, each `(ctx, service.XRequest) (service.XResponse, error)`.

- [ ] **Step 1: Failing test** — create `client_test.go` (full client↔server round-trip over an in-memory listener):

```go
package rpc

import (
	"context"
	"net"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
)

func TestClientServerStatusRoundTrip(t *testing.T) {
	a, b := net.Pipe()
	go func() { _ = handleConn(context.Background(), a, stubService{}) }()
	defer a.Close()

	c := NewClientConn(b)
	defer c.Close()
	resp, err := c.Status(context.Background(), service.StatusRequest{WorktreeID: "/x"})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if resp.ActiveGeneration != 7 || !resp.Fresh {
		t.Fatalf("bad response: %+v", resp)
	}
}

func TestDialDaemonDown(t *testing.T) {
	_, err := Dial(filepath_Join(t.TempDir(), "nonexistent.sock"))
	if err == nil {
		t.Fatal("dial of a missing socket should fail")
	}
	if !isErrDaemonDown(err) {
		t.Fatalf("missing socket should be ErrDaemonDown, got %v", err)
	}
}
```

> Add imports `"path/filepath"` (use `filepath.Join` directly; the `filepath_Join`/`isErrDaemonDown` placeholders above are just to keep the snippet importable — replace `filepath_Join` with `filepath.Join` and `isErrDaemonDown(err)` with `errors.Is(err, ErrDaemonDown)`, importing `"errors"`).

- [ ] **Step 2: Run — FAIL** (`NewClientConn`/`Dial`/`Status` undefined).

- [ ] **Step 3: Implement** — create `client.go`:

```go
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
		defer c.conn.SetDeadline(time.Time{})
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

func (c *Client) Status(ctx context.Context, req service.StatusRequest) (service.StatusResponse, error) {
	var resp service.StatusResponse
	err := c.Call(ctx, MethodStatus, req, &resp)
	return resp, err
}
```

Then add the remaining six wrappers (`Reconcile`, `Search`, `Trace`, `WaitFresh`, `Rebuild`, `DeadLetters`) following the exact shape of `Status`/`Register`, each pairing its `service.XRequest`/`service.XResponse` with the matching `Method*` constant.

- [ ] **Step 4: Run — PASS** both client tests.
- [ ] **Step 5: Commit** — `git add internal/enginev2/rpc/client.go internal/enginev2/rpc/client_test.go && git commit -m "feat(rpc): client Dial/Call with daemon-down detection + typed wrappers"`

---

## Gate A (before Phase B)

- [ ] `gofmt -l internal/enginev2/rpc` → empty.
- [ ] `go vet ./internal/enginev2/rpc/` → clean.
- [ ] `make build`, `make lint`, `go test ./... -race` → green.
- [ ] Confirm no import cycle: `go list -deps ./internal/enginev2/service | grep enginev2/rpc` prints nothing (service must not depend on rpc).
- [ ] Independent `codex-bg` review of the Phase A diff; address findings before Phase B.

## Self-Review notes (author)

- **Spec coverage:** §4.5 server (framing, dispatch table, error codes, panic recovery, notification-no-reply) = Tasks 2–3; client (Dial daemon-down, Call, typed wrappers) = Tasks 1,4.
- **Notification semantics:** `dispatch` returns `notification=true` only when `req.ID == nil`; all error branches also honor that so a malformed notification stays silent (except a parse error where no id could be recovered → replied with a null id, per JSON-RPC).
- **Frame safety:** `maxFrameBytes` caps allocation from a hostile `Content-Length`; `io.ReadFull` handles short reads.
- **Grep-confirm at implementation:** `service.Service` method set + the eight `Method*` constants are verbatim from `rpc.go`/`service.go` (checked 2026-07-19). Replace the two snippet placeholders (`errorf`/`fmtErrorf` indirection in server.go; `filepath_Join`/`isErrDaemonDown` in the client test) with the direct forms noted inline.
