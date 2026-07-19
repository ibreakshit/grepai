package rpc

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"testing"

	"github.com/yoanbernabeu/grepai/internal/enginev2/service"
)

// stubService returns canned responses; only Status is used here. It embeds
// service.Service (nil) so it satisfies the interface with just Status defined.
type stubService struct{ service.Service }

func (stubService) Status(_ context.Context, _ service.StatusRequest) (service.StatusResponse, error) {
	return service.StatusResponse{ActiveGeneration: 7, Pending: 0, Fresh: true, DeadLetters: 0}, nil
}

// Search panics so the panic-recovery path can be exercised.
func (stubService) Search(_ context.Context, _ service.SearchRequest) (service.SearchResponse, error) {
	panic("boom")
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func TestServerErrorCodeMatrix(t *testing.T) {
	cases := []struct {
		name  string
		frame []byte
		want  int
	}{
		{"parse", []byte(`{not json`), CodeParse},
		{"invalid-request-no-version", mustJSON(t, Request{ID: json.RawMessage(`1`), Method: MethodStatus}), CodeInvalidRequest},
		{"invalid-request-no-method", mustJSON(t, Request{JSONRPC: Version, ID: json.RawMessage(`1`)}), CodeInvalidRequest},
		{"method-not-found", mustJSON(t, Request{JSONRPC: Version, ID: json.RawMessage(`1`), Method: "grepai.nope"}), CodeMethodNotFound},
		{"bad-params", mustJSON(t, Request{JSONRPC: Version, ID: json.RawMessage(`1`), Method: MethodSearch, Params: json.RawMessage(`123`)}), CodeInvalidParams},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			go func() { _ = handleConn(context.Background(), a, stubService{}) }()
			if err := writeFrame(b, tc.frame); err != nil {
				t.Fatalf("write: %v", err)
			}
			fr, err := readFrame(bufio.NewReader(b))
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			var r Response
			if err := json.Unmarshal(fr, &r); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if r.Error == nil || r.Error.Code != tc.want {
				t.Fatalf("%s: want code %d, got %+v", tc.name, tc.want, r.Error)
			}
		})
	}
}

func TestServerRecoversPanicAndConnSurvives(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	go func() { _ = handleConn(context.Background(), a, stubService{}) }()
	r := bufio.NewReader(b)

	// Search panics -> internal error.
	sreq := mustJSON(t, Request{JSONRPC: Version, ID: json.RawMessage(`1`), Method: MethodSearch, Params: mustJSON(t, service.SearchRequest{WorktreeID: "/x", Query: "q"})})
	if err := writeFrame(b, sreq); err != nil {
		t.Fatalf("write search: %v", err)
	}
	fr, err := readFrame(r)
	if err != nil {
		t.Fatalf("read search resp: %v", err)
	}
	var resp Response
	_ = json.Unmarshal(fr, &resp)
	if resp.Error == nil || resp.Error.Code != CodeInternal {
		t.Fatalf("panic should map to internal error, got %+v", resp.Error)
	}

	// The connection must still serve the next request.
	streq := mustJSON(t, Request{JSONRPC: Version, ID: json.RawMessage(`2`), Method: MethodStatus, Params: mustJSON(t, service.StatusRequest{WorktreeID: "/x"})})
	if err := writeFrame(b, streq); err != nil {
		t.Fatalf("write status: %v", err)
	}
	fr2, err := readFrame(r)
	if err != nil {
		t.Fatalf("conn should survive panic, read failed: %v", err)
	}
	var resp2 Response
	_ = json.Unmarshal(fr2, &resp2)
	if resp2.Error != nil || string(resp2.ID) != "2" {
		t.Fatalf("status after panic should succeed with id 2, got %+v id=%s", resp2.Error, resp2.ID)
	}
}

func TestServerNotificationGetsNoReply(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	go func() { _ = handleConn(context.Background(), a, stubService{}) }()
	r := bufio.NewReader(b)

	// A notification has no id; the server must not reply.
	note := mustJSON(t, Request{JSONRPC: Version, Method: MethodStatus, Params: mustJSON(t, service.StatusRequest{WorktreeID: "/x"})})
	if err := writeFrame(b, note); err != nil {
		t.Fatalf("write notification: %v", err)
	}
	// Follow with a real request; the first frame read back must be its reply
	// (id 9) — proof the notification produced no frame.
	req := mustJSON(t, Request{JSONRPC: Version, ID: json.RawMessage(`9`), Method: MethodStatus, Params: mustJSON(t, service.StatusRequest{WorktreeID: "/x"})})
	if err := writeFrame(b, req); err != nil {
		t.Fatalf("write request: %v", err)
	}
	fr, err := readFrame(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var resp Response
	_ = json.Unmarshal(fr, &resp)
	if string(resp.ID) != "9" {
		t.Fatalf("expected reply to id=9 (notification must get no reply), got id=%s", resp.ID)
	}
}

func TestServerDispatchesStatusRawFrame(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	go func() { _ = handleConn(context.Background(), a, stubService{}) }()

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
