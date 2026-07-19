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
