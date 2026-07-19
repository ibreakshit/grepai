package rpc

import (
	"context"
	"errors"
	"net"
	"path/filepath"
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
	_, err := Dial(filepath.Join(t.TempDir(), "nonexistent.sock"))
	if err == nil {
		t.Fatal("dial of a missing socket should fail")
	}
	if !errors.Is(err, ErrDaemonDown) {
		t.Fatalf("missing socket should be ErrDaemonDown, got %v", err)
	}
}
