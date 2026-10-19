package server

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/shxntanu/rift/internal/proto"
)

func TestServeContextCancelsBlockedHandshake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	done := make(chan error, 1)
	go func() { done <- ServeContext(ctx, server) }()
	if err := client.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var magic [len(proto.Magic)]byte
	if _, err := io.ReadFull(client, magic[:]); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation left a server blocked in handshake")
	}
}
