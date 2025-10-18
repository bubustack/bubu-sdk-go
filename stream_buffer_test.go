package sdk

import (
	"context"
	"fmt"
	"testing"

	bobravozgrpcproto "github.com/bubustack/bobravoz-grpc/proto/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// makePacket is kept for potential future use in tests; currently unused.
// nolint:unused
func makePacket(meta string, sizeHint int) *bobravozgrpcproto.DataPacket {
	// Use sizeHint only to vary payload size approximately
	payload := map[string]any{"k": meta}
	for i := 0; i < sizeHint; i++ {
		payload["p"+string(rune('a'+(i%26)))] = i
	}
	// Build a minimal Struct; tests do not rely on exact wire size
	s, _ := structpb.NewStruct(payload)
	return &bobravozgrpcproto.DataPacket{
		Metadata: map[string]string{"id": meta},
		Payload:  s,
	}
}

// Test that oversize messages are dropped by client buffer
func TestClientBufferAddOversizeDrops(t *testing.T) {
	t.Setenv("BUBU_GRPC_CLIENT_BUFFER_MAX_MESSAGES", "10")
	t.Setenv("BUBU_GRPC_CLIENT_BUFFER_MAX_BYTES", "64") // tiny budget
	b := newClientMessageBuffer()

	bigStr := make([]byte, 128)
	s, _ := structpb.NewStruct(map[string]any{"big": string(bigStr)})
	pkt := &bobravozgrpcproto.DataPacket{Payload: s}
	if added := b.add(pkt); added {
		t.Fatalf("expected oversize drop, got added")
	}
}

// Test that overflow is dropped when limits reached
func TestClientBufferOverflowDrops(t *testing.T) {
	t.Setenv("BUBU_GRPC_CLIENT_BUFFER_MAX_MESSAGES", "2")
	t.Setenv("BUBU_GRPC_CLIENT_BUFFER_MAX_BYTES", "1024")
	b := newClientMessageBuffer()

	p1 := &bobravozgrpcproto.DataPacket{Metadata: map[string]string{"i": "1"}}
	p2 := &bobravozgrpcproto.DataPacket{Metadata: map[string]string{"i": "2"}}
	if !b.add(p1) || !b.add(p2) {
		t.Fatalf("expected first two adds to succeed")
	}
	p3 := &bobravozgrpcproto.DataPacket{Metadata: map[string]string{"i": "3"}}
	if b.add(p3) {
		t.Fatalf("expected overflow drop on third add")
	}
}

// Test flush order and that buffer is emptied on success
func TestClientBufferFlushOrder(t *testing.T) {
	t.Setenv("BUBU_GRPC_CLIENT_BUFFER_MAX_MESSAGES", "10")
	t.Setenv("BUBU_GRPC_CLIENT_BUFFER_MAX_BYTES", "1048576")
	b := newClientMessageBuffer()

	p1 := &bobravozgrpcproto.DataPacket{Metadata: map[string]string{"i": "1"}}
	p2 := &bobravozgrpcproto.DataPacket{Metadata: map[string]string{"i": "2"}}
	if !b.add(p1) || !b.add(p2) {
		t.Fatalf("expected adds to succeed")
	}

	var sent []*bobravozgrpcproto.DataPacket
	flushed := b.flush(context.Background(), func(p *bobravozgrpcproto.DataPacket) error {
		sent = append(sent, p)
		return nil
	})
	if flushed != 2 {
		t.Fatalf("expected flushed=2, got %d", flushed)
	}
	if len(sent) != 2 || sent[0] != p1 || sent[1] != p2 {
		t.Fatalf("unexpected send order: %+v", sent)
	}
}

// Test flush stops on error and retains remaining messages
func TestClientBufferFlushStopsOnError(t *testing.T) {
	t.Setenv("BUBU_GRPC_CLIENT_BUFFER_MAX_MESSAGES", "10")
	t.Setenv("BUBU_GRPC_CLIENT_BUFFER_MAX_BYTES", "1048576")
	b := newClientMessageBuffer()

	p1 := &bobravozgrpcproto.DataPacket{Metadata: map[string]string{"i": "1"}}
	p2 := &bobravozgrpcproto.DataPacket{Metadata: map[string]string{"i": "2"}}
	p3 := &bobravozgrpcproto.DataPacket{Metadata: map[string]string{"i": "3"}}
	_ = b.add(p1)
	_ = b.add(p2)
	_ = b.add(p3)

	count := 0
	flushed := b.flush(context.Background(), func(p *bobravozgrpcproto.DataPacket) error {
		count++
		if count == 2 {
			return fmt.Errorf("send error")
		}
		return nil
	})
	if flushed != 1 {
		t.Fatalf("expected flushed=1 before error, got %d", flushed)
	}
}
