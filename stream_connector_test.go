package sdk

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/tractatus/envelope"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestStreamMessageToPublishRequestAudio(t *testing.T) {
	msg := engram.StreamMessage{
		Audio: &engram.AudioFrame{
			PCM:          []byte{0x00, 0x01},
			SampleRateHz: 16000,
			Channels:     1,
			Codec:        "pcm16",
			Timestamp:    10 * time.Millisecond,
		},
	}
	req, err := streamMessageToPublishRequest(msg)
	require.NoError(t, err)
	audio := req.GetAudio()
	require.NotNil(t, audio)
	require.Equal(t, uint64(10), audio.GetTimestampMs())
	require.Equal(t, int32(16000), audio.GetSampleRateHz())
	require.Equal(t, []byte{0x00, 0x01}, audio.GetPcm())
}

func TestPublishRequestToStreamMessageAudio(t *testing.T) {
	req := &transportpb.PublishRequest{
		Frame: &transportpb.PublishRequest_Audio{
			Audio: &transportpb.AudioFrame{
				Pcm:          []byte{0x02, 0x03},
				SampleRateHz: 8000,
				Channels:     1,
				Codec:        "g711",
				TimestampMs:  25,
			},
		},
	}
	msg, err := publishRequestToStreamMessage(req)
	require.NoError(t, err)
	require.NotNil(t, msg.Audio)
	require.Equal(t, []byte{0x02, 0x03}, msg.Audio.PCM)
	require.Equal(t, 25*time.Millisecond, msg.Audio.Timestamp)
}

func TestStreamMessageToPublishRequestEnvelope(t *testing.T) {
	msg := engram.StreamMessage{
		Kind:      "data",
		MessageID: "msg-1",
		Timestamp: time.UnixMilli(123).UTC(),
		Metadata:  map[string]string{"storyRun": "abc"},
		Payload:   []byte(`{"foo":"bar"}`),
		Inputs:    []byte(`{"baz":1}`),
		Transports: []engram.TransportDescriptor{
			{Name: "default", Kind: "live"},
		},
	}
	req, err := streamMessageToPublishRequest(msg)
	require.NoError(t, err)
	binary := req.GetBinary()
	require.NotNil(t, binary)
	require.Equal(t, envelope.MIMEType, binary.GetMimeType())
	env, err := envelope.FromBinaryFrame(binary)
	require.NoError(t, err)
	require.Equal(t, "data", env.Kind)
	require.Equal(t, "msg-1", env.MessageID)
	require.Equal(t, int64(123), env.TimestampMs)
	require.Equal(t, "abc", env.Metadata["storyRun"])
	require.Equal(t, []byte(`{"foo":"bar"}`), []byte(env.Payload))
	require.Equal(t, []byte(`{"baz":1}`), []byte(env.Inputs))
	require.Len(t, env.Transports, 1)
	require.Equal(t, "default", env.Transports[0].Name)
}

func TestStreamMessageToPublishRequestBinaryFrame(t *testing.T) {
	msg := engram.StreamMessage{
		Binary: &engram.BinaryFrame{
			Payload:   []byte{0x0A, 0x0B},
			MimeType:  "application/octet-stream",
			Timestamp: 42 * time.Millisecond,
		},
	}
	req, err := streamMessageToPublishRequest(msg)
	require.NoError(t, err)
	binary := req.GetBinary()
	require.NotNil(t, binary)
	require.Equal(t, []byte{0x0A, 0x0B}, binary.GetPayload())
	require.Equal(t, "application/octet-stream", binary.GetMimeType())
	require.Equal(t, uint64(42), binary.GetTimestampMs())
}

func TestPublishRequestToStreamMessageBinary(t *testing.T) {
	envFrame, err := envelope.ToBinaryFrame(&envelope.Envelope{
		Kind:        "telemetry",
		MessageID:   "env-42",
		TimestampMs: 900,
		Metadata:    map[string]string{"step": "test"},
		Payload:     []byte(`{"ok":true}`),
	})
	require.NoError(t, err)
	req := &transportpb.PublishRequest{
		Frame: &transportpb.PublishRequest_Binary{
			Binary: envFrame,
		},
	}
	msg, err := publishRequestToStreamMessage(req)
	require.NoError(t, err)
	require.Equal(t, "telemetry", msg.Kind)
	require.Equal(t, "env-42", msg.MessageID)
	require.Equal(t, time.UnixMilli(900).UTC(), msg.Timestamp)
	require.Equal(t, map[string]string{"step": "test"}, msg.Metadata)
	require.Equal(t, []byte(`{"ok":true}`), msg.Payload)
	require.Nil(t, msg.Binary)
}

func TestPublishRequestToStreamMessageBinaryPassthrough(t *testing.T) {
	req := &transportpb.PublishRequest{
		Frame: &transportpb.PublishRequest_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:     []byte("bin"),
				MimeType:    "text/plain",
				TimestampMs: 7,
			},
		},
	}
	msg, err := publishRequestToStreamMessage(req)
	require.NoError(t, err)
	require.Equal(t, []byte("bin"), msg.Payload)
	require.NotNil(t, msg.Binary)
	require.Equal(t, "text/plain", msg.Binary.MimeType)
	require.Equal(t, 7*time.Millisecond, msg.Binary.Timestamp)
}

func TestShouldSubscribeBinary(t *testing.T) {
	t.Run("nil info defaults to true", func(t *testing.T) {
		require.True(t, shouldSubscribeBinary(bindingReference{}))
	})
	t.Run("explicit binary types", func(t *testing.T) {
		ref := bindingReference{Info: &transportpb.BindingInfo{BinaryTypes: []string{"application/json"}}}
		require.True(t, shouldSubscribeBinary(ref))
	})
	t.Run("audio only does not subscribe binary", func(t *testing.T) {
		ref := bindingReference{Info: &transportpb.BindingInfo{AudioCodecs: []string{"pcm16"}}}
		require.False(t, shouldSubscribeBinary(ref))
	})
	t.Run("no media declarations defaults to true", func(t *testing.T) {
		ref := bindingReference{Info: &transportpb.BindingInfo{}}
		require.True(t, shouldSubscribeBinary(ref))
	})
}

func TestConnectorControlLoop_DefaultHandlerAck(t *testing.T) {
	srv := &stubConnectorServer{
		control: func(stream transportpb.TransportConnector_ControlServer) error {
			require.NoError(t, stream.Send(&transportpb.ControlDirective{Type: "start"}))
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, "ack", resp.GetType())
			require.Equal(t, "start", resp.GetMetadata()["type"])
			require.Equal(t, "false", resp.GetMetadata()["handled"])
			return nil
		},
	}
	client := newTestTransportConnectorClient(t, srv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := streamRuntimeOptions{messageTimeout: time.Second}
	require.NoError(t, connectorControlLoop(ctx, client, bindingReference{}, defaultControlDirectiveHandler{}, opts))
}

func TestConnectorControlLoop_DefaultHandlerCapabilities(t *testing.T) {
	srv := &stubConnectorServer{
		control: func(stream transportpb.TransportConnector_ControlServer) error {
			require.NoError(t, stream.Send(&transportpb.ControlDirective{
				Type: "connector.capabilities",
				Metadata: map[string]string{
					"audio.codec": "pcm16",
				},
			}))
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, "ack", resp.GetType())
			require.Equal(t, "connector.capabilities", resp.GetMetadata()["type"])
			require.Equal(t, "true", resp.GetMetadata()["handled"])
			require.Equal(t, "capabilities", resp.GetMetadata()["reason"])
			return nil
		},
	}
	client := newTestTransportConnectorClient(t, srv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := streamRuntimeOptions{messageTimeout: time.Second}
	require.NoError(t, connectorControlLoop(ctx, client, bindingReference{}, defaultControlDirectiveHandler{}, opts))
}

func TestConnectorControlLoop_CustomHandler(t *testing.T) {
	srv := &stubConnectorServer{
		control: func(stream transportpb.TransportConnector_ControlServer) error {
			require.NoError(t, stream.Send(&transportpb.ControlDirective{Type: "heartbeat"}))
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, "custom", resp.GetType())
			require.Equal(t, "true", resp.GetMetadata()["ok"])
			return nil
		},
	}
	client := newTestTransportConnectorClient(t, srv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := &recordingControlHandler{seen: make(chan engram.ControlDirective, 1)}
	opts := streamRuntimeOptions{messageTimeout: time.Second}
	require.NoError(t, connectorControlLoop(ctx, client, bindingReference{}, handler, opts))
	select {
	case directive := <-handler.seen:
		require.Equal(t, "heartbeat", directive.Type)
	case <-time.After(time.Second):
		t.Fatal("expected directive to reach handler")
	}
}

type recordingControlHandler struct {
	seen chan engram.ControlDirective
}

func (h *recordingControlHandler) HandleControlDirective(
	ctx context.Context,
	directive engram.ControlDirective,
) (*engram.ControlDirective, error) {
	if h.seen != nil {
		h.seen <- directive
	}
	return &engram.ControlDirective{
		Type:     "custom",
		Metadata: map[string]string{"ok": "true"},
	}, nil
}

type stubConnectorServer struct {
	transportpb.UnimplementedTransportConnectorServer
	control func(stream transportpb.TransportConnector_ControlServer) error
}

func (s *stubConnectorServer) Control(stream transportpb.TransportConnector_ControlServer) error {
	if s.control != nil {
		return s.control(stream)
	}
	return nil
}

func newTestTransportConnectorClient(
	t *testing.T,
	server transportpb.TransportConnectorServer,
) transportpb.TransportConnectorClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	transportpb.RegisterTransportConnectorServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.Dial()
	}
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, waitForReady(waitCtx, conn))
	t.Cleanup(func() {
		_ = conn.Close()
		grpcServer.Stop()
	})
	return transportpb.NewTransportConnectorClient(conn)
}
