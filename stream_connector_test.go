package sdk

import (
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/core/contracts"
	coretransport "github.com/bubustack/core/runtime/transport"
	transportconnector "github.com/bubustack/core/runtime/transport/connector"
	"github.com/bubustack/tractatus/envelope"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestStreamMessageToPublishRequestAudio(t *testing.T) {
	msg := engram.StreamMessage{
		Kind:      "speech",
		MessageID: "audio-1",
		Timestamp: time.UnixMilli(1234).UTC(),
		Metadata:  map[string]string{"storyRun": "sr-01"},
		Envelope: &transportpb.StreamEnvelope{
			StreamId:  "stream-1",
			Sequence:  42,
			Partition: "p0",
		},
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
	meta := req.GetMetadata()
	require.Equal(t, "sr-01", meta["storyRun"])
	require.Equal(t, "speech", meta[metadataEnvelopeKindKey])
	require.Equal(t, "audio-1", meta[metadataEnvelopeMessageIDKey])
	require.Equal(t, strconv.FormatInt(msg.Timestamp.UTC().UnixMilli(), 10), meta[metadataEnvelopeTimeKey])
	streamEnv := req.GetEnvelope()
	require.NotNil(t, streamEnv)
	require.Equal(t, "stream-1", streamEnv.GetStreamId())
	require.Equal(t, uint64(42), streamEnv.GetSequence())
	require.Equal(t, "p0", streamEnv.GetPartition())
}

func TestPublishRequestToStreamMessageAudio(t *testing.T) {
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	inputs, err := structpb.NewStruct(map[string]any{"baz": 1})
	require.NoError(t, err)
	req := &transportpb.PublishRequest{
		Metadata: map[string]string{
			metadataEnvelopeKindKey:      "speech",
			metadataEnvelopeMessageIDKey: "msg-123",
			metadataEnvelopeTimeKey:      strconv.FormatInt(25, 10),
			"storyRun":                   "sr-55",
		},
		Envelope: &transportpb.StreamEnvelope{
			StreamId:  "stream-2",
			Sequence:  7,
			Partition: "us-east-1",
		},
		Payload: payload,
		Inputs:  inputs,
		Transports: []*transportpb.TransportDescriptor{
			{Name: "livekit", Kind: "media", Mode: "bi"},
		},
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
	require.Equal(t, "speech", msg.Kind)
	require.Equal(t, "msg-123", msg.MessageID)
	require.Equal(t, time.UnixMilli(25).UTC(), msg.Timestamp)
	require.Equal(t, map[string]string{"storyRun": "sr-55"}, msg.Metadata)
	require.Equal(t, `{"foo":"bar"}`, string(msg.Payload))
	require.Equal(t, `{"baz":1}`, string(msg.Inputs))
	require.Len(t, msg.Transports, 1)
	require.Equal(t, "livekit", msg.Transports[0].Name)
	require.Equal(t, "media", msg.Transports[0].Kind)
	require.NotNil(t, msg.Envelope)
	require.Equal(t, "stream-2", msg.Envelope.StreamId)
	require.Equal(t, uint64(7), msg.Envelope.Sequence)
	require.Equal(t, "us-east-1", msg.Envelope.Partition)
}

func TestPublishRequestToStreamMessageRejectsAudioWithoutPCM(t *testing.T) {
	req := &transportpb.PublishRequest{
		Frame: &transportpb.PublishRequest_Audio{
			Audio: &transportpb.AudioFrame{
				SampleRateHz: 16000,
				Channels:     1,
			},
		},
	}

	msg, err := publishRequestToStreamMessage(req)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Equal(t, engram.StreamMessage{}, msg)
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

func TestStreamMessageEnvelopeClonesTypedTransportConfig(t *testing.T) {
	type nestedMap map[string]string
	type nestedSlice []map[string]int

	msg := engram.StreamMessage{
		Transports: []engram.TransportDescriptor{
			{
				Name: "primary",
				Kind: "livekit",
				Config: map[string]any{
					"labels": nestedMap{"room": "alpha"},
					"routes": nestedSlice{{"priority": 1}},
				},
			},
		},
	}

	env := streamMessageEnvelope(msg)
	require.NotNil(t, env)
	require.Len(t, env.Transports, 1)

	msg.Transports[0].Config["labels"].(nestedMap)["room"] = "beta"
	msg.Transports[0].Config["routes"].(nestedSlice)[0]["priority"] = 2

	labels, ok := env.Transports[0].Config["labels"].(nestedMap)
	require.True(t, ok)
	require.Equal(t, "alpha", labels["room"])

	routes, ok := env.Transports[0].Config["routes"].(nestedSlice)
	require.True(t, ok)
	require.Equal(t, 1, routes[0]["priority"])
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

func TestStreamMessageToPublishRequestBinaryFrameMirroredPayloadBypassesEnvelope(t *testing.T) {
	msg := engram.StreamMessage{
		Payload: []byte{0x0A, 0x0B},
		Binary: &engram.BinaryFrame{
			Payload:   []byte{0x0A, 0x0B},
			MimeType:  "application/custom-binary",
			Timestamp: 7 * time.Millisecond,
		},
	}

	req, err := streamMessageToPublishRequest(msg)
	require.NoError(t, err)
	binary := req.GetBinary()
	require.NotNil(t, binary)
	require.Equal(t, []byte{0x0A, 0x0B}, binary.GetPayload())
	require.Equal(t, "application/custom-binary", binary.GetMimeType())
	require.Equal(t, uint64(7), binary.GetTimestampMs())
}

func TestStreamMessageToPublishRequestRejectsMultipleFrameTypes(t *testing.T) {
	msg := engram.StreamMessage{
		Audio: &engram.AudioFrame{PCM: []byte{0x01}},
		Video: &engram.VideoFrame{Payload: []byte{0x02}, Codec: "vp8"},
	}

	req, err := streamMessageToPublishRequest(msg)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Nil(t, req)
}

func TestStreamMessageToPublishRequestRejectsBinaryPayloadMismatch(t *testing.T) {
	msg := engram.StreamMessage{
		Payload: []byte(`{"ok":true}`),
		Binary: &engram.BinaryFrame{
			Payload:  []byte("raw"),
			MimeType: "application/octet-stream",
		},
	}

	req, err := streamMessageToPublishRequest(msg)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Nil(t, req)
}

func TestStreamMessageToPublishRequestRejectsErrorKindWithoutPayload(t *testing.T) {
	msg := engram.StreamMessage{Kind: engram.StreamMessageKindError}

	req, err := streamMessageToPublishRequest(msg)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Nil(t, req)
}

func TestStreamMessageToPublishRequestRejectsMessageIDWithSurroundingWhitespace(t *testing.T) {
	msg := engram.StreamMessage{
		MessageID: " msg-1 ",
		Payload:   []byte(`{"ok":true}`),
	}

	req, err := streamMessageToPublishRequest(msg)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Nil(t, req)
}

func TestStreamMessageToPublishRequestRejectsEmptyMetadataKey(t *testing.T) {
	msg := engram.StreamMessage{
		Payload:  []byte(`{"ok":true}`),
		Metadata: map[string]string{"": "value"},
	}

	req, err := streamMessageToPublishRequest(msg)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Nil(t, req)
}

func TestStreamMessageToPublishRequestRejectsHeartbeatWithPayload(t *testing.T) {
	msg := engram.StreamMessage{
		Kind:    engram.StreamMessageKindHeartbeat,
		Payload: []byte(`{"ok":true}`),
	}

	req, err := streamMessageToPublishRequest(msg)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Nil(t, req)
}

func TestStreamMessageToPublishRequestRejectsAudioWithoutPCM(t *testing.T) {
	msg := engram.StreamMessage{
		Audio: &engram.AudioFrame{
			SampleRateHz: 16000,
			Channels:     1,
		},
	}

	req, err := streamMessageToPublishRequest(msg)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Nil(t, req)
}

func TestStreamMessageToPublishRequestRejectsEncodedVideoWithoutCodec(t *testing.T) {
	msg := engram.StreamMessage{
		Video: &engram.VideoFrame{
			Payload: []byte{0x02},
		},
	}

	req, err := streamMessageToPublishRequest(msg)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Nil(t, req)
}

func TestStreamMessageToPublishRequestRejectsRawVideoWithoutDimensions(t *testing.T) {
	msg := engram.StreamMessage{
		Video: &engram.VideoFrame{
			Payload: []byte{0x02},
			Raw:     true,
		},
	}

	req, err := streamMessageToPublishRequest(msg)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Nil(t, req)
}

func TestStreamMessageToPublishRequestRejectsReservedEnvelopeMimeWithoutEnvelopeFields(t *testing.T) {
	msg := engram.StreamMessage{
		Binary: &engram.BinaryFrame{
			Payload:  []byte("raw"),
			MimeType: envelope.MIMEType,
		},
	}

	req, err := streamMessageToPublishRequest(msg)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Nil(t, req)
}

func TestStreamMessageToPublishRequestRejectsReservedEnvelopeMimeWithParametersWithoutEnvelopeFields(t *testing.T) {
	msg := engram.StreamMessage{
		Binary: &engram.BinaryFrame{
			Payload:  []byte(`{"ok":true}`),
			MimeType: envelope.MIMEType + "; charset=utf-8",
		},
	}

	req, err := streamMessageToPublishRequest(msg)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Nil(t, req)
}

func TestStreamMessageToPublishRequestRejectsInvalidBinaryMimeType(t *testing.T) {
	msg := engram.StreamMessage{
		Binary: &engram.BinaryFrame{
			Payload:  []byte("raw"),
			MimeType: "not a mime type",
		},
	}

	req, err := streamMessageToPublishRequest(msg)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Nil(t, req)
}

func TestStreamMessageToPublishRequestRejectsReservedEnvelopeMimePayloadMismatch(t *testing.T) {
	msg := engram.StreamMessage{
		Kind: "telemetry",
		Binary: &engram.BinaryFrame{
			Payload:  []byte("raw"),
			MimeType: envelope.MIMEType,
		},
	}

	req, err := streamMessageToPublishRequest(msg)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Nil(t, req)
}

func TestStreamMessageToPublishRequestRejectsReservedEnvelopeMimeCaseInsensitivePayloadMismatch(t *testing.T) {
	msg := engram.StreamMessage{
		Kind:    "telemetry",
		Payload: []byte(`{"ok":true}`),
		Binary: &engram.BinaryFrame{
			Payload:  []byte(`{"ok":false}`),
			MimeType: strings.ToUpper(envelope.MIMEType),
		},
	}

	req, err := streamMessageToPublishRequest(msg)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Nil(t, req)
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
	require.NotNil(t, msg.Binary)
	require.Equal(t, []byte(`{"ok":true}`), msg.Binary.Payload)
	require.Equal(t, defaultEnvelopePayloadMIME, msg.Binary.MimeType)
	require.Equal(t, 900*time.Millisecond, msg.Binary.Timestamp)
	msg.Payload[0] = 'X'
	require.Equal(t, byte('X'), msg.Binary.Payload[0])
}

func TestPopulateMessageFromEnvelopeClonesTypedTransportConfig(t *testing.T) {
	type nestedMap map[string]string
	type nestedSlice []map[string]int

	env := &envelope.Envelope{
		Transports: []envelope.TransportDescriptor{
			{
				Name: "primary",
				Kind: "livekit",
				Config: map[string]any{
					"labels": nestedMap{"room": "alpha"},
					"routes": nestedSlice{{"priority": 1}},
				},
			},
		},
	}

	var msg engram.StreamMessage
	populateMessageFromEnvelope(&msg, env)
	require.Len(t, msg.Transports, 1)

	env.Transports[0].Config["labels"].(nestedMap)["room"] = "beta"
	env.Transports[0].Config["routes"].(nestedSlice)[0]["priority"] = 2

	labels, ok := msg.Transports[0].Config["labels"].(nestedMap)
	require.True(t, ok)
	require.Equal(t, "alpha", labels["room"])

	routes, ok := msg.Transports[0].Config["routes"].(nestedSlice)
	require.True(t, ok)
	require.Equal(t, 1, routes[0]["priority"])
}

func TestPublishRequestToStreamMessageRejectsEncodedVideoWithoutCodec(t *testing.T) {
	req := &transportpb.PublishRequest{
		Frame: &transportpb.PublishRequest_Video{
			Video: &transportpb.VideoFrame{
				Payload: []byte{0x02},
			},
		},
	}

	msg, err := publishRequestToStreamMessage(req)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Equal(t, engram.StreamMessage{}, msg)
}

func TestPublishRequestToStreamMessageRejectsErrorEnvelopeWithoutPayload(t *testing.T) {
	envFrame, err := envelope.ToBinaryFrame(&envelope.Envelope{
		Kind: engram.StreamMessageKindError,
	})
	require.NoError(t, err)

	req := &transportpb.PublishRequest{
		Frame: &transportpb.PublishRequest_Binary{
			Binary: envFrame,
		},
	}

	msg, err := publishRequestToStreamMessage(req)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Equal(t, engram.StreamMessage{}, msg)
}

func TestPublishRequestToStreamMessageDecodesReservedEnvelopeMimeWithParameters(t *testing.T) {
	envFrame, err := envelope.ToBinaryFrame(&envelope.Envelope{
		Kind:      "telemetry",
		MessageID: "env-params",
		Payload:   []byte(`{"ok":true}`),
	})
	require.NoError(t, err)
	envFrame.MimeType = envelope.MIMEType + "; charset=utf-8"

	req := &transportpb.PublishRequest{
		Frame: &transportpb.PublishRequest_Binary{
			Binary: envFrame,
		},
	}

	msg, err := publishRequestToStreamMessage(req)
	require.NoError(t, err)
	require.Equal(t, "telemetry", msg.Kind)
	require.Equal(t, "env-params", msg.MessageID)
	require.Equal(t, []byte(`{"ok":true}`), msg.Payload)
}

func TestPublishRequestToStreamMessageDecodesReservedEnvelopeMimeCaseInsensitive(t *testing.T) {
	envFrame, err := envelope.ToBinaryFrame(&envelope.Envelope{
		Kind:      "telemetry",
		MessageID: "env-upper",
		Payload:   []byte(`{"ok":true}`),
	})
	require.NoError(t, err)
	envFrame.MimeType = strings.ToUpper(envelope.MIMEType)

	req := &transportpb.PublishRequest{
		Frame: &transportpb.PublishRequest_Binary{
			Binary: envFrame,
		},
	}

	msg, err := publishRequestToStreamMessage(req)
	require.NoError(t, err)
	require.Equal(t, "telemetry", msg.Kind)
	require.Equal(t, "env-upper", msg.MessageID)
	require.Equal(t, []byte(`{"ok":true}`), msg.Payload)
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

func TestPublishRequestToStreamMessageBinaryDefaultsEmptyMimeType(t *testing.T) {
	req := &transportpb.PublishRequest{
		Frame: &transportpb.PublishRequest_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:     []byte("bin"),
				TimestampMs: 7,
			},
		},
	}
	msg, err := publishRequestToStreamMessage(req)
	require.NoError(t, err)
	require.NotNil(t, msg.Binary)
	require.Equal(t, "application/octet-stream", msg.Binary.MimeType)
}

func TestPublishRequestToStreamMessageRejectsInvalidBinaryMimeType(t *testing.T) {
	req := &transportpb.PublishRequest{
		Frame: &transportpb.PublishRequest_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("bin"),
				MimeType: "not a mime type",
			},
		},
	}
	msg, err := publishRequestToStreamMessage(req)
	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Equal(t, engram.StreamMessage{}, msg)
}

func TestPublishRequestToStreamMessageBinaryPassthroughRoundTrip(t *testing.T) {
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

	roundTrip, err := streamMessageToPublishRequest(msg)
	require.NoError(t, err)
	require.NotNil(t, roundTrip.GetBinary())
	require.Equal(t, []byte("bin"), roundTrip.GetBinary().GetPayload())
	require.Equal(t, "text/plain", roundTrip.GetBinary().GetMimeType())
	require.Nil(t, roundTrip.GetPayload())
	require.Nil(t, roundTrip.GetInputs())
}

func TestPublishRequestToStreamMessageMetadataFallback(t *testing.T) {
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	req := &transportpb.PublishRequest{
		Metadata: map[string]string{
			metadataEnvelopeKindKey:      "data",
			metadataEnvelopeMessageIDKey: "xyz",
			metadataEnvelopeTimeKey:      strconv.FormatInt(123, 10),
			"storyRun":                   "sr-2",
		},
		Payload: payload,
		Frame: &transportpb.PublishRequest_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("bin"),
				MimeType: "text/plain",
			},
		},
	}
	msg, err := publishRequestToStreamMessage(req)
	require.NoError(t, err)
	require.Equal(t, "data", msg.Kind)
	require.Equal(t, "xyz", msg.MessageID)
	require.Equal(t, time.UnixMilli(123).UTC(), msg.Timestamp)
	require.Equal(t, map[string]string{"storyRun": "sr-2"}, msg.Metadata)
	require.Equal(t, []byte("bin"), msg.Payload)
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

func TestShouldDropSubscribeMessage(t *testing.T) {
	t.Run("drops metadata heartbeat", func(t *testing.T) {
		drop, reason := shouldDropSubscribeMessage(engram.StreamMessage{
			Metadata: map[string]string{"bubu-heartbeat": "true"},
		})
		require.True(t, drop)
		require.Equal(t, "heartbeat", reason)
	})

	t.Run("drops kind heartbeat", func(t *testing.T) {
		drop, reason := shouldDropSubscribeMessage(engram.StreamMessage{Kind: "heartbeat"})
		require.True(t, drop)
		require.Equal(t, "heartbeat", reason)
	})

	t.Run("drops noop", func(t *testing.T) {
		drop, reason := shouldDropSubscribeMessage(engram.StreamMessage{Kind: "noop"})
		require.True(t, drop)
		require.Equal(t, "noop", reason)
	})

	t.Run("drops fully empty packet", func(t *testing.T) {
		drop, reason := shouldDropSubscribeMessage(engram.StreamMessage{})
		require.True(t, drop)
		require.Equal(t, "empty", reason)
	})

	t.Run("keeps audio packet", func(t *testing.T) {
		drop, reason := shouldDropSubscribeMessage(engram.StreamMessage{
			Audio: &engram.AudioFrame{PCM: []byte{0x01}},
		})
		require.False(t, drop)
		require.Equal(t, "", reason)
	})

	t.Run("keeps payload packet", func(t *testing.T) {
		drop, reason := shouldDropSubscribeMessage(engram.StreamMessage{
			Payload: []byte(`{"hook":{"event":"storyrun.ready"}}`),
		})
		require.False(t, drop)
		require.Equal(t, "", reason)
	})
}

func TestConnectorControlLoop_DefaultHandlerAck(t *testing.T) {
	srv := &stubConnectorServer{
		control: func(stream transportpb.TransportConnectorService_ControlServer) error {
			require.NoError(t, stream.Send(&transportpb.ControlResponse{
				Action: transportpb.ControlAction_CONTROL_ACTION_START,
			}))
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, transportpb.ControlAction_CONTROL_ACTION_ACK, resp.GetAction())
			require.Equal(t, "start", resp.GetMetadata()["type"])
			require.Equal(t, "false", resp.GetMetadata()["handled"])
			return nil
		},
	}
	client := newTestTransportConnectorClient(t, srv)
	ctx := t.Context()
	opts := streamRuntimeOptions{messageTimeout: time.Second}
	err := connectorControlLoop(ctx, client, bindingReference{}, defaultControlDirectiveHandler{}, opts)
	require.ErrorIs(t, err, io.EOF)
}

func TestConnectorControlLoop_DefaultHandlerCapabilities(t *testing.T) {
	srv := &stubConnectorServer{
		control: func(stream transportpb.TransportConnectorService_ControlServer) error {
			require.NoError(t, stream.Send(&transportpb.ControlResponse{
				Action: transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_CAPABILITIES,
				Metadata: map[string]string{
					"audio.codec": "pcm16",
				},
			}))
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, transportpb.ControlAction_CONTROL_ACTION_ACK, resp.GetAction())
			require.Equal(t, "connector.capabilities", resp.GetMetadata()["type"])
			require.Equal(t, "true", resp.GetMetadata()["handled"])
			require.Equal(t, "capabilities", resp.GetMetadata()["reason"])
			return nil
		},
	}
	client := newTestTransportConnectorClient(t, srv)
	ctx := t.Context()
	opts := streamRuntimeOptions{messageTimeout: time.Second}
	err := connectorControlLoop(ctx, client, bindingReference{}, defaultControlDirectiveHandler{}, opts)
	require.ErrorIs(t, err, io.EOF)
}

func TestConnectorControlLoop_CustomHandler(t *testing.T) {
	srv := &stubConnectorServer{
		control: func(stream transportpb.TransportConnectorService_ControlServer) error {
			require.NoError(t, stream.Send(&transportpb.ControlResponse{
				Action: transportpb.ControlAction_CONTROL_ACTION_HEARTBEAT,
			}))
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, transportpb.ControlAction_CONTROL_ACTION_UNSPECIFIED, resp.GetAction())
			require.Equal(t, "custom", resp.GetCustomAction())
			require.Equal(t, "true", resp.GetMetadata()["ok"])
			return nil
		},
	}
	client := newTestTransportConnectorClient(t, srv)
	ctx := t.Context()
	handler := &recordingControlHandler{seen: make(chan engram.ControlDirective, 1)}
	opts := streamRuntimeOptions{messageTimeout: time.Second}
	err := connectorControlLoop(ctx, client, bindingReference{}, handler, opts)
	require.ErrorIs(t, err, io.EOF)
	select {
	case directive := <-handler.seen:
		require.Equal(t, "heartbeat", directive.Type)
	case <-time.After(time.Second):
		t.Fatal("expected directive to reach handler")
	}
}

func TestConnectorControlLoop_ClosedControlRequestsChannelDoesNotBusyLoop(t *testing.T) {
	srv := &stubConnectorServer{
		control: func(stream transportpb.TransportConnectorService_ControlServer) error {
			require.NoError(t, stream.Send(&transportpb.ControlResponse{
				Action: transportpb.ControlAction_CONTROL_ACTION_NOOP,
			}))
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, transportpb.ControlAction_CONTROL_ACTION_ACK, resp.GetAction())
			return nil
		},
	}
	client := newTestTransportConnectorClient(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	controlRequests := make(chan *transportpb.ControlRequest)
	close(controlRequests)
	opts := streamRuntimeOptions{
		messageTimeout:  time.Second,
		controlRequests: controlRequests,
	}

	done := make(chan error, 1)
	go func() {
		done <- connectorControlLoop(ctx, client, bindingReference{}, defaultControlDirectiveHandler{}, opts)
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected connectorControlLoop to continue processing when control request channel is closed")
	}
}

func TestConnectorControlLoop_SendsBindingReferenceMetadataOnly(t *testing.T) {
	srv := &stubConnectorServer{
		control: func(stream transportpb.TransportConnectorService_ControlServer) error {
			md, ok := metadata.FromIncomingContext(stream.Context())
			require.True(t, ok)
			require.Equal(t, []string{coretransport.ProtocolVersion}, md.Get(coretransport.ProtocolMetadataKey))
			require.Equal(t, []string{"runtime-ns/binding-a"}, md.Get(controlBindingMetadataKey))
			require.NotContains(t, md.Get(controlBindingMetadataKey)[0], "{")
			require.NoError(t, stream.Send(&transportpb.ControlResponse{
				Action: transportpb.ControlAction_CONTROL_ACTION_NOOP,
			}))
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, transportpb.ControlAction_CONTROL_ACTION_ACK, resp.GetAction())
			return nil
		},
	}
	client := newTestTransportConnectorClient(t, srv)
	ctx := t.Context()
	opts := streamRuntimeOptions{messageTimeout: time.Second}
	ref := bindingReference{
		Name:      "binding-a",
		Namespace: "runtime-ns",
		Raw:       `{"binding":{"driver":"demo","endpoint":"secret:9000"}}`,
	}
	err := connectorControlLoop(ctx, client, ref, defaultControlDirectiveHandler{}, opts)
	require.ErrorIs(t, err, io.EOF)
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

type startupGatedStreamingEngram struct {
	started chan struct{}
}

func (startupGatedStreamingEngram) Init(context.Context, struct{}, *engram.Secrets) error { return nil }

func (e startupGatedStreamingEngram) Stream(ctx context.Context, _ <-chan engram.InboundMessage, _ chan<- engram.StreamMessage) error { //nolint:lll
	if e.started != nil {
		close(e.started)
	}
	<-ctx.Done()
	return ctx.Err()
}

type stubConnectorServer struct {
	transportpb.UnimplementedTransportConnectorServiceServer
	data    func(stream transportpb.TransportConnectorService_DataServer) error
	control func(stream transportpb.TransportConnectorService_ControlServer) error
}

func (s *stubConnectorServer) Data(stream transportpb.TransportConnectorService_DataServer) error {
	if s.data != nil {
		return s.data(stream)
	}
	return nil
}

func (s *stubConnectorServer) Control(stream transportpb.TransportConnectorService_ControlServer) error {
	if s.control != nil {
		return s.control(stream)
	}
	return nil
}

func newTestTransportConnectorClient(
	t *testing.T,
	server transportpb.TransportConnectorServiceServer,
) transportpb.TransportConnectorServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	transportpb.RegisterTransportConnectorServiceServer(grpcServer, server)
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
	require.NoError(t, transportconnector.WaitForReady(waitCtx, conn))
	t.Cleanup(func() {
		_ = conn.Close()
		grpcServer.Stop()
	})
	return transportpb.NewTransportConnectorServiceClient(conn)
}

func TestRunTransportSessionFailsWhenConnectorReadyMissingStartupCapabilitiesMetadata(t *testing.T) {
	prevDial := connectorDial
	t.Cleanup(func() {
		connectorDial = prevDial
	})

	allowReady := make(chan struct{})
	recvErrCh := make(chan error, 1)
	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			<-stream.Context().Done()
			return nil
		},
		control: func(stream transportpb.TransportConnectorService_ControlServer) error {
			<-allowReady
			require.NoError(t, stream.Send(&transportpb.ControlResponse{
				Action: transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_READY,
			}))
			_, err := stream.Recv()
			recvErrCh <- err
			<-stream.Context().Done()
			return nil
		},
	}
	client := newTestTransportConnectorClient(t, srv)
	connectorDial = func(context.Context, string, envResolver, ...grpc.DialOption) (*TransportConnectorClient, error) {
		return &TransportConnectorClient{client: client}, nil
	}

	started := make(chan struct{})
	ctx := t.Context()

	errCh := make(chan error, 1)
	go func() {
		errCh <- runTransportSession(
			ctx,
			"connector:9000",
			bindingReference{},
			startupGatedStreamingEngram{started: started},
			newEnvResolver(map[string]string{
				contracts.GRPCMessageTimeoutEnv: "250ms",
			}),
		)
	}()

	select {
	case <-started:
		t.Fatal("expected Engram.Stream not to start before connector.ready")
	case <-time.After(100 * time.Millisecond):
	}

	close(allowReady)

	select {
	case err := <-errCh:
		require.Error(t, err)
		require.ErrorContains(t, err, "invalid connector.ready startup metadata")
		require.ErrorContains(t, err, coretransport.StartupCapabilitiesMetadataKey)
	case <-time.After(time.Second):
		t.Fatal("expected runTransportSession to fail when startup capability metadata is missing")
	}

	select {
	case <-started:
		t.Fatal("expected Engram.Stream not to start when startup capability metadata is missing")
	default:
	}

	select {
	case recvErr := <-recvErrCh:
		require.Error(t, recvErr)
		require.ErrorContains(t, recvErr, "context canceled")
	case <-time.After(time.Second):
		t.Fatal("expected control stream recv to unblock after startup failure")
	}
}

func TestRunTransportSessionFailsWhenConnectorReadyStartupCapabilitiesMetadataInvalid(t *testing.T) {
	prevDial := connectorDial
	t.Cleanup(func() {
		connectorDial = prevDial
	})

	recvErrCh := make(chan error, 1)
	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			<-stream.Context().Done()
			return nil
		},
		control: func(stream transportpb.TransportConnectorService_ControlServer) error {
			require.NoError(t, stream.Send(&transportpb.ControlResponse{
				Action: transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_READY,
				Metadata: map[string]string{
					coretransport.StartupCapabilitiesMetadataKey: "legacy",
				},
			}))
			_, err := stream.Recv()
			recvErrCh <- err
			<-stream.Context().Done()
			return nil
		},
	}
	client := newTestTransportConnectorClient(t, srv)
	connectorDial = func(context.Context, string, envResolver, ...grpc.DialOption) (*TransportConnectorClient, error) {
		return &TransportConnectorClient{client: client}, nil
	}

	started := make(chan struct{})
	err := runTransportSession(
		context.Background(),
		"connector:9000",
		bindingReference{},
		startupGatedStreamingEngram{started: started},
		newEnvResolver(nil),
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid connector.ready startup metadata")
	require.ErrorContains(t, err, "invalid "+coretransport.StartupCapabilitiesMetadataKey)
	select {
	case <-started:
		t.Fatal("expected Engram.Stream not to start when startup capability metadata is invalid")
	default:
	}
	select {
	case recvErr := <-recvErrCh:
		require.Error(t, recvErr)
		require.ErrorContains(t, recvErr, "context canceled")
	case <-time.After(time.Second):
		t.Fatal("expected control stream recv to unblock after startup failure")
	}
}

func TestRunTransportSessionFailsWhenConnectorReadyHandshakeMissing(t *testing.T) {
	prevDial := connectorDial
	t.Cleanup(func() {
		connectorDial = prevDial
	})

	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			<-stream.Context().Done()
			return nil
		},
		control: func(stream transportpb.TransportConnectorService_ControlServer) error {
			<-stream.Context().Done()
			return nil
		},
	}
	client := newTestTransportConnectorClient(t, srv)
	connectorDial = func(context.Context, string, envResolver, ...grpc.DialOption) (*TransportConnectorClient, error) {
		return &TransportConnectorClient{client: client}, nil
	}

	started := make(chan struct{})
	err := runTransportSession(
		context.Background(),
		"connector:9000",
		bindingReference{},
		startupGatedStreamingEngram{started: started},
		newEnvResolver(map[string]string{
			contracts.GRPCMessageTimeoutEnv: "15ms",
		}),
	)
	require.ErrorIs(t, err, errControlStartupHandshakeTimeout)
	select {
	case <-started:
		t.Fatal("expected Engram.Stream not to start when connector.ready handshake is missing")
	default:
	}
}

func TestRunTransportSessionWaitsForStartupCapabilitiesWhenReadyRequiresThem(t *testing.T) {
	prevDial := connectorDial
	t.Cleanup(func() {
		connectorDial = prevDial
	})

	allowCapabilities := make(chan struct{})
	readyAcked := make(chan struct{})
	capabilitiesAcked := make(chan struct{})
	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			<-stream.Context().Done()
			return nil
		},
		control: func(stream transportpb.TransportConnectorService_ControlServer) error {
			require.NoError(t, stream.Send(&transportpb.ControlResponse{
				Action: transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_READY,
				Metadata: map[string]string{
					coretransport.StartupCapabilitiesMetadataKey: coretransport.StartupCapabilitiesRequired,
				},
			}))
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, transportpb.ControlAction_CONTROL_ACTION_ACK, resp.GetAction())
			require.Equal(t, "connector.ready", resp.GetMetadata()["type"])
			close(readyAcked)

			<-allowCapabilities

			require.NoError(t, stream.Send(&transportpb.ControlResponse{
				Action: transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_CAPABILITIES,
				Metadata: map[string]string{
					"audio.codec": "pcm16",
				},
			}))
			resp, err = stream.Recv()
			require.NoError(t, err)
			require.Equal(t, transportpb.ControlAction_CONTROL_ACTION_ACK, resp.GetAction())
			require.Equal(t, "connector.capabilities", resp.GetMetadata()["type"])
			close(capabilitiesAcked)

			<-stream.Context().Done()
			return nil
		},
	}
	client := newTestTransportConnectorClient(t, srv)
	connectorDial = func(context.Context, string, envResolver, ...grpc.DialOption) (*TransportConnectorClient, error) {
		return &TransportConnectorClient{client: client}, nil
	}

	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- runTransportSession(
			ctx,
			"connector:9000",
			bindingReference{},
			startupGatedStreamingEngram{started: started},
			newEnvResolver(nil),
		)
	}()

	select {
	case <-readyAcked:
	case <-time.After(time.Second):
		t.Fatal("expected connector.ready acknowledgement")
	}

	select {
	case <-started:
		t.Fatal("expected Engram.Stream not to start before required startup capabilities arrive")
	case <-time.After(100 * time.Millisecond):
	}

	close(allowCapabilities)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected Engram.Stream to start after startup capabilities arrive")
	}

	select {
	case <-capabilitiesAcked:
	case <-time.After(time.Second):
		t.Fatal("expected connector.capabilities acknowledgement")
	}

	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
		require.ErrorContains(t, err, "context canceled")
	case <-time.After(time.Second):
		t.Fatal("expected runTransportSession to exit after cancellation")
	}
}

func TestRunTransportSessionFailsWhenRequiredStartupCapabilitiesMissing(t *testing.T) {
	prevDial := connectorDial
	t.Cleanup(func() {
		connectorDial = prevDial
	})

	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			<-stream.Context().Done()
			return nil
		},
		control: func(stream transportpb.TransportConnectorService_ControlServer) error {
			require.NoError(t, stream.Send(&transportpb.ControlResponse{
				Action: transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_READY,
				Metadata: map[string]string{
					coretransport.StartupCapabilitiesMetadataKey: coretransport.StartupCapabilitiesRequired,
				},
			}))
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, transportpb.ControlAction_CONTROL_ACTION_ACK, resp.GetAction())
			require.Equal(t, "connector.ready", resp.GetMetadata()["type"])
			<-stream.Context().Done()
			return nil
		},
	}
	client := newTestTransportConnectorClient(t, srv)
	connectorDial = func(context.Context, string, envResolver, ...grpc.DialOption) (*TransportConnectorClient, error) {
		return &TransportConnectorClient{client: client}, nil
	}

	started := make(chan struct{})
	err := runTransportSession(
		context.Background(),
		"connector:9000",
		bindingReference{},
		startupGatedStreamingEngram{started: started},
		newEnvResolver(map[string]string{
			contracts.GRPCMessageTimeoutEnv: "15ms",
		}),
	)
	require.ErrorIs(t, err, errControlStartupHandshakeTimeout)
	select {
	case <-started:
		t.Fatal("expected Engram.Stream not to start when required startup capabilities are missing")
	default:
	}
}

func TestConnectorDataRecvLoop_DeduplicatesByEnvelopeSequence(t *testing.T) {
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)

	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			resp := &transportpb.DataResponse{
				Metadata: map[string]string{
					metadataEnvelopeKindKey: "data",
				},
				Envelope: &transportpb.StreamEnvelope{
					StreamId:  "downstream-step",
					Sequence:  9,
					Partition: "p0",
				},
				Payload: payload,
				Frame: &transportpb.DataResponse_Binary{
					Binary: &transportpb.BinaryFrame{
						Payload:  []byte("bin"),
						MimeType: "text/plain",
					},
				},
			}
			require.NoError(t, stream.Send(resp))
			require.NoError(t, stream.Send(resp))
			return nil
		},
	}

	client := newTestTransportConnectorClient(t, srv)
	ctx := t.Context()
	stream, err := client.Data(ctx)
	require.NoError(t, err)

	in := make(chan engram.InboundMessage, 4)
	require.NoError(t, connectorDataRecvLoop(ctx, stream, in, streamRuntimeOptions{}))
	require.Len(t, in, 1)

	msg := <-in
	require.NotNil(t, msg.Envelope)
	require.Equal(t, "downstream-step", msg.Envelope.GetStreamId())
	require.Equal(t, uint64(9), msg.Envelope.GetSequence())
	require.Equal(t, "p0", msg.Envelope.GetPartition())
}

func TestConnectorDataRecvLoop_PendingDeduperCapacityExceededReturnsOverflow(t *testing.T) {
	mkResp := func(seq uint64) *transportpb.DataResponse {
		payload, err := structpb.NewStruct(map[string]any{"seq": seq})
		require.NoError(t, err)
		return &transportpb.DataResponse{
			Metadata: map[string]string{
				metadataEnvelopeKindKey: "data",
			},
			Envelope: &transportpb.StreamEnvelope{
				StreamId:  "downstream-step",
				Sequence:  seq,
				Partition: "p0",
			},
			Payload: payload,
			Frame: &transportpb.DataResponse_Binary{
				Binary: &transportpb.BinaryFrame{
					Payload:  []byte("bin"),
					MimeType: "text/plain",
				},
			},
		}
	}

	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			for _, seq := range []uint64{1, 2, 3} {
				require.NoError(t, stream.Send(mkResp(seq)))
			}
			return nil
		},
	}

	client := newTestTransportConnectorClient(t, srv)
	ctx := t.Context()
	stream, err := client.Data(ctx)
	require.NoError(t, err)

	in := make(chan engram.InboundMessage, 8)
	err = connectorDataRecvLoop(ctx, stream, in, streamRuntimeOptions{
		packetDeduper: newPacketDeduper(2),
	})
	require.ErrorIs(t, err, errPacketDeduperPendingOverflow)
	require.Len(t, in, 2)
}

func TestConnectorDataRecvLoop_ChannelSendTimeoutDropDoesNotSuppressLaterDuplicateInSameSession(t *testing.T) {
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)

	packet := &transportpb.DataResponse{
		Metadata: map[string]string{
			metadataEnvelopeKindKey: "data",
		},
		Envelope: &transportpb.StreamEnvelope{
			StreamId:  "downstream-step",
			Sequence:  9,
			Partition: "p0",
		},
		Payload: payload,
		Frame: &transportpb.DataResponse_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("bin"),
				MimeType: "text/plain",
			},
		},
	}

	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			require.NoError(t, stream.Send(packet))
			time.Sleep(25 * time.Millisecond)
			require.NoError(t, stream.Send(packet))
			return nil
		},
	}

	client := newTestTransportConnectorClient(t, srv)
	ctx := t.Context()
	stream, err := client.Data(ctx)
	require.NoError(t, err)

	in := make(chan engram.InboundMessage)
	delivered := make(chan engram.InboundMessage, 1)
	go func() {
		time.Sleep(15 * time.Millisecond)
		delivered <- <-in
	}()

	require.NoError(t, connectorDataRecvLoop(ctx, stream, in, streamRuntimeOptions{
		channelSendTimeout: 10 * time.Millisecond,
	}))

	select {
	case msg := <-delivered:
		require.NotNil(t, msg.Envelope)
		require.Equal(t, "downstream-step", msg.Envelope.GetStreamId())
		require.Equal(t, uint64(9), msg.Envelope.GetSequence())
		require.Equal(t, "p0", msg.Envelope.GetPartition())
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected duplicate packet replay to be delivered after initial timeout-drop")
	}
}

func TestConnectorDataRecvLoop_KeepsPartitionScopedSequencesDistinct(t *testing.T) {
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)

	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			for _, partition := range []string{"p0", "p1"} {
				require.NoError(t, stream.Send(&transportpb.DataResponse{
					Metadata: map[string]string{
						metadataEnvelopeKindKey: "data",
					},
					Envelope: &transportpb.StreamEnvelope{
						StreamId:  "downstream-step",
						Sequence:  7,
						Partition: partition,
					},
					Payload: payload,
					Frame: &transportpb.DataResponse_Binary{
						Binary: &transportpb.BinaryFrame{
							Payload:  []byte(partition),
							MimeType: "text/plain",
						},
					},
				}))
			}
			return nil
		},
	}

	client := newTestTransportConnectorClient(t, srv)
	ctx := t.Context()
	stream, err := client.Data(ctx)
	require.NoError(t, err)

	in := make(chan engram.InboundMessage, 4)
	require.NoError(t, connectorDataRecvLoop(ctx, stream, in, streamRuntimeOptions{}))
	require.Len(t, in, 2)

	first := <-in
	second := <-in
	require.NotNil(t, first.Envelope)
	require.NotNil(t, second.Envelope)
	require.Equal(t, uint64(7), first.Envelope.GetSequence())
	require.Equal(t, uint64(7), second.Envelope.GetSequence())
	require.NotEqual(t, first.Envelope.GetPartition(), second.Envelope.GetPartition())
}

func TestConnectorDataRecvLoop_DeduplicatesHookMessageIDWithoutSequence(t *testing.T) {
	payload := &envelope.Envelope{
		Version:   envelope.LatestVersion,
		Kind:      envelope.KindHook,
		MessageID: "ns/storyrun:storyrun.ready",
		Payload:   []byte(`{"type":"storyrun.ready","hook":{"event":"storyrun.ready"}}`),
		Inputs:    []byte(`{"userPrompt":"say hi"}`),
	}
	frame, err := envelope.ToBinaryFrame(payload)
	require.NoError(t, err)

	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			resp := &transportpb.DataResponse{
				Metadata: map[string]string{
					"kind":                       envelope.KindHook,
					metadataEnvelopeMessageIDKey: "ns/storyrun:storyrun.ready",
				},
				Frame: &transportpb.DataResponse_Binary{Binary: frame},
			}
			require.NoError(t, stream.Send(resp))
			require.NoError(t, stream.Send(resp))
			return nil
		},
	}

	client := newTestTransportConnectorClient(t, srv)
	ctx := t.Context()
	stream, err := client.Data(ctx)
	require.NoError(t, err)

	in := make(chan engram.InboundMessage, 4)
	require.NoError(t, connectorDataRecvLoop(ctx, stream, in, streamRuntimeOptions{}))
	require.Len(t, in, 1)

	msg := <-in
	require.Equal(t, envelope.KindHook, msg.Kind)
	require.Equal(t, "ns/storyrun:storyrun.ready", msg.MessageID)
	require.JSONEq(t, `{"type":"storyrun.ready","hook":{"event":"storyrun.ready"}}`, string(msg.Payload))
	require.JSONEq(t, `{"userPrompt":"say hi"}`, string(msg.Inputs))
}

func TestConnectorDataRecvLoop_DeduplicatesExplicitMessageIDWithoutSequence(t *testing.T) {
	payload := &envelope.Envelope{
		Version:   envelope.LatestVersion,
		Kind:      "data",
		MessageID: "shared-message-id",
		Payload:   []byte(`{"ok":true}`),
	}
	frame, err := envelope.ToBinaryFrame(payload)
	require.NoError(t, err)

	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			resp := &transportpb.DataResponse{
				Metadata: map[string]string{
					"kind":                       "data",
					metadataEnvelopeMessageIDKey: "shared-message-id",
				},
				Frame: &transportpb.DataResponse_Binary{Binary: frame},
			}
			require.NoError(t, stream.Send(resp))
			require.NoError(t, stream.Send(resp))
			return nil
		},
	}

	client := newTestTransportConnectorClient(t, srv)
	ctx := t.Context()
	stream, err := client.Data(ctx)
	require.NoError(t, err)

	in := make(chan engram.InboundMessage, 4)
	require.NoError(t, connectorDataRecvLoop(ctx, stream, in, streamRuntimeOptions{}))
	require.Len(t, in, 1)

	msg := <-in
	require.Equal(t, "shared-message-id", msg.MessageID)
	require.JSONEq(t, `{"ok":true}`, string(msg.Payload))
}

func TestConnectorDataRecvLoop_EmitsDeliveryReceiptForSequencedPackets(t *testing.T) {
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	packet := &transportpb.DataPacket{
		Metadata: map[string]string{
			metadataEnvelopeKindKey: "data",
		},
		Envelope: &transportpb.StreamEnvelope{
			StreamId:  "downstream-step",
			Sequence:  5,
			Partition: "p0",
		},
		Payload: payload,
		Frame: &transportpb.DataPacket_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("bin"),
				MimeType: "text/plain",
			},
		},
	}
	expectedSize := proto.Size(packet)

	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			require.NoError(t, stream.Send(&transportpb.DataResponse{
				Metadata: packet.GetMetadata(),
				Envelope: packet.GetEnvelope(),
				Payload:  packet.GetPayload(),
				Frame:    &transportpb.DataResponse_Binary{Binary: packet.GetBinary()},
			}))
			return nil
		},
	}

	client := newTestTransportConnectorClient(t, srv)
	ctx := t.Context()
	stream, err := client.Data(ctx)
	require.NoError(t, err)

	in := make(chan engram.InboundMessage, 2)
	receipts := make(chan *transportpb.ControlRequest, 2)
	require.NoError(t, connectorDataRecvLoop(ctx, stream, in, streamRuntimeOptions{controlRequests: receipts}))
	require.Len(t, in, 1)

	msg := <-in
	select {
	case receipt := <-receipts:
		t.Fatalf("unexpected receipt before processing completion: %+v", receipt)
	default:
	}

	msg.Done()
	receipt := <-receipts
	require.Equal(t, downstreamDeliveryReceiptType, receipt.GetCustomAction())
	require.Equal(t, "downstream-step", receipt.GetMetadata()[deliveryReceiptStreamIDKey])
	require.Equal(t, "5", receipt.GetMetadata()[deliveryReceiptSequenceKey])
	require.Equal(t, "p0", receipt.GetMetadata()[deliveryReceiptPartitionKey])
	require.Equal(t, strconv.Itoa(expectedSize), receipt.GetMetadata()[deliveryReceiptSizeBytesKey])

	msg.Done()
	select {
	case duplicate := <-receipts:
		t.Fatalf("unexpected duplicate receipt: %+v", duplicate)
	default:
	}
}

func TestConnectorDataRecvLoop_PartialChunkTrafficKeepsSessionAlive(t *testing.T) {
	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			base := &transportpb.StreamEnvelope{
				StreamId:   "downstream-step",
				Sequence:   12,
				Partition:  "p0",
				ChunkId:    "chunk-keepalive",
				ChunkCount: 3,
			}
			parts := []string{"hel", "lo-", "world"}
			for idx, part := range parts {
				env := cloneStreamEnvelope(base)
				env.ChunkIndex = uint32(idx)
				env.ChunkBytes = uint32(len(part))
				require.NoError(t, stream.Send(&transportpb.DataResponse{
					Metadata: map[string]string{
						metadataEnvelopeKindKey: "data",
					},
					Envelope: env,
					Frame: &transportpb.DataResponse_Binary{
						Binary: &transportpb.BinaryFrame{
							Payload:  []byte(part),
							MimeType: "text/plain",
						},
					},
				}))
				if idx < len(parts)-1 {
					time.Sleep(45 * time.Millisecond)
				}
			}
			return nil
		},
	}

	client := newTestTransportConnectorClient(t, srv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Data(ctx)
	require.NoError(t, err)

	hw := newHangWatcher(ctx, 70*time.Millisecond, cancel)
	defer hw.Stop()

	in := make(chan engram.InboundMessage, 2)
	err = connectorDataRecvLoop(ctx, stream, in, streamRuntimeOptions{
		hangWatcher: hw,
	})
	require.NoError(t, err)
	require.Len(t, in, 1)

	msg := <-in
	require.Equal(t, uint64(12), msg.Envelope.GetSequence())
	require.Equal(t, []byte("hello-world"), msg.Binary.Payload)
}

func TestConnectorDataRecvLoop_SharedDeduperSuppressesCompletedMessageIDAcrossSessions(t *testing.T) {
	payload := &envelope.Envelope{
		Version:   envelope.LatestVersion,
		Kind:      "data",
		MessageID: "shared-message-id",
		Payload:   []byte(`{"ok":true}`),
	}
	frame, err := envelope.ToBinaryFrame(payload)
	require.NoError(t, err)

	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			return stream.Send(&transportpb.DataResponse{
				Metadata: map[string]string{
					"kind":                       "data",
					metadataEnvelopeMessageIDKey: "shared-message-id",
				},
				Frame: &transportpb.DataResponse_Binary{Binary: frame},
			})
		},
	}

	client := newTestTransportConnectorClient(t, srv)
	ctx := t.Context()
	sharedDeduper := newPacketDeduper(defaultPacketDedupeEntries)

	stream1, err := client.Data(ctx)
	require.NoError(t, err)
	first := make(chan engram.InboundMessage, 2)
	require.NoError(t, connectorDataRecvLoop(ctx, stream1, first, streamRuntimeOptions{packetDeduper: sharedDeduper}))
	require.Len(t, first, 1)
	(<-first).Done()

	stream2, err := client.Data(ctx)
	require.NoError(t, err)
	second := make(chan engram.InboundMessage, 2)
	require.NoError(t, connectorDataRecvLoop(ctx, stream2, second, streamRuntimeOptions{packetDeduper: sharedDeduper}))
	require.Len(t, second, 0)
}

func TestConnectorDataRecvLoop_RedeliversUnprocessedMessageIDAcrossSessions(t *testing.T) {
	payload := &envelope.Envelope{
		Version:   envelope.LatestVersion,
		Kind:      "data",
		MessageID: "shared-message-id",
		Payload:   []byte(`{"ok":true}`),
	}
	frame, err := envelope.ToBinaryFrame(payload)
	require.NoError(t, err)

	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			return stream.Send(&transportpb.DataResponse{
				Metadata: map[string]string{
					"kind":                       "data",
					metadataEnvelopeMessageIDKey: "shared-message-id",
				},
				Frame: &transportpb.DataResponse_Binary{Binary: frame},
			})
		},
	}

	client := newTestTransportConnectorClient(t, srv)
	ctx := t.Context()
	sharedDeduper := newPacketDeduper(defaultPacketDedupeEntries)

	stream1, err := client.Data(ctx)
	require.NoError(t, err)
	first := make(chan engram.InboundMessage, 2)
	require.NoError(t, connectorDataRecvLoop(ctx, stream1, first, streamRuntimeOptions{packetDeduper: sharedDeduper}))
	require.Len(t, first, 1)

	stream2, err := client.Data(ctx)
	require.NoError(t, err)
	second := make(chan engram.InboundMessage, 2)
	require.NoError(t, connectorDataRecvLoop(ctx, stream2, second, streamRuntimeOptions{packetDeduper: sharedDeduper}))
	require.Len(t, second, 1)
}

func TestConnectorDataRecvLoop_RedeliversUnprocessedSequencedPacketAcrossSessions(t *testing.T) {
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	packet := &transportpb.DataPacket{
		Metadata: map[string]string{
			metadataEnvelopeKindKey: "data",
		},
		Envelope: &transportpb.StreamEnvelope{
			StreamId:  "downstream-step",
			Sequence:  5,
			Partition: "p0",
		},
		Payload: payload,
		Frame: &transportpb.DataPacket_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("bin"),
				MimeType: "text/plain",
			},
		},
	}

	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			return stream.Send(&transportpb.DataResponse{
				Metadata: packet.GetMetadata(),
				Envelope: packet.GetEnvelope(),
				Payload:  packet.GetPayload(),
				Frame:    &transportpb.DataResponse_Binary{Binary: packet.GetBinary()},
			})
		},
	}

	client := newTestTransportConnectorClient(t, srv)
	ctx := t.Context()
	sharedDeduper := newPacketDeduper(defaultPacketDedupeEntries)
	receipts := make(chan *transportpb.ControlRequest, 2)

	stream1, err := client.Data(ctx)
	require.NoError(t, err)
	first := make(chan engram.InboundMessage, 2)
	require.NoError(t, connectorDataRecvLoop(ctx, stream1, first, streamRuntimeOptions{
		packetDeduper:   sharedDeduper,
		controlRequests: receipts,
	}))
	require.Len(t, first, 1)

	stream2, err := client.Data(ctx)
	require.NoError(t, err)
	second := make(chan engram.InboundMessage, 2)
	require.NoError(t, connectorDataRecvLoop(ctx, stream2, second, streamRuntimeOptions{
		packetDeduper:   sharedDeduper,
		controlRequests: receipts,
	}))
	require.Len(t, second, 1)

	select {
	case receipt := <-receipts:
		t.Fatalf("unexpected receipt before replayed packet completed: %+v", receipt)
	default:
	}

	(<-second).Done()
	receipt := <-receipts
	require.Equal(t, downstreamDeliveryReceiptType, receipt.GetCustomAction())
	require.Equal(t, "downstream-step", receipt.GetMetadata()[deliveryReceiptStreamIDKey])
	require.Equal(t, "5", receipt.GetMetadata()[deliveryReceiptSequenceKey])
	require.Equal(t, "p0", receipt.GetMetadata()[deliveryReceiptPartitionKey])
}

func TestConnectorDataRecvLoop_DeliveryReceiptQueueSaturatedDoesNotHangDoneAndAllowsReplay(t *testing.T) {
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	packet := &transportpb.DataPacket{
		Metadata: map[string]string{
			metadataEnvelopeKindKey: "data",
		},
		Envelope: &transportpb.StreamEnvelope{
			StreamId:  "downstream-step",
			Sequence:  5,
			Partition: "p0",
		},
		Payload: payload,
		Frame: &transportpb.DataPacket_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("bin"),
				MimeType: "text/plain",
			},
		},
	}

	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			return stream.Send(&transportpb.DataResponse{
				Metadata: packet.GetMetadata(),
				Envelope: packet.GetEnvelope(),
				Payload:  packet.GetPayload(),
				Frame:    &transportpb.DataResponse_Binary{Binary: packet.GetBinary()},
			})
		},
	}

	client := newTestTransportConnectorClient(t, srv)
	ctx := t.Context()

	sharedDeduper := newPacketDeduper(defaultPacketDedupeEntries)
	controlRequests := make(chan *transportpb.ControlRequest, 1)
	controlRequests <- &transportpb.ControlRequest{CustomAction: "pre-filled"}

	stream1, err := client.Data(ctx)
	require.NoError(t, err)
	first := make(chan engram.InboundMessage, 2)
	require.NoError(t, connectorDataRecvLoop(ctx, stream1, first, streamRuntimeOptions{
		packetDeduper:   sharedDeduper,
		controlRequests: controlRequests,
	}))
	require.Len(t, first, 1)

	doneReturned := make(chan struct{})
	go func() {
		(<-first).Done()
		close(doneReturned)
	}()
	select {
	case <-doneReturned:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected processing completion to return even when delivery receipt queue is saturated")
	}

	stream2, err := client.Data(ctx)
	require.NoError(t, err)
	second := make(chan engram.InboundMessage, 2)
	require.NoError(t, connectorDataRecvLoop(ctx, stream2, second, streamRuntimeOptions{
		packetDeduper:   sharedDeduper,
		controlRequests: controlRequests,
	}))
	require.Len(t, second, 1)
}

func TestConnectorDataRecvLoop_SuppressesCompletedSequencedPacketAcrossSessionsAndResendsReceipt(t *testing.T) {
	payload, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	packet := &transportpb.DataPacket{
		Metadata: map[string]string{
			metadataEnvelopeKindKey: "data",
		},
		Envelope: &transportpb.StreamEnvelope{
			StreamId:  "downstream-step",
			Sequence:  5,
			Partition: "p0",
		},
		Payload: payload,
		Frame: &transportpb.DataPacket_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("bin"),
				MimeType: "text/plain",
			},
		},
	}

	srv := &stubConnectorServer{
		data: func(stream transportpb.TransportConnectorService_DataServer) error {
			return stream.Send(&transportpb.DataResponse{
				Metadata: packet.GetMetadata(),
				Envelope: packet.GetEnvelope(),
				Payload:  packet.GetPayload(),
				Frame:    &transportpb.DataResponse_Binary{Binary: packet.GetBinary()},
			})
		},
	}

	client := newTestTransportConnectorClient(t, srv)
	ctx := t.Context()
	sharedDeduper := newPacketDeduper(defaultPacketDedupeEntries)
	receipts := make(chan *transportpb.ControlRequest, 4)

	stream1, err := client.Data(ctx)
	require.NoError(t, err)
	first := make(chan engram.InboundMessage, 2)
	require.NoError(t, connectorDataRecvLoop(ctx, stream1, first, streamRuntimeOptions{
		packetDeduper:   sharedDeduper,
		controlRequests: receipts,
	}))
	require.Len(t, first, 1)
	(<-first).Done()

	firstReceipt := <-receipts
	require.Equal(t, downstreamDeliveryReceiptType, firstReceipt.GetCustomAction())
	require.Equal(t, "downstream-step", firstReceipt.GetMetadata()[deliveryReceiptStreamIDKey])
	require.Equal(t, "5", firstReceipt.GetMetadata()[deliveryReceiptSequenceKey])
	require.Equal(t, "p0", firstReceipt.GetMetadata()[deliveryReceiptPartitionKey])

	stream2, err := client.Data(ctx)
	require.NoError(t, err)
	second := make(chan engram.InboundMessage, 2)
	require.NoError(t, connectorDataRecvLoop(ctx, stream2, second, streamRuntimeOptions{
		packetDeduper:   sharedDeduper,
		controlRequests: receipts,
	}))
	require.Len(t, second, 0)

	replayReceipt := <-receipts
	require.Equal(t, downstreamDeliveryReceiptType, replayReceipt.GetCustomAction())
	require.Equal(t, "downstream-step", replayReceipt.GetMetadata()[deliveryReceiptStreamIDKey])
	require.Equal(t, "5", replayReceipt.GetMetadata()[deliveryReceiptSequenceKey])
	require.Equal(t, "p0", replayReceipt.GetMetadata()[deliveryReceiptPartitionKey])
}
