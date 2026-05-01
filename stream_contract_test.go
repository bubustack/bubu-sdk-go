package sdk

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/tractatus/envelope"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/stretchr/testify/require"
)

// TestStreamMessageToPublishRequestRejectsEmpty verifies that a message with no
// Kind, no payload, no audio/video/binary, and no inputs is rejected.
func TestStreamMessageToPublishRequestRejectsEmpty(t *testing.T) {
	msg := engram.StreamMessage{} // truly empty
	_, err := streamMessageToPublishRequest(msg)
	require.Error(t, err)
}

// TestStreamMessageToPublishRequestAllowsKindOnly verifies that a metadata-only
// message with a non-empty Kind is published as an envelope (Fix 3.14).
func TestStreamMessageToPublishRequestAllowsKindOnly(t *testing.T) {
	msg := engram.StreamMessage{
		Kind:     "telemetry",
		Metadata: map[string]string{"k": "v"},
	}
	req, err := streamMessageToPublishRequest(msg)
	require.NoError(t, err)
	require.NotNil(t, req.GetBinary(), "expected envelope binary frame")
}

func TestStreamMessageToPublishRequestEnvelopeForPayload(t *testing.T) {
	payload := []byte(`{"ok":true}`)
	msg := engram.StreamMessage{
		Kind:    "data",
		Payload: payload,
	}

	req, err := streamMessageToPublishRequest(msg)
	require.NoError(t, err)
	require.NotNil(t, req.GetBinary())

	frame := req.GetBinary()
	require.Equal(t, envelope.MIMEType, frame.GetMimeType())
	env, err := envelope.FromBinaryFrame(frame)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(env.Payload, &decoded))
	require.Equal(t, true, decoded["ok"])
}

func TestStreamMessageToPublishRequestRawBinaryWithInputsUsesEnvelope(t *testing.T) {
	payload := []byte(`{"text":"hello"}`)
	inputs := []byte(`{"userPrompt":"hello"}`)
	msg := engram.StreamMessage{
		Kind:    "data",
		Payload: payload,
		Inputs:  inputs,
		Binary: &engram.BinaryFrame{
			Payload:  payload,
			MimeType: "application/json",
		},
	}

	req, err := streamMessageToPublishRequest(msg)
	require.NoError(t, err)
	require.NotNil(t, req.GetBinary())
	require.Equal(t, envelope.MIMEType, req.GetBinary().GetMimeType())
	require.NotNil(t, req.GetInputs())

	roundTrip, err := publishRequestToStreamMessage(req)
	require.NoError(t, err)
	require.Equal(t, "data", roundTrip.Kind)
	require.Equal(t, payload, roundTrip.Payload)
	require.Equal(t, inputs, roundTrip.Inputs)
	require.NotNil(t, roundTrip.Binary)
	require.Equal(t, "application/json", roundTrip.Binary.MimeType)
}

func TestStreamMessageToPublishRequestMirroredJSONBinaryUsesEnvelopeForPacketTemplates(t *testing.T) {
	payload := []byte(`{"text":"bonjour","sender":"speaker"}`)
	msg := engram.StreamMessage{
		Kind:    "chat.message.v1",
		Payload: payload,
		Binary: &engram.BinaryFrame{
			Payload:  payload,
			MimeType: "application/json",
		},
	}

	req, err := streamMessageToPublishRequest(msg)

	require.NoError(t, err)
	require.NotNil(t, req.GetBinary())
	require.Equal(t, envelope.MIMEType, req.GetBinary().GetMimeType())
	require.NotNil(t, req.GetPayload())
	require.Equal(t, "bonjour", req.GetPayload().AsMap()["text"])
}

func TestStreamMessageToPublishRequestRejectsRawBinaryWithInputsWithoutMirroredPayload(t *testing.T) {
	msg := engram.StreamMessage{
		Kind:   "data",
		Inputs: []byte(`{"userPrompt":"hello"}`),
		Binary: &engram.BinaryFrame{
			Payload:  []byte("opaque"),
			MimeType: "application/octet-stream",
		},
	}

	req, err := streamMessageToPublishRequest(msg)

	require.ErrorIs(t, err, engram.ErrInvalidStreamMessage)
	require.Nil(t, req)
}

func TestStreamMessageToPublishRequestRejectsInvalidEnvelopePayloadJSON(t *testing.T) {
	msg := engram.StreamMessage{
		Kind:    "data",
		Payload: []byte(`{"text":`),
	}

	req, err := streamMessageToPublishRequest(msg)

	require.Error(t, err)
	require.Contains(t, err.Error(), "envelope payload is not valid JSON")
	require.Nil(t, req)
}

func TestPublishRequestToStreamMessageRejectsInvalidEnvelopePayloadJSON(t *testing.T) {
	req := &transportpb.PublishRequest{
		Frame: &transportpb.PublishRequest_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte(`{"payload":`),
				MimeType: envelope.MIMEType,
			},
		},
	}

	_, err := publishRequestToStreamMessage(req)

	require.Error(t, err)
	require.Contains(t, err.Error(), "envelope decode failed")
}

func TestPublishRequestToStreamMessageRejectsEnvelopeTimestampOverflow(t *testing.T) {
	req := &transportpb.PublishRequest{
		Frame: &transportpb.PublishRequest_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:     []byte(`{"version":"v1","kind":"data","payload":{"ok":true}}`),
				MimeType:    envelope.MIMEType,
				TimestampMs: uint64(math.MaxInt64) + 1,
			},
		},
	}

	_, err := publishRequestToStreamMessage(req)

	require.Error(t, err)
	require.Contains(t, err.Error(), "timestamp")
}

func TestStreamMessageToPublishRequestRejectsProtoInvalidTransportCount(t *testing.T) {
	transports := make([]engram.TransportDescriptor, 11)
	for i := range transports {
		transports[i] = engram.TransportDescriptor{Name: "transport"}
	}
	payload := []byte(`{"payload":"value"}`)
	msg := engram.StreamMessage{
		Kind:       "data",
		Payload:    payload,
		Binary:     &engram.BinaryFrame{Payload: payload, MimeType: "application/json"},
		Transports: transports,
	}

	_, err := streamMessageToPublishRequest(msg)

	require.Error(t, err)
	require.Contains(t, err.Error(), "transport publish request invalid")
}

func TestPublishRequestToStreamMessageRejectsProtoInvalidTransportCount(t *testing.T) {
	transports := make([]*transportpb.TransportDescriptor, 11)
	for i := range transports {
		transports[i] = &transportpb.TransportDescriptor{Name: "transport"}
	}
	req := &transportpb.PublishRequest{
		Frame: &transportpb.PublishRequest_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("payload"),
				MimeType: "application/octet-stream",
			},
		},
		Transports: transports,
	}

	_, err := publishRequestToStreamMessage(req)

	require.Error(t, err)
	require.Contains(t, err.Error(), "transport publish request invalid")
}
