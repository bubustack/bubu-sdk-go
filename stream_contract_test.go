package sdk

import (
	"encoding/json"
	"testing"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/tractatus/envelope"
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
