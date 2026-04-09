package testkit

import (
	"encoding/json"
	"strings"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/tractatus/envelope"
	"github.com/stretchr/testify/require"
)

func TestValidateStructuredError(t *testing.T) {
	good := runsv1alpha1.StructuredError{
		Version: runsv1alpha1.StructuredErrorVersionV1,
		Type:    runsv1alpha1.StructuredErrorTypeExecution,
		Message: "boom",
	}
	require.NoError(t, ValidateStructuredError(good))

	bad := good
	bad.Version = ""
	require.Error(t, ValidateStructuredError(bad))

	bad = good
	bad.ExitClass = runsv1alpha1.StructuredErrorExitClass("bogus")
	require.Error(t, ValidateStructuredError(bad))
}

func TestValidateStreamMessage(t *testing.T) {
	msg := engram.StreamMessage{Payload: []byte(`{"ok":true}`)}
	require.NoError(t, ValidateStreamMessage(msg))

	require.Error(t, ValidateStreamMessage(engram.StreamMessage{}))
}

func TestValidateStreamMessage_InvalidMediaFrame(t *testing.T) {
	msg := engram.StreamMessage{
		Audio: &engram.AudioFrame{
			SampleRateHz: 16000,
			Channels:     1,
		},
	}

	require.ErrorIs(t, ValidateStreamMessage(msg), engram.ErrInvalidStreamMessage)
}

func TestValidateStreamMessage_InvalidMetadataKey(t *testing.T) {
	msg := engram.StreamMessage{
		Payload:  []byte(`{"ok":true}`),
		Metadata: map[string]string{" bad ": "value"},
	}

	require.ErrorIs(t, ValidateStreamMessage(msg), engram.ErrInvalidStreamMessage)
}

func TestValidateStreamMessage_RejectsReservedEnvelopeMimeWithoutEnvelopeFields(t *testing.T) {
	msg := engram.StreamMessage{
		Binary: &engram.BinaryFrame{
			Payload:  []byte("raw"),
			MimeType: envelope.MIMEType,
		},
	}

	require.ErrorIs(t, ValidateStreamMessage(msg), engram.ErrInvalidStreamMessage)
}

func TestValidateStreamMessage_RejectsReservedEnvelopeMimeWithParametersWithoutEnvelopeFields(t *testing.T) {
	msg := engram.StreamMessage{
		Binary: &engram.BinaryFrame{
			Payload:  []byte("raw"),
			MimeType: envelope.MIMEType + "; charset=utf-8",
		},
	}

	require.ErrorIs(t, ValidateStreamMessage(msg), engram.ErrInvalidStreamMessage)
}

func TestValidateStreamMessage_RejectsReservedEnvelopeMimeCaseInsensitivePayloadMismatch(t *testing.T) {
	msg := engram.StreamMessage{
		Kind:    "telemetry",
		Payload: []byte(`{"ok":true}`),
		Binary: &engram.BinaryFrame{
			Payload:  []byte(`{"ok":false}`),
			MimeType: strings.ToUpper(envelope.MIMEType),
		},
	}

	require.ErrorIs(t, ValidateStreamMessage(msg), engram.ErrInvalidStreamMessage)
}

func TestValidateStreamMessage_ErrorEnvelope(t *testing.T) {
	payload, err := json.Marshal(runsv1alpha1.StructuredError{
		Version: runsv1alpha1.StructuredErrorVersionV1,
		Type:    runsv1alpha1.StructuredErrorTypeExecution,
		Message: "boom",
	})
	require.NoError(t, err)

	msg := engram.StreamMessage{
		Kind:    engram.StreamMessageKindError,
		Payload: payload,
	}
	require.NoError(t, ValidateStreamMessage(msg))

	bad := engram.StreamMessage{
		Kind:    engram.StreamMessageKindError,
		Payload: []byte(`{"type":"execution"}`),
	}
	require.Error(t, ValidateStreamMessage(bad))
}
