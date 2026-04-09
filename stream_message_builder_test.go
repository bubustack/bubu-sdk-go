package sdk

import (
	"testing"
	"time"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/stretchr/testify/require"
)

func TestNewStreamMessageOptions(t *testing.T) {
	ts := time.Unix(1717171717, 0).UTC()
	msg := NewStreamMessage(
		"telemetry",
		WithMessageID("abc-123"),
		WithTimestamp(ts),
		WithMetadata(map[string]string{"foo": "bar"}),
		WithJSONPayload([]byte(`{"ok":true}`)),
		WithInputs([]byte(`{"with":1}`)),
		WithTransports([]engram.TransportDescriptor{{Name: "default", Kind: "livekit"}}),
	)

	require.Equal(t, "telemetry", msg.Kind)
	require.Equal(t, "abc-123", msg.MessageID)
	require.Equal(t, ts, msg.Timestamp)
	require.Equal(t, map[string]string{"foo": "bar"}, msg.Metadata)
	require.Equal(t, []byte(`{"ok":true}`), msg.Payload)
	require.Equal(t, []byte(`{"with":1}`), msg.Inputs)
	require.Len(t, msg.Transports, 1)
	require.Equal(t, "default", msg.Transports[0].Name)
	require.Equal(t, "livekit", msg.Transports[0].Kind)
}

func TestWithBinaryPayload(t *testing.T) {
	ts := time.Second
	msg := NewStreamMessage("binary", WithBinaryPayload("application/octet-stream", []byte{0x01, 0x02}, ts))
	require.NotNil(t, msg.Binary)
	require.Equal(t, []byte{0x01, 0x02}, msg.Binary.Payload)
	require.Equal(t, "application/octet-stream", msg.Binary.MimeType)
	require.Equal(t, ts, msg.Binary.Timestamp)
}

func TestWithJSONData(t *testing.T) {
	opt, err := WithJSONData(map[string]any{"value": 42})
	require.NoError(t, err)

	msg := NewStreamMessage("json", opt)
	require.Equal(t, []byte(`{"value":42}`), msg.Payload)
}

func TestWithJSONDataError(t *testing.T) {
	ch := make(chan struct{}) // channels cannot be marshaled
	opt, err := WithJSONData(ch)
	require.Nil(t, opt)
	require.Error(t, err)
}

func TestWithMetadataCopies(t *testing.T) {
	meta := map[string]string{"foo": "bar"}
	msg := NewStreamMessage("copy", WithMetadata(meta))
	meta["foo"] = "baz"
	require.Equal(t, "bar", msg.Metadata["foo"])
}

func TestWithTransportsClone(t *testing.T) {
	transports := []engram.TransportDescriptor{{Name: "a"}}
	msg := NewStreamMessage("transports", WithTransports(transports))
	require.Equal(t, "a", msg.Transports[0].Name)
	transports[0].Name = "b"
	require.Equal(t, "a", msg.Transports[0].Name)
}

func TestNewStreamMessageTrimsKind(t *testing.T) {
	msg := NewStreamMessage("  telemetry  ")
	require.Equal(t, "telemetry", msg.Kind)
}

func TestWithJSONPayloadCopies(t *testing.T) {
	payload := []byte(`{"a":1}`)
	msg := NewStreamMessage("payload", WithJSONPayload(payload))
	payload[0] = '!'
	require.Equal(t, []byte(`{"a":1}`), msg.Payload)
}

func TestWithInputsCopies(t *testing.T) {
	inputs := []byte(`{"b":2}`)
	msg := NewStreamMessage("inputs", WithInputs(inputs))
	inputs[0] = '!'
	require.Equal(t, []byte(`{"b":2}`), msg.Inputs)
}

func TestWithJSONDataNil(t *testing.T) {
	opt, err := WithJSONData(nil)
	require.NoError(t, err)
	msg := NewStreamMessage("nil", opt)
	require.Nil(t, msg.Payload)
}
