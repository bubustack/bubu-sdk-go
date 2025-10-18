package sdk

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/bubustack/bubu-sdk-go/engram"
)

// StreamMessageOption configures an engram.StreamMessage produced via NewStreamMessage.
type StreamMessageOption func(*engram.StreamMessage)

// NewStreamMessage constructs an engram.StreamMessage pre-populated with the provided options.
// It trims the kind identifier and applies any options in order, enabling concise metadata-only
// messages that still round-trip through the transport envelope.
func NewStreamMessage(kind string, opts ...StreamMessageOption) engram.StreamMessage {
	msg := engram.StreamMessage{Kind: strings.TrimSpace(kind)}
	for _, opt := range opts {
		if opt != nil {
			opt(&msg)
		}
	}
	return msg
}

// WithMessageID sets the message identifier for correlation across steps.
func WithMessageID(id string) StreamMessageOption {
	return func(msg *engram.StreamMessage) {
		msg.MessageID = strings.TrimSpace(id)
	}
}

// WithTimestamp overrides the message timestamp. Zero values are ignored.
func WithTimestamp(ts time.Time) StreamMessageOption {
	return func(msg *engram.StreamMessage) {
		if !ts.IsZero() {
			msg.Timestamp = ts.UTC()
		}
	}
}

// WithMetadata merges the supplied metadata into the message, cloning the map to prevent
// callers from mutating shared state.
func WithMetadata(metadata map[string]string) StreamMessageOption {
	return func(msg *engram.StreamMessage) {
		if len(metadata) == 0 {
			return
		}
		if msg.Metadata == nil {
			msg.Metadata = make(map[string]string, len(metadata))
		}
		for k, v := range metadata {
			msg.Metadata[k] = v
		}
	}
}

// WithJSONPayload attaches a JSON payload (already marshaled). The byte slice is copied.
func WithJSONPayload(payload []byte) StreamMessageOption {
	return func(msg *engram.StreamMessage) {
		if len(payload) == 0 {
			return
		}
		msg.Payload = copyBytes(payload)
	}
}

// WithInputs attaches CEL-evaluated inputs (already marshaled JSON).
func WithInputs(inputs []byte) StreamMessageOption {
	return func(msg *engram.StreamMessage) {
		if len(inputs) == 0 {
			return
		}
		msg.Inputs = copyBytes(inputs)
	}
}

// WithTransports records the story's declared transports for downstream inspection.
func WithTransports(descriptors []engram.TransportDescriptor) StreamMessageOption {
	return func(msg *engram.StreamMessage) {
		if len(descriptors) == 0 {
			return
		}
		msg.Transports = make([]engram.TransportDescriptor, len(descriptors))
		for i := range descriptors {
			msg.Transports[i] = descriptors[i].Clone()
		}
	}
}

// WithBinaryPayload attaches an arbitrary binary payload plus MIME type. The payload is copied.
func WithBinaryPayload(mime string, payload []byte, timestamp time.Duration) StreamMessageOption {
	return func(msg *engram.StreamMessage) {
		if len(payload) == 0 {
			return
		}
		msg.Binary = &engram.BinaryFrame{
			Payload:   copyBytes(payload),
			MimeType:  strings.TrimSpace(mime),
			Timestamp: timestamp,
		}
	}
}

// WithJSONData marshals the provided value to JSON and attaches it as the payload. It returns
// an option alongside any marshaling error so callers can handle failures inline.
func WithJSONData(v any) (StreamMessageOption, error) {
	if v == nil {
		return nil, nil
	}
	bytes, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return WithJSONPayload(bytes), nil
}
