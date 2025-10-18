package sdk

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/tractatus/envelope"
)

func streamMessageEnvelope(msg engram.StreamMessage) *envelope.Envelope {
	env := &envelope.Envelope{Version: envelope.LatestVersion}
	var populated bool

	if copyEnvelopeHeaders(env, msg) {
		populated = true
	}
	if copyEnvelopeMetadataField(env, msg.Metadata) {
		populated = true
	}
	if copyEnvelopePayload(env, msg.Payload) {
		populated = true
	}
	if copyEnvelopeInputs(env, msg.Inputs) {
		populated = true
	}
	if copyEnvelopeTransports(env, msg.Transports) {
		populated = true
	}

	if !populated {
		return nil
	}
	return env
}

func copyEnvelopeHeaders(env *envelope.Envelope, msg engram.StreamMessage) bool {
	var updated bool
	if msg.Kind != "" {
		env.Kind = msg.Kind
		updated = true
	}
	if msg.MessageID != "" {
		env.MessageID = msg.MessageID
		updated = true
	}
	if !msg.Timestamp.IsZero() {
		env.TimestampMs = msg.Timestamp.UTC().UnixMilli()
		updated = true
	}
	return updated
}

func copyEnvelopeMetadataField(env *envelope.Envelope, metadata map[string]string) bool {
	if len(metadata) == 0 {
		return false
	}
	env.Metadata = cloneStringMap(metadata)
	return true
}

func copyEnvelopePayload(env *envelope.Envelope, payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	env.Payload = json.RawMessage(copyBytes(payload))
	return true
}

func copyEnvelopeInputs(env *envelope.Envelope, inputs []byte) bool {
	if len(inputs) == 0 {
		return false
	}
	env.Inputs = json.RawMessage(copyBytes(inputs))
	return true
}

func copyEnvelopeTransports(env *envelope.Envelope, transports []engram.TransportDescriptor) bool {
	if len(transports) == 0 {
		return false
	}
	env.Transports = make([]envelope.TransportDescriptor, len(transports))
	for i := range transports {
		src := transports[i]
		env.Transports[i] = envelope.TransportDescriptor{
			Name:   src.Name,
			Kind:   src.Kind,
			Mode:   src.Mode,
			Config: cloneConfigMap(src.Config),
		}
	}
	return true
}

func populateMessageFromEnvelope(msg *engram.StreamMessage, env *envelope.Envelope) {
	if env == nil || msg == nil {
		return
	}
	msg.Kind = strings.TrimSpace(env.Kind)
	msg.MessageID = strings.TrimSpace(env.MessageID)
	if env.TimestampMs > 0 {
		msg.Timestamp = time.UnixMilli(env.TimestampMs).UTC()
	}
	if len(env.Metadata) > 0 {
		msg.Metadata = cloneStringMap(env.Metadata)
	}
	if len(env.Payload) > 0 {
		msg.Payload = copyBytes(env.Payload)
	}
	if len(env.Inputs) > 0 {
		msg.Inputs = copyBytes(env.Inputs)
	}
	if len(env.Transports) > 0 {
		msg.Transports = make([]engram.TransportDescriptor, len(env.Transports))
		for i := range env.Transports {
			src := env.Transports[i]
			msg.Transports[i] = engram.TransportDescriptor{
				Name:   src.Name,
				Kind:   src.Kind,
				Mode:   src.Mode,
				Config: cloneConfigMap(src.Config),
			}
		}
	}
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func cloneConfigMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = cloneConfigValue(v)
	}
	return dst
}

func cloneConfigValue(v any) any {
	switch typed := v.(type) {
	case map[string]any:
		return cloneConfigMap(typed)
	case []any:
		out := make([]any, len(typed))
		for i := range typed {
			out[i] = cloneConfigValue(typed[i])
		}
		return out
	default:
		return typed
	}
}

func copyBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
