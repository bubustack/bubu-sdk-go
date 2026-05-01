package sdk

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/tractatus/envelope"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	metadataEnvelopeKindKey      = "bubu.envelope.kind"
	metadataEnvelopeMessageIDKey = "bubu.envelope.message_id"
	metadataEnvelopeTimeKey      = "bubu.envelope.timestamp_ms"
)

func applyStreamContextToPublishRequest(req *transportpb.PublishRequest, msg engram.StreamMessage) error {
	if req == nil {
		return fmt.Errorf("publish request is nil")
	}

	if meta := encodeStreamMetadata(msg); len(meta) > 0 {
		req.Metadata = meta
	}

	if shouldEncodeStructuredStreamPayload(req) {
		payload, err := jsonBytesToStruct(msg.Payload)
		if err != nil {
			return fmt.Errorf("payload decode failed: %w", err)
		}
		if payload != nil {
			req.Payload = payload
		}

		inputs, err := jsonBytesToStruct(msg.Inputs)
		if err != nil {
			return fmt.Errorf("inputs decode failed: %w", err)
		}
		if inputs != nil {
			req.Inputs = inputs
		}
	}

	transports := transportsToProto(msg.Transports)
	if len(transports) > 0 {
		req.Transports = transports
	}

	if msg.Envelope != nil {
		req.Envelope = cloneStreamEnvelope(msg.Envelope)
	}

	return nil
}

func shouldEncodeStructuredStreamPayload(req *transportpb.PublishRequest) bool {
	if req == nil {
		return true
	}
	binary := req.GetBinary()
	if binary == nil {
		return true
	}
	return strings.TrimSpace(binary.GetMimeType()) == envelope.MIMEType
}

func mergeRequestContextIntoStreamMessage(req *transportpb.PublishRequest, msg *engram.StreamMessage) error {
	if req == nil || msg == nil {
		return nil
	}

	hydrateStreamMessageMetadata(msg, req.GetMetadata())

	if len(msg.Payload) == 0 {
		payload, err := structToJSONBytes(req.GetPayload())
		if err != nil {
			return fmt.Errorf("payload marshal failed: %w", err)
		}
		if len(payload) > 0 {
			msg.Payload = payload
		}
	}

	if len(msg.Inputs) == 0 {
		inputs, err := structToJSONBytes(req.GetInputs())
		if err != nil {
			return fmt.Errorf("inputs marshal failed: %w", err)
		}
		if len(inputs) > 0 {
			msg.Inputs = inputs
		}
	}

	if len(msg.Transports) == 0 {
		msg.Transports = transportsFromProto(req.GetTransports())
	}

	if msg.Envelope == nil && req.GetEnvelope() != nil {
		msg.Envelope = cloneStreamEnvelope(req.GetEnvelope())
	}

	return nil
}

func cloneStreamEnvelope(env *transportpb.StreamEnvelope) *transportpb.StreamEnvelope {
	if env == nil {
		return nil
	}
	return &transportpb.StreamEnvelope{
		StreamId:   env.GetStreamId(),
		Sequence:   env.GetSequence(),
		Partition:  env.GetPartition(),
		ChunkId:    env.GetChunkId(),
		ChunkIndex: env.GetChunkIndex(),
		ChunkCount: env.GetChunkCount(),
		ChunkBytes: env.GetChunkBytes(),
		TotalBytes: env.GetTotalBytes(),
	}
}

func encodeStreamMetadata(msg engram.StreamMessage) map[string]string {
	meta := cloneStringMap(msg.Metadata)
	if msg.Kind == "" && msg.MessageID == "" && msg.Timestamp.IsZero() {
		return meta
	}
	if meta == nil {
		meta = make(map[string]string, 3)
	}
	if msg.Kind != "" {
		meta[metadataEnvelopeKindKey] = strings.TrimSpace(msg.Kind)
	}
	if msg.MessageID != "" {
		meta[metadataEnvelopeMessageIDKey] = strings.TrimSpace(msg.MessageID)
	}
	if !msg.Timestamp.IsZero() {
		meta[metadataEnvelopeTimeKey] = strconv.FormatInt(msg.Timestamp.UTC().UnixMilli(), 10)
	}
	return meta
}

func hydrateStreamMessageMetadata(msg *engram.StreamMessage, metadata map[string]string) {
	if msg == nil || len(metadata) == 0 {
		return
	}
	cloned := cloneStringMap(metadata)
	if kind, ok := cloned[metadataEnvelopeKindKey]; ok {
		msg.Kind = strings.TrimSpace(kind)
		delete(cloned, metadataEnvelopeKindKey)
	}
	if messageID, ok := cloned[metadataEnvelopeMessageIDKey]; ok {
		msg.MessageID = strings.TrimSpace(messageID)
		delete(cloned, metadataEnvelopeMessageIDKey)
	}
	if ts, ok := cloned[metadataEnvelopeTimeKey]; ok {
		if parsed, err := strconv.ParseInt(ts, 10, 64); err == nil && parsed > 0 {
			msg.Timestamp = time.UnixMilli(parsed).UTC()
		}
		delete(cloned, metadataEnvelopeTimeKey)
	}
	if len(cloned) > 0 {
		msg.Metadata = cloned
	}
}

func jsonBytesToStruct(data []byte) (*structpb.Struct, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	return structpb.NewStruct(payload)
}

func structToJSONBytes(st *structpb.Struct) ([]byte, error) {
	if st == nil {
		return nil, nil
	}
	return st.MarshalJSON()
}

func transportsToProto(transports []engram.TransportDescriptor) []*transportpb.TransportDescriptor {
	if len(transports) == 0 {
		return nil
	}
	out := make([]*transportpb.TransportDescriptor, len(transports))
	for i := range transports {
		td := transports[i]
		out[i] = &transportpb.TransportDescriptor{
			Name:        td.Name,
			Kind:        td.Kind,
			Mode:        td.Mode,
			TypedConfig: transportConfigToProto(td.TypedConfig),
		}
	}
	return out
}

func transportsFromProto(src []*transportpb.TransportDescriptor) []engram.TransportDescriptor {
	if len(src) == 0 {
		return nil
	}
	out := make([]engram.TransportDescriptor, 0, len(src))
	for _, td := range src {
		if td == nil {
			continue
		}
		out = append(out, engram.TransportDescriptor{
			Name:        td.GetName(),
			Kind:        td.GetKind(),
			Mode:        td.GetMode(),
			TypedConfig: transportConfigFromProto(td.GetTypedConfig()),
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func transportConfigToProto(src *engram.TransportConfig) *transportpb.TransportConfig {
	if src == nil {
		return nil
	}
	if strings.TrimSpace(src.TransportRef) == "" && strings.TrimSpace(src.ModeReason) == "" {
		return nil
	}
	return &transportpb.TransportConfig{
		TransportRef: strings.TrimSpace(src.TransportRef),
		ModeReason:   strings.TrimSpace(src.ModeReason),
	}
}

func transportConfigFromProto(src *transportpb.TransportConfig) *engram.TransportConfig {
	if src == nil {
		return nil
	}
	cfg := &engram.TransportConfig{
		TransportRef: strings.TrimSpace(src.GetTransportRef()),
		ModeReason:   strings.TrimSpace(src.GetModeReason()),
	}
	if cfg.TransportRef == "" && cfg.ModeReason == "" {
		return nil
	}
	return cfg
}
