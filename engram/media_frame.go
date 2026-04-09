package engram

import "time"

// AudioFrame represents PCM audio delivered through the streaming SDK.
type AudioFrame struct {
	// PCM contains raw little-endian PCM audio bytes for the frame.
	PCM []byte
	// SampleRateHz is the sampling rate in hertz (for example, 16000).
	SampleRateHz int32
	// Channels is the number of audio channels in PCM (for example, 1 for mono).
	Channels int32
	// Codec optionally names the codec when the frame is encoded instead of raw PCM.
	Codec string
	// Timestamp is the media timeline position for this frame.
	Timestamp time.Duration
}

// VideoFrame represents encoded or raw video delivered through the streaming SDK.
type VideoFrame struct {
	// Payload contains encoded bytes or raw pixel data for the frame.
	Payload []byte
	// Codec identifies the encoded video format (for example, "h264"), if applicable.
	Codec string
	// Width is the frame width in pixels.
	Width uint32
	// Height is the frame height in pixels.
	Height uint32
	// Timestamp is the media timeline position for this frame.
	Timestamp time.Duration
	// Raw reports whether Payload carries raw video pixels instead of encoded bytes.
	Raw bool
}

// BinaryFrame represents generic binary payloads exchanged over the streaming SDK.
type BinaryFrame struct {
	// Payload carries opaque binary bytes.
	Payload []byte
	// MimeType identifies the payload media type (for example, "application/octet-stream").
	MimeType string
	// Timestamp is an optional media timeline position for this frame.
	Timestamp time.Duration
}
