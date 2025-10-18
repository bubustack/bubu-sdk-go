package engram

import "time"

// AudioFrame represents PCM audio delivered through the streaming SDK.
type AudioFrame struct {
	PCM          []byte
	SampleRateHz int32
	Channels     int32
	Codec        string
	Timestamp    time.Duration
}

// VideoFrame represents encoded or raw video delivered through the streaming SDK.
type VideoFrame struct {
	Payload   []byte
	Codec     string
	Width     uint32
	Height    uint32
	Timestamp time.Duration
	Raw       bool
}

// BinaryFrame represents generic binary payloads exchanged over the streaming SDK.
type BinaryFrame struct {
	Payload   []byte
	MimeType  string
	Timestamp time.Duration
}
