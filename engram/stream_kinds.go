package engram

// StreamMessageKindData marks a StreamMessage as a normal application data packet.
const StreamMessageKindData = "data"

// StreamMessageKindHeartbeat marks a StreamMessage as a transport liveness heartbeat.
const StreamMessageKindHeartbeat = "heartbeat"

// StreamMessageKindNoop marks a StreamMessage as an intentionally empty no-op packet.
const StreamMessageKindNoop = "noop"

// StreamMessageKindError marks a StreamMessage payload as a StructuredError envelope.
const StreamMessageKindError = "error"
