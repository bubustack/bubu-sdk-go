package engram

import "context"

// ControlDirective represents a control-plane instruction flowing over the transport connector.
// Typical directive types include "start", "stop", or "codec-select".
type ControlDirective struct {
	// Type identifies the control instruction, such as "start", "stop", or "codec-select".
	Type string
	// Metadata carries optional directive-specific key/value hints from the connector.
	Metadata map[string]string
}

// ControlDirectiveHandler can be implemented by StreamingEngrams that want to react to
// transport control directives emitted by connectors.
type ControlDirectiveHandler interface {
	// HandleControlDirective processes an inbound directive. Returning a non-nil directive
	// sends a response back to the connector. Implementations may return nil to skip replies.
	HandleControlDirective(ctx context.Context, directive ControlDirective) (*ControlDirective, error)
}
