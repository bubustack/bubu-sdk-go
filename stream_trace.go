package sdk

import (
	"context"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/pkg/observability"
	"go.opentelemetry.io/otel/propagation"
)

func injectTraceContext(ctx context.Context, msg *engram.StreamMessage) {
	if msg == nil || !observability.TracePropagationEnabled() {
		return
	}
	metadata := cloneStringMap(msg.Metadata)
	if metadata == nil {
		metadata = make(map[string]string, 2)
	}
	observability.Propagator().Inject(ctx, propagation.MapCarrier(metadata))
	msg.Metadata = metadata
}

// ExtractTraceContext restores tracing context from a StreamMessage's metadata so Engrams can
// start child spans that are linked to upstream steps.
func ExtractTraceContext(ctx context.Context, msg *engram.StreamMessage) context.Context {
	if msg == nil || len(msg.Metadata) == 0 || !observability.TracePropagationEnabled() {
		return ctx
	}
	return observability.Propagator().Extract(ctx, propagation.MapCarrier(msg.Metadata))
}
