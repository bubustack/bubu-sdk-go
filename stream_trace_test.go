package sdk

import (
	"context"
	"fmt"
	"testing"

	"github.com/bubustack/bobrapet/pkg/contracts"
	"github.com/bubustack/bubu-sdk-go/engram"
	"go.opentelemetry.io/otel/trace"
)

func TestExtractTraceContext(t *testing.T) {
	t.Setenv(contracts.TracePropagationEnv, "true")
	traceIDHex := "0af7651916cd43dd8448eb211c80319c"
	spanIDHex := "b9c7c989f97918e1"
	msg := &engram.StreamMessage{
		Metadata: map[string]string{
			"traceparent": fmt.Sprintf("00-%s-%s-01", traceIDHex, spanIDHex),
		},
	}

	ctx := ExtractTraceContext(context.Background(), msg)
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		t.Fatalf("expected valid span context from metadata")
	}
	if sc.TraceID().String() != traceIDHex {
		t.Fatalf("expected traceID %s, got %s", traceIDHex, sc.TraceID().String())
	}
	if sc.SpanID().String() != spanIDHex {
		t.Fatalf("expected spanID %s, got %s", spanIDHex, sc.SpanID().String())
	}
}

func TestExtractTraceContextNoMetadata(t *testing.T) {
	t.Setenv(contracts.TracePropagationEnv, "true")
	baseCtx := context.Background()
	ctx := ExtractTraceContext(baseCtx, &engram.StreamMessage{})
	if ctx != baseCtx {
		t.Fatalf("expected context to be unchanged when metadata missing")
	}
}
