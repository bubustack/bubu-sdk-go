package telemetry

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"google.golang.org/grpc"
)

// NewTracerProvider creates and configures a new OpenTelemetry TracerProvider.
// It sets up an OTLP exporter with a gRPC client. Configuration is primarily
// driven by standard OpenTelemetry environment variables (e.g., OTEL_EXPORTER_OTLP_ENDPOINT).
func NewTracerProvider(ctx context.Context, serviceName string) (*sdktrace.TracerProvider, error) {
	// Check if telemetry is explicitly enabled. If not, return a No-Op provider.
	if os.Getenv("BUBU_TELEMETRY_ENABLED") != "true" {
		tp := sdktrace.NewTracerProvider()
		otel.SetTracerProvider(tp) // Set a no-op provider to avoid nil panics
		return tp, nil
	}

	// Create a new OTLP gRPC exporter
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(), // Use insecure for in-cluster communication
		otlptracegrpc.WithDialOption(grpc.WithBlock()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
	}

	// Define the service resource attributes
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create telemetry resource: %w", err)
	}

	// Create a new TracerProvider with the exporter and resource
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()), // Sample all traces for now
	)

	// Set the global TracerProvider
	otel.SetTracerProvider(tp)

	// Set the global Propagator to trace context across service boundaries
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return tp, nil
}
