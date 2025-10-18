// Package metrics provides OpenTelemetry-based observability for the SDK and allows
// developers to register custom metrics for their engrams and impulses.
package metrics

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	meter metric.Meter

	// SDK built-in metrics
	hydrationSizeBytes   metric.Int64Histogram
	dehydrationSizeBytes metric.Int64Histogram
	streamThroughput     metric.Int64Counter
	k8sOperationDuration metric.Float64Histogram

	// Streaming client buffer metrics
	clientBufferAdds       metric.Int64Counter
	clientBufferSizeGauge  metric.Float64ObservableGauge
	clientBufferBytesGauge metric.Float64ObservableGauge
	clientBufferDrops      metric.Int64Counter
	clientBufferFlushes    metric.Int64Counter

	// Streaming reconnect metrics
	streamReconnectAttempts metric.Int64Counter
	streamReconnectFailures metric.Int64Counter
)

func init() {
	meter = otel.Meter("bubu-sdk")

	var err error
	hydrationSizeBytes, err = meter.Int64Histogram(
		"bubu.storage.hydration.size_bytes",
		metric.WithDescription("Size of hydrated data in bytes"),
		metric.WithUnit("By"),
	)
	_ = err
	// If instrument creation fails, continue without panicking

	dehydrationSizeBytes, err = meter.Int64Histogram(
		"bubu.storage.dehydration.size_bytes",
		metric.WithDescription("Size of dehydrated data in bytes"),
		metric.WithUnit("By"),
	)
	_ = err

	streamThroughput, err = meter.Int64Counter(
		"bubu.stream.messages_total",
		metric.WithDescription("Total messages processed in stream"),
	)
	_ = err

	k8sOperationDuration, err = meter.Float64Histogram(
		"bubu.k8s.operation.duration_seconds",
		metric.WithDescription("Duration of Kubernetes API operations"),
		metric.WithUnit("s"),
	)
	_ = err

	// Reconnect attempt/failure counters for streaming client
	streamReconnectAttempts, err = meter.Int64Counter(
		"bubu.stream.reconnect.attempts_total",
		metric.WithDescription("Total reconnect attempts by SDK client"),
	)
	_ = err
	streamReconnectFailures, err = meter.Int64Counter(
		"bubu.stream.reconnect.failures_total",
		metric.WithDescription("Total reconnect failures (terminal) by SDK client"),
	)
	_ = err

	// Client buffer: size/bytes gauges are registered by the buffer with callbacks
	clientBufferDrops, err = meter.Int64Counter(
		"bubu.stream.client_buffer.dropped_total",
		metric.WithDescription("Total messages dropped by SDK client buffer (oversize/overflow)"),
	)
	_ = err
	clientBufferFlushes, err = meter.Int64Counter(
		"bubu.stream.client_buffer.flushed_total",
		metric.WithDescription("Total messages flushed from SDK client buffer after reconnect"),
	)
	_ = err

	clientBufferAdds, err = meter.Int64Counter(
		"bubu.stream.client_buffer.added_total",
		metric.WithDescription("Total messages added to SDK client buffer due to transient errors"),
	)
	_ = err
}

// RecordHydrationSize records the size of hydrated data for observability.
func RecordHydrationSize(ctx context.Context, sizeBytes int64, stepRunID string) {
	hydrationSizeBytes.Record(ctx, sizeBytes,
		metric.WithAttributes(attribute.String("steprun_id", stepRunID)))
}

// RecordDehydrationSize records the size of dehydrated data for observability.
func RecordDehydrationSize(ctx context.Context, sizeBytes int64, stepRunID string) {
	dehydrationSizeBytes.Record(ctx, sizeBytes,
		metric.WithAttributes(attribute.String("steprun_id", stepRunID)))
}

// RecordStreamMessage increments the stream message counter.
// Direction should be "received" or "sent".
func RecordStreamMessage(ctx context.Context, direction string) {
	streamThroughput.Add(ctx, 1,
		metric.WithAttributes(attribute.String("direction", direction)))
}

// RecordK8sOperation records the duration of a Kubernetes API operation.
func RecordK8sOperation(ctx context.Context, operation string, durationSec float64, success bool) {
	k8sOperationDuration.Record(ctx, durationSec,
		metric.WithAttributes(
			attribute.String("operation", operation),
			attribute.Bool("success", success),
		))
}

// Client buffer metrics helpers

// RegisterClientBufferGauges registers async gauges for buffer size and bytes.
// The callbacks will be polled by the OTel SDK.
func RegisterClientBufferGauges(sizeFn func() float64, bytesFn func() float64) {
	// Best-effort registration; ignore errors to avoid impacting runtime
	clientBufferSizeGauge, _ = meter.Float64ObservableGauge(
		"bubu.stream.client_buffer.current_size",
		metric.WithDescription("Current number of messages buffered in SDK client"),
		metric.WithUnit("{messages}"),
		metric.WithFloat64Callback(func(_ context.Context, obs metric.Float64Observer) error {
			obs.Observe(sizeFn())
			return nil
		}),
	)
	clientBufferBytesGauge, _ = meter.Float64ObservableGauge(
		"bubu.stream.client_buffer.current_bytes",
		metric.WithDescription("Current total bytes buffered in SDK client"),
		metric.WithUnit("By"),
		metric.WithFloat64Callback(func(_ context.Context, obs metric.Float64Observer) error {
			obs.Observe(bytesFn())
			return nil
		}),
	)
	_ = clientBufferSizeGauge
	_ = clientBufferBytesGauge
}

// RecordClientBufferDrop increments drop counter with a reason label.
func RecordClientBufferDrop(ctx context.Context, reason string) {
	clientBufferDrops.Add(ctx, 1)
}

// RecordClientBufferFlush increments flush counter by count.
func RecordClientBufferFlush(ctx context.Context, count int) {
	if count > 0 {
		clientBufferFlushes.Add(ctx, int64(count))
	}
}

// RecordClientBufferAdded increments add counter with a reason label.
func RecordClientBufferAdded(ctx context.Context, reason string) {
	clientBufferAdds.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// Stream reconnect metrics helpers

// RecordStreamReconnectAttempt increments the reconnect attempts counter.
func RecordStreamReconnectAttempt(ctx context.Context) {
	streamReconnectAttempts.Add(ctx, 1)
}

// RecordStreamReconnectFailure increments the reconnect failures counter (terminal).
func RecordStreamReconnectFailure(ctx context.Context) {
	streamReconnectFailures.Add(ctx, 1)
}

// Custom metrics API for developers

// Counter creates a custom counter metric that developers can use in their engrams.
// Example:
//
//	counter, _ := metrics.Counter("myengram.records.processed_total", "Total records processed")
//	counter.Add(ctx, int64(len(records)))
func Counter(name, description string) (metric.Int64Counter, error) {
	return meter.Int64Counter(name, metric.WithDescription(description))
}

// Histogram creates a custom histogram metric for recording distributions.
// Example:
//
//	hist, _ := metrics.Histogram("myengram.processing.duration_seconds", "Processing duration", "s")
//	hist.Record(ctx, durationSec)
func Histogram(name, description, unit string) (metric.Float64Histogram, error) {
	return meter.Float64Histogram(name,
		metric.WithDescription(description),
		metric.WithUnit(unit))
}

// Gauge creates a custom gauge metric (async) that periodically calls the provided callback.
// Example:
//
//	metrics.Gauge("myengram.queue.size", "Current queue size", "items",
//	    func() float64 { return float64(len(queue)) })
func Gauge(name, description, unit string, callback func() float64) error {
	_, err := meter.Float64ObservableGauge(name,
		metric.WithDescription(description),
		metric.WithUnit(unit),
		metric.WithFloat64Callback(func(_ context.Context, obs metric.Float64Observer) error {
			obs.Observe(callback())
			return nil
		}))
	return err
}

// GetMeter returns the underlying OpenTelemetry meter for advanced use cases.
// Most developers should use Counter, Histogram, or Gauge instead.
func GetMeter() metric.Meter {
	return meter
}
