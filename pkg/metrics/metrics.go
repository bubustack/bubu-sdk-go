/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package metrics provides OpenTelemetry-based observability for the SDK and allows
// developers to register custom metrics for their engrams and impulses.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/bubustack/bubu-sdk-go/pkg/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

var (
	meter metric.Meter = metricnoop.NewMeterProvider().Meter("bubu-sdk")

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
	streamReconnectAttempts    metric.Int64Counter
	streamReconnectFailures    metric.Int64Counter
	streamBackpressureTimeouts metric.Int64Counter

	metricsDisabled bool

	clientBufferObserversMu    sync.RWMutex
	clientBufferObserverSeq    int64
	clientBufferSizeObservers  = make(map[int64]func() float64)
	clientBufferBytesObservers = make(map[int64]func() float64)
)

func init() {
	if err := initializeMetrics(); err != nil {
		log.Printf("bubu sdk: metrics disabled during initialization: %v", err)
	}
}

func initializeMetrics() error {
	if !observability.MetricsEnabled() {
		disableMetrics()
		return nil
	}
	return initializeMetricsWithMeter(otel.Meter("bubu-sdk"))
}

func initializeMetricsWithMeter(m metric.Meter) error {
	resetMetricInstruments()
	meter = m
	metricsDisabled = false

	var errs []error
	var err error
	hydrationSizeBytes, err = meter.Int64Histogram(
		"bubu.storage.hydration.size_bytes",
		metric.WithDescription("Size of hydrated data in bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("create bubu.storage.hydration.size_bytes: %w", err))
	}

	dehydrationSizeBytes, err = meter.Int64Histogram(
		"bubu.storage.dehydration.size_bytes",
		metric.WithDescription("Size of dehydrated data in bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("create bubu.storage.dehydration.size_bytes: %w", err))
	}

	streamThroughput, err = meter.Int64Counter(
		"bubu.stream.messages_total",
		metric.WithDescription("Total messages processed in stream"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("create bubu.stream.messages_total: %w", err))
	}

	k8sOperationDuration, err = meter.Float64Histogram(
		"bubu.k8s.operation.duration_seconds",
		metric.WithDescription("Duration of Kubernetes API operations"),
		metric.WithUnit("s"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("create bubu.k8s.operation.duration_seconds: %w", err))
	}

	// Reconnect attempt/failure counters for streaming client
	streamReconnectAttempts, err = meter.Int64Counter(
		"bubu.stream.reconnect.attempts_total",
		metric.WithDescription("Total reconnect attempts by SDK client"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("create bubu.stream.reconnect.attempts_total: %w", err))
	}
	streamReconnectFailures, err = meter.Int64Counter(
		"bubu.stream.reconnect.failures_total",
		metric.WithDescription("Total reconnect failures (terminal) by SDK client"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("create bubu.stream.reconnect.failures_total: %w", err))
	}

	// Client buffer: size/bytes gauges are registered by the buffer with callbacks
	clientBufferDrops, err = meter.Int64Counter(
		"bubu.stream.client_buffer.dropped_total",
		metric.WithDescription("Total messages dropped by SDK client buffer (oversize/overflow)"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("create bubu.stream.client_buffer.dropped_total: %w", err))
	}
	clientBufferFlushes, err = meter.Int64Counter(
		"bubu.stream.client_buffer.flushed_total",
		metric.WithDescription("Total messages flushed from SDK client buffer after reconnect"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("create bubu.stream.client_buffer.flushed_total: %w", err))
	}

	clientBufferAdds, err = meter.Int64Counter(
		"bubu.stream.client_buffer.added_total",
		metric.WithDescription("Total messages added to SDK client buffer due to transient errors"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("create bubu.stream.client_buffer.added_total: %w", err))
	}

	streamBackpressureTimeouts, err = meter.Int64Counter(
		"bubu.stream.backpressure.timeouts_total",
		metric.WithDescription("Total timeouts encountered while delivering stream messages due to backpressure"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("create bubu.stream.backpressure.timeouts_total: %w", err))
	}

	if len(errs) > 0 {
		disableMetrics()
		return errors.Join(errs...)
	}
	return nil
}

func disableMetrics() {
	resetMetricInstruments()
	meter = metricnoop.NewMeterProvider().Meter("bubu-sdk")
	metricsDisabled = true
}

func resetMetricInstruments() {
	var (
		int64Histogram    metric.Int64Histogram
		int64Counter      metric.Int64Counter
		float64Histogram  metric.Float64Histogram
		float64Observable metric.Float64ObservableGauge
	)
	hydrationSizeBytes = int64Histogram
	dehydrationSizeBytes = int64Histogram
	streamThroughput = int64Counter
	k8sOperationDuration = float64Histogram
	clientBufferAdds = int64Counter
	clientBufferSizeGauge = float64Observable
	clientBufferBytesGauge = float64Observable
	clientBufferDrops = int64Counter
	clientBufferFlushes = int64Counter
	streamReconnectAttempts = int64Counter
	streamReconnectFailures = int64Counter
	streamBackpressureTimeouts = int64Counter
}

// RecordHydrationSize records the size of hydrated data for observability.
func RecordHydrationSize(ctx context.Context, sizeBytes int64) {
	if metricsDisabled {
		return
	}
	hydrationSizeBytes.Record(ctx, sizeBytes)
}

// RecordDehydrationSize records the size of dehydrated data for observability.
func RecordDehydrationSize(ctx context.Context, sizeBytes int64) {
	if metricsDisabled {
		return
	}
	dehydrationSizeBytes.Record(ctx, sizeBytes)
}

// RecordStreamMessage increments the stream message counter.
// Direction should be "received" or "sent".
func RecordStreamMessage(ctx context.Context, direction string) {
	if metricsDisabled {
		return
	}
	streamThroughput.Add(ctx, 1,
		metric.WithAttributes(attribute.String("direction", direction)))
}

// RecordK8sOperation records the duration of a Kubernetes API operation.
func RecordK8sOperation(ctx context.Context, operation string, durationSec float64, success bool) {
	if metricsDisabled {
		return
	}
	k8sOperationDuration.Record(ctx, durationSec,
		metric.WithAttributes(
			attribute.String("operation", operation),
			attribute.Bool("success", success),
		))
}

// Client buffer metrics helpers

// RegisterClientBufferGauges registers async gauges for buffer size and bytes.
// The callbacks will be polled by the OTel SDK.
func RegisterClientBufferGauges(sizeFn func() float64, bytesFn func() float64) func() {
	if metricsDisabled {
		return func() {}
	}
	clientBufferObserversMu.Lock()
	defer clientBufferObserversMu.Unlock()
	if clientBufferSizeGauge == nil {
		clientBufferSizeGauge, _ = meter.Float64ObservableGauge(
			"bubu.stream.client_buffer.current_size",
			metric.WithDescription("Current number of messages buffered in SDK client"),
			metric.WithUnit("{messages}"),
			metric.WithFloat64Callback(observeClientBufferSizes),
		)
	}
	if clientBufferBytesGauge == nil {
		clientBufferBytesGauge, _ = meter.Float64ObservableGauge(
			"bubu.stream.client_buffer.current_bytes",
			metric.WithDescription("Current total bytes buffered in SDK client"),
			metric.WithUnit("By"),
			metric.WithFloat64Callback(observeClientBufferBytes),
		)
	}
	id := clientBufferObserverSeq
	clientBufferObserverSeq++
	clientBufferSizeObservers[id] = sizeFn
	clientBufferBytesObservers[id] = bytesFn
	return func() {
		clientBufferObserversMu.Lock()
		delete(clientBufferSizeObservers, id)
		delete(clientBufferBytesObservers, id)
		clientBufferObserversMu.Unlock()
	}
}

// observeClientBufferSizes iterates all registered client buffer size
// callbacks under an RLock and records their values via the provided OTel
// observer (`pkg/metrics/metrics.go:179-223`).
func observeClientBufferSizes(_ context.Context, obs metric.Float64Observer) error {
	clientBufferObserversMu.RLock()
	defer clientBufferObserversMu.RUnlock()
	for id, fn := range clientBufferSizeObservers {
		if fn == nil {
			continue
		}
		func(observerID int64, sizeFn func() float64) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("bubu sdk: client buffer size callback %d panicked: %v", observerID, r)
				}
			}()
			obs.Observe(sizeFn())
		}(id, fn)
	}
	return nil
}

func observeClientBufferBytes(_ context.Context, obs metric.Float64Observer) error {
	clientBufferObserversMu.RLock()
	defer clientBufferObserversMu.RUnlock()
	for id, fn := range clientBufferBytesObservers {
		if fn == nil {
			continue
		}
		func(observerID int64, bytesFn func() float64) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("bubu sdk: client buffer bytes callback %d panicked: %v", observerID, r)
				}
			}()
			obs.Observe(bytesFn())
		}(id, fn)
	}
	return nil
}

// RecordClientBufferDrop increments drop counter with a reason label.
func RecordClientBufferDrop(ctx context.Context, reason string) {
	if metricsDisabled {
		return
	}
	clientBufferDrops.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// RecordClientBufferFlush increments flush counter by count.
func RecordClientBufferFlush(ctx context.Context, count int) {
	if metricsDisabled || count <= 0 {
		return
	}
	clientBufferFlushes.Add(ctx, int64(count))
}

// RecordClientBufferAdded increments add counter with a reason label.
func RecordClientBufferAdded(ctx context.Context, reason string) {
	if metricsDisabled {
		return
	}
	clientBufferAdds.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// Stream reconnect metrics helpers

// RecordStreamReconnectAttempt increments the reconnect attempts counter with reason labeling.
func RecordStreamReconnectAttempt(ctx context.Context, reason string) {
	if metricsDisabled {
		return
	}
	streamReconnectAttempts.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// RecordStreamReconnectFailure increments the reconnect failures counter (terminal) with reason labeling.
func RecordStreamReconnectFailure(ctx context.Context, reason string) {
	if metricsDisabled {
		return
	}
	streamReconnectFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// RecordStreamBackpressureTimeout records occurrences of backpressure timeouts.
// Stage should denote where the timeout happened (e.g., "client_receiver", "server_reader").
func RecordStreamBackpressureTimeout(ctx context.Context, stage string) {
	if metricsDisabled {
		return
	}
	streamBackpressureTimeouts.Add(ctx, 1, metric.WithAttributes(attribute.String("stage", stage)))
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
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("bubu sdk: gauge %q callback panicked: %v", name, r)
					}
				}()
				obs.Observe(callback())
			}()
			return nil
		}))
	return err
}

// GetMeter returns the underlying OpenTelemetry meter for advanced use cases.
// Most developers should use Counter, Histogram, or Gauge instead.
func GetMeter() metric.Meter {
	return meter
}
