package metrics

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

type failingMeter struct {
	metricnoop.Meter
}

func (failingMeter) Int64Histogram(name string, options ...metric.Int64HistogramOption) (metric.Int64Histogram, error) {
	return nil, errors.New("boom")
}

func TestInitializeMetricsWithMeter_DisablesMetricsOnInstrumentError(t *testing.T) {
	prevMeter := meter
	prevDisabled := metricsDisabled
	prevHydration := hydrationSizeBytes
	prevDehydration := dehydrationSizeBytes
	prevStream := streamThroughput
	prevK8s := k8sOperationDuration
	prevAdds := clientBufferAdds
	prevSizeGauge := clientBufferSizeGauge
	prevBytesGauge := clientBufferBytesGauge
	prevDrops := clientBufferDrops
	prevFlushes := clientBufferFlushes
	prevReconnectAttempts := streamReconnectAttempts
	prevReconnectFailures := streamReconnectFailures
	prevBackpressure := streamBackpressureTimeouts
	t.Cleanup(func() {
		meter = prevMeter
		metricsDisabled = prevDisabled
		hydrationSizeBytes = prevHydration
		dehydrationSizeBytes = prevDehydration
		streamThroughput = prevStream
		k8sOperationDuration = prevK8s
		clientBufferAdds = prevAdds
		clientBufferSizeGauge = prevSizeGauge
		clientBufferBytesGauge = prevBytesGauge
		clientBufferDrops = prevDrops
		clientBufferFlushes = prevFlushes
		streamReconnectAttempts = prevReconnectAttempts
		streamReconnectFailures = prevReconnectFailures
		streamBackpressureTimeouts = prevBackpressure
	})

	err := initializeMetricsWithMeter(failingMeter{})
	if err == nil {
		t.Fatal("expected metrics initialization to fail")
	}
	if !metricsDisabled {
		t.Fatal("expected metrics to be disabled after initialization failure")
	}

	RecordHydrationSize(context.Background(), 1)
	RecordStreamMessage(context.Background(), "sent")
}

func TestInitializeMetricsWithMeter_SucceedsWithNoopMeter(t *testing.T) {
	prevMeter := meter
	prevDisabled := metricsDisabled
	t.Cleanup(func() {
		meter = prevMeter
		metricsDisabled = prevDisabled
	})

	err := initializeMetricsWithMeter(metricnoop.NewMeterProvider().Meter("test"))
	if err != nil {
		t.Fatalf("expected metrics initialization to succeed: %v", err)
	}
	if metricsDisabled {
		t.Fatal("expected metrics to remain enabled")
	}

	unregister := RegisterClientBufferGauges(func() float64 { return 1 }, func() float64 { return 2 })
	if unregister == nil {
		t.Fatal("expected unregister callback")
	}
	unregister()
}
