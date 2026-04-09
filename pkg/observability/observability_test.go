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

package observability

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/bubustack/core/contracts"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func resetForTest(t *testing.T) {
	t.Helper()
	resetConfigForTesting()
}

func TestMetricsEnabledFromEnv(t *testing.T) {
	t.Setenv(contracts.SDKMetricsEnabledEnv, "false")
	resetForTest(t)
	if MetricsEnabled() {
		t.Fatalf("expected metrics disabled via env")
	}
}

func TestTracingEnabledDefault(t *testing.T) {
	_ = os.Unsetenv(contracts.SDKTracingEnabledEnv)
	resetForTest(t)
	if !TracingEnabled() {
		t.Fatalf("expected tracing enabled by default")
	}
}

func TestTracePropagationEnabledInjectsHeadersWhenEnabled(t *testing.T) {
	t.Setenv(contracts.TracePropagationEnv, "true")
	resetForTest(t)

	if !TracePropagationEnabled() {
		t.Fatalf("expected trace propagation enabled")
	}

	traceID, err := trace.TraceIDFromHex("00112233445566778899aabbccddeeff")
	if err != nil {
		t.Fatalf("failed to create trace ID: %v", err)
	}
	spanID, err := trace.SpanIDFromHex("0011223344556677")
	if err != nil {
		t.Fatalf("failed to create span ID: %v", err)
	}

	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	}))
	member, err := baggage.NewMember("tenant", "engram")
	if err != nil {
		t.Fatalf("failed to create baggage member: %v", err)
	}
	bg, err := baggage.New(member)
	if err != nil {
		t.Fatalf("failed to create baggage: %v", err)
	}
	ctx = baggage.ContextWithBaggage(ctx, bg)

	carrier := propagation.MapCarrier{}
	Propagator().Inject(ctx, carrier)

	if carrier.Get("traceparent") == "" {
		t.Fatalf("expected traceparent header to be injected when propagation is enabled")
	}
	if carrier.Get("baggage") == "" {
		t.Fatalf("expected baggage header to be injected when propagation is enabled")
	}
}

func TestTracePropagationDisabledSkipsHeaderInjection(t *testing.T) {
	t.Setenv(contracts.TracePropagationEnv, "false")
	resetForTest(t)

	if TracePropagationEnabled() {
		t.Fatalf("expected trace propagation disabled")
	}

	traceID, err := trace.TraceIDFromHex("00112233445566778899aabbccddeeff")
	if err != nil {
		t.Fatalf("failed to create trace ID: %v", err)
	}
	spanID, err := trace.SpanIDFromHex("0011223344556677")
	if err != nil {
		t.Fatalf("failed to create span ID: %v", err)
	}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	}))

	carrier := propagation.MapCarrier{}
	Propagator().Inject(ctx, carrier)

	if carrier.Get("traceparent") != "" {
		t.Fatalf("did not expect traceparent header when propagation is disabled")
	}
	if carrier.Get("baggage") != "" {
		t.Fatalf("did not expect baggage header when propagation is disabled")
	}
}

func TestTracePropagationEnabledRoundTripExtractsTraceAndBaggage(t *testing.T) {
	t.Setenv(contracts.TracePropagationEnv, "true")
	resetForTest(t)

	if !TracePropagationEnabled() {
		t.Fatalf("expected trace propagation enabled")
	}

	traceID, err := trace.TraceIDFromHex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("failed to create trace ID: %v", err)
	}
	spanID, err := trace.SpanIDFromHex("bbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatalf("failed to create span ID: %v", err)
	}

	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	}))
	member, err := baggage.NewMember("tenant", "engram")
	if err != nil {
		t.Fatalf("failed to create baggage member: %v", err)
	}
	bg, err := baggage.New(member)
	if err != nil {
		t.Fatalf("failed to create baggage: %v", err)
	}
	ctx = baggage.ContextWithBaggage(ctx, bg)

	carrier := propagation.MapCarrier{}
	Propagator().Inject(ctx, carrier)
	extracted := Propagator().Extract(context.Background(), carrier)

	extractedSpan := trace.SpanContextFromContext(extracted)
	if !extractedSpan.IsValid() {
		t.Fatalf("expected extracted span context to be valid")
	}
	if extractedSpan.TraceID() != traceID {
		t.Fatalf("trace ID mismatch: got %s want %s", extractedSpan.TraceID(), traceID)
	}
	if extractedSpan.SpanID() != spanID {
		t.Fatalf("span ID mismatch: got %s want %s", extractedSpan.SpanID(), spanID)
	}
	if got := baggage.FromContext(extracted).Member("tenant").Value(); got != "engram" {
		t.Fatalf("baggage mismatch: got %q want %q", got, "engram")
	}
}

func TestTracePropagationDisabledIgnoresInboundHeadersOnExtract(t *testing.T) {
	t.Setenv(contracts.TracePropagationEnv, "false")
	resetForTest(t)

	if TracePropagationEnabled() {
		t.Fatalf("expected trace propagation disabled")
	}

	carrier := propagation.MapCarrier{
		"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
		"baggage":     "tenant=engram",
	}

	extracted := Propagator().Extract(context.Background(), carrier)
	if trace.SpanContextFromContext(extracted).IsValid() {
		t.Fatalf("did not expect valid extracted span context when propagation is disabled")
	}
	if members := baggage.FromContext(extracted).Members(); len(members) != 0 {
		t.Fatalf("did not expect extracted baggage when propagation is disabled")
	}
}

func TestParseBoolEnvWarnsOnInvalidValue(t *testing.T) {
	t.Setenv(contracts.TracePropagationEnv, "definitely-not-bool")
	resetForTest(t)

	var buf bytes.Buffer
	prevLogger := slog.Default()
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)
	t.Cleanup(func() {
		slog.SetDefault(prevLogger)
	})

	if !TracePropagationEnabled() {
		t.Fatalf("expected invalid value to fall back to default true")
	}
	if got := buf.String(); got == "" || !strings.Contains(got, contracts.TracePropagationEnv) {
		t.Fatalf("expected warning log for invalid env, got %q", got)
	}
}
