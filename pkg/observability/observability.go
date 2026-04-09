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
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/bubustack/core/contracts"
	"github.com/bubustack/core/runtime/featuretoggles"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

var (
	initOnce                sync.Once
	metricsEnabled          bool
	tracingEnabled          bool
	tracePropagationEnabled bool
	prop                    propagation.TextMapPropagator
	noopTracerProvider      = noop.NewTracerProvider()
)

func resetConfigForTesting() {
	initOnce = sync.Once{}
	metricsEnabled = false
	tracingEnabled = false
	tracePropagationEnabled = false
	prop = nil
}

func initConfig() {
	toggles := featuretoggles.Features{
		MetricsEnabled:          parseBoolEnv(contracts.SDKMetricsEnabledEnv, true),
		TelemetryEnabled:        parseBoolEnv(contracts.SDKTracingEnabledEnv, true),
		TracePropagationEnabled: parseBoolEnv(contracts.TracePropagationEnv, true),
	}

	featuretoggles.Apply(toggles, featuretoggles.Sink{
		EnableTelemetry: func(enabled bool) {
			tracingEnabled = enabled
		},
		EnableTracePropagation: func(enabled bool) {
			tracePropagationEnabled = enabled
			if enabled {
				prop = propagation.NewCompositeTextMapPropagator(
					propagation.TraceContext{},
					propagation.Baggage{},
				)
			} else {
				prop = propagation.NewCompositeTextMapPropagator()
			}
		},
		EnableMetrics: func(enabled bool) {
			metricsEnabled = enabled
		},
	})
}

// MetricsEnabled reports whether SDK metrics emission is enabled. Defaults to true.
func MetricsEnabled() bool {
	initOnce.Do(initConfig)
	return metricsEnabled
}

// TracingEnabled reports whether SDK tracing is enabled. Defaults to true.
func TracingEnabled() bool {
	initOnce.Do(initConfig)
	return tracingEnabled
}

// TracePropagationEnabled reports whether OTEL propagators are active.
func TracePropagationEnabled() bool {
	initOnce.Do(initConfig)
	return tracePropagationEnabled
}

// Propagator returns the configured propagator without mutating the process-global OTEL state.
func Propagator() propagation.TextMapPropagator {
	initOnce.Do(initConfig)
	if prop != nil {
		return prop
	}
	return otel.GetTextMapPropagator()
}

// Tracer returns a trace.Tracer that honors the tracing toggle.
func Tracer(name string) trace.Tracer {
	if !TracingEnabled() {
		return noopTracerProvider.Tracer(name)
	}
	return otel.Tracer(name)
}

func parseBoolEnv(key string, def bool) bool {
	val, ok := os.LookupEnv(key)
	if !ok || val == "" {
		return def
	}
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	default:
		slog.Default().Warn("ignoring invalid observability env override", "env", key, "value", val, "default", def)
		return def
	}
}
