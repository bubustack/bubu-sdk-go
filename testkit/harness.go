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

package testkit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/bubustack/bubu-sdk-go/engram"
	"go.opentelemetry.io/otel/trace"
)

// BatchHarness runs a BatchEngram with explicit config/inputs and a synthetic ExecutionContext.
// It is intended for unit tests that want to exercise Init + Process without a controller.
type BatchHarness[C any, I any] struct {
	// Engram is the implementation under test.
	Engram engram.BatchEngram[C, I]
	// Config is passed to Engram.Init.
	Config C
	// Inputs is passed to Engram.Process.
	Inputs I
	// Secrets are expanded the same way as in SDK runtime before Init is called.
	Secrets map[string]string
	// StoryInfo seeds the synthetic execution context passed to Process.
	StoryInfo engram.StoryInfo
	// Logger overrides the default logger used in the synthetic execution context.
	Logger *slog.Logger
	// Tracer overrides the default no-op tracer used in the synthetic execution context.
	Tracer trace.Tracer
	// CELContext is injected into the synthetic execution context for template/CEL consumers.
	CELContext map[string]any
}

// Run executes Init + Process and returns the resulting output.
func (h BatchHarness[C, I]) Run(ctx context.Context) (*engram.Result, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if h.Engram == nil {
		return nil, errors.New("engram is nil")
	}
	logger := h.Logger
	if logger == nil {
		logger = slog.Default()
	}
	tracer := h.Tracer
	if tracer == nil {
		tracer = trace.NewNoopTracerProvider().Tracer("bubu-sdk-go/testkit") //nolint:staticcheck
	}
	secrets, err := engram.NewSecretsWithError(ctx, h.Secrets)
	if err != nil {
		return nil, fmt.Errorf("failed to expand secrets: %w", err)
	}
	if err := h.Engram.Init(ctx, h.Config, secrets); err != nil {
		return nil, err
	}
	execCtx := engram.NewExecutionContextWithCELContext(logger, tracer, h.StoryInfo, h.CELContext)
	return h.Engram.Process(ctx, execCtx, h.Inputs)
}

// StreamHarness runs a StreamingEngram with provided input messages and returns collected outputs.
// It mimics the SDK runtime by closing the output channel after Stream returns.
type StreamHarness[C any] struct {
	// Engram is the implementation under test.
	Engram engram.StreamingEngram[C]
	// Config is passed to Engram.Init.
	Config C
	// Secrets are expanded the same way as in SDK runtime before Init is called.
	Secrets map[string]string
	// StoryInfo seeds the synthetic execution context used by Stream.
	StoryInfo engram.StoryInfo
	// Logger overrides the default logger used in the synthetic execution context.
	Logger *slog.Logger
	// Tracer overrides the default no-op tracer used in the synthetic execution context.
	Tracer trace.Tracer
	// CELContext is injected into the synthetic execution context for template/CEL consumers.
	CELContext map[string]any
	// Inputs are converted to InboundMessages and delivered to Stream in order.
	Inputs []engram.StreamMessage
	// OnInputProcessed runs when an input message calls Done through the SDK
	// receipt hook. Tests can use this to verify acknowledgement behavior.
	OnInputProcessed func(engram.StreamMessage)
}

// Run executes Init + Stream and returns the collected output messages.
func (h StreamHarness[C]) Run(ctx context.Context) ([]engram.StreamMessage, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if h.Engram == nil {
		return nil, errors.New("engram is nil")
	}
	logger := h.Logger
	if logger == nil {
		logger = slog.Default() //nolint:ineffassign,staticcheck
	}
	tracer := h.Tracer
	if tracer == nil {
		tracer = trace.NewNoopTracerProvider().Tracer("bubu-sdk-go/testkit") //nolint:ineffassign,staticcheck
	}
	secrets, err := engram.NewSecretsWithError(ctx, h.Secrets)
	if err != nil {
		return nil, fmt.Errorf("failed to expand secrets: %w", err)
	}
	if err := h.Engram.Init(ctx, h.Config, secrets); err != nil {
		return nil, err
	}

	in := make(chan engram.InboundMessage, len(h.Inputs))
	out := make(chan engram.StreamMessage, len(h.Inputs))
	for _, msg := range h.Inputs {
		inbound := engram.NewInboundMessage(msg)
		if h.OnInputProcessed != nil {
			current := msg
			inbound = engram.BindProcessingReceipt(inbound, func() {
				h.OnInputProcessed(current)
			})
		}
		in <- inbound
	}
	close(in)

	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		defer func() {
			if recovered := recover(); recovered != nil {
				errCh <- fmt.Errorf("engram stream panicked: %v", recovered)
			}
		}()
		errCh <- h.Engram.Stream(ctx, in, out)
	}()

	outputs := make([]engram.StreamMessage, 0)
	for msg := range out {
		outputs = append(outputs, msg)
	}

	return outputs, <-errCh
}
