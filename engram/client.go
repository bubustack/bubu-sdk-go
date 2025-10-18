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

// package engram defines the core interfaces that developers implement to create
// components for the bobrapet ecosystem. These interfaces provide a structured,
// type-safe framework for building everything from simple, single-task jobs to
// complex, long-running event listeners.
package engram

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bubustack/bubu-sdk-go/k8s"
	"go.opentelemetry.io/otel/trace"
)

// Secrets provides sandboxed access to the secrets mapped to an Engram's StepRun.
// The SDK runtime populates this object from environment variables injected by the
// bobrapet controller, ensuring that Engrams only have access to the secrets
// explicitly declared in their corresponding Step definition in the Story.
type Secrets struct {
	rawSecrets map[string]string
}

// NewSecrets creates a new Secrets object, expanding descriptor-style env/file references.
// Callers must pass a non-nil context; cancellation stops in-flight I/O and returns the
// secrets collected so far to honor shutdown deadlines.
func NewSecrets(ctx context.Context, rawSecrets map[string]string) *Secrets {
	if ctx == nil {
		panic("engram.NewSecrets requires a non-nil context")
	}
	if rawSecrets == nil {
		rawSecrets = make(map[string]string)
	}
	if len(rawSecrets) == 0 {
		return &Secrets{rawSecrets: rawSecrets}
	}

	logger := slog.Default()
	expanded := make(map[string]string, len(rawSecrets))

	for logicalName, descriptor := range rawSecrets {
		if err := ctx.Err(); err != nil {
			logger.Warn("secret expansion interrupted", "error", err)
			return &Secrets{rawSecrets: expanded}
		}
		switch {
		case strings.HasPrefix(descriptor, "file:"):
			if err := expandSecretsFromFiles(ctx, descriptor, expanded, logger); err != nil {
				if errors.Is(err, context.Canceled) {
					logger.Warn("secret file expansion canceled", "descriptor", descriptor, "error", err)
					return &Secrets{rawSecrets: expanded}
				}
				logger.Warn("secret file expansion incomplete", "descriptor", descriptor, "error", err)
			}
		case strings.HasPrefix(descriptor, "env:"):
			if err := expandSecretsFromEnv(ctx, descriptor, expanded, logger); err != nil {
				logger.Warn("secret env expansion canceled", "descriptor", descriptor, "error", err)
				return &Secrets{rawSecrets: expanded}
			}
		default:
			expanded[logicalName] = descriptor
		}
	}

	if len(expanded) == 0 && len(rawSecrets) > 0 {
		return &Secrets{rawSecrets: rawSecrets}
	}
	return &Secrets{rawSecrets: expanded}
}

func expandSecretsFromFiles(
	ctx context.Context,
	descriptor string,
	dest map[string]string,
	logger *slog.Logger,
) error {
	dirPath := strings.TrimPrefix(descriptor, "file:")
	return filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			logger.Warn("failed to read secret file", "path", path, "error", err)
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		key := filepath.Base(path)
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			logger.Warn("failed to load secret file content", "path", path, "error", readErr)
			return nil
		}
		dest[key] = strings.TrimRight(string(content), "\n")
		return nil
	})
}

func expandSecretsFromEnv(
	ctx context.Context,
	descriptor string,
	dest map[string]string,
	logger *slog.Logger,
) error {
	prefix := strings.TrimPrefix(descriptor, "env:")
	for _, envVar := range os.Environ() {
		if err := ctx.Err(); err != nil {
			return err
		}
		parts := strings.SplitN(envVar, "=", 2)
		if len(parts) != 2 {
			continue
		}
		name, value := parts[0], parts[1]
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		key := strings.TrimPrefix(name, prefix)
		if key == "" {
			logger.Warn("secret env prefix matched empty key", "prefix", prefix, "envVar", name)
			continue
		}
		dest[key] = value
	}
	return nil
}

// Get returns a specific secret by its key.
func (s *Secrets) Get(key string) (string, bool) {
	val, ok := s.rawSecrets[key]
	return val, ok
}

// GetAll returns a copy of the secret keys. The values are redacted
// to prevent accidental logging of sensitive data.
func (s *Secrets) GetAll() map[string]string {
	redacted := make(map[string]string, len(s.rawSecrets))
	for k := range s.rawSecrets {
		redacted[k] = "[REDACTED]"
	}
	return redacted
}

// Format implements fmt.Formatter to prevent accidental logging of secrets.
// It ensures that printing the Secrets struct (e.g., with %+v) does not leak values.
func (s *Secrets) Format(f fmt.State, verb rune) {
	_, _ = f.Write([]byte("[redacted secrets]"))
}

// Raw returns the underlying raw secrets map. This should be used with extreme
// caution and only when direct, unredacted access is absolutely necessary.
// It is the developer's responsibility to ensure these values are not logged.
func (s *Secrets) Raw() map[string]string {
	return s.rawSecrets
}

// ExecutionContext provides metadata and utilities for a single execution of an Engram.
// It serves as a dependency injection container for services provided by the SDK
// runtime, such as logging, tracing, and information about the current Story.
// This context is passed to the `Process` method of a `BatchEngram`.
type ExecutionContext struct {
	logger    *slog.Logger
	tracer    trace.Tracer
	storyInfo StoryInfo
}

// NewExecutionContext is a constructor used internally by the SDK runtime.
func NewExecutionContext(logger *slog.Logger, tracer trace.Tracer, storyInfo StoryInfo) *ExecutionContext {
	return &ExecutionContext{
		logger:    logger,
		tracer:    tracer,
		storyInfo: storyInfo,
	}
}

// StoryInfo contains metadata about the currently executing Story and Step.
type StoryInfo struct {
	StoryName        string `json:"storyName"`
	StoryRunID       string `json:"storyRunID"`
	StepName         string `json:"stepName"`
	StepRunID        string `json:"stepRunID"`
	StepRunNamespace string `json:"stepRunNamespace"`
}

// StoryInfo returns metadata about the currently executing Story and Step.
//
// This includes the Story name, StoryRun ID, Step name, and StepRun ID,
// which are useful for logging, tracing, and correlation.
func (e *ExecutionContext) StoryInfo() StoryInfo {
	return e.storyInfo
}

// Logger returns the slog.Logger configured for this execution context.
//
// This logger should be used for all engram logging to ensure consistent
// formatting and integration with the SDK's observability stack.
func (e *ExecutionContext) Logger() *slog.Logger {
	return e.logger
}

// Tracer returns the OpenTelemetry tracer for this execution context.
//
// Use this tracer to create spans for internal operations, enabling
// distributed tracing across the execution pipeline.
func (e *ExecutionContext) Tracer() trace.Tracer {
	return e.tracer
}

// Result is the universal return type for a BatchEngram's Process method.
// It encapsulates the output data. The SDK uses this structure to determine
// the output of the step.
type Result struct {
	// Data is the output of the Engram. This can be any serializable type.
	// The SDK will automatically handle JSON marshaling and, if configured,
	// transparently offload the data to external storage if it exceeds the
	// size threshold.
	Data any
}

// NewResultFrom wraps the provided data in a Result. It keeps examples and callers
// working with a single helper so future metadata can be attached centrally.
func NewResultFrom(data any) *Result {
	return &Result{Data: data}
}

// Engram is the foundational interface for all executable components in bobrapet.
// It establishes a common initialization contract.
//
// The generic parameter `C` represents a developer-defined struct for static
// configuration. The SDK runtime will automatically unmarshal the `with` block
// from the Engram or Impulse resource's YAML definition into this struct and pass
// it to the `Init` method.
type Engram[C any] interface {
	// Init is called once at the start of an Engram's lifecycle. It is the ideal
	// place for setting up connections, validating configuration, or performing
	// any other one-time setup.
	Init(ctx context.Context, config C, secrets *Secrets) error
}

// BatchEngram is the interface for components that run as a single, finite task,
// typically executed as a Kubernetes Job. This is the most common type of Engram,
// used for data processing, API calls, and other script-like operations.
//
// The generic parameters `C` (configuration) and `I` (inputs) allow for
// complete type safety from the YAML definition to your Go code.
type BatchEngram[C any, I any] interface {
	Engram[C]
	// Process contains the core business logic of the Engram. It is called exactly
	// once per execution. The SDK provides the typed inputs `I`, and expects a
	// `Result` in return, which it uses to report the output and status of the
	// StepRun. An error should be returned for any failures.
	Process(ctx context.Context, execCtx *ExecutionContext, inputs I) (*Result, error)
}

// Impulse is the interface for long-running components that act as triggers for
// Stories. They typically listen for external events (e.g., via webhooks,
// message queues) and use the provided Kubernetes client to create new StoryRuns.
// An Impulse usually runs as a Kubernetes Deployment.
type Impulse[C any] interface {
	Engram[C]
	// Run is the main entry point for the Impulse's long-running process. The SDK
	// calls this method after `Init`, providing a pre-configured Kubernetes client
	// for interacting with bobrapet resources (like creating StoryRuns). The method
	// should block until the Impulse's work is complete or the context is canceled.
	Run(ctx context.Context, client *k8s.Client) error
}

// StreamMessage represents a single message in a bidirectional stream with metadata.
// Metadata enables tracing and correlation across streaming pipeline steps.
type StreamMessage struct {
	// Kind declares the semantic intent of the packet (e.g., "data", "heartbeat").
	Kind string
	// MessageID is an optional caller-defined identifier that assists with deduplication.
	MessageID string
	// Timestamp captures when the packet was produced. Zero-value timestamps are omitted.
	Timestamp time.Time
	// Metadata contains tracing information (StoryRunID, StepName, etc.) from DataPacket.
	// This should be propagated through the streaming pipeline to maintain observability.
	Metadata map[string]string
	// Payload is the JSON-encoded data to be processed. Prefer Binary for new code paths.
	Payload []byte
	// Audio carries PCM audio frames when present.
	Audio *AudioFrame
	// Video carries encoded or raw video frames when present.
	Video *VideoFrame
	// Binary carries arbitrary non-audio/video frames when present.
	Binary *BinaryFrame
	// Inputs contains the evaluated step 'with:' configuration (CEL-resolved per packet).
	// This is analogous to BUBU_INPUTS in batch mode - dynamic configuration that can
	// reference outputs from previous steps. The Hub evaluates this before forwarding.
	// Empty if the step has no 'with:' block or evaluation failed.
	Inputs []byte
	// Transports mirrors the Story's declared transports, allowing engrams to decide whether
	// to keep payloads on the hot path (e.g., LiveKit) or fall back to storage without
	// rereading pod environment.
	Transports []TransportDescriptor
}

// StreamingEngram is the interface for components that handle real-time,
// continuous data streams. They are typically used for tasks like transformations,
// filtering, or routing of data between other streaming systems. A StreamingEngram
// usually runs as a Kubernetes Deployment and communicates over gRPC.
type StreamingEngram[C any] interface {
	Engram[C]
	// Stream is the core method for handling bidirectional data flow with metadata.
	// The SDK provides channels for receiving and sending StreamMessage which includes
	// both payload and metadata. Metadata should be propagated to enable tracing across
	// the streaming pipeline. The method should process messages from `in` and write
	// results to `out` until the input channel is closed or the context is canceled.
	Stream(ctx context.Context, in <-chan StreamMessage, out chan<- StreamMessage) error
}
