// package engram defines the core interfaces that developers implement to create
// components for the bobrapet ecosystem. These interfaces provide a structured,
// type-safe framework for building everything from simple, single-task jobs to
// complex, long-running event listeners.
package engram

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

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

// NewSecrets creates a new Secrets object. This is used internally by the SDK.
func NewSecrets(rawSecrets map[string]string) *Secrets {
	if rawSecrets == nil {
		rawSecrets = make(map[string]string)
	}
	// Expand descriptor-style secrets provided via env by the operator.
	// Supported formats for values in rawSecrets (from BUBU_SECRET_* envs):
	//   - "file:<dirPath>"  → load all files in the directory as key/value
	//   - "env:<PREFIX>"    → load all environment variables starting with PREFIX
	// Any other value is treated as a literal and stored under its key as-is.
	expanded := make(map[string]string, len(rawSecrets))

	for logicalName, descriptor := range rawSecrets {
		switch {
		case strings.HasPrefix(descriptor, "file:"):
			dirPath := strings.TrimPrefix(descriptor, "file:")
			// Best-effort directory read; do not fail hard for individual files
			_ = filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					// Skip unreadable entries; callers can inspect logs if needed
					return nil
				}
				if d.IsDir() {
					return nil
				}
				// Use filename as the secret key
				key := filepath.Base(path)
				// Read the file content
				b, readErr := os.ReadFile(path)
				if readErr != nil {
					return nil
				}
				expanded[key] = strings.TrimRight(string(b), "\n")
				return nil
			})

		case strings.HasPrefix(descriptor, "env:"):
			prefix := strings.TrimPrefix(descriptor, "env:")
			for _, env := range os.Environ() {
				parts := strings.SplitN(env, "=", 2)
				if len(parts) != 2 {
					continue
				}
				name, value := parts[0], parts[1]
				if strings.HasPrefix(name, prefix) {
					key := strings.TrimPrefix(name, prefix)
					if key == "" {
						continue
					}
					expanded[key] = value
				}
			}

		default:
			// Literal value; store under the provided logical key
			expanded[logicalName] = descriptor
		}
	}

	// If expansion yielded nothing (e.g., invalid descriptors), fall back to rawSecrets
	if len(expanded) == 0 && len(rawSecrets) > 0 {
		// Keep behavior backwards compatible
		return &Secrets{rawSecrets: rawSecrets}
	}
	return &Secrets{rawSecrets: expanded}
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
	// Metadata contains tracing information (StoryRunID, StepName, etc.) from DataPacket.
	// This should be propagated through the streaming pipeline to maintain observability.
	Metadata map[string]string
	// Payload is the JSON-encoded data to be processed.
	Payload []byte
	// Inputs contains the evaluated step 'with:' configuration (CEL-resolved per packet).
	// This is analogous to BUBU_INPUTS in batch mode - dynamic configuration that can
	// reference outputs from previous steps. The Hub evaluates this before forwarding.
	// Empty if the step has no 'with:' block or evaluation failed.
	Inputs []byte
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
