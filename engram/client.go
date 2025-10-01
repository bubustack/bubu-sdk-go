// package engram defines the core interfaces that developers implement to create
// components for the bobrapet ecosystem. These interfaces provide a structured,
// type-safe framework for building everything from simple, single-task jobs to
// complex, long-running event listeners.
package engram

import (
	"context"
	"log/slog"

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
	return &Secrets{rawSecrets: rawSecrets}
}

// Get returns a specific secret by its key.
func (s *Secrets) Get(key string) (string, bool) {
	val, ok := s.rawSecrets[key]
	return val, ok
}

// GetAll returns the entire map of secrets.
func (s *Secrets) GetAll() map[string]string {
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
	StoryName  string `json:"storyName"`
	StoryRunID string `json:"storyRunID"`
	StepName   string `json:"stepName"`
	StepRunID  string `json:"stepRunID"`
}

func (e *ExecutionContext) StoryInfo() StoryInfo {
	return e.storyInfo
}

func (e *ExecutionContext) Logger() *slog.Logger {
	return e.logger
}

func (e *ExecutionContext) Tracer() trace.Tracer {
	return e.tracer
}

// Result is the universal return type for a BatchEngram's Process method.
// It encapsulates both the output data and any potential runtime error that
// occurred. The SDK uses this structure to determine whether the step succeeded
// or failed and to record the output.
type Result struct {
	// Data is the output of the Engram. This can be any serializable type.
	// The SDK will automatically handle JSON marshaling and, if configured,
	// transparently offload the data to external storage if it exceeds the
	// size threshold.
	Data interface{}
	// Error, if non-nil, indicates that the Engram's execution failed.
	// The error message will be captured and recorded in the StepRun's status.
	// The presence of an error here will cause the StepRun to be marked as 'Failed'.
	Error error
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

// StreamingEngram is the interface for components that handle real-time,
// continuous data streams. They are typically used for tasks like transformations,
// filtering, or routing of data between other streaming systems. A StreamingEngram
// usually runs as a Kubernetes Deployment and communicates over gRPC.
type StreamingEngram[C any] interface {
	Engram[C]
	// Stream is the core method for handling bidirectional data flow. The SDK
	// provides an input channel (`in`) for receiving data and an output channel
	// (`out`) for sending data. The method should process data from `in` and
	// write results to `out` until the input channel is closed or the context
	// is canceled.
	Stream(ctx context.Context, in <-chan []byte, out chan<- []byte) error
}
