package engram

import (
	"context"
	"log/slog"

	"github.com/bubustack/bubu-sdk-go/impulse"
	"go.opentelemetry.io/otel/trace"
)

// Secrets provides access to the secrets mapped to the Engram for a specific Step.
// Secrets are raw key-value pairs and are not automatically unmarshaled.
type Secrets struct {
	rawSecrets map[string]string
}

// NewSecrets creates a new Secrets object.
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

// ExecutionContext provides all the runtime information for a single execution of an Engram.
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

// Result is the required return value for a BatchEngram's Process method.
type Result struct {
	Data  interface{}
	Error error
}

// Engram is the fundamental interface for all executable components.
// It is generic over the configuration type `C`, which must be a pointer.
// The SDK will automatically unmarshal the `with` block into this type.
type Engram[C any] interface {
	Init(ctx context.Context, config C, secrets *Secrets) error
}

// BatchEngram is for tasks that run as a single, short-lived Kubernetes Job.
// It is generic over the configuration type `C` and the input type `I`.
// The SDK will automatically unmarshal the context into these types.
type BatchEngram[C any, I any] interface {
	Engram[C]
	Process(ctx context.Context, execCtx *ExecutionContext, inputs I) (*Result, error)
}

// Impulse is for long-running components that listen for external events.
// It is generic over the configuration type `C`.
type Impulse[C any] interface {
	Engram[C]
	Run(ctx context.Context, client *impulse.Client) error
}

// StreamingEngram is for long-running, real-time data processing tasks.
// It is generic over the configuration type `C`.
type StreamingEngram[C any] interface {
	Engram[C]
	Stream(ctx context.Context, in <-chan []byte, out chan<- []byte) error
}
