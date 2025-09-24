package engram

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// Config provides access to the static configuration of an Engram, as defined
// in the EngramTemplate and overridden in the Story.
type Config struct {
	rawConfig map[string]interface{}
}

// Secrets provides access to the secrets mapped to the Engram for a specific
// Step in a Story.
type Secrets struct {
	rawSecrets map[string]string
}

// Get returns the raw, unstructured configuration map.
func (c *Config) Get() map[string]interface{} {
	return c.rawConfig
}

func (c *Config) Unmarshal(target interface{}) error {
	b, err := json.Marshal(c.rawConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal raw config for unmarshaling: %w", err)
	}
	return json.Unmarshal(b, target)
}

// Get returns a specific secret by its key (as defined in the EngramTemplate).
func (s *Secrets) Get(key string) (string, bool) {
	val, ok := s.rawSecrets[key]
	return val, ok
}

// GetAll returns the entire map of secrets.
func (s *Secrets) GetAll() map[string]string {
	return s.rawSecrets
}

// ExecutionContext provides all the runtime information for a single execution
// of an Engram.
type ExecutionContext struct {
	logger    *slog.Logger
	tracer    trace.Tracer
	inputs    map[string]interface{}
	storyInfo StoryInfo
}

// NewExecutionContext is a constructor used internally by the SDK runtime.
func NewExecutionContext(logger *slog.Logger, tracer trace.Tracer, inputs map[string]interface{}, storyInfo StoryInfo) *ExecutionContext {
	return &ExecutionContext{
		logger:    logger,
		tracer:    tracer,
		inputs:    inputs,
		storyInfo: storyInfo,
	}
}

// StoryInfo contains metadata about the currently executing Story and Step.
type StoryInfo struct {
	StoryName  string
	StoryRunID string
	StepName   string
	StepRunID  string
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

func (e *ExecutionContext) Inputs() map[string]interface{} {
	return e.inputs
}

// Result is the required return value for a BatchEngram's Process method.
type Result struct {
	Data  interface{}
	Error error
}

// Engram is the fundamental interface for all executable components.
type Engram interface {
	Init(ctx context.Context, config *Config, secrets *Secrets) error
}

// BatchEngram is for tasks that run as a single, short-lived Kubernetes Job.
type BatchEngram interface {
	Engram
	Process(ctx context.Context, execCtx *ExecutionContext) (*Result, error)
}
