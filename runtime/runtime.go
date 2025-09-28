package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log/slog"
	"os"

	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/storage"
	"go.opentelemetry.io/otel"
)

// ExecutionContextData is the structure of the JSON file provided by the
// bobrapet controller to the SDK sidecar.
type ExecutionContextData struct {
	Inputs    map[string]interface{} `json:"inputs"`
	Config    map[string]interface{} `json:"config"`
	Secrets   map[string]string      `json:"secrets"`
	StoryInfo engram.StoryInfo       `json:"storyInfo"`
	StoryRef  *refs.StoryReference   `json:"storyRef,omitempty"`
}

// LoadExecutionContextData loads the execution context from the well-known file path.
func LoadExecutionContextData(path string) (*ExecutionContextData, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &ExecutionContextData{
				Inputs:    make(map[string]interface{}),
				Config:    make(map[string]interface{}),
				Secrets:   make(map[string]string),
				StoryInfo: engram.StoryInfo{},
			}, nil
		}
		return nil, fmt.Errorf("failed to read execution context file: %w", err)
	}

	var execCtxData ExecutionContextData
	if err := json.Unmarshal(data, &execCtxData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal execution context data: %w", err)
	}

	return &execCtxData, nil
}

// unmarshalFromMap is a helper to convert a map[string]interface{} to a struct
// by going through JSON.
func unmarshalFromMap[T any](data map[string]interface{}) (T, error) {
	var target T
	bytes, err := json.Marshal(data)
	if err != nil {
		return target, fmt.Errorf("failed to marshal map for unmarshaling: %w", err)
	}
	if err := json.Unmarshal(bytes, &target); err != nil {
		return target, fmt.Errorf("failed to unmarshal into target struct: %w", err)
	}
	return target, nil
}

// Runtime is the internal engine that manages the lifecycle of a batch Engram.
type Runtime[C any, I any] struct {
	engram         engram.BatchEngram[C, I]
	storageManager *storage.StorageManager
}

// New creates a new SDK runtime for a given Engram.
func New[C any, I any](e engram.BatchEngram[C, I]) (*Runtime[C, I], error) {
	sm, err := storage.NewManager(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to create storage manager: %w", err)
	}
	return &Runtime[C, I]{
		engram:         e,
		storageManager: sm,
	}, nil
}

// Execute runs the full lifecycle of a batch Engram.
func (r *Runtime[C, I]) Execute(ctx context.Context, inputs map[string]interface{}) error {
	// Read the execution context from the well-known file.
	execCtxData, err := LoadExecutionContextData("/var/run/bubu/context.json")
	if err != nil {
		return fmt.Errorf("failed to load execution context: %w", err)
	}

	// If inputs are not provided directly, use the ones from the context file.
	if inputs == nil {
		// Hydrate inputs before unmarshaling.
		hydratedInputs, err := r.storageManager.Hydrate(ctx, execCtxData.Inputs)
		if err != nil {
			return fmt.Errorf("failed to hydrate inputs: %w", err)
		}
		inputs = hydratedInputs.(map[string]interface{})
	}

	// Initialize logger and tracer.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	tracer := otel.Tracer("bubu-sdk")

	// Unmarshal the dynamic inputs into the static type I.
	typedInputs, err := unmarshalFromMap[I](inputs)
	if err != nil {
		return fmt.Errorf("failed to unmarshal inputs: %w", err)
	}

	execCtx := engram.NewExecutionContext(logger, tracer, execCtxData.StoryInfo)

	// Unmarshal config.
	config, err := unmarshalFromMap[C](execCtxData.Config)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}
	secrets := engram.NewSecrets(execCtxData.Secrets)

	if err := r.engram.Init(ctx, config, secrets); err != nil {
		return fmt.Errorf("engram initialization failed: %w", err)
	}

	// 2. Process
	result, err := r.engram.Process(ctx, execCtx, typedInputs)
	if err != nil {
		// Write error to a well-known file for the controller to find.
		_ = os.WriteFile("/var/run/bubu/error.json", []byte(err.Error()), 0644)
		return fmt.Errorf("engram processing failed: %w", err)
	}
	if result.Error != nil {
		_ = os.WriteFile("/var/run/bubu/error.json", []byte(result.Error.Error()), 0644)
		return fmt.Errorf("engram returned an error: %w", result.Error)
	}

	// 3. Dehydrate and write output
	output, err := r.storageManager.Dehydrate(ctx, result.Data, execCtxData.StoryInfo.StepRunID)
	if err != nil {
		return fmt.Errorf("failed to dehydrate output: %w", err)
	}

	outputBytes, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("failed to marshal output: %w", err)
	}

	// Write output to a well-known file.
	if err := os.WriteFile("/var/run/bubu/output.json", outputBytes, 0644); err != nil {
		return fmt.Errorf("failed to write output: %w", err)
	}

	return nil
}
