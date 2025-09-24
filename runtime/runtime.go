package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/storage"
)

// Runtime is the internal engine that manages the lifecycle of a batch Engram.
type Runtime struct {
	engram         engram.BatchEngram
	storageManager *storage.StorageManager
}

// New creates a new SDK runtime for a given Engram.
func New(e engram.BatchEngram) (*Runtime, error) {
	sm, err := storage.NewManager(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage manager: %w", err)
	}
	return &Runtime{
		engram:         e,
		storageManager: sm,
	}, nil
}

// Execute runs the full lifecycle of a batch Engram.
func (r *Runtime) Execute(ctx context.Context) error {
	// In a real implementation, this would be read from a file or env var
	// passed by the controller.
	execCtxData := make(map[string]interface{})

	execCtx := engram.NewExecutionContext(nil, nil, execCtxData, engram.StoryInfo{})

	// 1. Init
	// In a real implementation, config and secrets would be passed here.
	if err := r.engram.Init(ctx, &engram.Config{}, &engram.Secrets{}); err != nil {
		return fmt.Errorf("engram initialization failed: %w", err)
	}

	// 2. Process
	result, err := r.engram.Process(ctx, execCtx)
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
	output, err := r.storageManager.Dehydrate(ctx, result.Data, "steprun-id")
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
