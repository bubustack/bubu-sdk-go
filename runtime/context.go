package runtime

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/bubu-sdk-go/engram"
)

// ExecutionContextData is the structure of the data provided by the
// bobrapet controller to the SDK.
type ExecutionContextData struct {
	Inputs    map[string]interface{} `json:"inputs"`
	Config    map[string]interface{} `json:"config"`
	Secrets   map[string]string      `json:"secrets"`
	StoryInfo engram.StoryInfo       `json:"storyInfo"`
	StoryRef  *refs.StoryReference   `json:"storyRef,omitempty"`
}

// LoadExecutionContextData loads the execution context from environment variables.
func LoadExecutionContextData() (*ExecutionContextData, error) {
	execCtxData := &ExecutionContextData{
		Inputs:  make(map[string]interface{}),
		Config:  make(map[string]interface{}),
		Secrets: make(map[string]string),
		StoryInfo: engram.StoryInfo{
			StoryName:  os.Getenv("BUBU_STORY_NAME"),
			StoryRunID: os.Getenv("BUBU_STORYRUN_ID"),
			StepName:   os.Getenv("BUBU_STEP_NAME"),
			StepRunID:  os.Getenv("BUBU_STEPRUN_NAME"),
		},
	}

	if inputsStr := os.Getenv("BUBU_INPUTS"); inputsStr != "" {
		if err := json.Unmarshal([]byte(inputsStr), &execCtxData.Inputs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal BUBU_INPUTS: %w", err)
		}
	}

	// Config and Secrets would be populated here by scanning for environment
	// variables with specific prefixes, e.g., BUBU_CONFIG_ and BUBU_SECRET_.

	return execCtxData, nil
}

// UnmarshalFromMap is a helper to convert a map[string]interface{} to a struct
// by going through JSON.
func UnmarshalFromMap[T any](data map[string]interface{}) (T, error) {
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
