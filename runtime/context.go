package runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/bubu-sdk-go/engram"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ExecutionContextData is the structure of the data provided by the
// bobrapet controller to the SDK.
type ExecutionContextData struct {
	Inputs    map[string]interface{} `json:"inputs"`
	Config    map[string]interface{} `json:"config"`
	Secrets   map[string]string      `json:"secrets"`
	StoryInfo engram.StoryInfo       `json:"storyInfo"`
	StoryRef  *refs.StoryReference   `json:"storyRef,omitempty"`
	StartedAt metav1.Time            `json:"startedAt"`
}

// LoadExecutionContextData loads the execution context from environment variables.
func LoadExecutionContextData() (*ExecutionContextData, error) {
	startedAt := metav1.Now()
	if startedAtStr := os.Getenv("BUBU_STARTED_AT"); startedAtStr != "" {
		if t, err := time.Parse(time.RFC3339, startedAtStr); err == nil {
			startedAt = metav1.NewTime(t)
		}
	}

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
		StartedAt: startedAt,
	}

	if inputsStr := os.Getenv("BUBU_INPUTS"); inputsStr != "" {
		if err := json.Unmarshal([]byte(inputsStr), &execCtxData.Inputs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal BUBU_INPUTS: %w", err)
		}
	}

	// Load config from BUBU_CONFIG_*
	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, "BUBU_CONFIG_") {
			continue
		}
		// env format: KEY=VALUE
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimPrefix(parts[0], "BUBU_CONFIG_")
		execCtxData.Config[key] = parts[1]
	}

	// Load secrets from BUBU_SECRET_*
	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, "BUBU_SECRET_") {
			continue
		}
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimPrefix(parts[0], "BUBU_SECRET_")
		execCtxData.Secrets[key] = parts[1]
	}

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
