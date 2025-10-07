package runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/mitchellh/mapstructure"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ExecutionContextData is the structure of the data provided by the
// bobrapet controller to the SDK.
type ExecutionContextData struct {
	Inputs    map[string]any       `json:"inputs"`
	Config    map[string]any       `json:"config"`
	Secrets   map[string]string    `json:"secrets"`
	StoryInfo engram.StoryInfo     `json:"storyInfo"`
	StoryRef  *refs.StoryReference `json:"storyRef,omitempty"`
	StartedAt metav1.Time          `json:"startedAt"`
}

// LoadExecutionContextData loads the execution context from environment variables.
func LoadExecutionContextData() (*ExecutionContextData, error) {
	startedAt := metav1.Now()
	if startedAtStr := os.Getenv("BUBU_STARTED_AT"); startedAtStr != "" {
		if t, err := time.Parse(time.RFC3339Nano, startedAtStr); err == nil {
			startedAt = metav1.NewTime(t.UTC())
		} else if t, err := time.Parse(time.RFC3339, startedAtStr); err == nil {
			startedAt = metav1.NewTime(t.UTC())
		}
	}

	execCtxData := &ExecutionContextData{
		Inputs:  make(map[string]any),
		Config:  make(map[string]any),
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

	// Load config from BUBU_CONFIG JSON (used by streaming engrams and impulses)
	if configStr := os.Getenv("BUBU_CONFIG"); configStr != "" {
		if err := json.Unmarshal([]byte(configStr), &execCtxData.Config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal BUBU_CONFIG: %w", err)
		}
	}

	// Load config and secrets from environment variables in a single pass.
	// IMPORTANT: BUBU_CONFIG_* variables override any keys from BUBU_CONFIG JSON.
	// This allows:
	//  1. Local development overrides without modifying resource specs
	//  2. Cluster-level policy injection via PodPresets/Webhooks
	// However, this can cause confusion if used unintentionally. To avoid conflicts:
	//  - Operators should not set BUBU_CONFIG_* unless explicitly overriding
	//  - Users should be aware that env vars take precedence over YAML config
	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key, value := parts[0], parts[1]

		if strings.HasPrefix(key, "BUBU_CONFIG_") {
			configKey := strings.TrimPrefix(key, "BUBU_CONFIG_")
			execCtxData.Config[configKey] = value
		} else if strings.HasPrefix(key, "BUBU_SECRET_") {
			secretKey := strings.TrimPrefix(key, "BUBU_SECRET_")
			execCtxData.Secrets[secretKey] = value
		}
	}

	return execCtxData, nil
}

// UnmarshalFromMap is a helper to convert a map[string]any to a struct
// using mapstructure for efficient and robust conversion.
func UnmarshalFromMap[T any](data map[string]any) (T, error) {
	var target T
	config := &mapstructure.DecoderConfig{
		// This enables the decoder to handle type conversions automatically,
		// for example, converting a string "123" to an int 123.
		WeaklyTypedInput: true,
		Result:           &target,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			// Support parsing time.Duration from strings like "5s", "1m"
			func(from reflect.Type, to reflect.Type, data any) (any, error) {
				if to == reflect.TypeOf(time.Duration(0)) {
					switch v := data.(type) {
					case string:
						d, err := time.ParseDuration(v)
						if err != nil {
							return nil, err
						}
						return d, nil
					case int64:
						return time.Duration(v), nil
					case float64:
						return time.Duration(v), nil
					}
				}
				return data, nil
			},
		),
	}

	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return target, fmt.Errorf("failed to create mapstructure decoder: %w", err)
	}

	if err := decoder.Decode(data); err != nil {
		return target, fmt.Errorf("failed to decode map into target struct: %w", err)
	}
	return target, nil
}
