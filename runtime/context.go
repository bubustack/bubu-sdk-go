package runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
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
	Storage   *StorageConfig       `json:"storage,omitempty"`
	Execution ExecutionInfo        `json:"execution"`
}

// ExecutionInfo holds runtime parameters for the current step execution.
type ExecutionInfo struct {
	Mode          string
	StepTimeout   time.Duration
	MaxInlineSize int
	GRPCPort      int
}

// StorageConfig holds the configuration for object storage.
type StorageConfig struct {
	Provider string
	S3       *S3StorageConfig
	Timeout  time.Duration
}

// S3StorageConfig holds S3-specific storage configuration.
type S3StorageConfig struct {
	Bucket   string
	Region   string
	Endpoint string
}

// LoadExecutionContextData loads the execution context from environment variables.
func LoadExecutionContextData() (*ExecutionContextData, error) {
	startedAt := parseStartedAtFromEnv()
	execCtxData := buildBaseExecutionContextData(startedAt)

	if err := fillInputsFromEnv(execCtxData); err != nil {
		return nil, err
	}
	if err := fillConfigFromEnvJSON(execCtxData); err != nil {
		return nil, err
	}
	fillExecutionInfoFromEnv(execCtxData)
	fillStorageConfigFromEnv(execCtxData)
	applyOverridesFromEnv(execCtxData)
	return execCtxData, nil
}

func parseStartedAtFromEnv() metav1.Time {
	if startedAtStr := os.Getenv("BUBU_STARTED_AT"); startedAtStr != "" {
		if t, err := time.Parse(time.RFC3339Nano, startedAtStr); err == nil {
			return metav1.NewTime(t.UTC())
		}
		if t, err := time.Parse(time.RFC3339, startedAtStr); err == nil {
			return metav1.NewTime(t.UTC())
		}
	}
	return metav1.Now()
}

func buildBaseExecutionContextData(startedAt metav1.Time) *ExecutionContextData {
	return &ExecutionContextData{
		Inputs:  make(map[string]any),
		Config:  make(map[string]any),
		Secrets: make(map[string]string),
		StoryInfo: engram.StoryInfo{
			StoryName:        os.Getenv("BUBU_STORY_NAME"),
			StoryRunID:       os.Getenv("BUBU_STORYRUN_ID"),
			StepName:         os.Getenv("BUBU_STEP_NAME"),
			StepRunID:        os.Getenv("BUBU_STEPRUN_NAME"),
			StepRunNamespace: os.Getenv("BUBU_STEPRUN_NAMESPACE"),
		},
		StartedAt: startedAt,
	}
}

func fillInputsFromEnv(execCtxData *ExecutionContextData) error {
	if inputsStr := os.Getenv("BUBU_INPUTS"); inputsStr != "" {
		if err := json.Unmarshal([]byte(inputsStr), &execCtxData.Inputs); err != nil {
			return fmt.Errorf("failed to unmarshal BUBU_INPUTS: %w", err)
		}
	}
	return nil
}

func fillConfigFromEnvJSON(execCtxData *ExecutionContextData) error {
	if configStr := os.Getenv("BUBU_CONFIG"); configStr != "" {
		if err := json.Unmarshal([]byte(configStr), &execCtxData.Config); err != nil {
			return fmt.Errorf("failed to unmarshal BUBU_CONFIG: %w", err)
		}
	}
	return nil
}

func fillExecutionInfoFromEnv(execCtxData *ExecutionContextData) {
	execCtxData.Execution.Mode = os.Getenv("BUBU_EXECUTION_MODE")
	if timeoutStr := os.Getenv("BUBU_STEP_TIMEOUT"); timeoutStr != "" {
		if d, err := time.ParseDuration(timeoutStr); err == nil {
			execCtxData.Execution.StepTimeout = d
		}
	}
	if maxSizeStr := os.Getenv("BUBU_MAX_INLINE_SIZE"); maxSizeStr != "" {
		if size, err := strconv.Atoi(maxSizeStr); err == nil {
			execCtxData.Execution.MaxInlineSize = size
		}
	}
	if grpcPortStr := os.Getenv("BUBU_GRPC_PORT"); grpcPortStr != "" {
		if port, err := strconv.Atoi(grpcPortStr); err == nil {
			execCtxData.Execution.GRPCPort = port
		}
	}
}

func fillStorageConfigFromEnv(execCtxData *ExecutionContextData) {
	if provider := os.Getenv("BUBU_STORAGE_PROVIDER"); provider != "" {
		execCtxData.Storage = &StorageConfig{Provider: provider}
		if timeoutStr := os.Getenv("BUBU_STORAGE_TIMEOUT"); timeoutStr != "" {
			if d, err := time.ParseDuration(timeoutStr); err == nil {
				execCtxData.Storage.Timeout = d
			}
		}
		if provider == "s3" {
			execCtxData.Storage.S3 = &S3StorageConfig{
				Bucket:   os.Getenv("BUBU_STORAGE_S3_BUCKET"),
				Region:   os.Getenv("BUBU_STORAGE_S3_REGION"),
				Endpoint: os.Getenv("BUBU_STORAGE_S3_ENDPOINT"),
			}
		}
	}
}

func applyOverridesFromEnv(execCtxData *ExecutionContextData) {
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
