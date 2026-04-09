/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package runtime

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/core/contracts"
	"github.com/mitchellh/mapstructure"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const runtimeStrictUnmarshalEnv = "BUBU_RUNTIME_STRICT_UNMARSHAL"

// ExecutionContextData is the structure of the data provided by the
// bobrapet controller to the SDK.
type ExecutionContextData struct {
	// Inputs carries the hydrated step inputs visible to the current engram.
	Inputs map[string]any `json:"inputs"`
	// Config carries the hydrated static configuration for the current engram.
	Config map[string]any `json:"config"`
	// Secrets contains the secret descriptors or literal values injected for the step.
	Secrets map[string]string `json:"secrets"`
	// StoryInfo identifies the current Story, StoryRun, Step, and StepRun.
	StoryInfo engram.StoryInfo `json:"storyInfo"`
	// CELContext exposes controller-provided CEL inputs and prior step outputs.
	CELContext map[string]any `json:"celContext,omitempty"`
	// Transports lists the declared story transports available to the runtime.
	Transports []engram.TransportDescriptor `json:"transports,omitempty"`
	// StoryRef identifies the Story resource when the controller provides it explicitly.
	StoryRef *refs.StoryReference `json:"storyRef,omitempty"`
	// StartedAt records when the controller says the current execution began.
	StartedAt metav1.Time `json:"startedAt"`
	// Storage configures the shared object storage backend, when enabled.
	Storage *StorageConfig `json:"storage,omitempty"`
	// Execution carries runtime tuning such as timeouts and inline-size limits.
	Execution ExecutionInfo `json:"execution"`
}

// ExecutionInfo holds runtime parameters for the current step execution.
type ExecutionInfo struct {
	// Mode is the controller-selected execution mode for the step.
	Mode string
	// StepTimeout is the maximum runtime allowed for the step.
	StepTimeout time.Duration
	// MaxInlineSize is the maximum payload size kept inline before offload.
	MaxInlineSize int
	// GRPCPort is the listening port used by gRPC-based runtimes.
	GRPCPort int
}

// StorageConfig holds the configuration for object storage.
type StorageConfig struct {
	// Provider identifies the backing storage implementation (for example, `s3`).
	Provider string
	// S3 carries provider-specific settings when Provider is `s3`.
	S3 *S3StorageConfig
	// Timeout bounds storage read/write operations when set.
	Timeout time.Duration
}

// S3StorageConfig holds S3-specific storage configuration.
type S3StorageConfig struct {
	// Bucket is the target S3 bucket name.
	Bucket string
	// Region is the AWS region for the bucket.
	Region string
	// Endpoint overrides the default S3 endpoint when set.
	Endpoint string
}

// LoadExecutionContextData loads the execution context from environment variables.
func LoadExecutionContextData() (*ExecutionContextData, error) {
	data := &ExecutionContextData{
		Inputs:  make(map[string]any),
		Config:  make(map[string]any),
		Secrets: make(map[string]string),
	}

	data.StoryInfo = loadStoryInfoFromEnv()
	data.Execution.Mode = os.Getenv(contracts.ExecutionModeEnv)
	data.StartedAt = parseStartedAtFromEnv()

	if inputs, err := loadJSONMapEnv(contracts.TriggerDataEnv); err != nil {
		return nil, err
	} else if len(inputs) > 0 {
		data.Inputs = inputs
	}

	if config, err := loadJSONMapEnv(contracts.StepConfigEnv); err != nil {
		return nil, err
	} else if len(config) > 0 {
		data.Config = config
	}
	if celCtx, err := loadJSONMapEnv(contracts.TemplateContextEnv); err != nil {
		return nil, err
	} else if len(celCtx) > 0 {
		data.CELContext = celCtx
	}

	if transports, err := loadTransportsFromEnv(); err != nil {
		return nil, err
	} else if len(transports) > 0 {
		data.Transports = transports
	}

	applyConfigAndSecretOverrides(data)
	applyExecutionSettingsFromEnv(&data.Execution)
	data.Storage = loadStorageConfigFromEnv()

	return data, nil
}

func loadStoryInfoFromEnv() engram.StoryInfo {
	return engram.StoryInfo{
		StoryName:        os.Getenv(contracts.StoryNameEnv),
		StoryRunID:       os.Getenv(contracts.StoryRunIDEnv),
		StepName:         os.Getenv(contracts.StepNameEnv),
		StepRunID:        os.Getenv(contracts.StepRunNameEnv),
		StepRunNamespace: os.Getenv(contracts.StepRunNamespaceEnv),
	}
}

func parseStartedAtFromEnv() metav1.Time {
	if startedAtStr := os.Getenv(contracts.StartedAtEnv); startedAtStr != "" {
		if t, err := time.Parse(time.RFC3339, startedAtStr); err == nil {
			return metav1.Time{Time: t}
		}
	}
	return metav1.NewTime(time.Now())
}

func loadJSONMapEnv(key string) (map[string]any, error) {
	raw := os.Getenv(key)
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal %s: %w", key, err)
	}
	return payload, nil
}

func loadTransportsFromEnv() ([]engram.TransportDescriptor, error) {
	raw := strings.TrimSpace(os.Getenv(contracts.TransportsEnv))
	if raw == "" {
		return nil, nil
	}
	var entries []map[string]any
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil, fmt.Errorf("failed to unmarshal %s: %w", contracts.TransportsEnv, err)
	}
	transports := make([]engram.TransportDescriptor, 0, len(entries))
	seenNames := make(map[string]struct{}, len(entries))
	for i, entry := range entries {
		td := engram.TransportDescriptor{}
		if name, ok := entry["name"].(string); ok {
			td.Name = strings.TrimSpace(name)
		}
		if kind, ok := entry["kind"].(string); ok {
			td.Kind = strings.TrimSpace(kind)
		}
		if mode, ok := entry["mode"].(string); ok {
			td.Mode = strings.TrimSpace(mode)
		}
		if td.Name == "" {
			return nil, fmt.Errorf("invalid %s[%d]: name is required", contracts.TransportsEnv, i)
		}
		if td.Kind == "" {
			return nil, fmt.Errorf("invalid %s[%d]: kind is required", contracts.TransportsEnv, i)
		}
		if _, exists := seenNames[td.Name]; exists {
			return nil, fmt.Errorf("invalid %s[%d]: duplicate transport name %q", contracts.TransportsEnv, i, td.Name)
		}
		seenNames[td.Name] = struct{}{}
		if td.Mode == "" {
			td.Mode = "hot"
		} else if !isValidTransportMode(td.Mode) {
			return nil, fmt.Errorf(
				"invalid %s[%d]: mode %q must be hot or fallback",
				contracts.TransportsEnv,
				i,
				td.Mode,
			)
		}
		config := make(map[string]any)
		for k, v := range entry {
			switch k {
			case "name", "kind", "mode":
				continue
			default:
				config[k] = v
			}
		}
		if len(config) > 0 {
			td.Config = config
		}
		transports = append(transports, td)
	}
	return transports, nil
}

func isValidTransportMode(mode string) bool {
	switch mode {
	case "hot", "fallback":
		return true
	default:
		return false
	}
}

func applyConfigAndSecretOverrides(data *ExecutionContextData) {
	for _, envPair := range os.Environ() {
		key, value, found := strings.Cut(envPair, "=")
		if !found {
			continue
		}
		switch {
		case strings.HasPrefix(key, contracts.ConfigPrefixEnv):
			configKey := strings.TrimPrefix(key, contracts.ConfigPrefixEnv)
			if configKey != "" {
				data.Config[configKey] = value
			}
		case strings.HasPrefix(key, contracts.SecretPrefixEnv):
			secretKey := strings.TrimPrefix(key, contracts.SecretPrefixEnv)
			if secretKey == "" {
				continue
			}
			if strings.HasSuffix(secretKey, "_NAME") {
				continue
			}
			if strings.Contains(secretKey, "__") {
				// Bucket-scoped export used for descriptor expansion; skip SDK exposure.
				continue
			}
			data.Secrets[secretKey] = value
		}
	}
}

func applyExecutionSettingsFromEnv(exec *ExecutionInfo) {
	if timeout := parseDurationEnv(contracts.StepTimeoutEnv); timeout > 0 {
		exec.StepTimeout = timeout
	}
	if maxInline, ok := parsePositiveIntEnv(contracts.MaxInlineSizeEnv); ok {
		exec.MaxInlineSize = maxInline
	}
	if grpcPort, ok := parsePositiveIntEnv(contracts.GRPCPortEnv); ok {
		exec.GRPCPort = grpcPort
	}
}

func parseDurationEnv(key string) time.Duration {
	if raw := strings.TrimSpace(os.Getenv(key)); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			warnInvalidRuntimeEnv(key, raw, "a positive duration string such as 5s")
			return 0
		}
		if d <= 0 {
			warnInvalidRuntimeEnv(key, raw, "a positive duration string such as 5s")
			return 0
		}
		return d
	}
	return 0
}

func parsePositiveIntEnv(key string) (int, bool) {
	if raw := strings.TrimSpace(os.Getenv(key)); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil {
			warnInvalidRuntimeEnv(key, raw, "a positive integer")
			return 0, false
		}
		if value <= 0 {
			warnInvalidRuntimeEnv(key, raw, "a positive integer")
			return 0, false
		}
		return value, true
	}
	return 0, false
}

func warnInvalidRuntimeEnv(key string, raw string, expected string) {
	slog.Default().Warn("Ignoring invalid runtime env override", "env", key, "value", raw, "expected", expected)
}

func loadStorageConfigFromEnv() *StorageConfig {
	provider := strings.TrimSpace(os.Getenv(contracts.StorageProviderEnv))
	if provider == "" {
		return nil
	}
	cfg := &StorageConfig{Provider: provider}

	if timeout := parseDurationEnv(contracts.StorageTimeoutEnv); timeout > 0 {
		cfg.Timeout = timeout
	}

	if provider != "s3" {
		return cfg
	}

	cfg.S3 = &S3StorageConfig{
		Bucket:   strings.TrimSpace(os.Getenv(contracts.StorageS3BucketEnv)),
		Region:   strings.TrimSpace(os.Getenv(contracts.StorageS3RegionEnv)),
		Endpoint: strings.TrimSpace(os.Getenv(contracts.StorageS3EndpointEnv)),
	}

	return cfg
}

// UnmarshalFromMap is a helper to convert a map[string]any to a struct
// using mapstructure for efficient and robust conversion.
func UnmarshalFromMap[T any](data map[string]any) (T, error) {
	var target T
	config := &mapstructure.DecoderConfig{
		// This enables the decoder to handle type conversions automatically,
		// for example, converting a string "123" to an int 123.
		WeaklyTypedInput: true,
		ErrorUnused:      strictUnmarshalFromMapEnabled(),
		Result:           &target,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			// Support parsing time.Duration from strings like "5s", "1m"
			func(from reflect.Type, to reflect.Type, data any) (any, error) {
				if to == reflect.TypeFor[time.Duration]() {
					switch v := data.(type) {
					case string:
						d, err := time.ParseDuration(v)
						if err != nil {
							return nil, err
						}
						return d, nil
					case time.Duration:
						return v, nil
					}
					if isNumericDurationValue(data) {
						return nil, fmt.Errorf("duration values must be strings like 5s, not %T", data)
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

func strictUnmarshalFromMapEnabled() bool {
	raw := strings.TrimSpace(os.Getenv(runtimeStrictUnmarshalEnv))
	if raw == "" {
		return false
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		warnInvalidRuntimeEnv(runtimeStrictUnmarshalEnv, raw, "a boolean (true/false)")
		return false
	}
	return enabled
}

func isNumericDurationValue(value any) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, uintptr,
		float32, float64:
		return true
	default:
		return false
	}
}
