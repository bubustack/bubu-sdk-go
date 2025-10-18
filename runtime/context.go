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
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/contracts"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/mitchellh/mapstructure"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ExecutionContextData is the structure of the data provided by the
// bobrapet controller to the SDK.
type ExecutionContextData struct {
	Inputs            map[string]any                 `json:"inputs"`
	Config            map[string]any                 `json:"config"`
	Secrets           map[string]string              `json:"secrets"`
	StoryInfo         engram.StoryInfo               `json:"storyInfo"`
	Transports        []engram.TransportDescriptor   `json:"transports,omitempty"`
	StoryRef          *refs.StoryReference           `json:"storyRef,omitempty"`
	StartedAt         metav1.Time                    `json:"startedAt"`
	Storage           *StorageConfig                 `json:"storage,omitempty"`
	Execution         ExecutionInfo                  `json:"execution"`
	RequestedManifest []runsv1alpha1.ManifestRequest `json:"requestedManifest,omitempty"`
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
	data := &ExecutionContextData{
		Inputs:  make(map[string]any),
		Config:  make(map[string]any),
		Secrets: make(map[string]string),
	}

	data.StoryInfo = loadStoryInfoFromEnv()
	data.Execution.Mode = os.Getenv(contracts.ExecutionModeEnv)
	data.StartedAt = parseStartedAtFromEnv()

	if inputs, err := loadJSONMapEnv(contracts.InputsEnv); err != nil {
		return nil, err
	} else if len(inputs) > 0 {
		data.Inputs = inputs
	}

	if config, err := loadJSONMapEnv(contracts.ConfigEnv); err != nil {
		return nil, err
	} else if len(config) > 0 {
		data.Config = config
	}

	if transports, err := loadTransportsFromEnv(); err != nil {
		return nil, err
	} else if len(transports) > 0 {
		data.Transports = transports
	}

	if manifest, err := loadManifestSpecFromEnv(); err != nil {
		return nil, err
	} else if len(manifest) > 0 {
		data.RequestedManifest = manifest
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

func loadManifestSpecFromEnv() ([]runsv1alpha1.ManifestRequest, error) {
	raw := os.Getenv(contracts.ManifestSpecEnv)
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var manifest []runsv1alpha1.ManifestRequest
	if err := json.Unmarshal([]byte(raw), &manifest); err != nil {
		return nil, fmt.Errorf("failed to unmarshal %s: %w", contracts.ManifestSpecEnv, err)
	}
	return manifest, nil
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
	for _, entry := range entries {
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
		if td.Mode == "" {
			td.Mode = "hot"
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
	if raw := os.Getenv(key); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			return d
		}
	}
	return 0
}

func parsePositiveIntEnv(key string) (int, bool) {
	if raw := os.Getenv(key); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value > 0 {
			return value, true
		}
	}
	return 0, false
}

func loadStorageConfigFromEnv() *StorageConfig {
	provider := os.Getenv(contracts.StorageProviderEnv)
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
		Bucket:   os.Getenv(contracts.StorageS3BucketEnv),
		Region:   os.Getenv(contracts.StorageS3RegionEnv),
		Endpoint: os.Getenv(contracts.StorageS3EndpointEnv),
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
