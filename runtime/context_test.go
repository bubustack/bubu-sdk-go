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
	"bytes"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/core/contracts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const sampleTransportConfig = `[
	{
		"name":"rt",
		"kind":"livekit",
		"mode":"hot",
		"livekit":{
			"room":"abc",
			"participant":"def"
		}
	}
]`

func TestLoadExecutionContextData(t *testing.T) {
	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	tests := []struct {
		name    string
		envVars map[string]string
		want    *ExecutionContextData
		wantErr bool
	}{
		{
			name: "complete context",
			envVars: map[string]string{
				contracts.TriggerDataEnv:          `{"key":"value","number":123}`,
				contracts.StepConfigEnv:           `{"key1":"value1","key2":"value2"}`,
				contracts.SecretPrefixEnv + "API": "secret123",
				contracts.StoryNameEnv:            "test-story",
				contracts.StoryRunIDEnv:           "run-123",
				contracts.StepNameEnv:             "test-step",
				contracts.StepRunNameEnv:          "step-run-123",
				contracts.StartedAtEnv:            nowStr,
			},
			want: &ExecutionContextData{
				Inputs:  map[string]any{"key": "value", "number": float64(123)}, // JSON unmarshals numbers to float64
				Config:  map[string]any{"key1": "value1", "key2": "value2"},
				Secrets: map[string]string{"API": "secret123"},
				StoryInfo: engram.StoryInfo{
					StoryName:  "test-story",
					StoryRunID: "run-123",
					StepName:   "test-step",
					StepRunID:  "step-run-123",
				},
				StartedAt: metav1.NewTime(now.UTC()),
			},
			wantErr: false,
		},
		{
			name: "minimal context",
			envVars: map[string]string{
				contracts.StoryNameEnv: "minimal-story",
			},
			want: &ExecutionContextData{
				Inputs:  make(map[string]any),
				Config:  make(map[string]any),
				Secrets: make(map[string]string),
				StoryInfo: engram.StoryInfo{
					StoryName: "minimal-story",
				},
			},
			wantErr: false,
		},
		{
			name: "transports parsed",
			envVars: map[string]string{
				contracts.TransportsEnv: sampleTransportConfig,
			},
			want: &ExecutionContextData{
				Inputs:    make(map[string]any),
				Config:    make(map[string]any),
				Secrets:   make(map[string]string),
				StoryInfo: engram.StoryInfo{},
				Transports: []engram.TransportDescriptor{
					{
						Name: "rt",
						Kind: "livekit",
						Mode: "hot",
						Config: map[string]any{
							"livekit": map[string]any{
								"participant": "def",
								"room":        "abc",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid JSON inputs",
			envVars: map[string]string{
				contracts.TriggerDataEnv: `{invalid json}`,
			},
			wantErr: true,
		},
		{
			name:    "empty environment",
			envVars: map[string]string{},
			want: &ExecutionContextData{
				Inputs:    make(map[string]any),
				Config:    make(map[string]any),
				Secrets:   make(map[string]string),
				StoryInfo: engram.StoryInfo{},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up environment variables for the test case
			for k, v := range tt.envVars {
				err := os.Setenv(k, v)
				if err != nil {
					t.Fatalf("Setenv() error = %v", err)
				}
			}
			// Ensure variables are cleaned up after the test
			defer func() {
				for k := range tt.envVars {
					err := os.Unsetenv(k)
					if err != nil {
						t.Fatalf("Unsetenv() error = %v", err)
					}
				}
			}()

			got, err := LoadExecutionContextData()

			if (err != nil) != tt.wantErr {
				t.Errorf("LoadExecutionContextData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if got == nil {
					t.Fatal("LoadExecutionContextData() returned nil, want non-nil")
				}
				// Ignore the StartedAt field for cases where it wasn't explicitly provided.
				if tt.name == "minimal context" || tt.name == "empty environment" || tt.name == "transports parsed" {
					tt.want.StartedAt = got.StartedAt
				}

				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("LoadExecutionContextData() got = %+v, want %+v", got, tt.want)
				}
			}
		})
	}
}

func TestLoadExecutionContextData_AllowsNullInputs(t *testing.T) {
	t.Setenv(contracts.TriggerDataEnv, "null")
	data, err := LoadExecutionContextData()
	if err != nil {
		t.Fatalf("LoadExecutionContextData() error = %v", err)
	}
	if data.Inputs == nil {
		t.Fatalf("expected Inputs to be initialized, got nil")
	}
	if len(data.Inputs) != 0 {
		t.Fatalf("expected Inputs to be empty map, got %v", data.Inputs)
	}
}

func TestLoadExecutionContextData_LoadsCELContext(t *testing.T) {
	t.Setenv(contracts.TemplateContextEnv, `{"steps":{"a":{"ok":true}},"inputs":{"name":"demo"}}`)

	data, err := LoadExecutionContextData()
	if err != nil {
		t.Fatalf("LoadExecutionContextData() error = %v", err)
	}
	if data.CELContext == nil {
		t.Fatal("expected CELContext to be populated")
	}
	steps, ok := data.CELContext["steps"].(map[string]any)
	if !ok {
		t.Fatalf("expected steps map in CELContext, got %#v", data.CELContext["steps"])
	}
	if _, ok := steps["a"]; !ok {
		t.Fatalf("expected step entry in CELContext, got %#v", steps)
	}
}

func TestLoadTransportsFromEnvDefaultsModeToHot(t *testing.T) {
	t.Setenv(contracts.TransportsEnv, `[{"name":"rt","kind":"livekit"}]`)

	transports, err := loadTransportsFromEnv()
	if err != nil {
		t.Fatalf("loadTransportsFromEnv() error = %v", err)
	}
	if len(transports) != 1 {
		t.Fatalf("expected 1 transport, got %d", len(transports))
	}
	if transports[0].Mode != "hot" { //nolint:goconst
		t.Fatalf("expected default mode hot, got %q", transports[0].Mode)
	}
}

func TestLoadTransportsFromEnvRejectsMissingName(t *testing.T) {
	t.Setenv(contracts.TransportsEnv, `[{"kind":"livekit","mode":"hot"}]`)

	_, err := loadTransportsFromEnv()
	if err == nil {
		t.Fatal("expected missing name to be rejected")
	}
	if got := err.Error(); got != "invalid BUBU_TRANSPORTS[0]: name is required" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadTransportsFromEnvRejectsMissingKind(t *testing.T) {
	t.Setenv(contracts.TransportsEnv, `[{"name":"rt","mode":"hot"}]`)

	_, err := loadTransportsFromEnv()
	if err == nil {
		t.Fatal("expected missing kind to be rejected")
	}
	if got := err.Error(); got != "invalid BUBU_TRANSPORTS[0]: kind is required" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadTransportsFromEnvRejectsInvalidMode(t *testing.T) {
	t.Setenv(contracts.TransportsEnv, `[{"name":"rt","kind":"livekit","mode":"bi"}]`)

	_, err := loadTransportsFromEnv()
	if err == nil {
		t.Fatal("expected invalid mode to be rejected")
	}
	if got := err.Error(); got != `invalid BUBU_TRANSPORTS[0]: mode "bi" must be hot or fallback` {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadTransportsFromEnvRejectsDuplicateNames(t *testing.T) {
	t.Setenv(contracts.TransportsEnv, `[
		{"name":"rt","kind":"livekit","mode":"hot"},
		{"name":"rt","kind":"webhook","mode":"fallback"}
	]`)

	_, err := loadTransportsFromEnv()
	if err == nil {
		t.Fatal("expected duplicate transport names to be rejected")
	}
	if got := err.Error(); got != `invalid BUBU_TRANSPORTS[1]: duplicate transport name "rt"` {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyExecutionSettingsFromEnvWarnsOnInvalidValues(t *testing.T) {
	t.Setenv(contracts.StepTimeoutEnv, "not-a-duration")
	t.Setenv(contracts.MaxInlineSizeEnv, "-1")
	t.Setenv(contracts.GRPCPortEnv, "abc")

	var buf bytes.Buffer
	prev := slog.Default()
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)
	defer slog.SetDefault(prev)

	exec := ExecutionInfo{
		StepTimeout:   5 * time.Second,
		MaxInlineSize: 256,
		GRPCPort:      8443,
	}

	applyExecutionSettingsFromEnv(&exec)

	if exec.StepTimeout != 5*time.Second {
		t.Fatalf("expected invalid step timeout override to be ignored, got %s", exec.StepTimeout)
	}
	if exec.MaxInlineSize != 256 {
		t.Fatalf("expected invalid max inline size override to be ignored, got %d", exec.MaxInlineSize)
	}
	if exec.GRPCPort != 8443 {
		t.Fatalf("expected invalid grpc port override to be ignored, got %d", exec.GRPCPort)
	}

	output := buf.String()
	for _, key := range []string{contracts.StepTimeoutEnv, contracts.MaxInlineSizeEnv, contracts.GRPCPortEnv} {
		if !strings.Contains(output, key) {
			t.Fatalf("expected warning log for %s, got %s", key, output)
		}
	}
}

func TestLoadStorageConfigFromEnvWarnsOnInvalidTimeout(t *testing.T) {
	t.Setenv(contracts.StorageProviderEnv, "s3")
	t.Setenv(contracts.StorageTimeoutEnv, "0")

	var buf bytes.Buffer
	prev := slog.Default()
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)
	defer slog.SetDefault(prev)

	cfg := loadStorageConfigFromEnv()
	if cfg == nil {
		t.Fatal("expected storage config to be created")
	}
	if cfg.Timeout != 0 {
		t.Fatalf("expected invalid storage timeout override to be ignored, got %s", cfg.Timeout)
	}
	if !strings.Contains(buf.String(), contracts.StorageTimeoutEnv) {
		t.Fatalf("expected warning log for %s, got %s", contracts.StorageTimeoutEnv, buf.String())
	}
}

func TestLoadStorageConfigFromEnvTrimsValues(t *testing.T) {
	t.Setenv(contracts.StorageProviderEnv, " s3 ")
	t.Setenv(contracts.StorageS3BucketEnv, " bucket-a ")
	t.Setenv(contracts.StorageS3RegionEnv, " us-east-1 ")
	t.Setenv(contracts.StorageS3EndpointEnv, " https://example.invalid ")

	cfg := loadStorageConfigFromEnv()
	if cfg == nil {
		t.Fatal("expected storage config to be created")
	}
	if cfg.Provider != "s3" {
		t.Fatalf("expected trimmed provider, got %q", cfg.Provider)
	}
	if cfg.S3 == nil {
		t.Fatal("expected S3 config to be created")
	}
	if cfg.S3.Bucket != "bucket-a" || cfg.S3.Region != "us-east-1" || cfg.S3.Endpoint != "https://example.invalid" {
		t.Fatalf("expected trimmed S3 fields, got %+v", cfg.S3)
	}
}

func TestLoadStorageConfigFromEnvIgnoresWhitespaceProvider(t *testing.T) {
	t.Setenv(contracts.StorageProviderEnv, "   ")

	if cfg := loadStorageConfigFromEnv(); cfg != nil {
		t.Fatalf("expected whitespace provider to be ignored, got %+v", cfg)
	}
}

func TestUnmarshalFromMap(t *testing.T) {
	type TestStruct struct {
		StringField string  `mapstructure:"stringField"`
		IntField    int     `mapstructure:"intField"`
		BoolField   bool    `mapstructure:"boolField"`
		FloatField  float64 `mapstructure:"floatField"`
	}

	tests := []struct {
		name    string
		data    map[string]any
		want    TestStruct
		wantErr bool
	}{
		{
			name: "all fields present",
			data: map[string]any{
				"stringField": "test",
				"intField":    42,
				"boolField":   true,
				"floatField":  3.14,
			},
			want: TestStruct{
				StringField: "test",
				IntField:    42,
				BoolField:   true,
				FloatField:  3.14,
			},
			wantErr: false,
		},
		{
			name: "type conversion - string to int",
			data: map[string]any{
				"intField": "123",
			},
			want: TestStruct{
				IntField: 123,
			},
			wantErr: false,
		},
		{
			name: "partial fields",
			data: map[string]any{
				"stringField": "partial",
			},
			want: TestStruct{
				StringField: "partial",
			},
			wantErr: false,
		},
		{
			name:    "empty map",
			data:    map[string]any{},
			want:    TestStruct{},
			wantErr: false,
		},
		{
			name: "extra fields ignored",
			data: map[string]any{
				"stringField": "test",
				"extraField":  "ignored",
			},
			want: TestStruct{
				StringField: "test",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UnmarshalFromMap[TestStruct](tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalFromMap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				if got.StringField != tt.want.StringField {
					t.Errorf("StringField = %v, want %v", got.StringField, tt.want.StringField)
				}
				if got.IntField != tt.want.IntField {
					t.Errorf("IntField = %v, want %v", got.IntField, tt.want.IntField)
				}
				if got.BoolField != tt.want.BoolField {
					t.Errorf("BoolField = %v, want %v", got.BoolField, tt.want.BoolField)
				}
				if got.FloatField != tt.want.FloatField {
					t.Errorf("FloatField = %v, want %v", got.FloatField, tt.want.FloatField)
				}
			}
		})
	}
}

func TestUnmarshalFromMap_NestedStructs(t *testing.T) {
	type Nested struct {
		Field string `mapstructure:"field"`
	}
	type Parent struct {
		Name   string `mapstructure:"name"`
		Nested Nested `mapstructure:"nested"`
	}

	data := map[string]any{
		"name": "parent",
		"nested": map[string]any{
			"field": "nested-value",
		},
	}

	got, err := UnmarshalFromMap[Parent](data)
	if err != nil {
		t.Fatalf("UnmarshalFromMap() error = %v", err)
	}

	if got.Name != "parent" {
		t.Errorf("Name = %v, want parent", got.Name)
	}
	if got.Nested.Field != "nested-value" {
		t.Errorf("Nested.Field = %v, want nested-value", got.Nested.Field)
	}
}

func TestUnmarshalFromMap_StrictRejectsUnusedFields(t *testing.T) {
	t.Setenv(runtimeStrictUnmarshalEnv, "true")

	type C struct {
		Name string `mapstructure:"name"`
	}

	_, err := UnmarshalFromMap[C](map[string]any{
		"name":  "ok",
		"extra": "unexpected",
	})
	if err == nil {
		t.Fatal("expected strict unmarshal mode to reject unused fields")
	}
	if !strings.Contains(err.Error(), "extra") {
		t.Fatalf("expected error to mention unused field, got %v", err)
	}
}

func TestStrictUnmarshalFromMapEnabledWarnsOnInvalidEnv(t *testing.T) {
	t.Setenv(runtimeStrictUnmarshalEnv, "definitely-not-bool")

	var buf bytes.Buffer
	prev := slog.Default()
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)
	defer slog.SetDefault(prev)

	if strictUnmarshalFromMapEnabled() {
		t.Fatal("expected invalid strict unmarshal env value to fall back to disabled")
	}
	if got := buf.String(); got == "" || !strings.Contains(got, runtimeStrictUnmarshalEnv) {
		t.Fatalf("expected warning log for invalid strict unmarshal env, got %q", got)
	}
}

func TestUnmarshalFromMap_Duration(t *testing.T) {
	type C struct {
		Timeout time.Duration `mapstructure:"timeout"`
	}

	// String-based duration
	cfg, err := UnmarshalFromMap[C](map[string]any{"timeout": "150ms"})
	if err != nil {
		t.Fatalf("UnmarshalFromMap duration string: %v", err)
	}
	if cfg.Timeout != 150*time.Millisecond {
		t.Errorf("Timeout = %v, want 150ms", cfg.Timeout)
	}

	// Numeric durations are rejected to force explicit units.
	_, err = UnmarshalFromMap[C](map[string]any{"timeout": int64(time.Second)})
	if err == nil {
		t.Fatalf("expected error for numeric duration, got nil")
	}
	if !strings.Contains(err.Error(), "duration values must be strings like 5s") {
		t.Fatalf("unexpected numeric duration error: %v", err)
	}

	_, err = UnmarshalFromMap[C](map[string]any{"timeout": float64(time.Second)})
	if err == nil {
		t.Fatalf("expected error for float duration, got nil")
	}
	if !strings.Contains(err.Error(), "duration values must be strings like 5s") {
		t.Fatalf("unexpected float duration error: %v", err)
	}

	_, err = UnmarshalFromMap[C](map[string]any{"timeout": uint(1)})
	if err == nil {
		t.Fatalf("expected error for unsigned numeric duration, got nil")
	}
	if !strings.Contains(err.Error(), "duration values must be strings like 5s") {
		t.Fatalf("unexpected unsigned numeric duration error: %v", err)
	}

	_, err = UnmarshalFromMap[C](map[string]any{"timeout": float32(1)})
	if err == nil {
		t.Fatalf("expected error for float32 duration, got nil")
	}
	if !strings.Contains(err.Error(), "duration values must be strings like 5s") {
		t.Fatalf("unexpected float32 duration error: %v", err)
	}

	// Invalid string should error
	_, err = UnmarshalFromMap[C](map[string]any{"timeout": "not-a-duration"})
	if err == nil {
		t.Errorf("expected error for invalid duration string, got nil")
	}
}

func TestUnmarshalFromMap_NilDataReturnsZeroValue(t *testing.T) {
	type C struct {
		Name string `mapstructure:"name"`
	}

	cfg, err := UnmarshalFromMap[C](nil)
	if err != nil {
		t.Fatalf("expected nil data to decode to zero value, got %v", err)
	}
	if cfg != (C{}) {
		t.Fatalf("expected zero value result, got %+v", cfg)
	}
}

// Helper function to clean environment variables
func cleanEnv(t *testing.T) {
	t.Helper()
	prefixes := []string{contracts.PrefixEnv}
	for _, prefix := range prefixes {
		for _, env := range os.Environ() {
			if len(env) > len(prefix) && env[:len(prefix)] == prefix {
				key := env[:indexOf(env, "=")]
				err := os.Unsetenv(key)
				if err != nil {
					t.Fatalf("Unsetenv() error = %v", err)
				}
			}
		}
	}
}

func indexOf(s, substr string) int {
	for i := 0; i < len(s); i++ {
		if i+len(substr) <= len(s) && s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func TestExecutionContextData_StartedAt(t *testing.T) {
	// Test with valid RFC3339 time
	validTime := time.Now().Format(time.RFC3339)
	err := os.Setenv(contracts.StartedAtEnv, validTime)
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	defer func() {
		err = os.Unsetenv(contracts.StartedAtEnv)
		if err != nil {
			t.Fatalf("Unsetenv() error = %v", err)
		}
	}()

	data, err := LoadExecutionContextData()
	if err != nil {
		t.Fatalf("LoadExecutionContextData() error = %v", err)
	}

	// Verify time was parsed
	if data.StartedAt.IsZero() {
		t.Error("StartedAt should not be zero when valid time is provided")
	}
}

func TestExecutionContextData_DefaultValues(t *testing.T) {
	cleanEnv(t)

	data, err := LoadExecutionContextData()
	if err != nil {
		t.Fatalf("LoadExecutionContextData() error = %v", err)
	}

	// Verify defaults
	if data.Inputs == nil {
		t.Error("Inputs should be initialized to empty map")
	}
	if data.Config == nil {
		t.Error("Config should be initialized to empty map")
	}
	if data.Secrets == nil {
		t.Error("Secrets should be initialized to empty map")
	}
}
