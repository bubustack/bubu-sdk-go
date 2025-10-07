package runtime

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/bubustack/bubu-sdk-go/engram"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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
				"BUBU_INPUTS":       `{"key":"value","number":123}`,
				"BUBU_CONFIG_KEY1":  "value1",
				"BUBU_CONFIG_KEY2":  "value2",
				"BUBU_SECRET_API":   "secret123",
				"BUBU_STORY_NAME":   "test-story",
				"BUBU_STORYRUN_ID":  "run-123",
				"BUBU_STEP_NAME":    "test-step",
				"BUBU_STEPRUN_NAME": "step-run-123",
				"BUBU_STARTED_AT":   nowStr,
			},
			want: &ExecutionContextData{
				Inputs:  map[string]any{"key": "value", "number": float64(123)}, // JSON unmarshals numbers to float64
				Config:  map[string]any{"KEY1": "value1", "KEY2": "value2"},
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
				"BUBU_STORY_NAME": "minimal-story",
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
			name: "invalid JSON inputs",
			envVars: map[string]string{
				"BUBU_INPUTS": `{invalid json}`,
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
				os.Setenv(k, v)
			}
			// Ensure variables are cleaned up after the test
			defer func() {
				for k := range tt.envVars {
					os.Unsetenv(k)
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
				// Ignore the StartedAt field for minimal and empty context as it's time-sensitive
				if tt.name == "minimal context" || tt.name == "empty environment" {
					tt.want.StartedAt = got.StartedAt
				}

				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("LoadExecutionContextData() got = %+v, want %+v", got, tt.want)
				}
			}
		})
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

	// Integer nanoseconds
	cfg, err = UnmarshalFromMap[C](map[string]any{"timeout": int64(time.Second)})
	if err != nil {
		t.Fatalf("UnmarshalFromMap duration int64: %v", err)
	}
	if cfg.Timeout != time.Second {
		t.Errorf("Timeout = %v, want 1s", cfg.Timeout)
	}

	// Invalid string should error
	_, err = UnmarshalFromMap[C](map[string]any{"timeout": "not-a-duration"})
	if err == nil {
		t.Errorf("expected error for invalid duration string, got nil")
	}
}

// Helper function to clean environment variables
func cleanEnv(t *testing.T) {
	t.Helper()
	prefixes := []string{"BUBU_"}
	for _, prefix := range prefixes {
		for _, env := range os.Environ() {
			if len(env) > len(prefix) && env[:len(prefix)] == prefix {
				key := env[:indexOf(env, "=")]
				os.Unsetenv(key)
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
	os.Setenv("BUBU_STARTED_AT", validTime)
	defer os.Unsetenv("BUBU_STARTED_AT")

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
