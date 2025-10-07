package engram

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
)

func TestSecrets_Get(t *testing.T) {
	tests := []struct {
		name       string
		secrets    map[string]string
		key        string
		wantValue  string
		wantExists bool
	}{
		{
			name: "existing secret",
			secrets: map[string]string{
				"API_KEY": "secret123",
			},
			key:        "API_KEY",
			wantValue:  "secret123",
			wantExists: true,
		},
		{
			name:       "non-existing secret",
			secrets:    map[string]string{},
			key:        "MISSING_KEY",
			wantValue:  "",
			wantExists: false,
		},
		{
			name: "empty string secret",
			secrets: map[string]string{
				"EMPTY": "",
			},
			key:        "EMPTY",
			wantValue:  "",
			wantExists: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSecrets(tt.secrets)
			gotValue, gotExists := s.Get(tt.key)

			if gotValue != tt.wantValue {
				t.Errorf("Get() value = %v, want %v", gotValue, tt.wantValue)
			}
			if gotExists != tt.wantExists {
				t.Errorf("Get() exists = %v, want %v", gotExists, tt.wantExists)
			}
		})
	}
}

func TestSecrets_GetAll(t *testing.T) {
	secrets := map[string]string{
		"KEY1": "value1",
		"KEY2": "value2",
	}

	s := NewSecrets(secrets)
	got := s.GetAll()

	if len(got) != len(secrets) {
		t.Errorf("GetAll() returned %v secrets, want %v", len(got), len(secrets))
	}

	// GetAll() should redact all values for security
	for k := range secrets {
		if got[k] != "[REDACTED]" {
			t.Errorf("GetAll()[%v] = %v, want [REDACTED]", k, got[k])
		}
	}
}

func TestNewSecrets_EnvPrefixExpansion(t *testing.T) {
	os.Setenv("PAY_apiKey", "abc")
	os.Setenv("PAY_token", "def")
	defer func() {
		os.Unsetenv("PAY_apiKey")
		os.Unsetenv("PAY_token")
	}()

	s := NewSecrets(map[string]string{"payments": "env:PAY_"})
	if v, ok := s.Get("apiKey"); !ok || v != "abc" {
		t.Fatalf("env expansion failed for apiKey: ok=%v v=%q", ok, v)
	}
	if v, ok := s.Get("token"); !ok || v != "def" {
		t.Fatalf("env expansion failed for token: ok=%v v=%q", ok, v)
	}
}

func TestNewSecrets_FileDirExpansion(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "username"), []byte("alice\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "password"), []byte("s3cr3t"), 0o600); err != nil {
		t.Fatal(err)
	}

	s := NewSecrets(map[string]string{"db": "file:" + dir})
	if v, ok := s.Get("username"); !ok || v != "alice" {
		t.Fatalf("file expansion failed for username: ok=%v v=%q", ok, v)
	}
	if v, ok := s.Get("password"); !ok || v != "s3cr3t" {
		t.Fatalf("file expansion failed for password: ok=%v v=%q", ok, v)
	}
}

func TestSecrets_Format(t *testing.T) {
	s := NewSecrets(map[string]string{"key": "value"})
	output := fmt.Sprintf("%v", s)
	assert.Equal(t, "[redacted secrets]", output)
}

func TestNewSecrets_NilInput(t *testing.T) {
	s := NewSecrets(nil)

	if s == nil {
		t.Fatal("NewSecrets(nil) returned nil")
	}

	// Should return empty map, not panic
	all := s.GetAll()
	if all == nil {
		t.Error("GetAll() should return empty map, not nil")
	}
	if len(all) != 0 {
		t.Errorf("GetAll() should return empty map, got %v entries", len(all))
	}
}

func TestNewExecutionContext(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	tracer := otel.Tracer("test")
	storyInfo := StoryInfo{
		StoryName:  "test-story",
		StoryRunID: "run-123",
		StepName:   "test-step",
		StepRunID:  "step-123",
	}

	ec := NewExecutionContext(logger, tracer, storyInfo)

	if ec == nil {
		t.Fatal("NewExecutionContext returned nil")
	}

	// Test getters
	if ec.Logger() != logger {
		t.Error("Logger() returned different logger")
	}
	if ec.Tracer() != tracer {
		t.Error("Tracer() returned different tracer")
	}

	gotInfo := ec.StoryInfo()
	if gotInfo.StoryName != storyInfo.StoryName {
		t.Errorf("StoryInfo().StoryName = %v, want %v", gotInfo.StoryName, storyInfo.StoryName)
	}
	if gotInfo.StoryRunID != storyInfo.StoryRunID {
		t.Errorf("StoryInfo().StoryRunID = %v, want %v", gotInfo.StoryRunID, storyInfo.StoryRunID)
	}
	if gotInfo.StepName != storyInfo.StepName {
		t.Errorf("StoryInfo().StepName = %v, want %v", gotInfo.StepName, storyInfo.StepName)
	}
	if gotInfo.StepRunID != storyInfo.StepRunID {
		t.Errorf("StoryInfo().StepRunID = %v, want %v", gotInfo.StepRunID, storyInfo.StepRunID)
	}
}

func TestResult(t *testing.T) {
	tests := []struct {
		name   string
		result *Result
	}{
		{
			name: "result with data",
			result: &Result{
				Data: map[string]any{"key": "value"},
			},
		},
		{
			name:   "result with no data",
			result: &Result{},
		},
		{
			name:   "empty result",
			result: &Result{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just verify the struct can be created and accessed
			if tt.result == nil {
				t.Error("Result should not be nil")
			}
		})
	}
}

// Test that interfaces can be satisfied (compile-time check)
type testBatchEngram struct{}

func (t *testBatchEngram) Init(ctx context.Context, config string, secrets *Secrets) error {
	return nil
}

func (t *testBatchEngram) Process(ctx context.Context, execCtx *ExecutionContext, inputs string) (*Result, error) {
	return &Result{Data: "processed"}, nil
}

type testStreamingEngram struct{}

func (t *testStreamingEngram) Init(ctx context.Context, config string, secrets *Secrets) error {
	return nil
}

func (t *testStreamingEngram) Stream(ctx context.Context, in <-chan StreamMessage, out chan<- StreamMessage) error {
	return nil
}

func TestInterfaceImplementations(t *testing.T) {
	// Compile-time verification that interfaces are satisfied
	var _ BatchEngram[string, string] = (*testBatchEngram)(nil)
	var _ StreamingEngram[string] = (*testStreamingEngram)(nil)
	var _ Engram[string] = (*testBatchEngram)(nil)
	var _ Engram[string] = (*testStreamingEngram)(nil)
	t.Log("compile-time interface checks passed")
}
