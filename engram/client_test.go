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

package engram

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bubustack/bubu-sdk-go/pkg/observability"
	"github.com/bubustack/tractatus/envelope"
	"github.com/stretchr/testify/assert"
)

const testMutatedValue = "mutated"

const testSecretValue = "value"

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
			s := NewSecrets(context.Background(), tt.secrets)
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

	s := NewSecrets(context.Background(), secrets)
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

func TestSecrets_NamesReturnsSortedCopy(t *testing.T) {
	s := NewSecrets(context.Background(), map[string]string{
		"zeta":  "3",
		"alpha": "1",
		"beta":  "2",
	})

	names := s.Names()
	assert.Equal(t, []string{"alpha", "beta", "zeta"}, names)

	names[0] = testMutatedValue
	assert.Equal(t, []string{"alpha", "beta", "zeta"}, s.Names())
}

func TestSecrets_SelectReturnsRequestedValuesOnly(t *testing.T) {
	s := NewSecrets(context.Background(), map[string]string{
		"API_KEY": "secret123",
		"TOKEN":   "token456",
	})

	selected := s.Select("TOKEN", "MISSING")
	assert.Equal(t, map[string]string{"TOKEN": "token456"}, selected)

	selected["TOKEN"] = "changed" //nolint:goconst
	value, ok := s.Get("TOKEN")
	assert.True(t, ok)
	assert.Equal(t, "token456", value)
}

func TestSecrets_AccessorsHandleNilReceiver(t *testing.T) {
	var s *Secrets

	value, ok := s.Get("missing")
	assert.False(t, ok)
	assert.Empty(t, value)
	assert.Empty(t, s.GetAll())
	assert.Empty(t, s.Names())
	assert.Empty(t, s.Select("missing"))
}

func TestNewSecrets_EnvPrefixExpansion(t *testing.T) {
	err := os.Setenv("PAY_apiKey", "abc")
	if err != nil {
		t.Fatal(err)
	}
	err = os.Setenv("PAY_token", "def")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = os.Unsetenv("PAY_apiKey")
		if err != nil {
			t.Fatal(err)
		}
		err = os.Unsetenv("PAY_token")
		if err != nil {
			t.Fatal(err)
		}
	}()

	s := NewSecrets(context.Background(), map[string]string{"payments": "env:PAY_"})
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

	s := NewSecrets(context.Background(), map[string]string{"db": "file:" + dir})
	if v, ok := s.Get("username"); !ok || v != "alice" {
		t.Fatalf("file expansion failed for username: ok=%v v=%q", ok, v)
	}
	if v, ok := s.Get("password"); !ok || v != "s3cr3t" {
		t.Fatalf("file expansion failed for password: ok=%v v=%q", ok, v)
	}
}

func TestNewSecrets_FileDirExpansionPreservesRelativePathsForNestedFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "db", "writer"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "db", "reader"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "db", "writer", "password"), []byte("writer-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "db", "reader", "password"), []byte("reader-secret"), 0o600); err != nil {
		t.Fatal(err)
	}

	s := NewSecrets(context.Background(), map[string]string{"db": "file:" + dir})
	if v, ok := s.Get("db/writer/password"); !ok || v != "writer-secret" {
		t.Fatalf("nested file expansion failed for db/writer/password: ok=%v v=%q", ok, v)
	}
	if v, ok := s.Get("db/reader/password"); !ok || v != "reader-secret" {
		t.Fatalf("nested file expansion failed for db/reader/password: ok=%v v=%q", ok, v)
	}
	if _, ok := s.Get("password"); ok {
		t.Fatal("nested file expansion must not collapse duplicate basenames")
	}
}

func TestNewSecrets_FileDirExpansionRejectsSymlinkedFiles(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(t.TempDir(), "outside-token")
	if err := os.WriteFile(target, []byte("top-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "token")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}

	s := NewSecrets(context.Background(), map[string]string{"db": "file:" + dir})
	if _, ok := s.Get("token"); ok {
		t.Fatal("symlinked files must not be imported as secrets")
	}
}

func TestNewSecrets_FileDirExpansionRejectsOversizedFiles(t *testing.T) {
	dir := t.TempDir()
	oversized := strings.Repeat("x", defaultSecretExpansionMaxFileBytes+1)
	if err := os.WriteFile(filepath.Join(dir, "huge"), []byte(oversized), 0o600); err != nil {
		t.Fatal(err)
	}

	s := NewSecrets(context.Background(), map[string]string{"db": "file:" + dir})
	if _, ok := s.Get("huge"); ok {
		t.Fatal("oversized secret files must not be imported")
	}
}

func TestNewSecrets_FileDirExpansionRejectsTooManyFiles(t *testing.T) {
	dir := t.TempDir()
	for i := range defaultSecretExpansionMaxFiles + 1 {
		name := filepath.Join(dir, fmt.Sprintf("secret-%03d", i))
		if err := os.WriteFile(name, []byte(testSecretValue), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	s := NewSecrets(context.Background(), map[string]string{"db": "file:" + dir})
	names := s.Names()
	if len(names) != 0 {
		t.Fatalf(
			"expected descriptor to fail closed when file count exceeds %d, got %d imported secrets",
			defaultSecretExpansionMaxFiles,
			len(names),
		)
	}
}

func TestNewSecrets_DoesNotFallbackToRawDescriptorOnFileExpansionFailure(t *testing.T) {
	missingDir := filepath.Join(t.TempDir(), "missing")
	s := NewSecrets(context.Background(), map[string]string{"db": "file:" + missingDir})

	if _, ok := s.Get("db"); ok {
		t.Fatal("failed file expansion must not expose the raw descriptor as a secret value")
	}
}

func TestNewSecrets_RedactsDescriptorInWarningLogs(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)
	defer slog.SetDefault(prev)

	missingDir := filepath.Join(t.TempDir(), "missing")
	NewSecrets(context.Background(), map[string]string{
		"db":     "file:" + missingDir,
		"badEnv": "env:",
	})

	output := buf.String()
	if strings.Contains(output, missingDir) {
		t.Fatalf("warning logs must not include raw secret paths: %s", output)
	}
	if strings.Contains(output, "file:"+missingDir) {
		t.Fatalf("warning logs must not include raw file descriptors: %s", output)
	}
	if strings.Contains(output, "\"descriptor\":") {
		t.Fatalf("warning logs must not include a raw descriptor field: %s", output)
	}
	if !strings.Contains(output, "\"secret\":\"db\"") || !strings.Contains(output, "\"descriptorKind\":\"file\"") {
		t.Fatalf("warning logs should retain the logical secret name and kind for file failures: %s", output)
	}
	if !strings.Contains(output, "\"secret\":\"badEnv\"") || !strings.Contains(output, "\"descriptorKind\":\"env\"") {
		t.Fatalf("warning logs should retain the logical secret name and kind for env failures: %s", output)
	}
}

func TestNewSecretsWithErrorReturnsPartialSecretsAndRedactedErrors(t *testing.T) {
	missingDir := filepath.Join(t.TempDir(), "missing")

	secrets, err := NewSecretsWithError(context.Background(), map[string]string{
		"literal": testSecretValue,
		"db":      "file:" + missingDir,
		"badEnv":  "env:",
	})
	if err == nil {
		t.Fatal("expected secret expansion error")
	}
	if !errors.Is(err, ErrSecretExpansionFailed) {
		t.Fatalf("expected ErrSecretExpansionFailed, got %v", err)
	}
	if secrets == nil {
		t.Fatal("expected partial secrets to be returned")
	}
	if v, ok := secrets.Get("literal"); !ok || v != testSecretValue {
		t.Fatalf("expected literal secret to be preserved, ok=%v v=%q", ok, v)
	}
	if _, ok := secrets.Get("db"); ok {
		t.Fatal("failed file expansion must not expose the raw descriptor")
	}
	if _, ok := secrets.Get("badEnv"); ok {
		t.Fatal("failed env expansion must not expose the raw descriptor")
	}
	msg := err.Error()
	if !strings.Contains(msg, `secret "db" (file)`) {
		t.Fatalf("expected file secret name in error, got: %s", msg)
	}
	if !strings.Contains(msg, `secret "badEnv" (env)`) {
		t.Fatalf("expected env secret name in error, got: %s", msg)
	}
	if strings.Contains(msg, missingDir) {
		t.Fatalf("error must not leak raw secret path: %s", msg)
	}
	if strings.Contains(msg, "file:"+missingDir) {
		t.Fatalf("error must not leak raw file descriptor: %s", msg)
	}
	if strings.Contains(msg, "env:") {
		t.Fatalf("error must not leak raw env descriptor: %s", msg)
	}
}

func TestNewSecretsWithErrorRejectsNilContext(t *testing.T) {
	_, err := NewSecretsWithError(nil, map[string]string{"key": testSecretValue}) //nolint:staticcheck
	if err == nil {
		t.Fatal("expected nil context to return an error")
	}
}

func TestNewSecretsNilContextFailsClosed(t *testing.T) {
	secrets := NewSecrets(nil, map[string]string{"literal": testSecretValue}) //nolint:staticcheck
	if secrets == nil {
		t.Fatal("expected empty secrets, got nil")
	}
	if _, ok := secrets.Get("literal"); ok {
		t.Fatal("expected nil-context NewSecrets call to fail closed")
	}
	if got := secrets.GetAll(); len(got) != 0 {
		t.Fatalf("expected no secrets after nil-context failure, got %v", got)
	}
}

func TestNewSecretsWithErrorFailsClosedForUnreadableDirectoryEntries(t *testing.T) {
	dir := t.TempDir()
	goodPath := filepath.Join(dir, "good.txt")
	badPath := filepath.Join(dir, "bad.txt")
	if err := os.WriteFile(goodPath, []byte("good"), 0o600); err != nil {
		t.Fatalf("write good secret: %v", err)
	}
	if err := os.WriteFile(badPath, []byte("bad"), 0o600); err != nil {
		t.Fatalf("write unreadable secret: %v", err)
	}
	if err := os.Chmod(badPath, 0o000); err != nil {
		t.Fatalf("chmod unreadable secret: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(badPath, 0o600)
	})
	if _, err := os.ReadFile(badPath); err == nil {
		t.Skip("filesystem permissions still allow reading chmod 000 file")
	}

	secrets, err := NewSecretsWithError(context.Background(), map[string]string{
		"literal": "value",
		"dir":     "file:" + dir,
	})
	if err == nil {
		t.Fatal("expected unreadable directory entry error")
	}
	if !errors.Is(err, ErrSecretExpansionFailed) {
		t.Fatalf("expected ErrSecretExpansionFailed, got %v", err)
	}
	if secrets == nil {
		t.Fatal("expected partial secrets for unrelated descriptors")
	}
	if got, ok := secrets.Get("literal"); !ok || got != "value" {
		t.Fatalf("expected literal secret to survive, ok=%v got=%q", ok, got)
	}
	if _, ok := secrets.Get("good.txt"); ok {
		t.Fatal("expected directory-backed secrets to fail closed when any entry is unreadable")
	}
}

func TestSecrets_Format(t *testing.T) {
	s := NewSecrets(context.Background(), map[string]string{"key": "value"})
	output := fmt.Sprintf("%v", s)
	assert.Equal(t, "[redacted secrets]", output)
}

func TestSecrets_LogValueRedactsStructuredLogging(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	secrets := NewSecrets(context.Background(), map[string]string{"apiKey": "secret-value"})

	logger.Info("testing secrets", "secrets", secrets)

	output := buf.String()
	if strings.Contains(output, "secret-value") {
		t.Fatalf("structured logging must not include secret values: %s", output)
	}
	if strings.Contains(output, "apiKey") {
		t.Fatalf("structured logging must not include secret keys by default: %s", output)
	}
	if !strings.Contains(output, "[redacted secrets]") {
		t.Fatalf("structured logging should emit the redacted sentinel, got: %s", output)
	}
}

func TestNewSecrets_NilInput(t *testing.T) {
	s := NewSecrets(context.Background(), nil)

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

func TestNewSecrets_ContextCancellationStopsExpansion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := NewSecrets(ctx, map[string]string{"literal": "value"})
	if _, ok := s.Get("literal"); ok {
		t.Fatalf("expected literal secrets to be skipped when context is canceled")
	}
}

func TestNewSecrets_EmptyEnvPrefixDoesNotExpandWholeEnvironment(t *testing.T) {
	if err := os.Setenv("BUBU_TEST_SECRET_ONE", "one"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("BUBU_TEST_SECRET_TWO", "two"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.Unsetenv("BUBU_TEST_SECRET_ONE")
		_ = os.Unsetenv("BUBU_TEST_SECRET_TWO")
	}()

	s := NewSecrets(context.Background(), map[string]string{
		"literal": "value",
		"bad":     "env:",
	})

	if v, ok := s.Get("literal"); !ok || v != "value" {
		t.Fatalf("literal secret should remain available, ok=%v v=%q", ok, v)
	}
	if _, ok := s.Get("BUBU_TEST_SECRET_ONE"); ok {
		t.Fatal("empty env prefix must not import the full process environment")
	}
	if _, ok := s.Get("BUBU_TEST_SECRET_TWO"); ok {
		t.Fatal("empty env prefix must not import the full process environment")
	}
}

func TestNewSecrets_LiteralSecretsOverrideExpandedCollisions(t *testing.T) {
	if err := os.Setenv("PAY_token", "expanded-token"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.Unsetenv("PAY_token")
	}()

	s := NewSecrets(context.Background(), map[string]string{
		"payments": "env:PAY_",
		"token":    "literal-token",
	})
	if v, ok := s.Get("token"); !ok || v != "literal-token" {
		t.Fatalf("literal secret should override expansion collisions, ok=%v v=%q", ok, v)
	}
}

func TestNewExecutionContext(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	tracer := observability.Tracer("test")
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

func TestNewExecutionContextWithCELContextClonesInput(t *testing.T) {
	celContext := map[string]any{
		"inputs": map[string]any{
			"message": "original",
		},
		"steps": []any{
			map[string]any{"name": "first"},
		},
	}

	ec := NewExecutionContextWithCELContext(nil, nil, StoryInfo{}, celContext)

	inputs := celContext["inputs"].(map[string]any)
	inputs["message"] = testMutatedValue
	steps := celContext["steps"].([]any)
	steps[0].(map[string]any)["name"] = "changed"

	got := ec.CELContext()
	if got["inputs"].(map[string]any)["message"] != "original" {
		t.Fatalf("constructor must isolate CEL context from caller mutation, got %v", got["inputs"])
	}
	if got["steps"].([]any)[0].(map[string]any)["name"] != "first" {
		t.Fatalf("constructor must deep copy nested CEL context values, got %v", got["steps"])
	}
}

func TestExecutionContextCELContextReturnsDefensiveCopy(t *testing.T) {
	ec := NewExecutionContextWithCELContext(nil, nil, StoryInfo{}, map[string]any{
		"inputs": map[string]any{
			"message": "original",
		},
		"steps": []any{
			map[string]any{"name": "first"},
		},
	})

	first := ec.CELContext()
	first["inputs"].(map[string]any)["message"] = testMutatedValue
	first["steps"].([]any)[0].(map[string]any)["name"] = "changed"

	second := ec.CELContext()
	if second["inputs"].(map[string]any)["message"] != "original" {
		t.Fatalf("CELContext must return a defensive copy, got %v", second["inputs"])
	}
	if second["steps"].([]any)[0].(map[string]any)["name"] != "first" {
		t.Fatalf("CELContext must deep copy nested values, got %v", second["steps"])
	}
}

func TestStreamMessageValidateRejectsMultipleFrameTypes(t *testing.T) {
	msg := StreamMessage{
		Audio: &AudioFrame{PCM: []byte{0x01}},
		Video: &VideoFrame{Payload: []byte{0x02}, Codec: "vp8"},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "audio, video") {
		t.Fatalf("expected frame names in validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsBinaryPayloadMismatch(t *testing.T) {
	msg := StreamMessage{
		Payload: []byte(`{"ok":true}`),
		Binary: &BinaryFrame{
			Payload:  []byte("raw"),
			MimeType: "application/octet-stream",
		},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "payload and binary payload must match") {
		t.Fatalf("expected binary mismatch validation error, got %v", err)
	}
}

func TestStreamMessageValidateAllowsBinaryPayloadMirror(t *testing.T) {
	msg := StreamMessage{
		Payload: []byte("raw"),
		Binary: &BinaryFrame{
			Payload:  []byte("raw"),
			MimeType: "application/octet-stream",
		},
	}

	if err := msg.Validate(); err != nil {
		t.Fatalf("expected payload-backed binary mirror to remain valid, got %v", err)
	}
}

func TestStreamMessageValidateRejectsKindWithSurroundingWhitespace(t *testing.T) {
	msg := StreamMessage{
		Kind:    " telemetry ",
		Payload: []byte(`{"ok":true}`),
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "must not have surrounding whitespace") {
		t.Fatalf("expected kind whitespace validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsMessageIDWithSurroundingWhitespace(t *testing.T) {
	msg := StreamMessage{
		MessageID: " msg-1 ",
		Payload:   []byte(`{"ok":true}`),
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "message_id must not have surrounding whitespace") {
		t.Fatalf("expected message_id whitespace validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsEmptyMetadataKey(t *testing.T) {
	msg := StreamMessage{
		Payload:  []byte(`{"ok":true}`),
		Metadata: map[string]string{"": "value"},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "metadata keys must not be empty") {
		t.Fatalf("expected empty metadata key validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsMetadataKeyWithSurroundingWhitespace(t *testing.T) {
	msg := StreamMessage{
		Payload:  []byte(`{"ok":true}`),
		Metadata: map[string]string{" trace-id ": "abc"},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "metadata key") || !strings.Contains(err.Error(), "surrounding whitespace") {
		t.Fatalf("expected metadata key whitespace validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsErrorKindWithoutPayload(t *testing.T) {
	msg := StreamMessage{Kind: StreamMessageKindError}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "error messages require payload") {
		t.Fatalf("expected error-kind payload validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsHeartbeatWithPayload(t *testing.T) {
	msg := StreamMessage{
		Kind:    StreamMessageKindHeartbeat,
		Payload: []byte(`{"ok":true}`),
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "heartbeat messages must not carry payload, metadata, or frames") {
		t.Fatalf("expected heartbeat validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsAudioWithoutPCM(t *testing.T) {
	msg := StreamMessage{
		Audio: &AudioFrame{
			SampleRateHz: 16000,
			Channels:     1,
		},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "audio frame missing pcm payload") {
		t.Fatalf("expected audio payload validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsAudioCodecWithSurroundingWhitespace(t *testing.T) {
	msg := StreamMessage{
		Audio: &AudioFrame{
			PCM:          []byte{0x01},
			SampleRateHz: 16000,
			Channels:     1,
			Codec:        " pcm16 ",
		},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "audio frame codec must not have surrounding whitespace") {
		t.Fatalf("expected audio codec whitespace validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsAudioWithoutSampleRateOrChannels(t *testing.T) {
	msg := StreamMessage{
		Audio: &AudioFrame{
			PCM:          []byte{0x01},
			SampleRateHz: 0,
			Channels:     0,
		},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "sample rate must be positive") {
		t.Fatalf("expected audio sample-rate validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsRawVideoWithoutDimensions(t *testing.T) {
	msg := StreamMessage{
		Video: &VideoFrame{
			Payload: []byte{0x02},
			Raw:     true,
		},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "raw video frame requires width and height") {
		t.Fatalf("expected raw video dimension validation error, got %v", err)
	}
}

func TestStreamMessageValidateAllowsEncodedVideoWithoutDimensions(t *testing.T) {
	msg := StreamMessage{
		Video: &VideoFrame{
			Payload: []byte{0x02},
			Codec:   "vp8",
		},
	}

	if err := msg.Validate(); err != nil {
		t.Fatalf("expected encoded video without explicit dimensions to remain valid, got %v", err)
	}
}

func TestStreamMessageValidateRejectsEncodedVideoWithoutCodec(t *testing.T) {
	msg := StreamMessage{
		Video: &VideoFrame{
			Payload: []byte{0x02},
		},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "encoded video frame requires codec") {
		t.Fatalf("expected encoded video codec validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsVideoCodecWithSurroundingWhitespace(t *testing.T) {
	msg := StreamMessage{
		Video: &VideoFrame{
			Payload: []byte{0x02},
			Codec:   " vp8 ",
		},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "video frame codec must not have surrounding whitespace") {
		t.Fatalf("expected video codec whitespace validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsNegativeBinaryTimestamp(t *testing.T) {
	msg := StreamMessage{
		Binary: &BinaryFrame{
			Payload:   []byte{0x01},
			MimeType:  "application/octet-stream",
			Timestamp: -1 * time.Millisecond,
		},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "binary frame timestamp must not be negative") {
		t.Fatalf("expected negative binary timestamp validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsBinaryWithoutPayload(t *testing.T) {
	msg := StreamMessage{
		Binary: &BinaryFrame{
			MimeType: "application/octet-stream",
		},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "binary frame missing payload") {
		t.Fatalf("expected missing binary payload validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsBinaryMimeTypeWithSurroundingWhitespace(t *testing.T) {
	msg := StreamMessage{
		Binary: &BinaryFrame{
			Payload:  []byte{0x01},
			MimeType: " application/octet-stream ",
		},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "binary frame mime type must not have surrounding whitespace") {
		t.Fatalf("expected binary MIME whitespace validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsInvalidBinaryMimeType(t *testing.T) {
	msg := StreamMessage{
		Binary: &BinaryFrame{
			Payload:  []byte{0x01},
			MimeType: "not a mime type",
		},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "mime type") || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("expected invalid MIME validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsReservedEnvelopeMimeWithoutEnvelopeFields(t *testing.T) {
	msg := StreamMessage{
		Binary: &BinaryFrame{
			Payload:  []byte("raw"),
			MimeType: envelope.MIMEType,
		},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "reserved for envelope payloads") {
		t.Fatalf("expected reserved envelope MIME validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsReservedEnvelopeMimePayloadMismatch(t *testing.T) {
	msg := StreamMessage{
		Kind: "telemetry",
		Binary: &BinaryFrame{
			Payload:  []byte("raw"),
			MimeType: envelope.MIMEType,
		},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "mirror the structured payload") {
		t.Fatalf("expected reserved envelope mirror validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsReservedEnvelopeMimeWithParametersWithoutEnvelopeFields(t *testing.T) {
	msg := StreamMessage{
		Binary: &BinaryFrame{
			Payload:  []byte(`{"ok":true}`),
			MimeType: envelope.MIMEType + "; charset=utf-8",
		},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "reserved for envelope payloads") {
		t.Fatalf("expected reserved envelope MIME validation error, got %v", err)
	}
}

func TestStreamMessageValidateRejectsReservedEnvelopeMimeCaseInsensitivePayloadMismatch(t *testing.T) {
	msg := StreamMessage{
		Kind:    "telemetry",
		Payload: []byte(`{"ok":true}`),
		Binary: &BinaryFrame{
			Payload:  []byte(`{"ok":false}`),
			MimeType: strings.ToUpper(envelope.MIMEType),
		},
	}

	err := msg.Validate()
	if !errors.Is(err, ErrInvalidStreamMessage) {
		t.Fatalf("expected ErrInvalidStreamMessage, got %v", err)
	}
	if !strings.Contains(err.Error(), "mirror the structured payload") {
		t.Fatalf("expected reserved envelope mirror validation error, got %v", err)
	}
}

func TestStreamMessageValidateAllowsReservedEnvelopeMimeWithParametersPayloadMirror(t *testing.T) {
	msg := StreamMessage{
		Kind:    "telemetry",
		Payload: []byte(`{"ok":true}`),
		Binary: &BinaryFrame{
			Payload:  []byte(`{"ok":true}`),
			MimeType: envelope.MIMEType + "; charset=utf-8",
		},
	}

	if err := msg.Validate(); err != nil {
		t.Fatalf("expected mirrored reserved envelope payload with parameters to remain valid, got %v", err)
	}
}

func TestStreamMessageValidateAllowsReservedEnvelopeMimePayloadMirror(t *testing.T) {
	msg := StreamMessage{
		Kind:    "telemetry",
		Payload: []byte(`{"ok":true}`),
		Binary: &BinaryFrame{
			Payload:  []byte(`{"ok":true}`),
			MimeType: envelope.MIMEType,
		},
	}

	if err := msg.Validate(); err != nil {
		t.Fatalf("expected mirrored reserved envelope payload to remain valid, got %v", err)
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

func TestNewResultFrom(t *testing.T) {
	payload := map[string]any{"message": "ok"}
	got := NewResultFrom(payload)
	assert.NotNil(t, got)
	assert.Equal(t, payload, got.Data)
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

func (t *testStreamingEngram) Stream(ctx context.Context, in <-chan InboundMessage, out chan<- StreamMessage) error {
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
