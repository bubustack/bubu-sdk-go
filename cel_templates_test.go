package sdk

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestResolveTemplatesEnforcesConfiguredOutputLimit(t *testing.T) {
	t.Setenv(templateMaxOutputBytesEnv, "4")

	_, err := resolveTemplates(
		context.Background(),
		slog.Default(),
		nil,
		nil,
		map[string]any{"value": `{{ "abcdef" }}`},
	)
	if err == nil {
		t.Fatal("expected template output limit error")
	}
	if !strings.Contains(err.Error(), "template output exceeds max bytes") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolveTemplatesRejectsOversizedPayloadInput(t *testing.T) {
	t.Setenv(templateMaxInputBytesEnv, "64")

	_, err := resolveTemplates(
		context.Background(),
		slog.Default(),
		nil,
		nil,
		map[string]any{"value": strings.Repeat("x", 512)},
	)
	if err == nil {
		t.Fatal("expected template payload input limit error")
	}
	if !strings.Contains(err.Error(), "template payload exceeds max input bytes") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolveTemplatesRejectsOversizedContextInput(t *testing.T) {
	t.Setenv(templateMaxInputBytesEnv, "96")

	_, err := resolveTemplates(
		context.Background(),
		slog.Default(),
		nil,
		map[string]any{
			"inputs": map[string]any{
				"blob": strings.Repeat("y", 512),
			},
		},
		map[string]any{"value": `{{ inputs.blob }}`},
	)
	if err == nil {
		t.Fatal("expected template context input limit error")
	}
	if !strings.Contains(err.Error(), "template context exceeds max input bytes") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolveTemplateMaxInputBytesFromEnv(t *testing.T) {
	t.Setenv(templateMaxInputBytesEnv, "2048")

	if got := resolveTemplateMaxInputBytes(); got != 2048 {
		t.Fatalf("resolveTemplateMaxInputBytes() = %d, want 2048", got)
	}
}

func TestResolveTemplateMaxInputBytesFallsBackOnInvalidEnv(t *testing.T) {
	t.Setenv(templateMaxInputBytesEnv, "invalid")

	if got := resolveTemplateMaxInputBytes(); got != defaultTemplateMaxInputBytes {
		t.Fatalf("resolveTemplateMaxInputBytes() = %d, want %d", got, defaultTemplateMaxInputBytes)
	}
}

func TestResolveTemplateEvaluationTimeoutFromEnv(t *testing.T) {
	t.Setenv(templateEvalTimeoutEnv, "7s")

	if got := resolveTemplateEvaluationTimeout(); got != 7*time.Second {
		t.Fatalf("resolveTemplateEvaluationTimeout() = %v, want 7s", got)
	}
}

func TestResolveTemplateEvaluationTimeoutFallsBackOnInvalidEnv(t *testing.T) {
	t.Setenv(templateEvalTimeoutEnv, "invalid")

	if got := resolveTemplateEvaluationTimeout(); got != defaultTemplateEvalTimeout {
		t.Fatalf("resolveTemplateEvaluationTimeout() = %v, want %v", got, defaultTemplateEvalTimeout)
	}
}

func TestResolveTemplatesRejectsPayloadWhenInputSizeCannotBeMeasured(t *testing.T) {
	_, err := resolveTemplates(
		context.Background(),
		slog.Default(),
		nil,
		nil,
		map[string]any{"bad": make(chan int)},
	)
	if err == nil {
		t.Fatal("expected payload size measurement error")
	}
	if !strings.Contains(err.Error(), "size check failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolveTemplatesRejectsContextWhenInputSizeCannotBeMeasured(t *testing.T) {
	_, err := resolveTemplates(
		context.Background(),
		slog.Default(),
		nil,
		map[string]any{"bad": make(chan int)},
		map[string]any{"value": "ok"},
	)
	if err == nil {
		t.Fatal("expected context size measurement error")
	}
	if !strings.Contains(err.Error(), "size check failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}
