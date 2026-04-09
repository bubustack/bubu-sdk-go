package cel

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/bubustack/core/templating"
)

func TestEvaluateExpression(t *testing.T) {
	eval, err := NewEvaluator(nil, Config{})
	if err != nil {
		t.Fatalf("NewEvaluator error: %v", err)
	}
	t.Cleanup(eval.Close)

	vars := map[string]any{"inputs": map[string]any{"value": 2}}
	out, err := eval.EvaluateExpression(context.Background(), "add inputs.value 3", vars)
	if err != nil {
		t.Fatalf("EvaluateExpression error: %v", err)
	}
	switch v := out.(type) {
	case int:
		if v != 5 {
			t.Fatalf("expected 5, got %#v", out)
		}
	case int64:
		if v != 5 {
			t.Fatalf("expected 5, got %#v", out)
		}
	case float64:
		if v != 5 {
			t.Fatalf("expected 5, got %#v", out)
		}
	default:
		t.Fatalf("expected numeric 5, got %T (%#v)", out, out)
	}
}

func TestExtractExpression(t *testing.T) {
	payload := map[string]any{
		templating.TemplateExprKey: "inputs.value",
		templating.TemplateVarsKey: map[string]any{"inputs": map[string]any{"value": 1}},
	}
	expr, ok := ExtractExpression(payload)
	if !ok {
		t.Fatalf("expected expression to be extracted")
	}
	if expr.Expr != "inputs.value" {
		t.Fatalf("unexpected expr: %s", expr.Expr)
	}
	if expr.Vars == nil {
		t.Fatalf("expected vars")
	}
}

func TestNewEvaluatorUsesLoggerForSuccessfulExpression(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	eval, err := NewEvaluator(logger, Config{})
	if err != nil {
		t.Fatalf("NewEvaluator error: %v", err)
	}
	t.Cleanup(eval.Close)

	_, err = eval.EvaluateExpression(context.Background(), "add 1 2", nil)
	if err != nil {
		t.Fatalf("EvaluateExpression error: %v", err)
	}

	logs := buf.String()
	if !strings.Contains(logs, "Template evaluation started") {
		t.Fatalf("expected start log, got %q", logs)
	}
	if !strings.Contains(logs, "Template evaluation succeeded") {
		t.Fatalf("expected success log, got %q", logs)
	}
}

func TestNewEvaluatorUsesLoggerForConditionErrors(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	eval, err := NewEvaluator(logger, Config{})
	if err != nil {
		t.Fatalf("NewEvaluator error: %v", err)
	}
	t.Cleanup(eval.Close)

	_, err = eval.EvaluateCondition(context.Background(), "inputs.value", map[string]any{
		"inputs": map[string]any{"value": "not-a-bool"},
	})
	if err == nil {
		t.Fatal("expected condition evaluation error")
	}

	logs := buf.String()
	if !strings.Contains(logs, "Template evaluation failed") {
		t.Fatalf("expected failure log, got %q", logs)
	}
	if !strings.Contains(logs, "condition") {
		t.Fatalf("expected condition log context, got %q", logs)
	}
}
