package cel

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/bubustack/core/templating"
)

// Expression holds a CEL expression and its variable bindings.
type Expression struct {
	// Expr is the CEL source expression to evaluate.
	Expr string
	// Vars contains the variable bindings available to Expr at evaluation time.
	Vars map[string]any
}

// ExtractExpression extracts a template expression wrapper from a map.
func ExtractExpression(value any) (Expression, bool) {
	m, ok := value.(map[string]any)
	if !ok {
		return Expression{}, false
	}
	exprRaw, ok := m[templating.TemplateExprKey]
	if !ok {
		return Expression{}, false
	}
	expr, ok := exprRaw.(string)
	if !ok || strings.TrimSpace(expr) == "" {
		return Expression{}, false
	}
	vars, _ := m[templating.TemplateVarsKey].(map[string]any)
	return Expression{Expr: expr, Vars: vars}, true
}

// Config aliases the core templating config for SDK evaluators.
type Config = templating.Config

// Evaluator wraps core templating to evaluate expressions inside engrams.
type Evaluator struct {
	inner  *templating.Evaluator
	logger *sdkCELLogger
}

// NewEvaluator creates a template evaluator using core templating.
func NewEvaluator(logger *slog.Logger, cfg Config) (*Evaluator, error) {
	inner, err := templating.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Evaluator{inner: inner, logger: newSDKCELLogger(logger)}, nil
}

// Close releases evaluator resources.
func (e *Evaluator) Close() {
	if e != nil && e.inner != nil {
		e.inner.Close()
	}
}

// EvaluateExpression evaluates a raw template expression with the provided variables.
func (e *Evaluator) EvaluateExpression(ctx context.Context, expr string, vars map[string]any) (any, error) {
	if e == nil || e.inner == nil {
		return nil, fmt.Errorf("template evaluator is nil")
	}
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return "", nil
	}
	if !strings.Contains(trimmed, "{{") {
		trimmed = "{{ " + trimmed + " }}"
	}
	start := time.Now()
	e.logStart(trimmed, "expression")
	resolved, err := e.inner.ResolveWithInputs(ctx, map[string]any{"value": trimmed}, vars)
	if err != nil {
		e.logError(err, trimmed, "expression", start)
		return nil, err
	}
	result := coerceTemplateScalar(resolved["value"])
	e.logSuccess(trimmed, "expression", start, result)
	return result, nil
}

// EvaluateCondition evaluates a boolean template expression.
// Returns true if the expression resolves to a truthy value.
// An empty expression returns true (no filter = pass all).
func (e *Evaluator) EvaluateCondition(ctx context.Context, expr string, vars map[string]any) (bool, error) {
	if e == nil || e.inner == nil {
		return false, fmt.Errorf("template evaluator is nil")
	}
	start := time.Now()
	e.logStart(expr, "condition")
	result, err := e.inner.EvaluateCondition(ctx, expr, vars)
	if err != nil {
		e.logError(err, expr, "condition", start)
		return false, err
	}
	e.logSuccess(expr, "condition", start, result)
	return result, nil
}

// ResolveTemplate resolves a string template or a map containing templates.
func (e *Evaluator) ResolveTemplate(ctx context.Context, template any, vars map[string]any) (any, error) {
	if e == nil || e.inner == nil {
		return nil, fmt.Errorf("template evaluator is nil")
	}
	expr := describeTemplateInput(template)
	start := time.Now()
	e.logStart(expr, "template")
	switch typed := template.(type) {
	case map[string]any:
		resolved, err := e.inner.ResolveWithInputs(ctx, typed, vars)
		if err != nil {
			e.logError(err, expr, "template", start)
			return nil, err
		}
		e.logSuccess(expr, "template", start, resolved)
		return resolved, nil
	case string:
		resolved, err := e.inner.ResolveWithInputs(ctx, map[string]any{"value": typed}, vars)
		if err != nil {
			e.logError(err, expr, "template", start)
			return nil, err
		}
		result := resolved["value"]
		e.logSuccess(expr, "template", start, result)
		return result, nil
	default:
		e.logSuccess(expr, "template", start, template)
		return template, nil
	}
}

func (e *Evaluator) logStart(expression, expressionType string) {
	if e == nil || e.logger == nil {
		return
	}
	e.logger.EvaluationStart(expression, expressionType)
}

func (e *Evaluator) logSuccess(expression, expressionType string, started time.Time, result any) {
	if e == nil || e.logger == nil {
		return
	}
	e.logger.EvaluationSuccess(expression, expressionType, time.Since(started), result)
}

func (e *Evaluator) logError(err error, expression, expressionType string, started time.Time) {
	if e == nil || e.logger == nil || err == nil {
		return
	}
	e.logger.EvaluationError(err, expression, expressionType, time.Since(started))
}

func describeTemplateInput(template any) string {
	switch typed := template.(type) {
	case string:
		return typed
	case map[string]any:
		return "<object>"
	default:
		return fmt.Sprintf("<%T>", template)
	}
}

func coerceTemplateScalar(value any) any {
	raw, ok := value.(string)
	if !ok {
		return value
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return raw
	}
	var decoded any
	if err := json.Unmarshal([]byte(trimmed), &decoded); err == nil {
		return decoded
	}
	return value
}
