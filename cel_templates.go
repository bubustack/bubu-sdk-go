package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"time"

	sdkenv "github.com/bubustack/bubu-sdk-go/pkg/env"
	"github.com/bubustack/core/templating"
)

const (
	defaultTemplateMaxOutputBytes = 1 * 1024 * 1024
	defaultTemplateMaxInputBytes  = 8 * 1024 * 1024
	defaultTemplateEvalTimeout    = 30 * time.Second
	templateMaxOutputBytesEnv     = "BUBU_TEMPLATE_MAX_OUTPUT_BYTES"
	templateMaxInputBytesEnv      = "BUBU_TEMPLATE_MAX_INPUT_BYTES"
	templateEvalTimeoutEnv        = "BUBU_TEMPLATE_EVALUATION_TIMEOUT"
)

func resolveCELTemplates(
	ctx context.Context,
	logger *slog.Logger,
	sm StorageManager,
	vars map[string]any,
	payload any) (any,
	error,
) {
	return resolveTemplates(ctx, logger, sm, vars, payload)
}

func resolveTemplates(ctx context.Context, logger *slog.Logger, sm StorageManager, vars map[string]any, payload any) (any, error) { //nolint:lll
	if payload == nil {
		return nil, nil
	}
	if logger == nil {
		logger = slog.Default()
	}

	start := time.Now()
	logger.Debug("Resolving templates",
		slog.Int("payloadFields", countTopLevelFields(payload)),
	)
	maxInputBytes := resolveTemplateMaxInputBytes()
	if err := validateTemplateInputSize("payload", payload, maxInputBytes); err != nil {
		logger.Error("Template payload rejected by input size limit", "error", err)
		return nil, err
	}

	normalizedVars, err := hydrateCELContext(ctx, sm, vars)
	if err != nil {
		logger.Error("Failed to hydrate template context", "error", err)
		return nil, err
	}
	if err := validateTemplateInputSize("context", normalizedVars, maxInputBytes); err != nil {
		logger.Error("Template context rejected by input size limit", "error", err)
		return nil, err
	}
	logger.Debug("Template context hydrated",
		slog.Duration("hydrationDuration", time.Since(start)),
	)

	eval, err := templating.New(templating.Config{
		EvaluationTimeout: resolveTemplateEvaluationTimeout(),
		MaxOutputBytes:    resolveTemplateMaxOutputBytes(),
		Deterministic:     false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize template evaluator: %w", err)
	}
	defer eval.Close()

	result, err := eval.ResolveValue(ctx, payload, normalizedVars)
	if err != nil {
		logger.Error("Template resolution failed",
			"error", err,
			slog.Duration("duration", time.Since(start)),
		)
		return nil, err
	}
	logger.Debug("Template resolution completed",
		slog.Duration("duration", time.Since(start)),
	)
	return result, nil
}

func normalizeCELVars(vars map[string]any) map[string]any {
	normalized := map[string]any{
		"inputs": map[string]any{},
		"steps":  map[string]any{},
	}
	maps.Copy(normalized, vars)
	if normalized["inputs"] == nil {
		normalized["inputs"] = map[string]any{}
	}
	if normalized["steps"] == nil {
		normalized["steps"] = map[string]any{}
	}
	return normalized
}

func hydrateCELContext(ctx context.Context, sm StorageManager, vars map[string]any) (map[string]any, error) {
	if vars == nil {
		return normalizeCELVars(nil), nil
	}
	if sm == nil {
		return normalizeCELVars(vars), nil
	}
	hydrated, err := sm.Hydrate(ctx, vars)
	if err != nil {
		return nil, fmt.Errorf("failed to hydrate CEL context: %w", err)
	}
	if hydrated == nil {
		return normalizeCELVars(nil), nil
	}
	hydratedMap, ok := hydrated.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("CEL context must be an object, got %T", hydrated)
	}
	return normalizeCELVars(hydratedMap), nil
}

func resolveTemplateMaxOutputBytes() int {
	return sdkenv.GetInt(templateMaxOutputBytesEnv, defaultTemplateMaxOutputBytes)
}

func resolveTemplateMaxInputBytes() int {
	return sdkenv.GetInt(templateMaxInputBytesEnv, defaultTemplateMaxInputBytes)
}

func resolveTemplateEvaluationTimeout() time.Duration {
	return sdkenv.GetDuration(templateEvalTimeoutEnv, defaultTemplateEvalTimeout)
}

func validateTemplateInputSize(scope string, value any, maxBytes int) error {
	if maxBytes <= 0 {
		return nil
	}
	sizeBytes, err := estimateJSONSizeStrict(value)
	if err != nil {
		return fmt.Errorf("template %s size check failed: %w", scope, err)
	}
	if sizeBytes <= maxBytes {
		return nil
	}
	return fmt.Errorf("template %s exceeds max input bytes (%d > %d)", scope, sizeBytes, maxBytes)
}

func estimateJSONSizeStrict(v any) (int, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	return len(raw), nil
}

// countTopLevelFields returns the number of top-level keys for maps,
// the length for slices, or 1 for scalar values.
func countTopLevelFields(v any) int {
	switch typed := v.(type) {
	case map[string]any:
		return len(typed)
	case []any:
		return len(typed)
	default:
		return 1
	}
}

// estimateJSONSize returns the approximate JSON-encoded size of a value.
// Returns 0 if marshaling fails.
func estimateJSONSize(v any) int {
	raw, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	return len(raw)
}
