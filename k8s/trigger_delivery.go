package k8s

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	sdkerrors "github.com/bubustack/bubu-sdk-go/pkg/errors"
	"github.com/bubustack/core/contracts"
	"github.com/bubustack/core/templating"
)

const (
	triggerDedupeModeEnv        = "BUBU_TRIGGER_DEDUPE_MODE"
	triggerDedupeKeyTemplateEnv = "BUBU_TRIGGER_DEDUPE_KEY_TEMPLATE"
	triggerRetryMaxAttemptsEnv  = "BUBU_TRIGGER_RETRY_MAX_ATTEMPTS"
	triggerRetryBaseDelayEnv    = "BUBU_TRIGGER_RETRY_BASE_DELAY"
	triggerRetryMaxDelayEnv     = "BUBU_TRIGGER_RETRY_MAX_DELAY"
	triggerRetryBackoffEnv      = "BUBU_TRIGGER_RETRY_BACKOFF"
	triggerThrottleRateEnv      = "BUBU_TRIGGER_THROTTLE_RATE_PER_SECOND"
	triggerThrottleBurstEnv     = "BUBU_TRIGGER_THROTTLE_BURST"
	triggerThrottleMaxInFlight  = "BUBU_TRIGGER_THROTTLE_MAX_IN_FLIGHT"
)

type triggerDeliveryPolicy struct {
	dedupe *triggerDedupePolicy
	retry  *triggerRetryPolicy
}

type triggerDedupePolicy struct {
	mode        string
	keyTemplate string
}

type triggerRetryPolicy struct {
	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
	backoff     string
}

func loadTriggerDeliveryPolicyFromEnv() (*triggerDeliveryPolicy, error) { //nolint:gocyclo
	mode := strings.ToLower(strings.TrimSpace(os.Getenv(triggerDedupeModeEnv)))
	keyTemplate := strings.TrimSpace(os.Getenv(triggerDedupeKeyTemplateEnv))
	maxAttemptsRaw := strings.TrimSpace(os.Getenv(triggerRetryMaxAttemptsEnv))
	baseDelayRaw := strings.TrimSpace(os.Getenv(triggerRetryBaseDelayEnv))
	maxDelayRaw := strings.TrimSpace(os.Getenv(triggerRetryMaxDelayEnv))
	backoff := strings.ToLower(strings.TrimSpace(os.Getenv(triggerRetryBackoffEnv)))

	if mode == "" && keyTemplate == "" && maxAttemptsRaw == "" && baseDelayRaw == "" && maxDelayRaw == "" && backoff == "" { //nolint:lll
		return nil, nil
	}

	policy := &triggerDeliveryPolicy{}
	if mode != "" || keyTemplate != "" {
		if mode == "" && keyTemplate != "" {
			return nil, fmt.Errorf("%s requires %s=key", triggerDedupeKeyTemplateEnv, triggerDedupeModeEnv)
		}
		switch mode {
		case "none", "token", "key": //nolint:goconst
		default:
			return nil, fmt.Errorf("%s must be one of none, token, key", triggerDedupeModeEnv)
		}
		policy.dedupe = &triggerDedupePolicy{
			mode:        mode,
			keyTemplate: keyTemplate,
		}
	}

	if maxAttemptsRaw != "" || baseDelayRaw != "" || maxDelayRaw != "" || backoff != "" {
		retry := &triggerRetryPolicy{backoff: backoff}
		if maxAttemptsRaw != "" {
			val, err := strconv.Atoi(maxAttemptsRaw)
			if err != nil || val < 0 {
				return nil, fmt.Errorf("%s must be a non-negative integer", triggerRetryMaxAttemptsEnv)
			}
			retry.maxAttempts = val
		}
		if baseDelayRaw != "" {
			parsed, err := parsePositiveDuration(baseDelayRaw)
			if err != nil {
				return nil, fmt.Errorf("%s invalid: %w", triggerRetryBaseDelayEnv, err)
			}
			retry.baseDelay = parsed
		}
		if maxDelayRaw != "" {
			parsed, err := parsePositiveDuration(maxDelayRaw)
			if err != nil {
				return nil, fmt.Errorf("%s invalid: %w", triggerRetryMaxDelayEnv, err)
			}
			retry.maxDelay = parsed
		}
		if backoff != "" {
			switch backoff {
			case "exponential", "linear", "constant":
			default:
				return nil, fmt.Errorf("%s must be one of exponential, linear, constant", triggerRetryBackoffEnv)
			}
		}
		policy.retry = retry
	}
	return policy, nil
}

func resolveTriggerTokenForPolicy(
	ctx context.Context,
	clientNamespace string,
	storyName string,
	storyNamespace string,
	inputs map[string]any,
	policy *triggerDeliveryPolicy,
	existingToken string,
) (string, error) {
	if policy == nil || policy.dedupe == nil {
		return existingToken, nil
	}

	switch policy.dedupe.mode {
	case "none", "":
		return existingToken, nil
	case "token":
		if strings.TrimSpace(existingToken) == "" {
			return "", fmt.Errorf("trigger token required by %s=token", triggerDedupeModeEnv)
		}
		return existingToken, nil
	case "key":
		if strings.TrimSpace(policy.dedupe.keyTemplate) == "" {
			return "", fmt.Errorf("trigger key template required by %s=key", triggerDedupeModeEnv)
		}
		computed, err := computeTriggerKey(ctx, clientNamespace, storyName, storyNamespace, inputs, policy.dedupe.keyTemplate)
		if err != nil {
			return "", err
		}
		if existingToken != "" && existingToken != computed {
			return "", fmt.Errorf("trigger token mismatch: provided token does not match %s output", triggerDedupeKeyTemplateEnv)
		}
		return computed, nil
	default:
		return "", fmt.Errorf("unsupported dedupe mode %q", policy.dedupe.mode)
	}
}

func computeTriggerKey(
	ctx context.Context,
	clientNamespace string,
	storyName string,
	storyNamespace string,
	inputs map[string]any,
	templateText string,
) (string, error) {
	eval, err := templating.New(templating.Config{Deterministic: true, EvaluationTimeout: 250 * time.Millisecond})
	if err != nil {
		return "", fmt.Errorf("init trigger template evaluator: %w", err)
	}
	defer eval.Close()

	impulseName := strings.TrimSpace(os.Getenv(contracts.ImpulseNameEnv))
	impulseNamespace := strings.TrimSpace(os.Getenv(contracts.ImpulseNamespaceEnv))
	if impulseNamespace == "" {
		impulseNamespace = clientNamespace
	}
	targetNamespace := strings.TrimSpace(storyNamespace)
	if targetNamespace == "" {
		targetNamespace = clientNamespace
	}

	vars := map[string]any{
		"inputs": inputs,
		"story": map[string]any{
			"name":      storyName,
			"namespace": targetNamespace,
		},
		"impulse": map[string]any{
			"name":      impulseName,
			"namespace": impulseNamespace,
		},
	}

	resolved, err := eval.ResolveTemplateString(ctx, templateText, vars)
	if err != nil {
		return "", fmt.Errorf("resolve trigger key template: %w", err)
	}
	key := ""
	switch v := resolved.(type) {
	case string:
		key = v
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("marshal trigger key: %w", err)
		}
		key = string(encoded)
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", fmt.Errorf("trigger key resolved to empty value")
	}
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:]), nil
}

func retryTriggerStory(
	ctx context.Context,
	policy *triggerRetryPolicy,
	run func(context.Context) (*runsv1alpha1.StoryRun, error),
) (*runsv1alpha1.StoryRun, error) {
	attempts := 1
	if policy != nil && policy.maxAttempts > 0 {
		attempts = policy.maxAttempts
	}
	if attempts <= 1 {
		return run(ctx)
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			delay := computeRetryDelay(policy, attempt-1)
			if delay > 0 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(delay):
				}
			}
		}
		result, err := run(ctx)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if !isRetryableTriggerError(err) || attempt == attempts {
			break
		}
	}
	return nil, lastErr
}

func isRetryableTriggerError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sdkerrors.ErrRetryable) {
		return true
	}
	return errors.Is(classifyK8sError(err), sdkerrors.ErrRetryable)
}

func computeRetryDelay(policy *triggerRetryPolicy, attempt int) time.Duration {
	if policy == nil {
		return 0
	}
	base := policy.baseDelay
	if base <= 0 {
		base = time.Second
	}
	maxDelay := policy.maxDelay
	if maxDelay <= 0 {
		maxDelay = 5 * time.Minute // sensible cap to prevent overflow
	}
	delay := base //nolint:ineffassign
	switch policy.backoff {
	case "linear":
		delay = base * time.Duration(attempt)
	case "constant":
		delay = base
	default:
		delay = base
		if attempt > 1 {
			// Cap the shift to prevent integer overflow (shift >= 63 wraps to negative).
			shift := min(attempt-1, 62)
			delay = base * time.Duration(1<<shift)
		}
	}
	if delay <= 0 || delay > maxDelay {
		delay = maxDelay
	}
	return jitterDuration(delay)
}

func parsePositiveDuration(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("duration must be set")
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse duration: %w", err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("duration must be positive")
	}
	return parsed, nil
}
