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

package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/bubustack/bubu-sdk-go/pkg/env"
	"github.com/bubustack/bubu-sdk-go/pkg/observability"
	"github.com/bubustack/bubu-sdk-go/runtime"
	"github.com/bubustack/core/contracts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
)

const (
	// ReasonTimeout indicates the step failed because it exceeded its deadline.
	ReasonTimeout = "Timeout"
)

// exitProcess allows tests to override how the SDK terminates on fatal errors.
var exitProcess = os.Exit

// getStepTimeout returns the timeout for batch step execution from env or default
func getStepTimeout() time.Duration {
	return env.GetDuration(contracts.StepTimeoutEnv, 30*time.Minute)
}

// RunBatch is the primary entry point for a BatchEngram. It provides a fully
// type-safe execution environment, handling all the boilerplate of context loading,
// data hydration, and status patching.
func RunBatch[C any, I any](ctx context.Context, e engram.BatchEngram[C, I]) error {
	ctx, _ = withDefaultLogger(ctx)
	defer publishCapturedLogs(ctx)
	logger := LoggerFromContext(ctx)

	execCtxData, err := runtime.LoadExecutionContextData()
	if err != nil {
		logger.Error("Failed to load execution context", "error", err)
		return fmt.Errorf("failed to load execution context: %w", err)
	}

	k8sClient, err := k8s.NewClient()
	if err != nil {
		logger.Error("Failed to create Kubernetes client", "error", err)
		return patchBootstrapFailure(ctx, execCtxData, nil, fmt.Errorf("failed to create k8s client: %w", err))
	}

	sm, err := storage.SharedManager(ctx)
	if err != nil {
		logger.Error("Failed to create storage manager", "error", err)
		return patchBootstrapFailure(ctx, execCtxData, k8sClient, fmt.Errorf("failed to create storage manager: %w", err))
	}

	return runWithClientsWithContext[C, I](ctx, e, k8sClient, sm, execCtxData)
}

func patchBootstrapFailure(
	ctx context.Context,
	execCtxData *runtime.ExecutionContextData,
	k8sClient K8sClient,
	cause error,
) error {
	client := k8sClient
	if client == nil {
		var err error
		client, err = k8s.SharedClient()
		if err != nil {
			return fmt.Errorf("%w (status patch failed: %v)", cause, err)
		}
	}
	exitCode := BatchExitCode(cause)
	if exitCode == 0 {
		exitCode = 1
	}
	if patchErr := patchFailureStatus(ctx, client, execCtxData, cause, exitCode); patchErr != nil {
		return fmt.Errorf("%w (status patch also failed: %v)", cause, patchErr)
	}
	return cause
}

// runWithClients contains the core logic for a batch execution, using injected clients.
func runWithClients[C any, I any](
	ctx context.Context,
	e engram.BatchEngram[C, I],
	k8sClient K8sClient,
	sm StorageManager,
) error {
	execCtxData, err := runtime.LoadExecutionContextData()
	if err != nil {
		return fmt.Errorf("failed to load execution context: %w", err)
	}
	return runWithClientsWithContext(ctx, e, k8sClient, sm, execCtxData)
}

func runWithClientsWithContext[C any, I any](
	ctx context.Context,
	e engram.BatchEngram[C, I],
	k8sClient K8sClient,
	sm StorageManager,
	execCtxData *runtime.ExecutionContextData,
) error {
	if execCtxData == nil {
		return fmt.Errorf("execution context data cannot be nil")
	}

	logger := LoggerFromContext(ctx)
	logExecutionContextDebug(logger, execCtxData)
	tracer := observability.Tracer("bubu-sdk")
	execCtx := engram.NewExecutionContextWithCELContext(logger, tracer, execCtxData.StoryInfo, execCtxData.CELContext)

	// Enforce timeout on batch execution to prevent runaway engrams
	// This ensures engrams receive context cancellation before Job-level SIGKILL
	stepTimeout := getStepTimeout()
	ctxWithTimeout, cancelTimeout := context.WithTimeout(ctx, stepTimeout)
	defer cancelTimeout()

	logger.Info("Starting batch execution with timeout",
		"timeout", stepTimeout,
		"stepRunID", execCtxData.StoryInfo.StepRunID)

	// Initialize the engram.
	if err := initializeEngram[C, I](ctxWithTimeout, e, sm, execCtxData); err != nil {
		logger.Error("Engram initialization failed", "error", err)
		if patchErr := patchFailureStatus(ctx, k8sClient, execCtxData, err, 1); patchErr != nil {
			return fmt.Errorf("%w (status patch also failed: %v)", err, patchErr)
		}
		return err
	}

	// Prepare typed inputs (hydrate + unmarshal with failure patching)
	inputs, err := hydrateAndUnmarshalInputs[C, I](ctxWithTimeout, sm, k8sClient, execCtxData)

	var (
		result            *engram.Result
		processErr        error
		terminateOverride *statusOverride
	)

	switch {
	case err != nil && ctxWithTimeout.Err() == context.DeadlineExceeded:
		// Timeout during input preparation — fall through to shared timeout handling.
		processErr = err
	case err != nil:
		// Non-timeout failure — already patched inside hydrateAndUnmarshalInputs.
		return err
	default:
		logTypedInputs(logger, inputs)

		// Process the inputs.
		result, processErr = callWithPanicRecovery[*engram.Result]("engram Process", func() (*engram.Result, error) {
			return e.Process(ctxWithTimeout, execCtx, inputs)
		})
		logProcessResult(logger, result, processErr)
	}

	// Check if timeout was hit (during input preparation or processing)
	timedOut := ctxWithTimeout.Err() == context.DeadlineExceeded
	var timeoutErr *BatchTimeoutError
	if timedOut {
		originalErr := processErr
		logger.Error("Batch execution timed out",
			"timeout", stepTimeout,
			"stepRunID", execCtxData.StoryInfo.StepRunID,
			"error", originalErr,
		)
		timeoutErr = &BatchTimeoutError{
			Timeout: stepTimeout,
			Cause:   originalErr,
		}
		processErr = timeoutErr
	}

	// Handle the result and patch the final status.
	// Use original context (not timed-out) for status patch to ensure it completes
	// even if engram execution exceeded timeout.
	patchStart := time.Now()
	succeeded, outputBytes, finalResultErr, patchErr := handleResultAndPatchStatus(
		ctx, sm, k8sClient, execCtxData,
		result, processErr, timedOut, terminateOverride,
	)
	processErr = finalResultErr
	patchDuration := time.Since(patchStart)

	// Critical: If timeout occurred, enforce process termination after status patch completes
	// to prevent zombie Jobs. This ensures the container exits even if engram code is blocking
	// in a syscall, infinite loop, or hung dependency.
	// Exit code 124 follows GNU timeout convention and allows retry policies to
	// differentiate timeouts from other failure modes.
	if timedOut {
		if timeoutErr == nil {
			timeoutErr = &BatchTimeoutError{Timeout: stepTimeout, Cause: processErr}
		}

		var exitErr error = timeoutErr
		exitCode := BatchExitCode(exitErr)

		if patchErr == nil {
			logger.Info("Timeout exceeded and status patched; exiting",
				"timeout", stepTimeout,
				"stepRunID", execCtxData.StoryInfo.StepRunID,
				"patchDuration", patchDuration,
				"exitCode", exitCode,
			)
		} else {
			exitErr = errors.Join(timeoutErr, patchErr)
			exitCode = BatchExitCode(exitErr)
			logger.Error("Timeout exceeded and status patch failed; exiting",
				"timeout", stepTimeout,
				"stepRunID", execCtxData.StoryInfo.StepRunID,
				"patchErr", patchErr,
				"exitCode", exitCode,
			)
		}

		publishCapturedLogs(ctx)
		exitProcess(exitCode)
		return exitErr
	}

	if patchErr != nil {
		return processErr
	}

	// Hybrid bridge: on successful batch execution, optionally forward output to Hub
	if succeeded && bridgeEnabled() {
		// Best-effort: bounded by bridge timeout; errors do not change step outcome
		bTimeout := getBridgeTimeout()
		bCtx, cancel := context.WithTimeout(ctx, bTimeout)
		defer cancel()
		if err := bridgeToHub(bCtx, outputBytes, execCtxData); err != nil {
			logger.Warn("Hybrid bridge (batch→stream) send failed; continuing without downstream delivery", "error", err)
		} else {
			logger.Info("Hybrid bridge (batch→stream) delivered output to Hub")
		}
	}

	return processErr
}

// initializeEngram unmarshals config and secrets and calls the engram's Init method.
func initializeEngram[
	C any, I any,
](
	ctx context.Context,
	e engram.BatchEngram[C, I],
	sm StorageManager,
	execCtxData *runtime.ExecutionContextData,
) error {
	configMap, err := hydrateConfig(ctx, sm, execCtxData.Config, execCtxData.CELContext)
	if err != nil {
		return fmt.Errorf("failed to hydrate config: %w", err)
	}
	config, err := runtime.UnmarshalFromMap[C](configMap)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}
	secrets, err := engram.NewSecretsWithError(ctx, execCtxData.Secrets)
	if err != nil {
		return fmt.Errorf("failed to expand secrets: %w", err)
	}
	if err := callWithPanicRecoveryNoValue("engram Init", func() error {
		return e.Init(ctx, config, secrets)
	}); err != nil {
		return fmt.Errorf("engram initialization failed: %w", err)
	}
	return nil
}

// dehydrateWithRetry attempts dehydration with retry logic for transient failures.
// If storage backend is unavailable after retries, the step fails - we never fall back to inline.
// This ensures infrastructure failures are visible and don't silently change behavior.
func dehydrateWithRetry(
	ctx context.Context,
	sm StorageManager,
	data any,
	stepRunID string,
	logger *slog.Logger,
) (outputBytes []byte, dehydrationErr error) {
	if data == nil {
		return nil, nil
	}

	// Attempt dehydration with retry for transient failures
	maxRetries := 3
	backoff := 1 * time.Second
	var lastErr error

	for attempt := range maxRetries {
		if attempt > 0 {
			logger.Warn("Retrying dehydration after failure",
				"attempt", attempt,
				"backoff", backoff,
				"error", lastErr,
			)
			// Use context-aware sleep to respect cancellation during backoff
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("dehydration retry aborted due to context cancellation: %w",
					ctx.Err(),
				)
			case <-time.After(backoff):
				// Continue to next attempt
			}
			backoff *= 2
		}

		output, err := sm.Dehydrate(ctx, data, stepRunID)
		if err == nil {
			// Dehydration succeeded
			bytes, mErr := json.Marshal(output)
			if mErr != nil {
				return nil, fmt.Errorf("output marshal error after successful dehydration: %w",
					mErr,
				)
			}
			return bytes, nil
		}
		lastErr = err

		// Don't retry on context cancellation or validation errors
		if ctx.Err() != nil || strings.Contains(err.Error(), "invalid storage path") {
			break
		}
	}

	// All retry attempts failed - fail the step hard
	// Do NOT fall back to inline; infrastructure failures should be visible
	return nil, fmt.Errorf("failed to dehydrate output after %d retries (storage backend unavailable): %w",
		maxRetries, lastErr,
	)
}

type statusOverride struct {
	phase          enums.Phase
	exitCode       int32
	exitClass      enums.ExitClass
	lastFailureMsg string
	failureErr     error
}

// handleResultAndPatchStatus determines the final phase from the Engram's result,
// and patches the StepRun status accordingly. It returns a wrapped error if
// processing or patching fails. Phase determination happens after dehydration
// to ensure storage failures are reflected in both phase and container exit code.
func handleResultAndPatchStatus(
	ctx context.Context,
	sm StorageManager,
	k8sClient K8sClient,
	execCtxData *runtime.ExecutionContextData,
	result *engram.Result,
	processErr error,
	timedOut bool,
	override *statusOverride,
) (bool, []byte, error, error) {
	logger := LoggerFromContext(ctx)

	outputBytes, dehydrationErr := attemptResultDehydration(ctx, sm, execCtxData, result, logger)

	// Apply the nil-output fallback early so schema validation sees the final value.
	if processErr == nil && dehydrationErr == nil && len(outputBytes) == 0 {
		outputBytes = []byte("{}")
	}

	if processErr == nil && dehydrationErr == nil {
		if err := validateBatchOutputs(ctx, k8sClient, execCtxData, outputBytes); err != nil {
			processErr = err
		}
	}

	phase, finalErr := initialPhaseAndError(processErr, override)
	phase, finalErr = applyDehydrationOutcome(phase, finalErr, dehydrationErr)

	if timedOut {
		phase = enums.PhaseTimeout
	}

	status := newStepRunStatus(execCtxData, phase, timedOut, finalErr)
	applyStatusOverride(&status, override, processErr, timedOut, phase, finalErr)
	// Always set Output when phase is Succeeded so downstream steps can reference .steps["x"].output.
	appendOutput(&status, outputBytes)

	outputLen := 0
	if outputBytes != nil {
		outputLen = len(outputBytes)
	}
	logger.Info("Patching StepRun status",
		"phase", phase,
		"stepRunID", execCtxData.StoryInfo.StepRunID,
		"outputBytes", outputLen,
	)
	if isDebugEnabled() {
		logger.Debug("StepRun status patch detail",
			slog.String("phase", string(phase)),
			slog.Bool("timedOut", timedOut),
			debugBytesAttr("outputPreview", outputBytes),
		)
	}

	patchErr := k8sClient.PatchStepRunStatus(ctx, execCtxData.StoryInfo.StepRunID, status)
	if patchErr != nil {
		logger.Error("status patch failed after processing", "patchErr", patchErr)
		return false, nil, combinePatchError(finalErr, patchErr), patchErr
	}

	if isDebugEnabled() {
		logger.Debug("StepRun status patch applied",
			slog.String("stepRun", execCtxData.StoryInfo.StepRunID),
			slog.String("phase", string(phase)),
		)
	}

	return phase == enums.PhaseSucceeded, outputBytes, finalErr, nil
}

func initialPhaseAndError(processErr error, override *statusOverride) (enums.Phase, error) {
	phase := enums.PhaseSucceeded
	finalErr := processErr

	if processErr != nil {
		phase = enums.PhaseFailed
	}

	if override != nil && processErr == nil {
		phase = override.phase
		if override.failureErr != nil {
			finalErr = override.failureErr
		}
	}

	return phase, finalErr
}

func attemptResultDehydration(
	ctx context.Context,
	sm StorageManager,
	execCtxData *runtime.ExecutionContextData,
	result *engram.Result,
	logger *slog.Logger,
) ([]byte, error) {
	stepStorageKey := storage.NamespacedKey(execCtxData.StoryInfo.StepRunNamespace, execCtxData.StoryInfo.StepRunID)
	if schemaID, schemaVersion := outputSchemaMetadata(execCtxData); schemaID != "" || schemaVersion != "" {
		ctx = storage.WithStorageSchema(ctx, schemaID, schemaVersion)
	}

	var data any
	if result != nil {
		data = result.Data
	}
	if data == nil {
		logger.Warn("Engram result or result.Data is nil; StepRun status will have no output",
			"stepRunID", execCtxData.StoryInfo.StepRunID,
			"resultNil", result == nil,
			"dataNil", data == nil,
		)
	}

	return dehydrateWithRetry(ctx, sm, data, stepStorageKey, logger)
}

func outputSchemaMetadata(execCtxData *runtime.ExecutionContextData) (string, string) {
	if execCtxData == nil {
		return "", ""
	}
	namespace := strings.TrimSpace(execCtxData.StoryInfo.StepRunNamespace)
	storyName := strings.TrimSpace(execCtxData.StoryInfo.StoryName)
	stepName := strings.TrimSpace(execCtxData.StoryInfo.StepName)
	engramName := strings.TrimSpace(os.Getenv(contracts.EngramNameEnv))

	var schema string
	switch {
	case namespace != "" && engramName != "":
		schema = fmt.Sprintf("bubu://engram/%s/%s/output", namespace, engramName)
	case namespace != "" && storyName != "" && stepName != "":
		schema = fmt.Sprintf("bubu://story/%s/%s/steps/%s/output", namespace, storyName, stepName)
	case storyName != "" && stepName != "":
		schema = fmt.Sprintf("bubu://story/%s/steps/%s/output", storyName, stepName)
	}

	schemaVersion := strings.TrimSpace(os.Getenv(contracts.EngramVersionEnv))
	if schemaVersion == "" {
		schemaVersion = strings.TrimSpace(os.Getenv(contracts.StoryVersionEnv))
	}

	return schema, schemaVersion
}

func applyDehydrationOutcome(phase enums.Phase, finalErr error, dehydrationErr error) (enums.Phase, error) {
	if dehydrationErr == nil {
		return phase, finalErr
	}
	return enums.PhaseFailed, combineErrors(finalErr, dehydrationErr)
}

func newStepRunStatus(
	execCtxData *runtime.ExecutionContextData,
	phase enums.Phase,
	timedOut bool,
	finalErr error,
) runsv1alpha1.StepRunStatus {
	finishedAt := metav1.Now()
	status := runsv1alpha1.StepRunStatus{
		Phase:      phase,
		FinishedAt: &finishedAt,
		Duration:   finishedAt.Sub(execCtxData.StartedAt.Time).String(),
	}

	applyReadyCondition(&status, phase, timedOut, finalErr)
	applyExitMetadata(&status, phase, timedOut, finalErr)
	appendStructuredError(&status, phase, timedOut, finalErr)

	return status
}

// appendStructuredError populates status.error with machine-readable error details
// when the step reaches a failure phase. The structured format uses a stable schema
// so downstream consumers (dashboards, alerting, CLI) can parse errors programmatically.
func appendStructuredError(status *runsv1alpha1.StepRunStatus, phase enums.Phase, timedOut bool, finalErr error) {
	if phase == enums.PhaseSucceeded || finalErr == nil {
		return
	}

	errType := classifyError(finalErr)

	errObj := runsv1alpha1.StructuredError{
		Version: runsv1alpha1.StructuredErrorVersionV1,
		Type:    errType,
		Message: sanitizePersistedErrorMessage(finalErr.Error(), maxErrorMessageBytes),
	}

	if provided, ok := structuredErrorFrom(finalErr); ok {
		errObj = mergeStructuredError(errObj, provided)
	}
	if timedOut {
		errObj.Type = runsv1alpha1.StructuredErrorTypeTimeout
	}
	errObj.Message = sanitizePersistedErrorMessage(errObj.Message, maxErrorMessageBytes)
	if status.ExitCode != 0 {
		exitCode := status.ExitCode
		errObj.ExitCode = &exitCode
	}
	if status.ExitClass != "" {
		errObj.ExitClass = runsv1alpha1.StructuredErrorExitClass(status.ExitClass)
		if errObj.Retryable == nil {
			retryable := isRetryableExitClass(status.ExitClass)
			errObj.Retryable = &retryable
		}
	}
	status.Error = &errObj
}

// maxErrorMessageBytes is the maximum byte length of error messages stored in
// StepRun status fields (LastFailureMsg, StructuredError.Message, condition
// Message). K8s etcd rejects objects larger than 1.5MB; capping messages at
// 8 KiB prevents bloated status objects and avoids etcd write failures when
// an error message contains a very large payload (e.g. deserialization errors
// that echo back the full input).
const maxErrorMessageBytes = 8192

var persistedErrorRedactionPatterns = []struct {
	re          *regexp.Regexp
	replacement string
}{
	{
		re:          regexp.MustCompile(`(?i)(authorization\s*:\s*(?:basic|bearer)\s+)([^\s,;]+)`),
		replacement: `${1}[REDACTED]`,
	},
	{
		re: regexp.MustCompile(
			`(?i)(["']?(?:api[_-]?key|x-api-key|access[_-]?token|refresh[_-]?token|token|password|secret|client[_-]?secret|authorization)["']?\s*[:=]\s*["']?)([^"'\\\s,;}{\]&]+)(["']?)`, //nolint:lll
		),
		replacement: `${1}[REDACTED]${3}`,
	},
}

// truncateErrorMessage caps msg at limit bytes while preserving valid UTF-8.
// The limit must be > 0; if it is not positive, msg is returned unchanged.
func truncateErrorMessage(msg string, limit int) string {
	if limit <= 0 || len(msg) <= limit {
		return msg
	}
	// Walk backwards from the limit to find a valid UTF-8 boundary.
	truncated := msg[:limit]
	for len(truncated) > 0 && !utf8.ValidString(truncated) {
		truncated = truncated[:len(truncated)-1]
	}
	return truncated
}

func sanitizePersistedErrorMessage(msg string, limit int) string { //nolint:unparam
	sanitized := msg
	for _, pattern := range persistedErrorRedactionPatterns {
		sanitized = pattern.re.ReplaceAllString(sanitized, pattern.replacement)
	}
	return truncateErrorMessage(sanitized, limit)
}

// classifyError determines a machine-readable error type from the error chain.
// IMPORTANT: The cases are order-dependent — more specific patterns must appear
// before broader ones (e.g. "storage" before the default "execution" fallback).
// The StructuredErrorProvider interface allows engrams to bypass string matching
// entirely; prefer implementing that interface over relying on this function.
func classifyError(err error) runsv1alpha1.StructuredErrorType {
	if err == nil {
		return runsv1alpha1.StructuredErrorTypeUnknown
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "dehydrat") || strings.Contains(msg, "storage"):
		return runsv1alpha1.StructuredErrorTypeStorage
	case strings.Contains(msg, "unmarshal") || strings.Contains(msg, "marshal"):
		return runsv1alpha1.StructuredErrorTypeSerialization
	case strings.Contains(msg, "schema") || strings.Contains(msg, "validation"):
		return runsv1alpha1.StructuredErrorTypeValidation
	case strings.Contains(msg, "initialization") || strings.Contains(msg, "Init"):
		return runsv1alpha1.StructuredErrorTypeInitialization
	default:
		return runsv1alpha1.StructuredErrorTypeExecution
	}
}

func isRetryableExitClass(exitClass enums.ExitClass) bool {
	switch exitClass {
	case enums.ExitClassRetry, enums.ExitClassRateLimited:
		return true
	default:
		return false
	}
}

func structuredErrorFrom(err error) (runsv1alpha1.StructuredError, bool) {
	if err == nil {
		return runsv1alpha1.StructuredError{}, false
	}
	var provider StructuredErrorProvider
	if errors.As(err, &provider) {
		return provider.StructuredError(), true
	}
	return runsv1alpha1.StructuredError{}, false
}

func mergeStructuredError(base, override runsv1alpha1.StructuredError) runsv1alpha1.StructuredError {
	if override.Version != "" {
		base.Version = override.Version
	}
	if override.Type != "" {
		base.Type = override.Type
	}
	if override.Message != "" {
		base.Message = override.Message
	}
	if override.Retryable != nil {
		base.Retryable = override.Retryable
	}
	if override.ExitClass != "" {
		base.ExitClass = override.ExitClass
	}
	if override.Code != "" {
		base.Code = override.Code
	}
	if override.Details != nil {
		base.Details = cloneRawExtension(override.Details)
	}
	return base
}

func cloneRawExtension(src *k8sruntime.RawExtension) *k8sruntime.RawExtension {
	if src == nil || len(src.Raw) == 0 {
		return nil
	}
	return &k8sruntime.RawExtension{Raw: append([]byte(nil), src.Raw...)}
}

func overrideExitClassFromError(err error) (enums.ExitClass, bool) {
	if err == nil {
		return "", false
	}
	serr, ok := structuredErrorFrom(err)
	if !ok {
		return "", false
	}
	if serr.ExitClass != "" {
		if parsed, ok := parseExitClass(string(serr.ExitClass)); ok {
			return parsed, true
		}
	}
	if serr.Retryable != nil {
		if *serr.Retryable {
			return enums.ExitClassRetry, true
		}
		return enums.ExitClassTerminal, true
	}
	return "", false
}

func parseExitClass(value string) (enums.ExitClass, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case string(enums.ExitClassSuccess):
		return enums.ExitClassSuccess, true
	case string(enums.ExitClassRetry):
		return enums.ExitClassRetry, true
	case string(enums.ExitClassTerminal):
		return enums.ExitClassTerminal, true
	case string(enums.ExitClassRateLimited):
		return enums.ExitClassRateLimited, true
	default:
		return "", false
	}
}

func applyReadyCondition(status *runsv1alpha1.StepRunStatus, phase enums.Phase, timedOut bool, finalErr error) {
	if phase == enums.PhaseSucceeded {
		setCondition(
			status,
			conditions.ConditionReady,
			metav1.ConditionTrue,
			conditions.ReasonCompleted,
			"Step completed successfully",
		)
		return
	}

	reason := conditions.ReasonExecutionFailed
	if timedOut {
		reason = ReasonTimeout
	}
	errMsg := "Step failed"
	if finalErr != nil {
		errMsg = sanitizePersistedErrorMessage(finalErr.Error(), maxErrorMessageBytes)
	}
	setCondition(status, conditions.ConditionReady, metav1.ConditionFalse, reason, errMsg)
}

func applyExitMetadata(status *runsv1alpha1.StepRunStatus, phase enums.Phase, timedOut bool, finalErr error) {
	if phase == enums.PhaseSucceeded {
		status.ExitCode = 0
		status.ExitClass = enums.ExitClassSuccess
		return
	}

	if timedOut {
		status.ExitCode = 124
		status.ExitClass = enums.ExitClassRetry
		return
	}

	status.ExitCode = 1
	status.ExitClass = enums.ExitClassTerminal
	if override, ok := overrideExitClassFromError(finalErr); ok {
		status.ExitClass = override
	}
}

func applyStatusOverride(
	status *runsv1alpha1.StepRunStatus,
	override *statusOverride,
	processErr error,
	timedOut bool,
	phase enums.Phase,
	finalErr error,
) {
	if override != nil && processErr == nil && !timedOut {
		if override.exitClass != "" {
			status.ExitClass = override.exitClass
		}
		if override.exitCode != 0 || override.phase != enums.PhaseSucceeded {
			status.ExitCode = override.exitCode
		}
		if override.lastFailureMsg != "" {
			status.LastFailureMsg = sanitizePersistedErrorMessage(override.lastFailureMsg, maxErrorMessageBytes)
		} else if override.failureErr == nil && phase == enums.PhaseSucceeded {
			status.LastFailureMsg = ""
		}
		return
	}

	if finalErr != nil {
		status.LastFailureMsg = sanitizePersistedErrorMessage(finalErr.Error(), maxErrorMessageBytes)
	}
}

func appendOutput(status *runsv1alpha1.StepRunStatus, outputBytes []byte) {
	if len(outputBytes) > 0 {
		status.Output = &k8sruntime.RawExtension{Raw: outputBytes}
	}
}

func combinePatchError(finalErr error, patchErr error) error {
	if finalErr != nil {
		return fmt.Errorf("engram completed with error: %w (status patch also failed: %v)", finalErr, patchErr)
	}
	return fmt.Errorf("engram completed successfully but status patch failed: %w", patchErr)
}

// patchFailureStatus builds and sends a failure status patch with consistent fields.
// If the context is already done (e.g. deadline exceeded) the patch is skipped and nil
// is returned so the caller can handle timeout/cancellation semantics instead.
func patchFailureStatus(
	ctx context.Context,
	k8sClient K8sClient,
	execCtxData *runtime.ExecutionContextData,
	err error,
	exitCode int,
) error {
	if ctx.Err() != nil {
		return nil
	}
	finishedAt := metav1.Now()
	exitClass := enums.ExitClassTerminal
	if override, ok := overrideExitClassFromError(err); ok {
		exitClass = override
	}
	errMsg := sanitizePersistedErrorMessage(err.Error(), maxErrorMessageBytes)
	status := runsv1alpha1.StepRunStatus{
		Phase:          enums.PhaseFailed,
		FinishedAt:     &finishedAt,
		Duration:       finishedAt.Sub(execCtxData.StartedAt.Time).String(),
		LastFailureMsg: errMsg,
		ExitCode:       int32(exitCode),
		ExitClass:      exitClass,
	}
	setCondition(&status, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonExecutionFailed, errMsg)
	appendStructuredError(&status, enums.PhaseFailed, false, err)
	return k8sClient.PatchStepRunStatus(ctx, execCtxData.StoryInfo.StepRunID, status)
}

// hydrateAndUnmarshalInputs hydrates raw inputs and unmarshals them into the target type I.
// On failure, it patches StepRun status and returns the error.
func hydrateAndUnmarshalInputs[C any, I any]( //nolint:gocyclo
	ctx context.Context, sm StorageManager, k8sClient K8sClient, execCtxData *runtime.ExecutionContextData,
) (I, error) {
	var zero I
	logger := LoggerFromContext(ctx)
	// Attach StepRunID to context for hydration metrics attribution.
	stepStorageKey := storage.NamespacedKey(execCtxData.StoryInfo.StepRunNamespace, execCtxData.StoryInfo.StepRunID)
	hctx := storage.WithStepRunID(ctx, stepStorageKey)

	// Record which input paths contain storage refs before hydration.
	// After hydration these become raw user data that may contain literal
	// "{{ ... }}" strings — must not be template-evaluated.
	storageRefPaths := findStorageRefPaths(execCtxData.Inputs)

	hydratedInputs, err := sm.Hydrate(hctx, execCtxData.Inputs)
	if err != nil {
		err = fmt.Errorf("failed to hydrate inputs: %w", err)
		if patchErr := patchFailureStatus(ctx, k8sClient, execCtxData, err, 1); patchErr != nil {
			return zero, fmt.Errorf("engram failed during input hydration: %w (status patch also failed: %v)",
				err, patchErr,
			)
		}
		return zero, err
	}

	if hydratedInputs == nil {
		hydratedInputs = map[string]any{}
	}

	// Defensive: ensure map for unmarshal
	rawMap, ok := hydratedInputs.(map[string]any)
	if !ok {
		err = fmt.Errorf("hydrated inputs have unexpected type %T (want map[string]any)", hydratedInputs)
		if patchErr := patchFailureStatus(ctx, k8sClient, execCtxData, err, 1); patchErr != nil {
			return zero, fmt.Errorf("engram failed during input validation: %w (status patch also failed: %v)", err, patchErr)
		}
		return zero, err
	}
	if rawMap == nil {
		rawMap = map[string]any{}
	}

	// Extract storage-ref-hydrated values before template evaluation so
	// raw user data (e.g. RSS bodies with literal {{ }}) is never parsed
	// as Go templates.
	extractedInputs := extractPaths(rawMap, storageRefPaths)
	if len(extractedInputs) > 0 {
		logger.Debug("Extracted storage-ref input values from template evaluation",
			slog.Int("count", len(extractedInputs)),
		)
	}

	resolvedInputs := any(rawMap)
	if !skipInputTemplating() {
		var err error
		resolvedInputs, err = resolveCELTemplates(ctx, logger, sm, execCtxData.CELContext, rawMap)
		if err != nil {
			err = fmt.Errorf("failed to resolve input templates: %w", err)
			if patchErr := patchFailureStatus(ctx, k8sClient, execCtxData, err, 1); patchErr != nil {
				return zero, fmt.Errorf("engram failed during input template resolution: %w (status patch also failed: %v)", err,
					patchErr)
			}
			return zero, err
		}
		if resolvedInputs == nil {
			resolvedInputs = map[string]any{}
		}
	}
	rawMap, ok = resolvedInputs.(map[string]any)
	if !ok {
		err = fmt.Errorf("resolved inputs have unexpected type %T (want map[string]any)", resolvedInputs)
		if patchErr := patchFailureStatus(ctx, k8sClient, execCtxData, err, 1); patchErr != nil {
			return zero, fmt.Errorf("engram failed during input template validation: %w (status patch also failed: %v)", err, patchErr) //nolint:lll
		}
		return zero, err
	}

	// Restore extracted storage-ref values into the resolved inputs.
	restorePaths(rawMap, extractedInputs)

	if err := validateBatchInputs(ctx, k8sClient, execCtxData, rawMap); err != nil {
		if patchErr := patchFailureStatus(ctx, k8sClient, execCtxData, err, 1); patchErr != nil {
			return zero, fmt.Errorf("engram failed during input schema validation: %w (status patch also failed: %v)", err, patchErr) //nolint:lll
		}
		return zero, err
	}

	inputs, err := runtime.UnmarshalFromMap[I](rawMap)
	if err != nil {
		err = fmt.Errorf("failed to unmarshal inputs into the target type: %w", err)
		if patchErr := patchFailureStatus(ctx, k8sClient, execCtxData, err, 1); patchErr != nil {
			return zero, fmt.Errorf("engram failed during input unmarshaling: %w (status patch also failed: %v)", err, patchErr)
		}
		return zero, err
	}
	return inputs, nil
}

func skipInputTemplating() bool {
	value := strings.TrimSpace(os.Getenv(contracts.SkipInputTemplatingEnv))
	if value == "" {
		return false
	}
	switch strings.ToLower(value) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

// combineErrors combines a processing error and a dehydration/marshal error into a single error.
func combineErrors(processErr, storageErr error) error {
	if processErr != nil && storageErr != nil {
		return fmt.Errorf("%v; %w", processErr, storageErr)
	}
	if storageErr != nil {
		return storageErr
	}
	return processErr
}

// bridgeEnabled returns whether the SDK should forward batch outputs to the Hub.
// Defaults to true to enable hybrid by default; set BUBU_HYBRID_BRIDGE=false to disable.
func bridgeEnabled() bool {
	if v := os.Getenv(contracts.HybridBridgeEnv); v != "" {
		// Accept common truthy/falsey values
		switch strings.ToLower(v) {
		case "0", "false", "no", "off":
			return false
		default:
			return true
		}
	}
	return true
}

// getBridgeTimeout returns the max duration to spend on the hub bridge (best-effort).
// Prevents extending Job lifetime indefinitely in case of hub outages.
func getBridgeTimeout() time.Duration {
	return env.GetDuration(contracts.HybridBridgeTimeoutEnv, 15*time.Second)
}

// bridgeToHub forwards hybrid outputs through the transport connector advertised in the binding.
func bridgeToHub(ctx context.Context, payloadJSON []byte, execCtxData *runtime.ExecutionContextData) error {
	// If payload is empty or nil, nothing to forward.
	if len(payloadJSON) == 0 {
		return nil
	}

	ctx, cancel := ensureBridgeTimeout(ctx)
	defer cancel()

	logger := LoggerFromContext(ctx)

	ref, err := bindingReferenceFromEnv()
	if errors.Is(err, errBindingEnvMissing) {
		logger.Debug("No transport binding provided; skipping hybrid bridge delivery")
		return nil
	}
	if err != nil {
		return err
	}

	endpoint := strings.TrimSpace(ref.endpoint())
	if endpoint == "" {
		return fmt.Errorf("transport binding missing endpoint for hybrid delivery")
	}

	env := newEnvResolver(ref.envOverrides()) //nolint:revive
	conn, err := connectorDial(ctx, endpoint, env)
	if err != nil {
		return fmt.Errorf("failed to dial transport connector %s: %w", endpoint, err)
	}
	if isDebugEnabled() {
		logger.Debug("Delivering hybrid payload via connector",
			slog.String("endpoint", endpoint),
			slog.String("driver", normalizedDriver(ref)),
			slog.Int("payloadBytes", len(payloadJSON)),
			debugBytesAttr("payloadPreview", payloadJSON),
		)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			logger.Warn("Failed to close transport connector", "error", closeErr)
		}
	}()

	stream, err := conn.Client().Data(ctx)
	if err != nil {
		return fmt.Errorf("failed to open data stream: %w", err)
	}

	msg, err := buildHybridStreamMessage(payloadJSON, execCtxData)
	if err != nil {
		return err
	}
	req, err := streamMessageToPublishRequest(msg)
	if err != nil {
		return err
	}
	dataReq := publishRequestToDataRequest(req)
	if err := stream.Send(dataReq); err != nil {
		return fmt.Errorf("failed to send hybrid payload: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("failed to close data stream: %w", err)
	}
	if isDebugEnabled() {
		logger.Debug("Hybrid payload delivered", "endpoint", endpoint)
	}
	return nil
}

func logTypedInputs(logger *slog.Logger, inputs any) {
	if !isDebugEnabled() || logger == nil {
		return
	}
	logger.Debug("Typed inputs prepared for engram", debugPreviewAttr("typedInputsPreview", inputs))
}

func logProcessResult(logger *slog.Logger, result *engram.Result, processErr error) {
	if !isDebugEnabled() || logger == nil {
		return
	}
	var data any
	if result != nil {
		data = result.Data
	}
	logger.Debug("Engram Process completed",
		slog.Bool("success", processErr == nil),
		debugPreviewAttr("resultPreview", data),
	)
}

func ensureBridgeTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	timeout := getBridgeTimeout()
	newCtx, cancel := context.WithTimeout(ctx, timeout)
	return newCtx, cancel
}

func buildHybridStreamMessage(
	payloadJSON []byte,
	execCtxData *runtime.ExecutionContextData,
) (engram.StreamMessage, error) {
	options := []StreamMessageOption{
		WithTimestamp(time.Now().UTC()),
	}
	if len(payloadJSON) > 0 {
		options = append(options, WithJSONPayload(payloadJSON))
	}
	if execCtxData != nil {
		options = append(options, WithMetadata(storyMetadata(execCtxData.StoryInfo)))
		inputsPayload, err := inputsJSON(execCtxData.Inputs)
		if err != nil {
			return engram.StreamMessage{}, fmt.Errorf("failed to marshal inputs payload: %w", err)
		}
		options = append(options, WithInputs(inputsPayload))
		options = append(options, WithTransports(cloneTransportDescriptors(execCtxData.Transports)))
	}
	return NewStreamMessage("data", options...), nil
}

// setCondition is a helper to add or update a condition in a StepRunStatus.
func setCondition(
	status *runsv1alpha1.StepRunStatus,
	condType string,
	condStatus metav1.ConditionStatus,
	reason string,
	message string,
) {
	newCond := metav1.Condition{
		Type:               condType,
		Status:             condStatus,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}

	for i, c := range status.Conditions {
		if c.Type == condType {
			// Update existing condition
			if c.Status != newCond.Status || c.Reason != newCond.Reason || c.Message != newCond.Message {
				status.Conditions[i] = newCond
			}
			return
		}
	}
	// Add new condition
	status.Conditions = append(status.Conditions, newCond)
}
