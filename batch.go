package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/bubustack/bubu-sdk-go/runtime"
	"github.com/bubustack/bubu-sdk-go/storage"
	"go.opentelemetry.io/otel"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
)

// getStepTimeout returns the timeout for batch step execution from env or default
func getStepTimeout() time.Duration {
	if v := os.Getenv("BUBU_STEP_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 30 * time.Minute // Default: 30 minutes
}

// RunBatch is the primary entry point for a BatchEngram. It provides a fully
// type-safe execution environment, handling all the boilerplate of context loading,
// data hydration, and status patching.
func RunBatch[C any, I any](ctx context.Context, e engram.BatchEngram[C, I]) error {
	sm, err := storage.NewManager(ctx)
	if err != nil {
		return fmt.Errorf("failed to create storage manager: %w", err)
	}

	k8sClient, err := k8s.NewClient()
	if err != nil {
		return fmt.Errorf("failed to create k8s client: %w", err)
	}

	return runWithClients[C, I](ctx, e, k8sClient, sm)
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

	logger := LoggerFromContext(ctx)
	tracer := otel.Tracer("bubu-sdk")
	execCtx := engram.NewExecutionContext(logger, tracer, execCtxData.StoryInfo)

	// Enforce timeout on batch execution to prevent runaway engrams
	// This ensures engrams receive context cancellation before Job-level SIGKILL
	stepTimeout := getStepTimeout()
	ctxWithTimeout, cancelTimeout := context.WithTimeout(ctx, stepTimeout)
	defer cancelTimeout()

	logger.Info("Starting batch execution with timeout",
		"timeout", stepTimeout,
		"stepRunID", execCtxData.StoryInfo.StepRunID)

	// Initialize the engram.
	if err := initializeEngram[C, I](ctxWithTimeout, e, execCtxData); err != nil {
		// We can't patch status if init fails, so just return the error.
		return err
	}

	// Prepare typed inputs (hydrate + unmarshal with failure patching)
	inputs, err := hydrateAndUnmarshalInputs[C, I](ctxWithTimeout, sm, k8sClient, execCtxData)
	if err != nil {
		return err
	}

	// Process the inputs.
	result, processErr := e.Process(ctxWithTimeout, execCtx, inputs)

	// Check if timeout was hit
	timedOut := (processErr != nil && ctxWithTimeout.Err() == context.DeadlineExceeded)
	if timedOut {
		logger.Error("Batch execution timed out",
			"timeout", stepTimeout,
			"stepRunID", execCtxData.StoryInfo.StepRunID)
		processErr = fmt.Errorf("execution timed out after %v: %w", stepTimeout, processErr)
	}

	// Handle the result and patch the final status.
	// Use original context (not timed-out) for status patch to ensure it completes
	// even if engram execution exceeded timeout.
	patchStart := time.Now()
	succeeded, outputBytes, patchErr := handleResultAndPatchStatus(
		ctx, sm, k8sClient, execCtxData,
		result, processErr, timedOut,
	)
	patchDuration := time.Since(patchStart)

	// Critical: If timeout occurred, enforce process termination after status patch completes
	// to prevent zombie Jobs. This ensures the container exits even if engram code is blocking
	// in a syscall, infinite loop, or hung dependency.
	// Exit code 124 follows GNU timeout convention and allows retry policies to
	// differentiate timeouts from other failure modes.
	if timedOut {
		if patchErr == nil {
			// Patch succeeded; wait for remainder of grace period to allow log flush and propagation
			// Grace period: max(5s, 10% of remaining time after patch)
			minGrace := 5 * time.Second
			remainingGrace := minGrace - patchDuration
			if remainingGrace > 0 {
				logger.Info("Timeout exceeded and status patched; sleeping before exit",
					"timeout", stepTimeout,
					"stepRunID", execCtxData.StoryInfo.StepRunID,
					"patchDuration", patchDuration,
					"remainingGrace", remainingGrace,
					"exitCode", 124)
				time.Sleep(remainingGrace)
			}
		} else {
			// Patch failed; log error but still exit to prevent zombie
			logger.Error("Timeout exceeded and status patch failed; forcing exit",
				"timeout", stepTimeout,
				"stepRunID", execCtxData.StoryInfo.StepRunID,
				"patchErr", patchErr,
				"exitCode", 124)
		}
		os.Exit(124) // Exit code 124: timeout (GNU timeout convention)
	}

	if patchErr != nil {
		return patchErr
	}

	// Hybrid bridge: on successful batch execution, optionally forward output to Hub
	if succeeded && bridgeEnabled() {
		// Best-effort: bounded by bridge timeout; errors do not change step outcome
		bTimeout := getBridgeTimeout()
		bCtx, cancel := context.WithTimeout(ctx, bTimeout)
		defer cancel()
		if err := bridgeToHub(bCtx, outputBytes); err != nil {
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
	execCtxData *runtime.ExecutionContextData,
) error {
	config, err := runtime.UnmarshalFromMap[C](execCtxData.Config)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}
	secrets := engram.NewSecrets(execCtxData.Secrets)
	if err := e.Init(ctx, config, secrets); err != nil {
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

	for attempt := 0; attempt < maxRetries; attempt++ {
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

// handleResultAndPatchStatus determines the final phase from the Engram's result,
// and patches the StepRun status accordingly. It returns a wrapped error if
// processing or patching fails. Phase determination happens after dehydration
// to ensure storage failures are reflected in both phase and container exit code.
func handleResultAndPatchStatus(
	ctx context.Context, sm StorageManager, k8sClient K8sClient, execCtxData *runtime.ExecutionContextData,
	result *engram.Result, processErr error, timedOut bool,
) (bool, []byte, error) {
	logger := LoggerFromContext(ctx)

	// Determine initial phase based on processing result
	phase := enums.PhaseSucceeded
	finalErr := processErr
	if processErr != nil {
		phase = enums.PhaseFailed
	}

	// Attempt dehydration with fallback
	var dataToDehydrate any
	if result != nil {
		dataToDehydrate = result.Data
	}

	outputBytes, dehydrationErr := dehydrateWithRetry(
		ctx, sm, dataToDehydrate, execCtxData.StoryInfo.StepRunID, logger,
	)

	// Handle dehydration failures - always fail the step if storage is unavailable
	if dehydrationErr != nil {
		phase = enums.PhaseFailed
		finalErr = combineErrors(finalErr, dehydrationErr)
	}

	// Build status patch
	finishedAt := metav1.Now()
	newStatus := runsv1alpha1.StepRunStatus{
		Phase:      phase,
		FinishedAt: &finishedAt,
		Duration:   finishedAt.Sub(execCtxData.StartedAt.Time).String(),
	}

	// Set exit code and class for operator retry policy classification
	// This allows intelligent retry decisions (timeout=124 → retry, logic error=1 → terminal)
	if phase == enums.PhaseSucceeded {
		newStatus.ExitCode = 0
		newStatus.ExitClass = enums.ExitClassSuccess
	} else {
		// Failed: distinguish timeout (124, retryable) from logic errors (1, terminal)
		if timedOut {
			newStatus.ExitCode = 124 // GNU timeout convention
			newStatus.ExitClass = enums.ExitClassRetry
		} else {
			newStatus.ExitCode = 1 // General application error
			newStatus.ExitClass = enums.ExitClassTerminal
		}
	}

	if len(outputBytes) > 0 {
		newStatus.Output = &k8sruntime.RawExtension{Raw: outputBytes}
	}
	if finalErr != nil {
		newStatus.LastFailureMsg = finalErr.Error()
	}

	// Patch StepRun status
	patchErr := k8sClient.PatchStepRunStatus(ctx, execCtxData.StoryInfo.StepRunID, newStatus)
	if patchErr != nil {
		logger.Error("status patch failed after successful processing",
			"patchErr", patchErr,
		)
		if finalErr != nil {
			return false, nil, fmt.Errorf("engram completed with error: %w (status patch also failed: %v)",
				finalErr, patchErr,
			)
		}
		return false, nil, fmt.Errorf("engram completed successfully but status patch failed: %w",
			patchErr,
		)
	}

	// Return final error (if any) for container exit code
	return phase == enums.PhaseSucceeded, outputBytes, finalErr
}

// patchFailureStatus builds and sends a failure status patch with consistent fields.
func patchFailureStatus(
	ctx context.Context, k8sClient K8sClient, execCtxData *runtime.ExecutionContextData,
	err error, exitCode int, exitClass enums.ExitClass,
) error {
	finishedAt := metav1.Now()
	status := runsv1alpha1.StepRunStatus{
		Phase:          enums.PhaseFailed,
		FinishedAt:     &finishedAt,
		Duration:       finishedAt.Sub(execCtxData.StartedAt.Time).String(),
		LastFailureMsg: err.Error(),
		ExitCode:       int32(exitCode),
		ExitClass:      exitClass,
	}
	return k8sClient.PatchStepRunStatus(ctx, execCtxData.StoryInfo.StepRunID, status)
}

// hydrateAndUnmarshalInputs hydrates raw inputs and unmarshals them into the target type I.
// On failure, it patches StepRun status and returns the error.
func hydrateAndUnmarshalInputs[C any, I any](
	ctx context.Context, sm StorageManager, k8sClient K8sClient, execCtxData *runtime.ExecutionContextData,
) (I, error) {
	var zero I
	// Attach StepRunID to context for hydration metrics attribution.
	hctx := storage.WithStepRunID(ctx, execCtxData.StoryInfo.StepRunID)
	hydratedInputs, err := sm.Hydrate(hctx, execCtxData.Inputs)
	if err != nil {
		err = fmt.Errorf("failed to hydrate inputs: %w", err)
		if patchErr := patchFailureStatus(ctx, k8sClient, execCtxData, err, 1, enums.ExitClassTerminal); patchErr != nil {
			return zero, fmt.Errorf("engram failed during input hydration: %w (status patch also failed: %v)",
				err, patchErr,
			)
		}
		return zero, err
	}

	// Defensive: ensure map for unmarshal
	rawMap, ok := hydratedInputs.(map[string]any)
	if !ok {
		err = fmt.Errorf("hydrated inputs have unexpected type %T (want map[string]any)", hydratedInputs)
		if patchErr := patchFailureStatus(ctx, k8sClient, execCtxData, err, 1, enums.ExitClassTerminal); patchErr != nil {
			return zero, fmt.Errorf("engram failed during input validation: %w (status patch also failed: %v)", err, patchErr)
		}
		return zero, err
	}

	inputs, err := runtime.UnmarshalFromMap[I](rawMap)
	if err != nil {
		err = fmt.Errorf("failed to unmarshal inputs into the target type: %w", err)
		if patchErr := patchFailureStatus(ctx, k8sClient, execCtxData, err, 1, enums.ExitClassTerminal); patchErr != nil {
			return zero, fmt.Errorf("engram failed during input unmarshaling: %w (status patch also failed: %v)", err, patchErr)
		}
		return zero, err
	}
	return inputs, nil
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
	if v := os.Getenv("BUBU_HYBRID_BRIDGE"); v != "" {
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
	if v := os.Getenv("BUBU_HYBRID_BRIDGE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 15 * time.Second
}

// getHubTarget resolves the Hub address. If DOWNSTREAM_HOST is set, use it.
// Otherwise, use the in-cluster DNS of the hub service in the current namespace.
func getHubTarget() string {
	if v := os.Getenv("DOWNSTREAM_HOST"); v != "" {
		return v
	}
	// Fallback to hub service DNS
	ns := k8s.ResolvePodNamespace()
	port := os.Getenv("BUBU_GRPC_PORT")
	if port == "" {
		port = DefaultGRPCPort
	}
	return fmt.Sprintf("bobravoz-grpc-hub.%s.svc.cluster.local:%s", ns, port)
}

// bridgeToHub sends a single DataPacket with the given JSON payload bytes to the Hub.
// It uses StreamToWithMetadata to automatically attach StoryRun metadata from env.
func bridgeToHub(ctx context.Context, payloadJSON []byte) error {
	// If payload is empty or nil, nothing to forward.
	if len(payloadJSON) == 0 {
		return nil
	}

	// If no deadline is set on the provided context, enforce a best-effort timeout
	// based on BUBU_HYBRID_BRIDGE_TIMEOUT to avoid hanging tests or jobs.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		to := getBridgeTimeout()
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, to)
		defer cancel()
	}

	target := getHubTarget()

	// Prepare single-message channels
	in := make(chan engram.StreamMessage, 1)
	out := make(chan engram.StreamMessage, 1)

	// Drain any responses so the client writer is never blocked
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for range out {
			// discard
		}
	}()

	// Enqueue one message and close input
	in <- engram.StreamMessage{Metadata: map[string]string{}, Payload: payloadJSON, Inputs: nil}
	close(in)

	err := StreamToWithMetadata(ctx, target, in, out)
	// Ensure drain goroutine exits even if stream never closed 'out'
	close(out)
	<-drainDone
	return err
}
