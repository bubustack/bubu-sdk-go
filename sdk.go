// Package sdk provides the primary entry points for executing bobrapet components.
//
// This package contains the runtime logic that bootstraps an Engram or Impulse,
// injects the necessary context from the environment, and manages its lifecycle.
// Developers typically interact with StartBatch, StartStreaming, or RunImpulse
// from their main.go file.
//
// # Entry Points
//
// For batch engrams (Jobs):
//
//	sdk.StartBatch(ctx, myEngram)
//
// For streaming engrams (Deployments with gRPC):
//
//	sdk.StartStreaming(ctx, myStreamingEngram)
//
// For impulses (Deployments that trigger Stories):
//
//	sdk.RunImpulse(ctx, myImpulse)
//
// # Environment-Driven Configuration
//
// The SDK is controlled entirely by environment variables injected by the
// bobrapet operator. SDK defaults are fallback values for local development.
// See docs/reference/config.md for the complete environment variable reference.
//
// # Concurrency and Cancellation
//
// All entry points respect context cancellation. Batch engrams enforce a timeout
// via BUBU_STEP_TIMEOUT. Streaming engrams implement graceful shutdown on SIGTERM
// with configurable drain timeouts via BUBU_GRPC_GRACEFUL_SHUTDOWN_TIMEOUT.
//
// # Error Handling
//
// Entry points return errors for initialization and execution failures. Batch
// engrams additionally patch StepRun status with exit codes for operator retry
// policy classification (exit code 124 for timeouts, 1 for logic errors, 0 for success).
package sdk

import (
	"context"
	"fmt"
	"os"

	"log/slog"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/k8s"
)

// ==== Logger injection via context ====
type ctxLoggerKey struct{}

// WithLogger stores a slog.Logger in the context for SDK use.
//
// This allows you to inject a custom configured logger (e.g., with specific
// log levels, handlers, or structured attributes) that the SDK will use for
// all internal logging. If not provided, the SDK defaults to JSON logging to stdout.
//
// Example:
//
//	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
//	ctx := sdk.WithLogger(context.Background(), logger)
//	sdk.StartBatch(ctx, myEngram)
//
// The logger is used for lifecycle events (init, shutdown), errors, and metrics.
// It does not intercept engram-specific logs; engrams should use their own loggers
// or retrieve the SDK logger via LoggerFromContext within their execution context.
func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	if logger == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxLoggerKey{}, logger)
}

// LoggerFromContext retrieves a slog.Logger from the context, or returns a default JSON logger.
//
// If no logger was previously stored via WithLogger, this function returns a new
// JSON logger writing to stdout with default settings. This ensures the SDK always
// has a valid logger without requiring explicit configuration for simple use cases.
//
// Thread-safe and idempotent.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	if ctx == nil {
		return slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	if l, ok := ctx.Value(ctxLoggerKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, nil))
}

// K8sClient defines the interface for Kubernetes operations required by the SDK.
//
// This interface abstracts the SDK's dependency on Kubernetes, enabling mocking in
// tests and providing a stable contract. Implementations must be safe for concurrent
// use by multiple goroutines.
//
// The SDK's default implementation (k8s.Client) provides:
//   - Automatic namespace resolution from environment variables
//   - Retry-on-conflict logic for status patches
//   - Phase transition validation to prevent state corruption
//   - OpenTelemetry metrics for operation latency and success/failure
//
// Custom implementations should follow the same concurrency and idempotency guarantees.
type K8sClient interface {
	// TriggerStory creates a new StoryRun for the named Story with the provided inputs.
	// The inputs map is marshaled to JSON and stored in the StoryRun's spec.inputs field.
	// Returns the created StoryRun on success, or an error if creation fails.
	// Respects context cancellation and deadlines.
	TriggerStory(ctx context.Context, storyName string, inputs map[string]any) (*runsv1alpha1.StoryRun, error)

	// PatchStepRunStatus updates the status of the named StepRun with the provided patch data.
	// The implementation should use field-wise merging to avoid clobbering controller-managed
	// fields and implement retry-on-conflict logic to handle concurrent updates.
	// Respects context cancellation and deadlines.
	PatchStepRunStatus(ctx context.Context, stepRunName string, patchData runsv1alpha1.StepRunStatus) error
}

// StorageManager defines the interface for storage operations required by the SDK.
//
// This interface provides transparent data offloading for large inputs and outputs,
// automatically handling marshaling, storage backend operations, and reference tracking.
// Implementations must be safe for concurrent use by multiple goroutines.
//
// The SDK's default implementation (storage.Manager) provides:
//   - Automatic size-based offloading (configurable via BUBU_MAX_INLINE_SIZE)
//   - Recursive hydration/dehydration of nested structures
//   - Support for S3 and file storage backends
//   - Path traversal protection and validation
//   - OpenTelemetry metrics for operation latency and data sizes
//
// Storage references use the format {"$bubuStorageRef": "outputs/steprun-id/path.json"}.
type StorageManager interface {
	// Hydrate recursively scans a data structure for storage references and replaces
	// them with the actual content from the storage backend. Returns the hydrated
	// data on success, or an error if reading fails or a reference is invalid.
	// Respects context cancellation and enforces BUBU_STORAGE_TIMEOUT.
	Hydrate(ctx context.Context, data any) (any, error)

	// Dehydrate recursively checks the size of a data structure. If any part exceeds
	// the inline size limit (BUBU_MAX_INLINE_SIZE), it saves that part to the storage
	// backend and replaces it with a storage reference. Returns the dehydrated data
	// (potentially containing references) on success, or an error if writing fails.
	// Respects context cancellation and enforces BUBU_STORAGE_TIMEOUT.
	Dehydrate(ctx context.Context, data any, stepRunID string) (any, error)
}

// === Story Helpers ===

// StartStory triggers a new StoryRun for the named Story with the provided inputs.
//
// This is the primary mechanism for programmatically initiating workflows, typically
// used from within an Impulse. The SDK automatically resolves the correct namespace
// from environment variables (BUBU_TARGET_STORY_NAMESPACE or fallbacks), creates a
// Kubernetes client, and submits the StoryRun resource.
//
// Inputs are marshaled to JSON and stored in the StoryRun spec. The operator watches
// for new StoryRuns and orchestrates their execution.
//
// Returns the created StoryRun on success, or an error if client creation or StoryRun
// creation fails. Respects context cancellation and deadlines.
//
// Example:
//
//	sr, err := sdk.StartStory(ctx, "my-workflow", map[string]any{
//	    "userId": "12345",
//	    "action": "process",
//	})
//	if err != nil {
//	    return fmt.Errorf("failed to trigger story: %w", err)
//	}
//	log.Printf("Triggered StoryRun: %s", sr.Name)
func StartStory(ctx context.Context, storyName string, inputs map[string]any) (*runsv1alpha1.StoryRun, error) {
	k8sClient, err := k8s.NewClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %w", err)
	}
	return k8sClient.TriggerStory(ctx, storyName, inputs)
}

// StartBatch is the type-safe entry point for batch engrams (Kubernetes Jobs).
//
// This function infers both config type C and input type I from the engram implementation,
// providing full compile-time type safety. It orchestrates the complete lifecycle:
//  1. Load execution context from environment (BUBU_CONFIG, BUBU_INPUTS, etc.)
//  2. Unmarshal config and inputs into types C and I
//  3. Call engram.Init with typed config and secrets
//  4. Hydrate inputs from storage if needed
//  5. Call engram.Process with typed inputs and execution context
//  6. Dehydrate outputs to storage if they exceed size limits
//  7. Patch StepRun status with result, timing, and exit code
//
// Enforces timeout via BUBU_STEP_TIMEOUT with context cancellation. On timeout, patches
// status with exit code 124 (retryable) and forcefully exits to prevent zombie Jobs.
// On logic errors, patches with exit code 1 (terminal). On success, patches with exit code 0.
//
// Example:
//
//	type MyConfig struct { APIKey string `mapstructure:"apiKey"` }
//	type MyInputs struct { UserID string `mapstructure:"userId"` }
//
//	func main() {
//	    ctx := context.Background()
//	    if err := sdk.StartBatch(ctx, NewMyEngram()); err != nil {
//	        panic(err)  // Ensure non-zero exit for Job failure detection
//	    }
//	}
func StartBatch[C any, I any](ctx context.Context, e engram.BatchEngram[C, I]) error {
	return RunBatch(ctx, e)
}

// StartStreaming is the type-safe entry point for streaming engrams (Kubernetes Deployments with gRPC).
//
// This function infers config type C from the engram implementation, providing compile-time
// type safety. It orchestrates the complete lifecycle:
//  1. Load execution context from environment (BUBU_CONFIG, etc.)
//  2. Unmarshal config into type C
//  3. Call engram.Init with typed config and secrets
//  4. Start gRPC server on BUBU_GRPC_PORT (default 50051)
//  5. Register engram.Stream as the bidirectional streaming handler
//  6. Serve until context cancellation (SIGTERM) or error
//  7. Gracefully drain active streams before shutdown
//
// The gRPC server implements:
//   - Transparent heartbeat sending/filtering to detect connection hangs
//   - Backpressure handling with configurable timeouts
//   - Graceful shutdown with BUBU_GRPC_GRACEFUL_SHUTDOWN_TIMEOUT drain phase
//   - Optional TLS via BUBU_GRPC_TLS_CERT_FILE and BUBU_GRPC_TLS_KEY_FILE
//   - Configurable message size limits via BUBU_GRPC_MAX_RECV_BYTES and BUBU_GRPC_MAX_SEND_BYTES
//
// Example:
//
//	type MyConfig struct { BufferSize int `mapstructure:"bufferSize"` }
//
//	func main() {
//	    ctx := context.Background()
//	    if err := sdk.StartStreaming(ctx, NewMyStreamingEngram()); err != nil {
//	        panic(err)
//	    }
//	}
func StartStreaming[C any](ctx context.Context, e engram.StreamingEngram[C]) error {
	return StartStreamServer(ctx, e)
}
