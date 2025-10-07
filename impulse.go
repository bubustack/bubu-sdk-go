package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/bubustack/bubu-sdk-go/runtime"
)

// RunImpulse is the type-safe entry point for impulses (Kubernetes Deployments that trigger Stories).
//
// This function infers config type C from the impulse implementation, providing compile-time
// type safety. It orchestrates the complete lifecycle:
//  1. Load execution context from environment (BUBU_CONFIG, BUBU_IMPULSE_WITH, etc.)
//  2. Merge BUBU_IMPULSE_WITH JSON into config if provided (for operator injection)
//  3. Unmarshal config into type C
//  4. Call impulse.Init with typed config and secrets
//  5. Create pre-configured Kubernetes client with namespace resolution
//  6. Call impulse.Run with client, transferring control to long-running process
//
// The impulse's Run method should block until work completes or context is canceled.
// Typical use cases: webhook listeners, message queue consumers, schedulers, event watchers.
//
// Respects context cancellation for graceful shutdown on SIGTERM. The impulse is responsible
// for handling shutdown signals within its Run implementation (e.g., draining in-flight requests).
//
// Example:
//
//	type MyConfig struct {
//	    WebhookPort int    `mapstructure:"webhookPort"`
//	    SecretToken string `mapstructure:"secretToken"`
//	}
//
//	type MyImpulse struct { /* ... */ }
//
//	func (m *MyImpulse) Init(ctx context.Context, cfg MyConfig, secrets *engram.Secrets) error {
//	    // Setup webhook server, validate token, etc.
//	    return nil
//	}
//
//	func (m *MyImpulse) Run(ctx context.Context, client *k8s.Client) error {
//	    // Listen for webhooks, trigger stories via client.TriggerStory(...)
//	    <-ctx.Done()
//	    return ctx.Err()
//	}
//
//	func main() {
//	    if err := sdk.RunImpulse(context.Background(), &MyImpulse{}); err != nil {
//	        panic(err)
//	    }
//	}
func RunImpulse[C any](ctx context.Context, i engram.Impulse[C]) error {
	LoggerFromContext(ctx).Info("Initializing Bubu SDK for Impulse execution")

	execCtxData, err := runtime.LoadExecutionContextData()
	if err != nil {
		return fmt.Errorf("failed to load execution context: %w", err)
	}

	// If provided, merge BUBU_IMPULSE_WITH JSON into config before unmarshaling.
	if withStr := os.Getenv("BUBU_IMPULSE_WITH"); withStr != "" {
		var withMap map[string]any
		if err := json.Unmarshal([]byte(withStr), &withMap); err != nil {
			return fmt.Errorf("failed to unmarshal BUBU_IMPULSE_WITH: %w", err)
		}
		for k, v := range withMap {
			execCtxData.Config[k] = v
		}
	}

	// Unmarshal config.
	config, err := runtime.UnmarshalFromMap[C](execCtxData.Config)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}
	secrets := engram.NewSecrets(execCtxData.Secrets)

	if err := i.Init(ctx, config, secrets); err != nil {
		return fmt.Errorf("impulse initialization failed: %w", err)
	}

	k8sClient, err := k8s.NewClient()
	if err != nil {
		return fmt.Errorf("failed to create k8s client: %w", err)
	}

	LoggerFromContext(ctx).Info("Starting Impulse")
	return i.Run(ctx, k8sClient)
}
