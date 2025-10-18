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
	"fmt"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/bubustack/bubu-sdk-go/runtime"
)

// RunImpulse is the type-safe entry point for impulses (Kubernetes Deployments that trigger Stories).
//
// This function infers config type C from the impulse implementation, providing compile-time
// type safety. It orchestrates the complete lifecycle:
//  1. Load execution context from environment (BUBU_CONFIG, BUBU_INPUTS, etc.)
//  2. Unmarshal config into type C
//  3. Call impulse.Init with typed config and secrets
//  4. Create pre-configured Kubernetes client with namespace resolution
//  5. Call impulse.Run with client, transferring control to long-running process
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
	logger := LoggerFromContext(ctx)
	logger.Info("Initializing Bubu SDK for Impulse execution")

	execCtxData, err := runtime.LoadExecutionContextData()
	if err != nil {
		return fmt.Errorf("failed to load execution context: %w", err)
	}
	logExecutionContextDebug(logger, execCtxData)

	// Unmarshal config.
	config, err := runtime.UnmarshalFromMap[C](execCtxData.Config)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}
	secrets := engram.NewSecrets(ctx, execCtxData.Secrets)

	if err := i.Init(ctx, config, secrets); err != nil {
		return fmt.Errorf("impulse initialization failed: %w", err)
	}

	k8sClient, err := k8s.NewClient()
	if err != nil {
		return fmt.Errorf("failed to create k8s client: %w", err)
	}

	logger.Info("Starting Impulse")
	return i.Run(ctx, k8sClient)
}
