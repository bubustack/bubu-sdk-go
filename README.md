# Bubu SDK for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/bubustack/bubu-sdk-go.svg)](https://pkg.go.dev/github.com/bubustack/bubu-sdk-go)

The official Go SDK for building powerful, type-safe components for **bobrapet**, the Kubernetes-native AI and data workflow engine.

This SDK is the cornerstone of the bobrapet ecosystem. It provides a set of clean, generic interfaces that abstract away the complexities of Kubernetes, allowing you to focus purely on your application's logic. Whether you're building a simple data transformation task or a complex, long-running event listener, the SDK provides the tools to build it elegantly and efficiently.

---

## Architectural Vision

The bobrapet ecosystem is built on a clear separation of concerns, embodied by two primary components: **Engrams** and **Impulses**.

```mermaid
graph TD
    subgraph "External World"
        direction LR
        A[External Event<br>(e.g., Webhook, Message Queue)]
    end

    subgraph "Kubernetes Cluster (bobrapet)"
        direction LR
        B(Impulse<br><br><i>Listens for events<br>Triggers Stories</i>) -- Starts --> C(StoryRun);
        C -- Manages --> D{StepRun 1<br>Engram A};
        C -- Manages --> E{StepRun 2<br>Engram B};
        C -- Manages --> F{...};
    end
    
    A --> B;
```

-   **Impulses**: The gateways to your workflows. Impulses are long-running services that listen for external events (like webhooks or message queues) and *trigger* `StoryRuns`. They are the "when" of your system.
-   **Engrams**: The building blocks of your workflows. Engrams are stateless, single-purpose components that execute a specific task (like making an API call or transforming data). They run as steps within a `StoryRun` and are the "what" of your system.

The SDK provides distinct, type-safe interfaces for building both of these component types.

---

## Key Features

-   **Type-Safe & Generic**: Define your configuration and inputs as native Go structs. The SDK handles the complex machinery of deserialization for you.
-   **Clean, Purpose-Driven Interfaces**: Simple, elegant interfaces for `BatchEngrams`, `StreamingEngrams`, and `Impulses`.
-   **Transparent Storage Offloading**: Automatically handle large inputs and outputs by seamlessly offloading them to S3 or a local filesystem, keeping your core logic clean.
-   **Simplified Kubernetes Integration**: A minimal, robust Kubernetes client that works automatically in-cluster or from your local machine via `kubeconfig`.

---

## Core Interfaces

| Interface         | Purpose                                                                                | Execution Mode |
| ----------------- | -------------------------------------------------------------------------------------- | -------------- |
| `BatchEngram`     | For tasks that run to completion, like a script or a single data transformation.       | `Job`          |
| `StreamingEngram` | For long-running, real-time data processing tasks that handle data via gRPC streams.   | `Deployment`   |
| `Impulse`         | For long-running services that listen for external events and trigger new `StoryRuns`. | `Deployment`   |

---

## Quickstart: Building a `BatchEngram`

A `BatchEngram` is the most common component type. It takes an input, performs a task, and produces an output.

#### 1. Define Your Types

Create strongly-typed structs for your engram's static configuration (`Config`) and runtime inputs (`Inputs`). This allows the SDK to provide type-safe unmarshaling.

```go
// my-engram/pkg/types/types.go
package types

// Config defines the static configuration for our engram.
// It's populated from the 'with' block of the Engram resource.
type Config struct {
    DefaultSalutation string `mapstructure:"defaultSalutation"`
}

// Inputs defines the data our engram receives at runtime for a single execution.
type Inputs struct {
    Name string `mapstructure:"name"`
}
```

#### 2. Implement the Engram

Implement the `engram.BatchEngram` interface. Your component should contain only its core business logic.

```go
// my-engram/pkg/engram/engram.go
package engram

import (
	"context"
	"fmt"

	sdk "github.com/bubustack/bubu-sdk-go/engram"
	"my-engram/pkg/types"
)

// GreeterEngram holds the state for our component.
type GreeterEngram struct {
	salutation string
}

func New() *GreeterEngram { return &GreeterEngram{} }

// Init is called once when the engram starts, receiving the static configuration.
func (g *GreeterEngram) Init(ctx context.Context, cfg types.Config, _ *sdk.Secrets) error {
	g.salutation = "Hello"
	if cfg.DefaultSalutation != "" {
		g.salutation = cfg.DefaultSalutation
	}
	return nil
}

// Process is the core logic of the engram. Because it's a BatchEngram, it receives an 'any'
// input type, which must be unmarshaled into the specific Inputs struct.
func (g *GreeterEngram) Process(ctx context.Context, ec *sdk.ExecutionContext, i any) (*sdk.Result, error) {
	var inputs types.Inputs
	if err := ec.UnmarshalInputs(i, &inputs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal inputs: %w", err)
	}

	if inputs.Name == "" {
		return nil, fmt.Errorf("input 'name' is required")
	}

	greeting := fmt.Sprintf("%s, %s!", g.salutation, inputs.Name)

	// The result's Data field will be the output of the StepRun.
	return &sdk.Result{
		Data: map[string]any{"greeting": greeting},
	}, nil
}
```

#### 3. Create the Main Entrypoint

The `main` function should contain a single call to `sdk.Start`. This universal function handles everything: detecting the execution mode (batch or stream), loading context, unmarshaling data, and calling your `Init` and `Process`/`Stream` methods.

```go
// my-engram/main.go
package main

import (
	"context"
	"log"

	sdk "github.com/bubustack/bubu-sdk-go"
	"my-engram/pkg/engram"
)

func main() {
	// The SDK's universal entrypoint handles everything from here.
	if err := sdk.Start(context.Background(), engram.New()); err != nil {
		// Use panic to ensure the process exits with a non-zero status code,
		// which is critical for Kubernetes Job failure detection.
		panic(fmt.Sprintf("Engram failed: %v", err))
	}
}
```

---

## Environment Variables Reference

The SDK's behavior is configured by environment variables injected by the `bobrapet` controller.

| Variable                      | Description                                                                 |
| ----------------------------- | --------------------------------------------------------------------------- |
| `BOBRAPET_EXECUTION_MODE`     | The mode of execution: `batch` (default) or `streaming`.                    |
| `BUBU_INPUTS`                 | A JSON string containing the resolved inputs for the engram.                |
| `BUBU_CONFIG_*`               | Key-value pairs that are merged into the config map before unmarshaling.    |
| `BUBU_SECRET_*`               | Key-value pairs for secrets, accessible via `engram.Secrets.Get(key)`.      |
| `BUBU_IMPULSE_WITH`           | A JSON string to be merged into an Impulse's configuration.                 |
| `BUBU_STORY_NAME`             | The name of the currently executing Story.                                  |
| `BUBU_STORYRUN_ID`            | The unique ID of the currently executing StoryRun.                          |
| `BUBU_STEP_NAME`              | The name of the current step within the Story.                              |
| `BUBU_STEPRUN_NAME`           | The unique ID of the currently executing StepRun.                           |
| `BUBU_POD_NAMESPACE`          | The Kubernetes namespace where the component is running.                    |
| `BUBU_GRPC_PORT`              | The port for the gRPC server in `StreamingEngram` mode (default: `8080`).   |
| `BUBU_STORAGE_PROVIDER`       | The storage backend for offloading: `s3` or `file`. (unset disables it)     |
| `BUBU_MAX_INLINE_SIZE`        | The size in bytes at which data is offloaded (default: `32768`).            |

For detailed S3 configuration, the SDK uses the standard AWS environment variables (`AWS_REGION`, `AWS_ACCESS_KEY_ID`, etc.).

