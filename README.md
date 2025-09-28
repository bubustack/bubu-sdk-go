# Bubu SDK for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/bubustack/bubu-sdk-go.svg)](https://pkg.go.dev/github.com/bubustack/bubu-sdk-go)

The official Go SDK for building type-safe `Engrams` and `Impulses` for the [bobrapet](https://github.com/bubustack/bobrapet) Kubernetes-native AI workflow engine.

This SDK provides a generic, type-safe interface that completely abstracts away Kubernetes internals and JSON unmarshaling, allowing you to focus purely on your application logic.

## Features

*   **Type-Safe & Generic**: Define your own structs for configuration and inputs. The SDK handles all validation and unmarshaling for you.
*   **Clean Interfaces**: Simple, generic interfaces for `BatchEngrams`, `Impulses`, and `StreamingEngrams`.
*   **Complete Abstraction**: Zero Kubernetes API interaction required. The SDK provides a clean client for triggering stories and handles all runtime context automatically.
*   **Transparent Storage Offloading**: Automatically offload large inputs and outputs to S3 or a local file store, keeping your Kubernetes resources lean.

## Core Concepts

### Impulse

An `Impulse` is a long-running component that triggers stories. It is generic over your configuration type `C`.

```go
type Impulse[C any] interface {
    Engram[C]
    Run(ctx context.Context, client *impulse.Client) error
}
```

### Engram

The `Engram` is the fundamental interface for all executable components. It is generic over a configuration type `C`, which must be a pointer to a struct you define. The SDK will automatically unmarshal the `with` block from your `EngramTemplate` or `Impulse` into this struct.

```go
type Engram[C any] interface {
    Init(ctx context.Context, config C, secrets *Secrets) error
}
```

### Batch Engram

A `BatchEngram` is for tasks that run to completion. It is generic over your configuration type `C` and your input type `I`.

```go
type BatchEngram[C any, I any] interface {
    Engram[C]
    Process(ctx context.Context, execCtx *ExecutionContext, inputs I) (*Result, error)
}
```

### Streaming Engram

A `StreamingEngram` is for long-running, real-time data processing tasks. It is also generic over the configuration type `C`.

```go
type StreamingEngram[C any] interface {
    Engram[C]
    Stream(ctx context.Context, in <-chan []byte, out chan<- []byte) error
}
```

### Dual-Mode Engrams

Because `BatchEngram` and `StreamingEngram` share the same `Engram[C]` base interface, you can create a "dual-mode" engram by simply implementing all the required methods on a single struct. The `bobrapet` controller will determine which mode to run based on the `WorkloadSpec` in your `EngramTemplate`.

## Getting Started: Creating a Batch Engram

**1. Define your types:**

Create structs that represent the shape of your configuration (`with` block) and inputs.

```go
// myengram.go
package main

// GreeterConfig defines the structure for this Engram's `with` block.
type GreeterConfig struct {
	// (No config needed for this simple example)
}

// GreeterInputs defines the structure for this Engram's inputs.
type GreeterInputs struct {
	Name string `json:"name"`
}
```

**2. Implement the `BatchEngram` interface:**

Your `Init` and `Process` methods will receive the fully-typed, unmarshaled `config` and `inputs` structs directly.

```go
// myengram.go
// ...

type GreeterEngram struct{}

func (g *GreeterEngram) Init(ctx context.Context, config *GreeterConfig, secrets *engram.Secrets) error {
    return nil
}

func (g *GreeterEngram) Process(ctx context.Context, execCtx *engram.ExecutionContext, inputs *GreeterInputs) (*engram.Result, error) {
    if inputs.Name == "" {
        return nil, fmt.Errorf("input 'name' is required")
    }
    greeting := fmt.Sprintf("Hello, %s!", inputs.Name)
    return &engram.Result{
        Data: map[string]interface{}{"greeting": greeting},
    }, nil
}
```

**3. Run your Engram:**

The `sdk.Run` function is generic and will manage the entire lifecycle.

```go
// main.go
package main

import (
    "context"
    "log"

    "github.com/bubustack/bubu-sdk-go"
)

func main() {
    if err := sdk.Run(context.Background(), &GreeterEngram{}); err != nil {
        log.Fatalf("Engram failed: %v", err)
    }
}
```

## Getting Started: Creating an Impulse

**1. Define your types:**

Define a struct for your `Impulse`'s `with` configuration.

```go
// myimpulse.go
package main

// HttpImpulseConfig defines the structure for this Impulse's `with` block.
type HttpImpulseConfig struct {
	Port string `json:"port"`
}
```

**2. Implement the `Impulse` interface:**

The `Init` method receives your typed config struct. The `Run` method receives the pre-configured client.

```go
// myimpulse.go
// ...

type HttpImpulse struct {
	client *impulse.Client
	port   string
}

func (h *HttpImpulse) Init(ctx context.Context, config *HttpImpulseConfig, secrets *engram.Secrets) error {
	h.port = config.Port
	if h.port == "" {
		h.port = ":8080" // Fallback port
	}
	return nil
}

func (h *HttpImpulse) Run(ctx context.Context, client *impulse.Client) error {
	h.client = client
	http.HandleFunc("/trigger", h.handleTrigger)
	fmt.Printf("Starting HTTP server on %s\n", h.port)
	return http.ListenAndServe(h.port, nil)
}

type TriggerPayload struct {
	URL string `json:"url"`
}

func (h *HttpImpulse) handleTrigger(w http.ResponseWriter, r *http.Request) {
	var payload TriggerPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	storyRun, err := h.client.TriggerStory(
		r.Context(),
		map[string]interface{}{"url": payload.URL},
	)
	if err != nil {
		log.Printf("Failed to create StoryRun: %v", err)
		http.Error(w, "Failed to create StoryRun", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	response := map[string]string{
		"message":      "StoryRun created successfully",
		"storyRunName": storyRun.Name,
	}
	json.NewEncoder(w).Encode(response)
}
```

**3. Run your Impulse:**

```go
// main.go
package main

import (
    "context"
    "log"

    "github.com/bubustack/bubu-sdk-go"
)

func main() {
	if err := sdk.RunImpulse(context.Background(), &HttpImpulse{}); err != nil {
		log.Fatalf("Impulse failed: %v", err)
	}
}
```

## Streaming Engrams

The `StreamingEngram` interface provides a type-safe way to handle real-time data streams. The SDK automatically initializes your component with its configuration before starting the stream.

**1. Define your types and implement the `StreamingEngram` interface:**

```go
// mystream.go
package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	"github.com/bubustack/bubu-sdk-go"
	"github.com/bubustack/bubu-sdk-go/engram"
)

// StreamConfig defines the structure for this Engram's `with` block.
type StreamConfig struct {
	ToUpper bool `json:"toUpper"`
}

type MyStreamingEngram struct {
	conf *StreamConfig
}

func (s *MyStreamingEngram) Init(ctx context.Context, config *StreamConfig, secrets *engram.Secrets) error {
	s.conf = config
	return nil
}

func (s *MyStreamingEngram) Stream(ctx context.Context, in <-chan []byte, out chan<- []byte) error {
	for data := range in {
		var input map[string]interface{}
		if err := json.Unmarshal(data, &input); err != nil {
			log.Printf("Error unmarshaling input: %v", err)
			continue
		}

		if s.conf.ToUpper {
			if val, ok := input["message"].(string); ok {
				input["message"] = strings.ToUpper(val)
			}
		}

		processedData, err := json.Marshal(input)
		if err != nil {
			log.Printf("Error marshaling output: %v", err)
			continue
		}
		out <- processedData
	}
	return nil
}
```

**2. Run your Streaming Engram:**

```go
// main.go
package main

import (
    "context"
    "log"

    "github.com/bubustack/bubu-sdk-go"
)

func main() {
    if err := sdk.StartStreamServer(context.Background(), &MyStreamingEngram{}); err != nil {
        log.Fatalf("Stream server failed: %v", err)
    }
}
```

## Storage Offloading

If your `Engram` produces outputs that are larger than `32KiB` (by default), the SDK will automatically offload them to a configured storage backend (like S3) and replace the output with a storage reference. The `bobrapet` controller will then automatically "re-hydrate" this data for the next step in the `Story`. This happens transparently, with no changes required in your `Engram` code.

You can configure the storage provider and size limit using environment variables.

## Configuration

The SDK runtime can be configured via the following environment variables, which are typically set by the `bobrapet` controller:

*   `BUBU_STORAGE_PROVIDER`: The storage backend to use. Supported values: `s3`, `file`. If not set, storage offloading is disabled.
*   `BUBU_STORAGE_PATH`: For the `file` provider, the base path to store files.
*   `BUBU_MAX_INLINE_SIZE`: The maximum size (in bytes) for an output to be kept inline in the `StoryRun` status. Defaults to `32768`.
*   `GRPC_PORT`: The port for the gRPC server to listen on for streaming `Engrams`. Defaults to `8080`.
*   `POD_NAMESPACE`: The namespace the pod is running in. Used by `Impulses` to create `StoryRuns` in the correct namespace.

**S3 Provider Configuration:**

The S3 provider uses the standard AWS SDK environment variables for configuration:
*   `AWS_REGION`
*   `AWS_ACCESS_KEY_ID`
*   `AWS_SECRET_ACCESS_KEY`
*   And others supported by the [default credential chain](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials).

