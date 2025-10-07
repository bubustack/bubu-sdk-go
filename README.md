# Bubu SDK for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/bubustack/bubu-sdk-go.svg)](https://pkg.go.dev/github.com/bubustack/bubu-sdk-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/bubustack/bubu-sdk-go)](https://goreportcard.com/report/github.com/bubustack/bubu-sdk-go)

The official Go SDK for building type-safe, production-grade components for **bobrapet**, the Kubernetes‑native AI and data workflow orchestration engine.

## What is this SDK for?

Use this SDK to build **engrams** (data processing tasks) and **impulses** (event listeners that trigger workflows). bobrapet orchestrates their execution as Kubernetes Jobs and Deployments, handling:

- **Type-safe configuration and inputs** — Define your interfaces as Go structs, get compile-time safety.
- **Automatic large payload handling** — Outputs exceeding size limits are transparently offloaded to S3/file storage.
- **Streaming pipelines** — Build real-time data processing chains with gRPC bidirectional streaming.
- **Retries and observability** — Exit codes inform retry policies; OpenTelemetry metrics and tracing built-in.

### When to use which mode

- Batch (Jobs): finite tasks with clear start/end; use `StartBatch`. Evidence: batch flow in `batch.go`.
- Streaming (Deployments with gRPC): continuous processing with backpressure/heartbeats; use `StartStreaming`. Evidence: `stream.go`.
- Impulse (Deployments): long‑running trigger that creates `StoryRun`s; use `RunImpulse`. Evidence: `impulse.go`.

---

## 5-minute quickstart

Let's build a simple batch engram that greets users.

### 1. Create your Go module

```bash
mkdir hello-engram && cd hello-engram
go mod init github.com/yourusername/hello-engram
go get github.com/bubustack/bubu-sdk-go@latest
```

### 2. Write the code

Create a single `main.go` file with all the necessary components.

```go
package main

import (
	"context"
	"fmt"
	"log"

	sdk "github.com/bubustack/bubu-sdk-go"
	"github.com/bubustack/bubu-sdk-go/engram"
)

// Config holds static configuration from the Engram resource 'with' block.
type Config struct {
	DefaultGreeting string `mapstructure:"defaultGreeting"`
}

// Inputs holds runtime data passed to this execution via StepRun inputs.
type Inputs struct {
	Name string `mapstructure:"name"`
}

// GreeterEngram implements the engram.Batch interface.
type GreeterEngram struct {
	greeting string
}

// NewGreeter creates a new GreeterEngram.
func NewGreeter() *GreeterEngram {
	return &GreeterEngram{}
}

// Init is called once when the engram starts.
func (g *GreeterEngram) Init(ctx context.Context, cfg Config, secrets *engram.Secrets) error {
	g.greeting = "Hello"
	if cfg.DefaultGreeting != "" {
		g.greeting = cfg.DefaultGreeting
	}
	return nil
}

// Process is the core logic. It receives typed inputs and returns a Result.
func (g *GreeterEngram) Process(ctx context.Context, ec *engram.ExecutionContext, inputs Inputs) (*engram.Result, error) {
	if inputs.Name == "" {
		return nil, fmt.Errorf("input 'name' is required")
	}
	message := fmt.Sprintf("%s, %s!", g.greeting, inputs.Name)
	return engram.NewResultFrom(map[string]any{
		"greeting": message,
	})
}

func main() {
	if err := sdk.StartBatch(context.Background(), NewGreeter()); err != nil {
		log.Fatalf("Engram failed: %v", err)
	}
}
```

### 3. Build the binary

```bash
go build -o hello-engram .
```

### 4. Containerize and deploy

Create a `Dockerfile`:

```dockerfile
# Use a smaller base image
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Build a static binary
RUN CGO_ENABLED=0 go build -o /hello-engram .

# Final stage
FROM alpine:latest
# Add non-root user
RUN addgroup -S app && adduser -S app -G app
USER app
# Copy binary and certificates
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /hello-engram /hello-engram
ENTRYPOINT ["/hello-engram"]
```

Build and push the image to your container registry:

```bash
docker build -t myregistry.io/hello-engram:latest .
docker push myregistry.io/hello-engram:latest
```

### 5. Deploy to Kubernetes

Create an `Engram` resource (`hello-engram.yaml`):

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Engram
metadata:
  name: hello-engram
spec:
  image: myregistry.io/hello-engram:latest
  with:
    defaultGreeting: "Greetings"
```

Create a `Story` that uses the engram (`greet-story.yaml`):

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Story
metadata:
  name: greet-users
spec:
  steps:
    - name: greet
      engram: hello-engram
      with:
        name: "{{ .inputs.userName }}"
```

Apply the resources and trigger a `StoryRun`:

```bash
kubectl apply -f hello-engram.yaml -f greet-story.yaml

kubectl create -f - <<EOF
apiVersion: bubustack.io/v1alpha1
kind: StoryRun
metadata:
  generateName: greet-users-
spec:
  storyRef:
    name: greet-users
  inputs:
    userName: "Bob"
EOF
```

---

## Core Concepts

### Engrams

**Engrams** are the building blocks of workflows. They are stateless, single-purpose components that execute a specific task.

| Type | Use Case | Kubernetes Workload |
|------|----------|---------------------|
| **BatchEngram** | Data transformations, API calls, ETL tasks | Job |
| **StreamingEngram** | Real-time data processing, filtering, routing | Deployment (gRPC server) |

### Impulses

**Impulses** are long-running services that listen for external events (webhooks, message queues, schedulers) and trigger StoryRuns.

| Component | Role | Kubernetes Workload |
|-----------|------|---------------------|
| **Impulse** | Event listener → Story trigger | Deployment |

---

## Key Features

### Type-Safe and Generic

Define your configuration and inputs as native Go structs. The SDK handles deserialization and validation:

```go
type Config struct {
    APIKey string `mapstructure:"apiKey"`
    Timeout time.Duration `mapstructure:"timeout"` // Supports duration parsing
}
```

### Transparent Storage Offloading

Large outputs are automatically offloaded to S3 or file storage, keeping Kubernetes resources lean:

```go
return &sdk.Result{
    Data: map[string]any{
        "largePayload": someLargeData, // Automatically offloaded if > BUBU_MAX_INLINE_SIZE
    },
}, nil
```

### Streaming Pipelines

Build real-time data processing chains with metadata propagation for tracing:

```go
func (s *Streamer) Stream(ctx context.Context, in <-chan engram.StreamMessage, out chan<- engram.StreamMessage) error {
    for msg := range in {
        // Process msg.Payload, propagate msg.Metadata for tracing
        out <- engram.StreamMessage{
            Metadata: msg.Metadata, // Preserve trace IDs
            Payload: processedData,
        }
    }
    return nil
}
```

### Retries and Exit Codes

The SDK patches StepRun status with exit codes that inform the operator's retry policy:

- **0**: Success
- **1**: Logic error (terminal, no retry)
- **124**: Timeout (retryable)

---

## Documentation

- **[Guides](https://bubustack.io/docs/)** — Tutorials for batch, streaming, impulse, and storage.
- **[API Reference](https://pkg.go.dev/github.com/bubustack/bubu-sdk-go)** — Complete Go API documentation on pkg.go.dev.
- **[Reference Docs](https://bubustack.io/docs/reference/)** — CRDs, gRPC contracts, environment variables, errors, and configuration.
- **[How-To Guides](https://bubustack.io/docs/howto/)** — Focused recipes for common tasks (handling large payloads, tuning backpressure, etc.).
- **[Troubleshooting](https://bubustack.io/docs/troubleshooting/)** — Diagnostics and fixes for timeouts, cancellations, backpressure, storage.

---

## Support Matrix

| Component | Version |
|-----------|---------|
| **Go** | 1.24+ |
| **Kubernetes** | 1.28+ (bobrapet operator compatibility) |
| **bobrapet operator** | v0.1.0+ |

---

## Environment Variables

The SDK is controlled entirely by environment variables injected by the bobrapet operator. See [docs/reference/config](https://bubustack.io/docs/reference/config) for the complete reference.

Key variables:

- `BUBU_STEP_TIMEOUT` — Batch execution timeout (default: 30m)
- `BUBU_STORAGE_PROVIDER` — Storage backend: `s3`, `file`, or unset (disabled)
- `BUBU_MAX_INLINE_SIZE` — Offload threshold in bytes (default: 1024)
- `BUBU_GRPC_PORT` — gRPC server port for streaming engrams (default: 50051)
- `BUBU_EXECUTION_MODE` — Set by operator: `batch` | `streaming` (evidence in controllers)

---

## Testing

```bash
# Run all tests
make test

# With coverage
make test-coverage

# Check doc coverage (100% required)
make doc-coverage

# Run all checks
make all
```

---

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines on:

- Development workflow
- Commit message conventions (Conventional Commits)
- Running tests and linters
- Documentation style

---

## License

[Apache License, Version 2.0](./LICENSE)

---

## Getting Help

- **Issues**: [GitHub Issues](https://github.com/bubustack/bubu-sdk-go/issues)
- **Docs**: [SDK Docs](https://bubustack.io/docs/sdk) directory
- **Engram Example**: [HTTP Request Engram](https://github.com/bubustack/engrams/tree/main/http-request-engram)

---

## Community

- **Contributing**: See [CONTRIBUTING.md](./CONTRIBUTING.md). We welcome contributions!
- **Security**: See [SECURITY.md](./SECURITY.md) for vulnerability reporting.
- **Support**: See [SUPPORT.md](./SUPPORT.md) for how to get help.
