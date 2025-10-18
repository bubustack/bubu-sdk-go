# 🧰 bubu-sdk-go — Official Go SDK for bobrapet

[![Go Reference](https://pkg.go.dev/badge/github.com/bubustack/bubu-sdk-go.svg)](https://pkg.go.dev/github.com/bubustack/bubu-sdk-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/bubustack/bubu-sdk-go)](https://goreportcard.com/report/github.com/bubustack/bubu-sdk-go)

The official Go SDK for building type-safe, production-grade components for **bobrapet**, the Kubernetes‑native AI and data workflow orchestration engine.

Quick links:
- SDK docs: https://bubustack.io/docs/sdk
- API Reference: https://pkg.go.dev/github.com/bubustack/bubu-sdk-go

## 🌟 Key Features

Use this SDK to build **engrams** (data processing tasks) and **impulses** (event listeners that trigger workflows). bobrapet orchestrates their execution as Kubernetes Jobs and Deployments, handling:

- **Type-safe configuration and inputs** — Define your interfaces as Go structs, get compile-time safety.
- **Automatic large payload handling** — Outputs exceeding size limits are transparently offloaded to S3/file storage.
- **Streaming pipelines** — Build real-time data processing chains with gRPC bidirectional streaming.
- **Retries and observability** — Exit codes inform retry policies; OpenTelemetry metrics/tracing hooks included (initialize an exporter in your app/infra).

## 🏗️ Architecture

High-level SDK architecture, execution modes, and operator integration are documented here:
- Overview: https://bubustack.io/docs/sdk

### 🧭 When to use which mode

- Batch (Jobs): finite tasks with clear start/end; use `StartBatch`. Evidence: batch flow in `batch.go`.
- Streaming (Deployments with gRPC): continuous processing with backpressure/heartbeats; use `StartStreaming`. Evidence: `stream.go`.
- Impulse (Deployments): long‑running trigger that creates `StoryRun`s; use `RunImpulse`. Evidence: `impulse.go`.

---

## 🚀 Quick Start

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

## 📚 Core concepts

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

## 🌟 Key features

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

## 📚 Documentation

- SDK: https://bubustack.io/docs/sdk
- Guides: https://bubustack.io/docs
- Reference: https://bubustack.io/docs/reference
- How-to: https://bubustack.io/docs/howto
- Troubleshooting: https://bubustack.io/docs/troubleshooting

---

## ✅ Support matrix

| Component | Version |
|-----------|---------|
| **Go** | 1.24+ |
| **Kubernetes** | 1.28+ (bobrapet operator compatibility) |
| **bobrapet operator** | v0.1.0+ |

---

## ⚙️ Environment variables

The SDK is controlled entirely by environment variables injected by the bobrapet operator. See [docs/reference/config](https://bubustack.io/docs/reference/config) for the complete reference.

Key variables:

- `BUBU_STEP_TIMEOUT` — Batch execution timeout (default: 30m)
- `BUBU_STORAGE_PROVIDER` — Storage backend: `s3`, `file`, or unset (disabled)
- `BUBU_MAX_INLINE_SIZE` — Offload threshold in bytes (default: 1024)
- `BUBU_GRPC_PORT` — gRPC server port for streaming engrams (default: 50051)
- `BUBU_EXECUTION_MODE` — Set by operator: `batch` | `streaming` (evidence in controllers)
- `BUBU_STORAGE_PATH` — Required when `BUBU_STORAGE_PROVIDER=file`; base directory for file store
- `BUBU_MAX_RECURSION_DEPTH` — Max traversal depth for hydrate/dehydrate (default: 10)
- `BUBU_STORAGE_TIMEOUT` — Timeout for storage operations (default: 5m)

---

## 🛠️ Local Development

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/bubustack/bubu-sdk-go.git
    cd bubu-sdk-go
    ```

2.  **Run tests:**
    ```bash
    make test
    ```

3.  **Lint:**
    ```bash
    make lint
    ```

4.  **Run all checks:**
    ```bash
    make all
    ```

---

## 📢 Support, Security, and Changelog

- See `SUPPORT.md` for how to get help and report issues.
- See `SECURITY.md` for vulnerability reporting and security posture.
- See `CHANGELOG.md` for version history.

## 🤝 Community

- Code of Conduct: see [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md) (Contributor Covenant v3.0)

## 📄 License

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