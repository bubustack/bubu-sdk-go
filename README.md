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
	"os"
	"os/signal"
	"syscall"

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
	// Create a context that listens for OS interrupt signals.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := sdk.StartBatch(ctx, NewGreeter()); err != nil {
		log.Printf("Engram failed: %v", err)
		os.Exit(sdk.BatchExitCode(err))
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

### Story authoring helpers

Generate valid Story manifests programmatically and observe executions without re-implementing controller-runtime plumbing:

```go
builder := storybuilder.New("greet-users", "workflows").
    WithLabels(map[string]string{"app": "hello"}).
    Pattern(enums.BatchPattern)

if err := builder.AddEngramStep(
    "say-hello",
    refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "hello-engram"}},
    nil,
    map[string]any{"defaultGreeting": "Greetings"},
); err != nil {
    log.Fatal(err)
}

story, err := builder.Build()
if err != nil {
    log.Fatal(err)
}
// apply 'story' using your favourite K8s client
```

Watching a StoryRun for status changes is equally direct:

```go
events, err := k8sClient.WatchStoryRun(ctx, "greet-users-abc123")
if err != nil {
    log.Fatal(err)
}
for ev := range events {
    if ev.Err != nil {
        log.Printf("watch error: %v", ev.Err)
        continue
    }
    log.Printf("storyrun phase: %s status: %s", ev.Type, ev.StoryRun.Status.Phase)
}
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

### Control Directives & Metadata Helpers

Streaming engrams connect to the transport sidecar advertised via `BUBU_TRANSPORT_BINDING`.
The SDK also opens the `TransportConnector_Control` stream so connectors can pause/resume
traffic or request codec changes. Implement `engram.ControlDirectiveHandler` on your engram
to observe directives:

```go
func (s *Streamer) HandleControlDirective(ctx context.Context, d engram.ControlDirective) (*engram.ControlDirective, error) {
    if d.Type == "pause" {
        s.paused.Store(true)
        return &engram.ControlDirective{
            Type:     "ack",
            Metadata: map[string]string{"handled": "true"},
        }, nil
    }
    return nil, nil // default handler will ACK unknown directives
}
```

For emitting telemetry or binary bursts without touching audio/video tracks, use the
`NewStreamMessage` builder with options such as `WithMetadata`, `WithJSONPayload`, or
`WithBinaryPayload`. Messages built this way automatically flow through the Binary lane:

```go
msg := sdk.NewStreamMessage(
    "telemetry",
    sdk.WithMetadata(map[string]string{"storyRun": storyRunID}),
    sdk.WithJSONPayload(marshalMetrics()),
)
out <- msg
```

### Retries and Exit Codes

The SDK patches StepRun status with exit codes that inform the operator's retry policy:

- **0**: Success
- **1**: Logic error (terminal, no retry)
- **124**: Timeout (retryable)

Use `sdk.BatchExitCode(err)` to translate returned errors into the correct process exit code so the operator can distinguish retryable timeouts from terminal failures.

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

### Core Execution Context
These variables provide engrams with essential runtime information about the workflow execution.

- `BUBU_STORY_NAME`, `BUBU_STORYRUN_ID` — The name of the parent Story and the ID of the current StoryRun.
- `BUBU_STEP_NAME`, `BUBU_STEPRUN_NAME`, `BUBU_STEPRUN_NAMESPACE` — The name of the current step, the ID of its StepRun, and its namespace. Essential for logging, metrics, and Kubernetes API interactions.
- `BUBU_EXECUTION_MODE` — `batch` or `streaming`.
- `BUBU_STEP_TIMEOUT` — Max duration for a batch step (e.g., `5m`). SDK exits with code 124 on timeout.
- `BUBU_MAX_INLINE_SIZE` — Max payload size (bytes) before outputs are offloaded to storage (default: `1024`).

### Payloads & Configuration
- `BUBU_INPUTS` — A JSON string containing the inputs for the current step.
- `BUBU_CONFIG` — A JSON string containing the static configuration from the Engram's `with` block.

### Target Resolution
- `BUBU_TRANSPORT_BINDING` — Name of the `TransportBinding` resource the SDK should read to discover upstream/downstream endpoints for streaming steps.

Key variables:

- `BUBU_SDK_METRICS_ENABLED`, `BUBU_SDK_TRACING_ENABLED` — Set to `false` to disable SDK OpenTelemetry emission (default `true`).

**Batch & Hybrid Execution**
- `BUBU_HYBRID_BRIDGE` — Controls batch-to-stream bridging; `true` (default) forwards successful batch outputs, `false` disables it.
- `BUBU_HYBRID_BRIDGE_TIMEOUT` — Timeout for the hybrid bridge send operation (default: 15s).

**gRPC & Streaming (Server & Client)**
- `BUBU_GRPC_PORT` — Port for the gRPC server in streaming engrams (default: 50051 — matches `controller.engram.defaultGRPCPort`).
- `BUBU_SDK_CHANNEL_BUFFER_SIZE` — Buffer size for gRPC streaming channels (default: 16).
- `BUBU_SDK_MESSAGE_TIMEOUT` — Timeout for individual message operations (default: 30s).
- `BUBU_SDK_CHANNEL_SEND_TIMEOUT` — Backpressure timeout for sending on a full channel (default: same as message timeout).
- `BUBU_SDK_HEARTBEAT_INTERVAL` — Interval for sending keepalive heartbeats (default: 10s).
- `BUBU_SDK_HANG_TIMEOUT` — Timeout for detecting a hung connection (default: 30s).
- `BUBU_SDK_GRACEFUL_SHUTDOWN_TIMEOUT` — Max drain period for the streaming server on SIGTERM (default: 20s).

**gRPC Client (Connecting to other services)**
- `BUBU_SDK_DIAL_TIMEOUT` — Timeout for establishing a new gRPC connection.
- `BUBU_SDK_RECONNECT_MAX_RETRIES` — Max reconnect attempts on transient failures (default: 10, 0 for unlimited).
- `BUBU_SDK_RECONNECT_BASE_BACKOFF` — Initial backoff duration for reconnects (default: 500ms).
- `BUBU_SDK_RECONNECT_MAX_BACKOFF` — Maximum backoff duration for reconnects (default: 30s).
- `BUBU_SDK_CLIENT_BUFFER_MAX_MESSAGES` — Max messages to buffer during a disconnect (default: 100).
- `BUBU_SDK_CLIENT_BUFFER_MAX_BYTES` — Max total size of messages to buffer (default: 10MiB).

**gRPC TLS (Client & Server)**
- `BUBU_GRPC_REQUIRE_TLS` — If `true`, enforces TLS for the streaming server.
- `BUBU_GRPC_TLS_CERT_FILE`, `BUBU_GRPC_TLS_KEY_FILE` — Server certificate and key files.
- `BUBU_GRPC_CA_FILE` — CA file for server to verify client certs (mTLS) or for client to verify server certs.
- `BUBU_GRPC_CLIENT_CERT_FILE`, `BUBU_GRPC_CLIENT_KEY_FILE` — Client certificate and key for mTLS.
- `BUBU_GRPC_ALLOW_INSECURE` — Set to `true` to allow plaintext client dials (requires `BUBU_HUB_ALLOW_INSECURE=true`).

**Storage & Offloading (S3 & File)**
- `BUBU_STORAGE_PATH` — Base directory when `BUBU_STORAGE_PROVIDER=file`.
- `BUBU_MAX_RECURSION_DEPTH` — Traversal depth for hydrate/dehydrate (default: 10).
- `BUBU_STORAGE_TIMEOUT` — Timeout for storage operations (default: 5m).
- `BUBU_S3_BUCKET`, `BUBU_S3_REGION`, `BUBU_S3_ENDPOINT` — S3 configuration.
- `BUBU_S3_FORCE_PATH_STYLE` — If `true`, forces path-style addressing (for MinIO).
- `BUBU_S3_TLS` — Set to `false` to allow insecure HTTP S3 endpoints.
- `BUBU_S3_ACCESS_KEY_ID`, `BUBU_S3_SECRET_ACCESS_KEY`, `BUBU_S3_SESSION_TOKEN` — Static S3 credentials.
- `BUBU_S3_SSE`, `BUBU_S3_SSE_KMS_KEY` — Server-side encryption settings.
- `BUBU_S3_MAX_PART_SIZE`, `BUBU_S3_CONCURRENCY` — Multipart upload tuning.
- `BUBU_S3_MAX_RETRIES`, `BUBU_S3_MAX_BACKOFF`, `BUBU_S3_TIMEOUT` — S3 client retry and timeout settings.
- `BUBU_STORAGE_OUTPUT_PREFIX`, `BUBU_STORAGE_INPUT_PREFIX` — Customize key prefixes for offloaded data.

**Kubernetes Client**
- `BUBU_K8S_USER_AGENT` — Custom user agent for K8s API requests.
- `BUBU_K8S_TIMEOUT` — K8s client REST config timeout (default: 30s).
- `BUBU_K8S_OPERATION_TIMEOUT` — Per-operation timeout for SDK helpers like TriggerStory (default: 30s).
- `BUBU_K8S_PATCH_MAX_RETRIES` — Max retries for status patches on conflict (default: 5).

**Legacy & Deprecated**
- `BUBU_GRPC_...` — Most `BUBU_GRPC_*` variables for client tuning are superseded by `BUBU_SDK_*` equivalents but are still honored for backward compatibility.


### Toolchain bootstrap

The SDK targets Kubernetes `v0.34.x`, which in turn requires Go 1.24 features. Builders without internet access can pre-stage a Go toolchain (for example by copying an internally mirrored `go1.24.x` tarball into `.toolchains/go1.24.x`) and run all Go commands through [`hack/go.sh`](hack/go.sh):

```bash
$ BUBU_TOOLCHAIN_PATH=$PWD/.toolchains/go1.24.5 hack/go.sh test ./...
```

The `Makefile` already uses this wrapper, so `make test`, `make vet`, etc. work in both connected and fully-offline environments. When `BUBU_TOOLCHAIN_PATH` is unset the wrapper preserves Go’s default `GOTOOLCHAIN=auto` behaviour, which continues to work on developer laptops where the preview toolchain is already cached.

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
