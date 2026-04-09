# 🧰 bubu-sdk-go — Official Go SDK for BubuStack

[![Go Reference](https://pkg.go.dev/badge/github.com/bubustack/bubu-sdk-go.svg)](https://pkg.go.dev/github.com/bubustack/bubu-sdk-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/bubustack/bubu-sdk-go)](https://goreportcard.com/report/github.com/bubustack/bubu-sdk-go)

`bubu-sdk-go` is the public Go SDK for building BubuStack components:

- **Engrams** for batch and streaming data processing
- **Impulses** for long-running external event listeners

This is the component boundary. Engrams and Impulses depend on this SDK, not on
`bobrapet` controller internals.

## Prerequisites

- Go 1.26.2 or newer (matching `go.mod`)
- Docker or another OCI-compatible image builder
- Access to a Kubernetes cluster supported by the current `bobrapet` release set

## Quick Links

- Go SDK docs: https://bubustack.io/docs/sdk/go-sdk
- Authoring guide: https://bubustack.io/docs/sdk/building-engrams
- Durable semantics: https://bubustack.io/docs/overview/durable-semantics
- Streaming contract: https://bubustack.io/docs/streaming/streaming-contract
- API reference: https://pkg.go.dev/github.com/bubustack/bubu-sdk-go

## What the SDK Handles

- Type-safe config, input, and secret binding
- StepRun status patching and structured errors
- Storage-ref hydration and large-payload offloading
- Streaming transport lifecycle, control directives, and replay-safe acknowledgements
- Trigger submission from Impulses via durable `StoryTrigger` requests
- Cross-process effect deduplication via `EffectClaim`
- Test harnesses and conformance suites for component authors

## Execution Modes

| Entry point | Use case | Kubernetes workload |
| --- | --- | --- |
| `sdk.StartBatch[C, I]` | Finite tasks with clear start/end | Job |
| `sdk.StartStreaming[C]` | Continuous processing with gRPC bidirectional streaming | Deployment |
| `sdk.RunImpulse[C]` | Long-running trigger services that submit durable `StoryTrigger` requests | Deployment |

`sdk.StartStory(...)` remains the helper Impulses use to trigger workflows, but
the latest contract no longer creates `StoryRun` objects directly. The SDK now:

1. submits a `StoryTrigger`
2. waits for controller resolution
3. returns the resolved `StoryRun`

## Quick Start

Create a minimal batch Engram:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	sdk "github.com/bubustack/bubu-sdk-go"
	"github.com/bubustack/bubu-sdk-go/engram"
)

type Config struct {
	DefaultMessage string `mapstructure:"defaultMessage"`
}

type Inputs struct {
	Name string `mapstructure:"name"`
}

type HelloEngram struct {
	message string
}

func (e *HelloEngram) Init(ctx context.Context, cfg Config, secrets *engram.Secrets) error {
	e.message = cfg.DefaultMessage
	if e.message == "" {
		e.message = "Hello"
	}
	return nil
}

func (e *HelloEngram) Process(ctx context.Context, execCtx *engram.ExecutionContext, inputs Inputs) (*engram.Result, error) {
	if inputs.Name == "" {
		return nil, fmt.Errorf("name is required")
	}
	return engram.NewResultFrom(map[string]any{
		"message": fmt.Sprintf("%s, %s!", e.message, inputs.Name),
	}), nil
}

func main() {
	if err := sdk.StartBatch(context.Background(), &HelloEngram{}); err != nil {
		log.Printf("engram failed: %v", err)
		os.Exit(sdk.BatchExitCode(err))
	}
}
```

Build it with:

```bash
go build ./...
```

Then follow the website guides for image build, `Engram` / `Story` manifests,
and deployment:

- https://bubustack.io/docs/sdk/go-sdk
- https://bubustack.io/docs/sdk/building-engrams

## Trigger Helpers for Impulses

The trigger helpers are intended for Impulses and other trusted automation
paths.

```go
ctx = sdk.WithTriggerToken(ctx, "source-event-id-123")
run, err := sdk.StartStory(ctx, "my-story", map[string]any{
	"event": "opened",
})
```

Available helpers:

- `sdk.StartStory(ctx, storyName, inputs)`
- `sdk.StartStoryInNamespace(ctx, storyName, namespace, inputs)`
- `sdk.StartStoryWithToken(ctx, storyName, token, inputs)`
- `sdk.StartStoryWithTokenInNamespace(ctx, storyName, namespace, token, inputs)`
- `sdk.StopStory(ctx, storyRunName)`
- `sdk.GetTargetStory()`

## Secrets

`engram.Secrets` is intentionally narrow. Prefer scoped accessors instead of
dumping the full secret map.

```go
apiKey, ok := secrets.Get("apiKey")
all := secrets.GetAll()              // returns a copy
names := secrets.Names()             // sorted key names
subset := secrets.Select("apiKey")   // bounded plaintext selection
```

Useful methods:

- `Get(key)`
- `GetAll()`
- `Names()`
- `Select(keys...)`

## Streaming Notes

- Streaming Engrams receive `engram.InboundMessage`, not raw `StreamMessage`.
- Call `msg.Done()` after successful processing or intentional drop.
- Structured JSON streaming outputs should keep the canonical JSON in
  `Payload` and mirror the same bytes into `Binary` with
  `MimeType: application/json`.
- Use raw `Binary` without `Payload` only for opaque media or non-JSON blobs.
- Startup now requires connector readiness metadata before the SDK starts the
  Engram stream loop.
- Startup capability negotiation uses strict latest-only
  `startup.capabilities=required|none` metadata.

See the full contract: https://bubustack.io/docs/streaming/streaming-contract

## Signals and Effects

- `sdk.EmitSignal(...)` records bounded progress/state data on the current StepRun.
- `sdk.RecordEffect(...)` appends an effect record to `StepRun.status.effects`.
- `sdk.ExecuteEffectOnce(...)` uses `EffectClaim` for cross-process reservation,
  renewal, recovery, and completion.

See:

- https://bubustack.io/docs/overview/durable-semantics
- https://bubustack.io/docs/api/effect-claims

## Testing

The SDK ships with:

- `testkit.BatchHarness`
- `testkit.StreamHarness`
- `conformance.BatchSuite`
- `conformance.StreamSuite`

Run the standard quality gates with:

```bash
make test
make lint
```

## Environment Variables

The operator injects runtime configuration for components. Do not hard-code the
env var set in downstream components; use the SDK and `core/contracts` as the
source of truth.

Common groups:

- **Execution context**
  - `BUBU_STORY_NAME`
  - `BUBU_STORYRUN_ID`
  - `BUBU_STEP_NAME`
  - `BUBU_STEPRUN_NAME`
  - `BUBU_STEP_TIMEOUT`
  - `BUBU_MAX_INLINE_SIZE`
- **Config and templating**
  - `BUBU_TRIGGER_DATA`
  - `BUBU_STEP_CONFIG`
  - `BUBU_TEMPLATE_CONTEXT`
- **Transport**
  - `BUBU_TRANSPORT_BINDING`
  - `BUBU_GRPC_PORT`
  - `BUBU_GRPC_CHANNEL_BUFFER_SIZE`
  - `BUBU_GRPC_CHANNEL_SEND_TIMEOUT`
  - `BUBU_GRPC_MESSAGE_TIMEOUT`
  - `BUBU_GRPC_HANG_TIMEOUT`
  - `BUBU_GRPC_GRACEFUL_SHUTDOWN_TIMEOUT`
  - `BUBU_GRPC_RECONNECT_MAX_RETRIES`
- **SDK observability**
  - `BUBU_SDK_METRICS_ENABLED`
  - `BUBU_SDK_TRACING_ENABLED`
- **Kubernetes client**
  - `BUBU_K8S_USER_AGENT`
  - `BUBU_K8S_TIMEOUT`
  - `BUBU_K8S_OPERATION_TIMEOUT`
  - `BUBU_K8S_PATCH_MAX_RETRIES`
- **Effects and signals**
  - `BUBU_EFFECT_MAX_DETAILS_BYTES`
  - `BUBU_SIGNAL_MAX_PAYLOAD_BYTES`

Use the website docs for the curated reference:

- https://bubustack.io/docs/sdk/go-sdk
- https://bubustack.io/docs/streaming/transport-settings

## Local Development

```bash
git clone https://github.com/bubustack/bubu-sdk-go.git
cd bubu-sdk-go
make test
make lint
```

## Support, Security, and Changelog

- Support: [SUPPORT.md](./SUPPORT.md)
- Security: [SECURITY.md](./SECURITY.md)
- Changelog: [CHANGELOG.md](./CHANGELOG.md)

## License

Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0.
