# Bubu SDK for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/bubustack/bubu-sdk-go.svg)](https://pkg.go.dev/github.com/bubustack/bubu-sdk-go)

The official Go SDK for building type-safe `Engrams` and `Impulses` for the [bobrapet](https://github.com/bubustack/bobrapet) Kubernetes-native AI workflow engine.

This SDK provides a generic, type-safe interface that abstracts Kubernetes internals and JSON unmarshaling, letting you focus on application logic.

## Features

- **Type-Safe & Generic**: Define your own structs for configuration and inputs. The SDK handles unmarshaling.
- **Clean Interfaces**: Simple interfaces for `BatchEngrams`, `Impulses`, and `StreamingEngrams`.
- **Transparent Storage Offloading**: Automatically offload large inputs/outputs to S3 or filesystem and hydrate on read.
- **Kubernetes Client**: Minimal client wrapper with in-cluster and kubeconfig fallback for local dev.

## Core Interfaces

### Engram
```go
type Engram[C any] interface {
	Init(ctx context.Context, config C, secrets *Secrets) error
}
```

### BatchEngram
```go
type BatchEngram[C any, I any] interface {
	Engram[C]
	Process(ctx context.Context, execCtx *ExecutionContext, inputs I) (*Result, error)
}
```

### Impulse
```go
type Impulse[C any] interface {
	Engram[C]
	Run(ctx context.Context, client *k8s.Client) error
}
```

### StreamingEngram
```go
type StreamingEngram[C any] interface {
	Engram[C]
	Stream(ctx context.Context, in <-chan []byte, out chan<- []byte) error
}
```

## Quickstart: Batch Engram

1) Define types
```go
// config and input types
 type GreeterConfig struct{}
 type GreeterInputs struct { Name string `json:"name"` }
```

2) Implement
```go
 type GreeterEngram struct{}
 func (g *GreeterEngram) Init(ctx context.Context, cfg GreeterConfig, _ *engram.Secrets) error { return nil }
 func (g *GreeterEngram) Process(ctx context.Context, ec *engram.ExecutionContext, in GreeterInputs) (*engram.Result, error) {
 	if in.Name == "" { return nil, fmt.Errorf("name required") }
 	return &engram.Result{ Data: map[string]any{"greeting": "Hello, "+in.Name+"!"} }, nil
 }
```

3) Main
```go
func main() {
	if err := sdk.Run[GreeterConfig, GreeterInputs](context.Background(), &GreeterEngram{}); err != nil {
		log.Fatal(err)
	}
}
```

## Quickstart: Impulse

1) Define config
```go
type HttpImpulseConfig struct { Port string `json:"port"` }
```

2) Implement
```go
type HttpImpulse struct{ port string }
func (h *HttpImpulse) Init(ctx context.Context, cfg HttpImpulseConfig, _ *engram.Secrets) error {
	if cfg.Port == "" { cfg.Port = ":8080" }
	h.port = cfg.Port
	return nil
}
func (h *HttpImpulse) Run(ctx context.Context, c *k8s.Client) error {
	// ... use c.TriggerStory(...)
	return http.ListenAndServe(h.port, nil)
}
```

3) Main
```go
func main() {
	if err := sdk.RunImpulse[HttpImpulseConfig](context.Background(), &HttpImpulse{}); err != nil {
		log.Fatal(err)
	}
}
```

## Quickstart: Streaming

```go
func main() {
	if err := sdk.StartStreamServer[StreamConfig](context.Background(), &MyStreamingEngram{}); err != nil {
		log.Fatal(err)
	}
}
```

## Environment

- **Execution**
  - `BOBRAPET_EXECUTION_MODE`: `batch` (default) or `streaming`.
  - `BUBU_STORY_NAME`, `BUBU_STORYRUN_ID`, `BUBU_STEP_NAME`, `BUBU_STEPRUN_NAME` set by controller.
  - `BUBU_STARTED_AT`: RFC3339 start time (optional).
- **Inputs/Config/Secrets**
  - `BUBU_INPUTS`: JSON of resolved inputs.
  - `BUBU_CONFIG_*`: key-value entries merged into config map.
  - `BUBU_SECRET_*`: key-value secrets made available via `engram.Secrets`.
  - `BUBU_IMPULSE_WITH`: JSON to merge into impulse config.
- **Streaming**
  - `BUBU_GRPC_PORT`: gRPC server port (default 8080).
- **Storage**
  - `BUBU_STORAGE_PROVIDER`: `s3` | `file` (unset disables offloading)
  - `BUBU_STORAGE_PATH`: base path for `file` provider
  - `BUBU_MAX_INLINE_SIZE`: bytes threshold (default 32768)
  - `BUBU_STORAGE_S3_BUCKET`, `BUBU_STORAGE_S3_REGION`, `BUBU_STORAGE_S3_ENDPOINT`
  - Standard AWS auth env vars are supported via AWS SDK
- **Namespace**
  - `BUBU_STEPRUN_NAMESPACE` or `BUBU_POD_NAMESPACE` preferred; fallback `POD_NAMESPACE` then `default`.
- **Kubernetes client (local dev)**
  - In-cluster config first; fallback to `KUBECONFIG` or `~/.kube/config`.

## Storage Offloading
- Values larger than the inline threshold are wrapped and stored via the configured provider.
- At runtime, inputs are hydrated before processing; outputs are dehydrated and a reference is placed in `StepRun.status.output`.

## Status Reporting
- The SDK patches `StepRun.status` with:
  - `phase` (`Succeeded`/`Failed`),
  - `output` (inline JSON or storage reference),
  - `finishedAt`, `duration`, and `lastFailureMsg` on error.

## Notes
- Use `sdk.Run[C,I]` for batch engrams. `sdk.Start` only supports streaming mode.
- `StreamTo` uses `grpc.DialContext` and protobuf JSON for payloads.

