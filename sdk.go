// package sdk provides the primary entry points for executing bobrapet components.
// It contains the runtime logic that bootstraps an Engram or Impulse, injects
// the necessary context from the environment, and manages its lifecycle.
// Developers will typically only interact with the `Run`, `RunImpulse`, or
// `StartStreamServer` functions in this package from their `main.go` file.
package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"log/slog"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	bobravozgrpcproto "github.com/bubustack/bobravoz-grpc/proto"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/bubustack/bubu-sdk-go/runtime"
	"github.com/bubustack/bubu-sdk-go/storage"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
)

// === Batch Execution ===

func asStreamingEngram[C any](e engram.Engram[C]) (engram.StreamingEngram[C], bool) {
	se, ok := e.(engram.StreamingEngram[C])
	return se, ok
}

func asBatchEngram[C any](e engram.Engram[C]) (engram.BatchEngram[C, any], bool) {
	be, ok := e.(engram.BatchEngram[C, any])
	return be, ok
}

// Start is a new, simplified entry point that handles both batch and streaming modes.
func Start[C any](ctx context.Context, engram engram.Engram[C]) error {
	mode := getExecutionMode()
	fmt.Printf("Starting in %s mode\n", mode)

	switch mode {
	case "streaming":
		streamingEngram, ok := asStreamingEngram(engram)
		if !ok {
			return fmt.Errorf("engram does not support streaming mode, but execution mode is 'streaming'")
		}
		// Assuming a generic StartStreamServer exists or is created.
		// This will require refactoring StartStreamServer to not be generic or to handle 'any'.
		return StartStreamServer(ctx, streamingEngram)
	default: // "batch" is the default
		batchEngram, ok := asBatchEngram(engram)
		if !ok {
			return fmt.Errorf("engram does not support batch mode, but execution mode is 'batch'")
		}
		return runBatch(ctx, batchEngram)
	}
}

// Run is the main entry point for BatchEngrams, which are designed to execute
// as short-lived, transactional tasks (typically Kubernetes Jobs).
//
// This function orchestrates the entire lifecycle of a batch execution:
//  1. It loads the execution context (config, secrets, inputs) from environment variables.
//  2. It initializes the storage manager for transparent data offloading.
//  3. It hydrates inputs, resolving any references to data in external storage.
//  4. It unmarshals the configuration and inputs into the developer-defined generic types.
//  5. It calls the Engram's `Init` and `Process` methods.
//  6. It captures the result, dehydrates the output if necessary, and patches the
//     StepRun resource in Kubernetes with the final status (`Succeeded` or `Failed`).
func Run[C any, I any](ctx context.Context, e engram.BatchEngram[C, I]) error {
	execCtxData, err := runtime.LoadExecutionContextData()
	if err != nil {
		return fmt.Errorf("failed to load execution context: %w", err)
	}

	// Unmarshal config.
	config, err := runtime.UnmarshalFromMap[C](execCtxData.Config)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}
	secrets := engram.NewSecrets(execCtxData.Secrets)

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	tracer := otel.Tracer("bubu-sdk")
	execCtx := engram.NewExecutionContext(logger, tracer, execCtxData.StoryInfo)

	if err := e.Init(ctx, config, secrets); err != nil {
		return fmt.Errorf("engram initialization failed: %w", err)
	}

	sm, err := storage.NewManager(ctx)
	if err != nil {
		return fmt.Errorf("failed to create storage manager: %w", err)
	}

	// Hydrate inputs to resolve any storage references before unmarshaling.
	hydratedInputs, err := sm.Hydrate(ctx, execCtxData.Inputs)
	if err != nil {
		return fmt.Errorf("failed to hydrate inputs: %w", err)
	}

	// Convert inputs to the target input type I.
	var typedInputs I
	switch v := hydratedInputs.(type) {
	case map[string]interface{}:
		typedInputs, err = runtime.UnmarshalFromMap[I](v)
		if err != nil {
			return fmt.Errorf("failed to unmarshal inputs into target type: %w", err)
		}
	default:
		// If the inputs are not a map, we try JSON roundtrip into the target type.
		bytes, marshalErr := json.Marshal(v)
		if marshalErr != nil {
			return fmt.Errorf("failed to marshal inputs for conversion: %w", marshalErr)
		}
		if unmarshalErr := json.Unmarshal(bytes, &typedInputs); unmarshalErr != nil {
			return fmt.Errorf("failed to unmarshal inputs into target type: %w", unmarshalErr)
		}
	}

	result, err := e.Process(ctx, execCtx, typedInputs)
	if err != nil {
		result = &engram.Result{Error: err}
		patchErr := patchStepRunStatus(ctx, sm, execCtxData, result, "Failed")
		return fmt.Errorf("engram processing failed: %w (patch error: %v)", err, patchErr)
	}
	if result.Error != nil {
		patchErr := patchStepRunStatus(ctx, sm, execCtxData, result, "Failed")
		if patchErr != nil {
			return fmt.Errorf("engram returned an error: %w (patch error: %v)", result.Error, patchErr)
		}
		return fmt.Errorf("engram returned an error: %w", result.Error)
	}

	return patchStepRunStatus(ctx, sm, execCtxData, result, "Succeeded")
}

// runBatch is a new internal function to handle batch execution with dynamic inputs.
func runBatch[C any](ctx context.Context, e engram.BatchEngram[C, any]) error {
	// Simplified initialization, bypassing the complex generic runtime for now.
	// This would evolve into a more complete non-generic runtime.
	execCtxData, err := runtime.LoadExecutionContextData()
	if err != nil {
		return fmt.Errorf("failed to load execution context: %w", err)
	}
	config, err := runtime.UnmarshalFromMap[C](execCtxData.Config) // Using 'C' for config
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}
	secrets := engram.NewSecrets(execCtxData.Secrets)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	tracer := otel.Tracer("bubu-sdk")
	execCtx := engram.NewExecutionContext(logger, tracer, execCtxData.StoryInfo)

	if err := e.Init(ctx, config, secrets); err != nil {
		return fmt.Errorf("engram initialization failed: %w", err)
	}

	sm, err := storage.NewManager(ctx)
	if err != nil {
		return fmt.Errorf("failed to create storage manager: %w", err)
	}

	// Use inputs from execution context, then hydrate to resolve any storage references.
	hydratedInputs, err := sm.Hydrate(ctx, execCtxData.Inputs)
	if err != nil {
		return fmt.Errorf("failed to hydrate inputs: %w", err)
	}

	result, err := e.Process(ctx, execCtx, hydratedInputs)
	if err != nil {
		// If the process function itself errors, we create a synthetic result
		// to pass to the patch function.
		result = &engram.Result{Error: err}
		patchErr := patchStepRunStatus(ctx, sm, execCtxData, result, enums.PhaseFailed)
		return fmt.Errorf("engram processing failed: %w (patch error: %v)", err, patchErr)
	}
	if result.Error != nil {
		patchErr := patchStepRunStatus(ctx, sm, execCtxData, result, enums.PhaseFailed)
		if patchErr != nil {
			return fmt.Errorf("engram returned an error: %w (patch error: %v)", result.Error, patchErr)
		}
		return fmt.Errorf("engram returned an error: %w", result.Error)
	}

	return patchStepRunStatus(ctx, sm, execCtxData, result, enums.PhaseSucceeded)
}

func patchStepRunStatus(ctx context.Context, sm *storage.StorageManager, execCtxData *runtime.ExecutionContextData, result *engram.Result, phase enums.Phase) error {
	// Handle the output: dehydrate if necessary and patch the StepRun status.
	output, err := sm.Dehydrate(ctx, result.Data, execCtxData.StoryInfo.StepRunID)
	if err != nil {
		return fmt.Errorf("failed to dehydrate output: %w", err)
	}

	outputBytes, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("failed to marshal output: %w", err)
	}

	k8sClient, err := k8s.NewClient()
	if err != nil {
		return fmt.Errorf("failed to create k8s client: %w", err)
	}

	finishedAt := metav1.Now()
	newStatus := runsv1alpha1.StepRunStatus{
		Phase:      phase,
		Output:     &k8sruntime.RawExtension{Raw: outputBytes},
		FinishedAt: &finishedAt,
		Duration:   finishedAt.Sub(execCtxData.StartedAt.Time).String(),
	}
	if result.Error != nil {
		newStatus.LastFailureMsg = result.Error.Error()
	}

	return k8sClient.PatchStepRunStatus(ctx, execCtxData.StoryInfo.StepRunID, newStatus)
}

// getExecutionMode determines the execution mode from an environment variable.
func getExecutionMode() string {
	// Align with controller: BOBRAPET_EXECUTION_MODE is set by bobrapet.
	mode := os.Getenv("BOBRAPET_EXECUTION_MODE")
	if mode == "" {
		return "batch" // Default mode
	}
	return mode
}

// === Story Helpers ===

// StartStory triggers a new StoryRun for a given Story. This is the primary
// mechanism for programmatically initiating workflows, typically used from within
// an Impulse.
//
// The SDK's Kubernetes client automatically resolves the correct namespace,
// allowing the caller to simply provide the name of the Story to run.
func StartStory(ctx context.Context, storyName string, inputs map[string]interface{}) (*runsv1alpha1.StoryRun, error) {
	k8sClient, err := k8s.NewClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %w", err)
	}
	return k8sClient.TriggerStory(ctx, storyName, inputs)
}

// StartStoryRun is an alias of StartStory for API clarity.
func StartStoryRun(ctx context.Context, storyName string, inputs map[string]interface{}) (*runsv1alpha1.StoryRun, error) {
	return StartStory(ctx, storyName, inputs)
}

// === Impulse Execution ===

// RunImpulse is the main entry point for an Impulse, which is a long-running
// service designed to listen for external events and trigger StoryRuns.
//
// This function orchestrates the initialization of an Impulse:
//  1. It loads the execution context, focusing on configuration and secrets.
//  2. It unmarshals the configuration into the developer-defined generic type.
//  3. It calls the Impulse's `Init` method for one-time setup.
//  4. It provides a pre-configured Kubernetes client to the `Run` method.
//  5. It then transfers control to the Impulse's `Run` method, which is expected
//     to block and manage the long-running process.
func RunImpulse[C any](ctx context.Context, i engram.Impulse[C]) error {
	fmt.Println("Initializing Bubu SDK for Impulse execution...")

	execCtxData, err := runtime.LoadExecutionContextData()
	if err != nil {
		return fmt.Errorf("failed to load execution context: %w", err)
	}

	// If provided, merge BUBU_IMPULSE_WITH JSON into config before unmarshaling.
	if withStr := os.Getenv("BUBU_IMPULSE_WITH"); withStr != "" {
		var withMap map[string]interface{}
		if err := json.Unmarshal([]byte(withStr), &withMap); err == nil {
			for k, v := range withMap {
				execCtxData.Config[k] = v
			}
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

	fmt.Println("Starting Impulse...")
	return i.Run(ctx, k8sClient)
}

// === Streaming Execution ===

// server is the gRPC server implementation for the SDK sidecar.
type server struct {
	bobravozgrpcproto.UnimplementedHubServer
	handler func(ctx context.Context, in <-chan []byte, out chan<- []byte) error
}

// Process is the gRPC bidirectional streaming endpoint.
func (s *server) Process(stream bobravozgrpcproto.Hub_ProcessServer) error {
	in := make(chan []byte)
	out := make(chan []byte)
	errChan := make(chan error, 1)

	// Goroutine to read from the gRPC stream and send to the handler's input channel.
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				close(in)
				return
			}
			// Marshal the payload into JSON bytes for the handler.
			payloadBytes, err := protojson.Marshal(req.Payload)
			if err != nil {
				// We can't easily propagate this error back, so we log it.
				log.Printf("Error marshaling payload from gRPC stream: %v", err)
				continue
			}
			in <- payloadBytes
		}
	}()

	// Goroutine to read from the handler's output channel and send to the gRPC stream.
	go func() {
		for data := range out {
			// Unmarshal the JSON bytes from the handler into a payload.
			payload := &structpb.Struct{}
			if err := protojson.Unmarshal(data, payload); err != nil {
				log.Printf("Error unmarshaling payload to gRPC stream: %v", err)
				continue
			}
			if err := stream.Send(&bobravozgrpcproto.DataPacket{Payload: payload}); err != nil {
				// We can't easily propagate this error back, so we log it.
				log.Printf("Error sending data to gRPC stream: %v", err)
			}
		}
	}()

	// Run the user-defined handler.
	go func() {
		errChan <- s.handler(stream.Context(), in, out)
		close(out)
	}()

	return <-errChan
}

// StartStreamServer is the main entry point for a StreamingEngram. This function
// bootstraps a long-running service that can process data in real-time over gRPC.
//
// This function orchestrates the lifecycle of a streaming service:
// 1. It loads the execution context for configuration and secrets.
// 2. It calls the StreamingEngram's `Init` method.
// 3. It starts a gRPC server on the configured port.
// 4. It registers the StreamingEngram's `Stream` method as the gRPC handler.
// 5. It gracefully handles server shutdown on context cancellation.
func StartStreamServer[C any](ctx context.Context, e engram.StreamingEngram[C]) error {
	fmt.Println("Initializing Bubu SDK for streaming execution...")

	execCtxData, err := runtime.LoadExecutionContextData()
	if err != nil {
		return fmt.Errorf("failed to load execution context: %w", err)
	}

	// Unmarshal config.
	config, err := runtime.UnmarshalFromMap[C](execCtxData.Config)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}
	secrets := engram.NewSecrets(execCtxData.Secrets)

	if err := e.Init(ctx, config, secrets); err != nil {
		return fmt.Errorf("streaming engram initialization failed: %w", err)
	}

	// Controller sets BUBU_GRPC_PORT
	port := os.Getenv("BUBU_GRPC_PORT")
	if port == "" {
		port = "8080"
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s := grpc.NewServer()
	bobravozgrpcproto.RegisterHubServer(s, &server{handler: e.Stream})

	log.Printf("gRPC server listening at %v", lis.Addr())
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- s.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		s.GracefulStop()
		return ctx.Err()
	case err := <-serveErr:
		return err
	}
}

// StreamTo connects to a downstream gRPC server and streams data to it.
// This is the client side of the SDK.
func StreamTo(ctx context.Context, target string, in <-chan []byte) error {
	conn, err := grpc.DialContext(ctx, target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("did not connect: %w", err)
	}
	defer conn.Close()

	client := bobravozgrpcproto.NewHubClient(conn)
	stream, err := client.Process(ctx)
	if err != nil {
		return fmt.Errorf("could not open stream: %w", err)
	}

	// Goroutine to send data from the input channel to the gRPC stream.
	go func() {
		for data := range in {
			// Unmarshal the JSON bytes from the handler into a payload.
			payload := &structpb.Struct{}
			if err := protojson.Unmarshal(data, payload); err != nil {
				log.Printf("Error unmarshaling payload to gRPC stream: %v", err)
				continue
			}
			if err := stream.Send(&bobravozgrpcproto.DataPacket{Payload: payload}); err != nil {
				log.Printf("Error sending data on stream: %v", err)
			}
		}
		stream.CloseSend()
	}()

	// Wait for the server to close the stream from its side.
	for {
		_, err := stream.Recv()
		if err != nil {
			// This is the expected exit path when the stream is closed.
			return nil
		}
		// We expect data back, so we should handle it.
		// Marshal the payload into JSON bytes for the handler.
		// Note: This part of the code is for the client side (StreamTo).
		// Depending on the logic, you might want to forward this to another channel.
		// For now, we'll just log it.
		// payloadBytes, err := protojson.Marshal(res.Payload)
		// if err != nil {
		// 	log.Printf("Error marshaling payload from gRPC stream: %v", err)
		// 	continue
		// }
		// log.Printf("Received data from server: %s", string(payloadBytes))

	}
}
