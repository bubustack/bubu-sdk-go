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

	bobravozgrpcproto "github.com/bubustack/bobravoz-grpc/proto"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/impulse"
	"github.com/bubustack/bubu-sdk-go/runtime"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

// === Batch Execution ===

// Run is the main entry point for a batch-based Engram.
// It initializes the SDK runtime and handles the lifecycle of a job-based execution.
func Run[C any, I any](ctx context.Context, e engram.BatchEngram[C, I]) error {
	fmt.Println("Initializing Bubu SDK for batch execution...")
	r, err := runtime.New[C, I](e)
	if err != nil {
		return fmt.Errorf("failed to initialize SDK runtime: %w", err)
	}

	// This is the new input handling logic.
	// It's kept separate from the runtime for now to support both dynamic and static inputs.
	inputs := make(map[string]interface{})
	if inputsStr := os.Getenv("BUBU_INPUTS"); inputsStr != "" {
		if err := json.Unmarshal([]byte(inputsStr), &inputs); err != nil {
			return fmt.Errorf("failed to unmarshal BUBU_INPUTS: %w", err)
		}
	}

	// The runtime's Execute method will need to be updated to accept the inputs.
	// For now, we'll assume a conceptual ExecuteWithInputs method exists.
	// This will be part of the larger SDK refactor.
	return r.Execute(ctx, inputs)
}

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
			return fmt.Errorf("engram does not support batch mode")
		}
		// This Run function will be a simplified version for dynamic inputs.
		return runBatch(ctx, batchEngram)
	}
}

// runBatch is a new internal function to handle batch execution with dynamic inputs.
func runBatch[C any](ctx context.Context, e engram.BatchEngram[C, any]) error {
	// Simplified initialization, bypassing the complex generic runtime for now.
	// This would evolve into a more complete non-generic runtime.
	execCtxData, err := runtime.LoadExecutionContextData("/var/run/bubu/context.json")
	if err != nil {
		return fmt.Errorf("failed to load execution context: %w", err)
	}
	config, err := unmarshalFromMap[C](execCtxData.Config) // Using 'C' for config
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

	inputs := make(map[string]interface{})
	if inputsStr := os.Getenv("BUBU_INPUTS"); inputsStr != "" {
		if err := json.Unmarshal([]byte(inputsStr), &inputs); err != nil {
			return fmt.Errorf("failed to unmarshal BUBU_INPUTS: %w", err)
		}
	}

	result, err := e.Process(ctx, execCtx, inputs)
	if err != nil {
		return fmt.Errorf("engram processing failed: %w", err)
	}
	if result.Error != nil {
		return fmt.Errorf("engram returned an error: %w", result.Error)
	}

	// Omitting output handling for brevity, but it would be here.
	return nil
}

// getExecutionMode determines the execution mode from an environment variable.
func getExecutionMode() string {
	mode := os.Getenv("BUBU_EXECUTION_MODE")
	if mode == "" {
		return "batch" // Default mode
	}
	return mode
}

// === Impulse Execution ===

// RunImpulse is the main entry point for an Impulse.
// It initializes the SDK runtime and handles the lifecycle of a long-running
// event listener.
func RunImpulse[C any](ctx context.Context, i engram.Impulse[C]) error {
	fmt.Println("Initializing Bubu SDK for Impulse execution...")

	execCtxData, err := runtime.LoadExecutionContextData("/var/run/bubu/context.json")
	if err != nil {
		return fmt.Errorf("failed to load execution context: %w", err)
	}

	// Unmarshal config.
	config, err := unmarshalFromMap[C](execCtxData.Config)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}
	secrets := engram.NewSecrets(execCtxData.Secrets)

	if err := i.Init(ctx, config, secrets); err != nil {
		return fmt.Errorf("impulse initialization failed: %w", err)
	}

	if execCtxData.StoryRef == nil || execCtxData.StoryRef.Name == "" {
		return fmt.Errorf("impulse context is missing required 'storyRef.name' field")
	}
	storyName := execCtxData.StoryRef.Name

	impulseClient, err := impulse.NewClient(storyName)
	if err != nil {
		return fmt.Errorf("failed to create impulse client: %w", err)
	}

	fmt.Println("Starting Impulse...")
	return i.Run(ctx, impulseClient)
}

// unmarshalFromMap is a helper to convert a map[string]interface{} to a struct
// by going through JSON.
func unmarshalFromMap[T any](data map[string]interface{}) (T, error) {
	var target T
	bytes, err := json.Marshal(data)
	if err != nil {
		return target, fmt.Errorf("failed to marshal map for unmarshaling: %w", err)
	}
	if err := json.Unmarshal(bytes, &target); err != nil {
		return target, fmt.Errorf("failed to unmarshal into target struct: %w", err)
	}
	return target, nil
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

// StartStreamServer is the main entry point for a streaming Engram.
// It initializes the SDK runtime and handles the lifecycle of a long-running service.
func StartStreamServer[C any](ctx context.Context, e engram.StreamingEngram[C]) error {
	fmt.Println("Initializing Bubu SDK for streaming execution...")

	execCtxData, err := runtime.LoadExecutionContextData("/var/run/bubu/context.json")
	if err != nil {
		return fmt.Errorf("failed to load execution context: %w", err)
	}

	// Unmarshal config.
	config, err := unmarshalFromMap[C](execCtxData.Config)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}
	secrets := engram.NewSecrets(execCtxData.Secrets)

	if err := e.Init(ctx, config, secrets); err != nil {
		return fmt.Errorf("streaming engram initialization failed: %w", err)
	}

	port := os.Getenv("GRPC_PORT")
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
	return s.Serve(lis)
}

// StreamTo connects to a downstream gRPC server and streams data to it.
// This is the client side of the SDK.
func StreamTo(ctx context.Context, target string, in <-chan []byte) error {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
