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

	bobravozgrpcproto "github.com/bubustack/bobravoz-grpc/proto"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/impulse"
	"github.com/bubustack/bubu-sdk-go/runtime"
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
	return r.Execute(ctx)
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
