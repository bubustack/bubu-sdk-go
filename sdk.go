package sdk

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	bobravozgrpcprotov1alpha1 "github.com/bubustack/bobravoz-grpc-proto"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/runtime"
	"github.com/bubustack/bubu-sdk-go/storage"
)

// === Batch Execution ===

// Run is the main entry point for a batch-based Engram.
// It initializes the SDK runtime and handles the lifecycle of a job-based execution.
func Run(ctx context.Context, e engram.BatchEngram) error {
	fmt.Println("Initializing Bubu SDK for batch execution...")
	r, err := runtime.New(e)
	if err != nil {
		return fmt.Errorf("failed to initialize SDK runtime: %w", err)
	}
	return r.Execute(ctx)
}

// === Streaming Execution ===

// StreamHandler is the function signature that Engram developers will implement
// to handle real-time data streams. It receives the input stream and can write
// to the output stream.
type StreamHandler func(ctx context.Context, in <-chan []byte, out chan<- []byte) error

// server is the gRPC server implementation for the SDK sidecar.
type server struct {
	bobravozgrpcprotov1alpha1.UnimplementedBobravozGRPCServiceServer
	handler StreamHandler
}

// Stream is the gRPC bidirectional streaming endpoint.
func (s *server) Stream(stream bobravozgrpcprotov1alpha1.BobravozGRPCService_StreamServer) error {
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
			in <- req.Data
		}
	}()

	// Goroutine to read from the handler's output channel and send to the gRPC stream.
	go func() {
		for data := range out {
			if err := stream.Send(&bobravozgrpcprotov1alpha1.StreamRequest{Data: data}); err != nil {
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

// StartStreamServer starts the gRPC server for the SDK sidecar.
// It listens on the port specified by the GRPC_PORT environment variable.
func StartStreamServer(handler StreamHandler) error {
	port := os.Getenv("GRPC_PORT")
	if port == "" {
		port = "8080"
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s := grpc.NewServer()
	bobravozgrpcprotov1alpha1.RegisterBobravozGRPCServiceServer(s, &server{handler: handler})

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

	client := bobravozgrpcprotov1alpha1.NewBobravozGRPCClient(conn)
	stream, err := client.Stream(ctx)
	if err != nil {
		return fmt.Errorf("could not open stream: %w", err)
	}

	// Goroutine to send data from the input channel to the gRPC stream.
	go func() {
		for data := range in {
			if err := stream.Send(&bobravozgrpcprotov1alpha1.StreamResponse{Data: data}); err != nil {
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
	}
}

// BubuSDK provides an interface to the BubuStack control plane and storage.
// This is the main entrypoint for Engram developers.
type BubuSDK struct {
	storageManager *storage.StorageManager
}

// New returns a new instance of the BubuSDK.
func New() (*BubuSDK, error) {
	storageManager, err := storage.NewManager(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage manager: %w", err)
	}

	return &BubuSDK{
		storageManager: storageManager,
	}, nil
}
