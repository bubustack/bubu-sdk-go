// Package transport provides the different mechanisms (Kubernetes API, gRPC, etc.)
// that the SDK runtime uses to communicate with the outside world.
package transport

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

// GRPCTarget is a transport-level representation of a downstream target.
type GRPCTarget struct {
	Endpoint string
}

// GRPCClient provides the client-side logic for an Engram to communicate with
// other Engrams via gRPC. It's used by the GRPCServer to fan-out results and
// can be used to establish peer-to-peer streaming connections.
type GRPCClient struct {
	logger *slog.Logger
}

// NewGRPCClient creates a new gRPC client.
func NewGRPCClient() *GRPCClient {
	return &GRPCClient{logger: slog.New(slog.NewJSONHandler(os.Stderr, nil))}
}

// Execute is a convenience wrapper that calls the parallel fan-out logic.
func (c *GRPCClient) Execute(ctx context.Context, targets []GRPCTarget, execCtx *engram.ExecutionContext) (map[string]*engram.Result, error) {
	return c.ExecuteParallel(ctx, targets, execCtx)
}

// ExecuteEngram sends a unary execution request to a single downstream Engram service.
func (c *GRPCClient) ExecuteEngram(ctx context.Context, endpoint string, execCtx *engram.ExecutionContext) (*engram.Result, error) {
	c.logger.Info("Executing downstream Engram", "endpoint", endpoint)
	// For inter-cluster communication, mTLS or other secure credentials should be used.
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client for endpoint '%s': %w", endpoint, err)
	}
	defer conn.Close()

	client := proto.NewEngramServiceClient(conn)
	protoReq, err := c.executionContextToProto(execCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to convert execution context to proto: %w", err)
	}

	resp, err := client.Execute(ctx, protoReq)
	if err != nil {
		return nil, fmt.Errorf("gRPC call to %s failed: %w", endpoint, err)
	}

	return c.protoToResult(resp)
}

// ExecuteStream establishes a long-lived, bidirectional stream with a target Engram.
func (c *GRPCClient) ExecuteStream(ctx context.Context, endpoint string) (proto.EngramService_ExecuteStreamClient, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client for stream to '%s': %w", endpoint, err)
	}
	// Note: The connection is not closed here as the stream is intended to be long-lived.
	// The caller (e.g., StreamFanOutSession) is responsible for managing the stream's lifecycle.
	client := proto.NewEngramServiceClient(conn)
	return client.ExecuteStream(ctx)
}

// SendStreamMessage marshals and sends a message over an active gRPC stream.
func (c *GRPCClient) SendStreamMessage(stream proto.EngramService_ExecuteStreamClient, msg *engram.StreamMessage) error {
	protoMsg, err := c.messageToProto(msg)
	if err != nil {
		return fmt.Errorf("failed to convert message to proto: %w", err)
	}
	return stream.Send(protoMsg)
}

// ReceiveStreamMessage blocks, receives a message from an active gRPC stream, and unmarshals it.
func (c *GRPCClient) ReceiveStreamMessage(stream proto.EngramService_ExecuteStreamClient) (*engram.StreamMessage, error) {
	protoMsg, err := stream.Recv()
	if err != nil {
		return nil, err // The error could be io.EOF if the stream is closed.
	}
	return c.protoToMessage(protoMsg)
}

// --- Private Proto Conversion Helpers ---

func (c *GRPCClient) executionContextToProto(execCtx *engram.ExecutionContext) (*proto.ExecuteRequest, error) {
	if execCtx == nil {
		// It's valid to have an empty context for downstream calls that don't pass data.
		return &proto.ExecuteRequest{}, nil
	}
	inputs, err := structpb.NewStruct(execCtx.Inputs())
	if err != nil {
		return nil, fmt.Errorf("failed to convert inputs to proto struct: %w", err)
	}
	return &proto.ExecuteRequest{
		Context: &proto.ExecutionContext{
			StoryRunId: execCtx.StoryInfo().StoryRunID,
			Inputs:     inputs,
		},
	}, nil
}

func (c *GRPCClient) protoToResult(resp *proto.ExecuteResponse) (*engram.Result, error) {
	var resErr error
	if resp.Error != "" {
		resErr = fmt.Errorf("%s", resp.Error)
	}
	return &engram.Result{
		Data:  resp.Data.AsMap(),
		Error: resErr,
	}, nil
}

func (c *GRPCClient) messageToProto(msg *engram.StreamMessage) (*proto.StreamMessage, error) {
	payload, err := structpb.NewStruct(msg.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to convert message payload to proto struct: %w", err)
	}
	// The current developer-facing Message only has a Payload. The proto has more
	// fields like SessionId and Type, which are unused for now but provide future
	// flexibility for more advanced streaming protocols.
	return &proto.StreamMessage{
		Payload: payload,
	}, nil
}

func (c *GRPCClient) protoToMessage(protoMsg *proto.StreamMessage) (*engram.StreamMessage, error) {
	return &engram.StreamMessage{
		Payload: protoMsg.Payload.AsMap(),
	}, nil
}
