// Package transport provides the different mechanisms (Kubernetes API, gRPC, etc.)
// that the SDK runtime uses to communicate with the outside world.
package transport

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// ExecutionHandler is an internal interface used by the GRPCServer to delegate
// the core logic of handling an execution request back to the runtime.
type ExecutionHandler interface {
	// Execute is called when a unary gRPC request is received.
	Execute(ctx context.Context, execCtx *engram.ExecutionContext) (*engram.Result, error)
}

// GRPCServer implements the gRPC server for a StreamEngram. It listens for
// incoming connections from other Engrams or from the Bobrapet control plane.
type GRPCServer struct {
	proto.UnimplementedEngramServiceServer
	k8sTransport *KubernetesTransport
	handler      ExecutionHandler
}

// NewGRPCServer creates a new gRPC server for the Engram.
func NewGRPCServer(k8sTransport *KubernetesTransport, handler ExecutionHandler) *GRPCServer {
	return &GRPCServer{
		k8sTransport: k8sTransport,
		handler:      handler,
	}
}

// ListenAndServe starts the gRPC server and begins listening for requests.
func (s *GRPCServer) ListenAndServe(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on tcp address '%s': %w", addr, err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterEngramServiceServer(grpcServer, s)
	slog.Info("gRPC server listening", "address", lis.Addr())
	return grpcServer.Serve(lis)
}

// Execute is the gRPC method that gets called for unary execution requests.
// It adapts the incoming protobuf request to the SDK's internal types,
// delegates to the handler (the StreamAdapter), and then handles fanning out
// the result to downstream Engrams.
func (s *GRPCServer) Execute(ctx context.Context, req *proto.ExecuteRequest) (*proto.ExecuteResponse, error) {
	slog.Info("Received gRPC Execute request")

	// The SDK should report that the step is 'Running' as soon as it's invoked.
	if err := s.k8sTransport.UpdateResult(ctx, &engram.Result{}); err != nil {
		slog.Error("Failed to update StepRun status to Running", "error", err)
		// This is a non-fatal error for this execution, but it indicates a problem.
	}

	execCtx, err := s.protoToExecutionContext(req.Context)
	if err != nil {
		return nil, fmt.Errorf("failed to convert incoming proto to execution context: %w", err)
	}

	// Delegate the core logic to the handler.
	// NOTE: This is a placeholder. A full streaming implementation is needed.
	result, err := s.handler.Execute(ctx, execCtx)
	if err != nil {
		// If the handler itself returns an error, fail the step.
		failResult := &engram.Result{Error: err}
		if updateErr := s.k8sTransport.UpdateResult(ctx, failResult); updateErr != nil {
			slog.Error("CRITICAL: Failed to update StepRun status after execution error", "error", updateErr)
		}
		return nil, err
	}

	// After the handler executes, check for downstream targets and fan-out the result.
	if err := s.handleDownstream(ctx, result); err != nil {
		slog.Error("Failed to handle downstream fan-out", "error", err)
		// This is logged but doesn't fail the current step. A failure in a downstream
		// step will be handled by that step's own lifecycle.
	}

	return s.resultToProto(result)
}

// handleDownstream fetches the latest StepRun definition to find downstream
// targets and then uses the gRPC client to fan-out the result.
func (s *GRPCServer) handleDownstream(ctx context.Context, result *engram.Result) error {
	latestCtx, err := s.k8sTransport.GetExecutionContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest execution context for downstream fan-out: %w", err)
	}

	var grpcTargets []GRPCTarget
	for _, target := range latestCtx.DownstreamTarget {
		if target.GRPCTarget != nil {
			grpcTargets = append(grpcTargets, GRPCTarget{Endpoint: target.GRPCTarget.Endpoint})
		}
	}

	if len(grpcTargets) > 0 {
		client := NewGRPCClient()
		// The ExecutionContext for downstream steps is constructed by the gRPC client
		// based on the current step's result. We pass the result's data here.
		// NOTE: This will panic if result.Data is not a map. This is a placeholder.
		dataMap, _ := result.Data.(map[string]interface{})
		downstreamCtx := engram.NewExecutionContext(slog.New(slog.NewJSONHandler(os.Stderr, nil)), nil, dataMap, engram.StoryInfo{
			StoryRunID: latestCtx.StoryRunID,
		})
		_, err := client.Execute(ctx, grpcTargets, downstreamCtx)
		if err != nil {
			return fmt.Errorf("downstream fan-out execution failed: %w", err)
		}
	}
	return nil
}

// protoToExecutionContext converts the incoming protobuf ExecutionContext into the
// SDK's internal ExecutionContext struct.
func (s *GRPCServer) protoToExecutionContext(protoCtx *proto.ExecutionContext) (*engram.ExecutionContext, error) {
	// The transport layer is only responsible for passing through the raw inputs.
	// The rich ExecutionContext with loggers and tracers is created in the runtime.
	inputs := make(map[string]interface{})
	if protoCtx.Inputs != nil {
		inputs = protoCtx.Inputs.AsMap()
	}

	// This creates a minimal context for the transport layer.
	return engram.NewExecutionContext(slog.New(slog.NewJSONHandler(os.Stderr, nil)), nil, inputs, engram.StoryInfo{
		StoryRunID: protoCtx.StoryRunId,
	}), nil
}

// resultToProto converts the SDK's internal Result struct into the protobuf
// ExecuteResponse message that will be sent over the wire.
func (s *GRPCServer) resultToProto(result *engram.Result) (*proto.ExecuteResponse, error) {
	var dataStruct *structpb.Struct
	if result.Data != nil {
		asMap, ok := result.Data.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("result data is not a map[string]interface{}, cannot convert to proto")
		}
		var err error
		dataStruct, err = structpb.NewStruct(asMap)
		if err != nil {
			return nil, fmt.Errorf("failed to convert result data to proto struct: %w", err)
		}
	}

	resp := &proto.ExecuteResponse{
		Data: dataStruct,
	}
	if result.Error != nil {
		resp.Error = result.Error.Error()
	}

	return resp, nil
}
