package sdk

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	bobravozgrpcproto "github.com/bubustack/bobravoz-grpc/proto"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/pkg/metrics"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/bubustack/bubu-sdk-go/runtime"
)

var (
	// rng is a shared random source for backoff jitter, protected by a mutex
	rng   = rand.New(rand.NewSource(time.Now().UnixNano()))
	rngMu sync.Mutex
)

const (
	// DefaultChannelBufferSize is the buffer size for gRPC streaming channels.
	//
	// A buffer of 16 provides reasonable throughput while limiting memory usage.
	// Override via BUBU_GRPC_CHANNEL_BUFFER_SIZE for workloads with different
	// latency/throughput profiles.
	DefaultChannelBufferSize = 16

	// DefaultGRPCPort is the default port for gRPC servers in streaming mode.
	//
	// Override via BUBU_GRPC_PORT. The operator typically sets this to 50051.
	DefaultGRPCPort = "50051"

	// DefaultMessageTimeout is the default timeout for individual message operations.
	//
	// Prevents indefinite hangs on network stalls. Override via BUBU_GRPC_MESSAGE_TIMEOUT.
	DefaultMessageTimeout = 30 * time.Second

	// DefaultMaxMessageSize is the default max message size for gRPC (10 MiB).
	//
	// Override via BUBU_GRPC_MAX_RECV_BYTES and BUBU_GRPC_MAX_SEND_BYTES.
	// Larger messages should use storage offloading instead of increasing this limit.
	DefaultMaxMessageSize = 10 * 1024 * 1024
)

func getEnvInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			return i
		}
	}
	return def
}

func getEnvBytes(name string, def int) int {
	// Accept plain integers as bytes
	return getEnvInt(name, def)
}

func getMessageTimeout() time.Duration {
	if v := os.Getenv("BUBU_GRPC_MESSAGE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return DefaultMessageTimeout
}

func getBackpressureTimeout() time.Duration {
	if v := os.Getenv("BUBU_GRPC_CHANNEL_SEND_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	// Fallback to message timeout if not specified
	return getMessageTimeout()
}

// getHeartbeatInterval returns the interval for sending heartbeats
func getHeartbeatInterval() time.Duration {
	if v := os.Getenv("BUBU_GRPC_HEARTBEAT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 10 * time.Second // Default: send heartbeat every 10s
}

// getHangTimeout returns the timeout for detecting connection hangs
func getHangTimeout() time.Duration {
	if v := os.Getenv("BUBU_GRPC_HANG_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 30 * time.Second // Default: 30s without any message = hang
}

// getGracefulShutdownTimeout returns the timeout for graceful shutdown drain phase
func getGracefulShutdownTimeout() time.Duration {
	if v := os.Getenv("BUBU_GRPC_GRACEFUL_SHUTDOWN_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	// Default 20s: leaves 10s margin before Kubernetes SIGKILL (default terminationGracePeriodSeconds=30s)
	// This provides adequate time for gRPC GracefulStop() to complete even under load.
	// Operators should tune both this value and terminationGracePeriodSeconds together.
	return 20 * time.Second
}

// isHeartbeat checks if a DataPacket is a heartbeat message
func isHeartbeat(metadata map[string]string) bool {
	return metadata != nil && metadata["bubu-heartbeat"] == "true"
}

// createHeartbeat creates a heartbeat DataPacket
func createHeartbeat() *bobravozgrpcproto.DataPacket {
	return &bobravozgrpcproto.DataPacket{
		Metadata: map[string]string{"bubu-heartbeat": "true"},
		Payload:  &structpb.Struct{},
	}
}

// server is the gRPC server implementation for the SDK sidecar.
type server struct {
	bobravozgrpcproto.UnimplementedHubServer
	handler       func(ctx context.Context, in <-chan engram.StreamMessage, out chan<- engram.StreamMessage) error
	activeStreams sync.WaitGroup // Tracks active Process() handlers for graceful shutdown coordination
}

// Process is the gRPC bidirectional streaming endpoint with transparent heartbeat support.
// Heartbeats are sent/received automatically and filtered from the user's handler,
// eliminating the need for per-message timeout goroutines while detecting connection hangs.
//
// Graceful shutdown: On context cancellation (e.g., SIGTERM), this method:
// 1. Closes the input channel to signal EOF to the user handler.
// 2. Waits for the user handler to close the output channel (indicates flush complete).
// 3. Drains remaining messages from output and sends via gRPC.
// 4. Returns after drain completes or timeout (BUBU_GRPC_GRACEFUL_SHUTDOWN_TIMEOUT).
func (s *server) Process(stream bobravozgrpcproto.Hub_ProcessServer) error {
	// Track this stream for graceful shutdown coordination
	s.activeStreams.Add(1)
	defer s.activeStreams.Done()

	streamCtx := stream.Context()
	ctx, cancelCtx := context.WithCancel(streamCtx)
	defer cancelCtx()

	// Buffer channels to reduce risk of blocking when one side stops first
	bufSize := getEnvInt("BUBU_GRPC_CHANNEL_BUFFER_SIZE", DefaultChannelBufferSize)
	in := make(chan engram.StreamMessage, bufSize)
	out := make(chan engram.StreamMessage, bufSize)

	g, gctx := errgroup.WithContext(ctx)

	// Graceful shutdown coordination
	shutdownInitiated := make(chan struct{})
	handlerDone := make(chan struct{})
	shutdownTimeout := getGracefulShutdownTimeout()

	// Heartbeat sender: sends periodic heartbeats to keep connection alive
	// and allow remote side to detect if we're hung
	g.Go(func() error {
		interval := getHeartbeatInterval()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case <-ticker.C:
				heartbeat := createHeartbeat()
				select {
				case <-gctx.Done():
					return gctx.Err()
				default:
					if err := stream.Send(heartbeat); err != nil {
						// Heartbeat send failure means connection is dead
						return fmt.Errorf("heartbeat send failed (connection dead): %w", err)
					}
				}
			}
		}
	})

	// Reader: from gRPC → user handler (with heartbeat filtering and hang detection)
	g.Go(func() error {
		defer close(in)

		// Track last received message (data or heartbeat) for hang detection.
		// Use atomic to avoid race between read (time.Since check) and write (after Recv).
		var lastRecvNano atomic.Int64
		lastRecvNano.Store(time.Now().UnixNano())
		hangTimeout := getHangTimeout()

		for {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
			}

			// Check for connection hang (no data OR heartbeats received)
			lastNano := lastRecvNano.Load()
			if time.Since(time.Unix(0, lastNano)) > hangTimeout {
				return fmt.Errorf("connection hang detected: no messages for %v (last recv: %v ago)", hangTimeout, time.Since(time.Unix(0, lastNano)))
			}

			// Simple blocking Recv - no timeout goroutine needed!
			// Heartbeats keep the connection alive, preventing indefinite hangs
			req, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return fmt.Errorf("error receiving from gRPC stream: %w", err)
			}

			lastRecvNano.Store(time.Now().UnixNano())

			// Filter heartbeats - don't pass to user handler
			if isHeartbeat(req.Metadata) {
				continue
			}

			// Convert to StreamMessage
			payloadBytes, err := protojson.Marshal(req.Payload)
			if err != nil {
				return fmt.Errorf("error marshaling payload from gRPC stream: %w", err)
			}
			var inputsBytes []byte
			if req.Inputs != nil {
				inputsBytes, err = protojson.Marshal(req.Inputs)
				if err != nil {
					return fmt.Errorf("error marshaling inputs from gRPC stream: %w", err)
				}
			}

			msg := engram.StreamMessage{
				Metadata: req.Metadata,
				Payload:  payloadBytes,
				Inputs:   inputsBytes,
			}

			// Record metric for received message
			metrics.RecordStreamMessage(gctx, "received")

			// Send to user handler with backpressure handling
			select {
			case <-gctx.Done():
				return gctx.Err()
			case in <- msg:
				// Successfully sent
			default:
				// Channel full; wait with explicit timer to allow early cancellation
				timer := time.NewTimer(getBackpressureTimeout())
				// Ensure timer is released in all branches without deferring in loop
				select {
				case <-gctx.Done():
					if !timer.Stop() {
						<-timer.C
					}
					return gctx.Err()
				case in <- msg:
					if !timer.Stop() {
						<-timer.C
					}
					// Successfully sent after retry
				case <-timer.C:
					// Handler not consuming; likely hung or slow
					return fmt.Errorf("timeout sending to handler: handler may be stuck or not reading from input channel")
				}
			}
		}
	})

	// Writer: from user handler → gRPC (simple, no timeout goroutines!)
	g.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case msg, ok := <-out:
				if !ok {
					return nil
				}

				// Convert StreamMessage to DataPacket
				payload := &structpb.Struct{}
				if err := protojson.Unmarshal(msg.Payload, payload); err != nil {
					return fmt.Errorf("error unmarshaling payload to gRPC stream: %w", err)
				}
				var inputs *structpb.Struct
				if len(msg.Inputs) > 0 {
					inputs = &structpb.Struct{}
					if err := protojson.Unmarshal(msg.Inputs, inputs); err != nil {
						return fmt.Errorf("error unmarshaling inputs to gRPC stream: %w", err)
					}
				}

				packet := &bobravozgrpcproto.DataPacket{
					Metadata: msg.Metadata,
					Payload:  payload,
					Inputs:   inputs,
				}

				// Record metric for sent message
				metrics.RecordStreamMessage(gctx, "sent")

				// Simple blocking Send - heartbeats keep connection alive
				if err := stream.Send(packet); err != nil {
					return fmt.Errorf("error sending data to gRPC stream: %w", err)
				}
			}
		}
	})

	// User handler - unchanged API
	g.Go(func() error {
		defer close(out)
		defer close(handlerDone)
		err := s.handler(gctx, in, out)
		return err
	})

	// Graceful shutdown coordinator: on context cancellation, drain pending messages
	g.Go(func() error {
		select {
		case <-gctx.Done():
			// Context canceled (SIGTERM or client disconnect)
			close(shutdownInitiated)
			logger := LoggerFromContext(gctx)
			logger.Info("Graceful shutdown initiated, draining stream")

			// Start drain immediately in background (concurrent with handler wait)
			drainCount := 0
			drainComplete := make(chan struct{})
			go func() {
				defer close(drainComplete)
				drainTimer := time.NewTimer(5 * time.Second)
				defer drainTimer.Stop()

			drainLoop:
				for {
					select {
					case msg, ok := <-out:
						if !ok {
							// Output channel closed, drain complete
							logger.Info("Drain complete", "messagesDrained", drainCount)
							break drainLoop
						}

						// Convert and send final messages
						payload := &structpb.Struct{}
						if err := protojson.Unmarshal(msg.Payload, payload); err != nil {
							logger.Error("Failed to marshal payload during drain, dropping message", "error", err)
							continue
						}

						var inputs *structpb.Struct
						if len(msg.Inputs) > 0 {
							inputs = &structpb.Struct{}
							if err := protojson.Unmarshal(msg.Inputs, inputs); err != nil {
								logger.Error("Failed to marshal inputs during drain, setting to nil", "error", err)
								inputs = nil
							}
						}

						packet := &bobravozgrpcproto.DataPacket{
							Metadata: msg.Metadata,
							Payload:  payload,
							Inputs:   inputs,
						}

						if err := stream.Send(packet); err != nil {
							// Send failed; connection likely dead, abort drain
							logger.Error("Failed to send message during drain", "error", err)
							break drainLoop
						}
						drainCount++

					case <-drainTimer.C:
						logger.Warn("Drain timeout exceeded, aborting", "drainedCount", drainCount)
						break drainLoop
					}
				}
			}()

			// Wait for handler to complete OR drain to finish (whichever is first)
			handlerTimer := time.NewTimer(shutdownTimeout)
			defer handlerTimer.Stop()

			select {
			case <-drainComplete:
				// Drain finished (handler likely exited quickly)
				logger.Info("Graceful shutdown complete (drain finished)")
				return nil
			case <-handlerDone:
				// Handler finished; wait for drain to complete
				logger.Info("User handler completed, waiting for drain")
				select {
				case <-drainComplete:
					logger.Info("Graceful shutdown complete")
					return nil
				case <-time.After(5 * time.Second):
					logger.Warn("Drain did not complete within 5s after handler exit")
					return nil
				}
			case <-handlerTimer.C:
				// Handler timeout; wait for drain with short margin
				logger.Warn("User handler did not complete within timeout, waiting for drain",
					"timeout", shutdownTimeout)
				select {
				case <-drainComplete:
					logger.Info("Graceful shutdown complete (drain finished after handler timeout)")
					return nil
				case <-time.After(5 * time.Second):
					logger.Warn("Drain did not complete within 5s after handler timeout")
					return nil
				}
			}

		case <-handlerDone:
			// Normal completion (not shutdown)
			return nil
		}
	})

	if err := g.Wait(); err != nil {
		cancelCtx()
		return err
	}
	return nil
}

// StartStreamServer is the main entry point for a StreamingEngram. This function
// bootstraps a long-running service that can process data in real-time over gRPC.
//
// This function orchestrates the lifecycle of a streaming service:
//  1. It loads the execution context for configuration and secrets.
//  2. It calls the StreamingEngram's `Init` method.
//  3. It starts a gRPC server on the configured port.
//  4. It registers the StreamingEngram's `Stream` method as the gRPC handler.
//  5. It gracefully handles server shutdown on context cancellation.
//
// # Streaming Delivery Guarantees
//
// The SDK provides reliable message delivery for direct engram-to-engram connections (peer-to-peer mode).
// In hub-and-spoke mode (primitives between streaming engrams), the Hub may drop messages if downstream
// engrams are not ready at the time of forwarding. For production use cases requiring guaranteed delivery:
//   - Use peer-to-peer mode (avoid primitives between streaming engrams), OR
//   - Implement application-level acknowledgment and retry in your engram logic, OR
//   - Wait for Hub buffering support (tracked in bobravoz-grpc roadmap)
func StartStreamServer[C any](ctx context.Context, e engram.StreamingEngram[C]) error {
	LoggerFromContext(ctx).Info("Initializing Bubu SDK for streaming execution...")

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
		port = DefaultGRPCPort
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	// gRPC server options
	var opts []grpc.ServerOption
	// Message size limits (bytes)
	maxRecv := getEnvBytes("BUBU_GRPC_MAX_RECV_BYTES", DefaultMaxMessageSize)
	if maxRecv > 0 {
		opts = append(opts, grpc.MaxRecvMsgSize(maxRecv))
	}
	maxSend := getEnvBytes("BUBU_GRPC_MAX_SEND_BYTES", DefaultMaxMessageSize)
	if maxSend > 0 {
		opts = append(opts, grpc.MaxSendMsgSize(maxSend))
	}

	// Optional TLS
	certFile := os.Getenv("BUBU_GRPC_TLS_CERT_FILE")
	keyFile := os.Getenv("BUBU_GRPC_TLS_KEY_FILE")
	if certFile != "" && keyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
		if err != nil {
			return fmt.Errorf("failed to load TLS certs: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	grpcServer := grpc.NewServer(opts...)
	svcImpl := &server{handler: e.Stream}
	bobravozgrpcproto.RegisterHubServer(grpcServer, svcImpl)

	LoggerFromContext(ctx).Info("gRPC server listening", "addr", lis.Addr())
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		logger := LoggerFromContext(ctx)
		logger.Info("Context canceled, initiating graceful shutdown")

		// Give active streams time to drain before stopping server
		// Wait for all active Process() handlers to complete their drain phase
		shutdownTimer := time.NewTimer(getGracefulShutdownTimeout() + 2*time.Second)
		defer shutdownTimer.Stop()

		// Wait for active streams with timeout protection
		drainComplete := make(chan struct{})
		go func() {
			svcImpl.activeStreams.Wait()
			close(drainComplete)
		}()

		select {
		case <-drainComplete:
			logger.Info("All active streams drained, stopping server")
		case <-shutdownTimer.C:
			logger.Warn("Graceful shutdown timeout reached, forcing server stop",
				"timeout", getGracefulShutdownTimeout()+2*time.Second,
				"note", "Some streams may not have completed drain; messages may be dropped")
			// Force stop even if streams haven't completed drain
			// This prevents indefinite hang but may drop messages
		}

		grpcServer.GracefulStop()
		logger.Info("gRPC server stopped")
		return ctx.Err()
	case err := <-serveErr:
		return err
	}
}

// StreamTo connects to a downstream gRPC server and streams data to it (client side).
//
// This function provides a simplified []byte channel API for backward compatibility.
// It wraps StreamToWithMetadata, converting []byte channels to StreamMessage channels
// with empty metadata and inputs fields.
//
// For new code that requires tracing, correlation, or dynamic per-message configuration,
// use StreamToWithMetadata directly.
//
// The function implements:
//   - Automatic reconnection on transient failures (configurable via BUBU_GRPC_RECONNECT_MAX_RETRIES)
//   - Exponential backoff with jitter (base/max configurable via env)
//   - Transparent heartbeat sending/receiving to detect connection hangs
//   - Optional TLS via BUBU_GRPC_CA_FILE or BUBU_GRPC_CLIENT_TLS=true
//   - Backpressure handling with configurable timeouts
//
// Blocks until the input channel is closed, context is canceled, or a permanent error occurs.
// Respects context cancellation for graceful shutdown.
//
// Example:
//
//	in := make(chan []byte, 16)
//	out := make(chan []byte, 16)
//
//	go func() {
//	    defer close(in)
//	    in <- []byte(`{"key": "value"}`)
//	}()
//
//	go func() {
//	    for msg := range out {
//	        log.Printf("Received: %s", msg)
//	    }
//	}()
//
//	if err := sdk.StreamTo(ctx, "downstream-service:50051", in, out); err != nil {
//	    return fmt.Errorf("streaming failed: %w", err)
//	}
func StreamTo(ctx context.Context, target string, in <-chan []byte, out chan<- []byte) error {
	// Convert []byte channels to StreamMessage channels for backward compatibility
	inMsg := make(chan engram.StreamMessage, DefaultChannelBufferSize)
	outMsg := make(chan engram.StreamMessage, DefaultChannelBufferSize)

	// Use a derived context to coordinate converter goroutines and stream lifecycle
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Goroutine to convert incoming []byte to StreamMessage (with empty metadata and inputs)
	go func() {
		defer close(inMsg)
		for {
			select {
			case <-streamCtx.Done():
				return
			case data, ok := <-in:
				if !ok {
					return
				}
				msg := engram.StreamMessage{
					Metadata: make(map[string]string), // Empty metadata for backward compat
					Payload:  data,
					Inputs:   nil, // No inputs for backward compat
				}
				select {
				case <-streamCtx.Done():
					return
				case inMsg <- msg:
				}
			}
		}
	}()

	// Goroutine to convert outgoing StreamMessage to []byte (drop metadata and inputs)
	go func() {
		defer close(out)
		for {
			select {
			case <-streamCtx.Done():
				return
			case msg, ok := <-outMsg:
				if !ok {
					return
				}
				select {
				case <-streamCtx.Done():
					return
				case out <- msg.Payload:
				}
			}
		}
	}()

	// Run the metadata-aware stream with coordinated context
	err := StreamToWithMetadata(streamCtx, target, inMsg, outMsg)
	// Ensure the outbound converter terminates even if the underlying stream never closes it
	close(outMsg)
	// Cancel converters waiting on context/selects
	cancel()
	return err
}

// attemptResult encapsulates the outcome of a streaming attempt
type attemptResult struct {
	err       error
	permanent bool // true if error is permanent and should not retry
	success   bool // true if attempt completed successfully
}

// StreamToWithMetadata connects to a downstream gRPC server with full metadata and inputs support (client side).
//
// This function provides the full StreamMessage API, enabling:
//   - Metadata propagation for tracing (StoryRunID, StepName, custom trace IDs)
//   - Per-message dynamic configuration via the Inputs field (analogous to BUBU_INPUTS in batch mode)
//   - End-to-end correlation across streaming pipeline steps
//
// The SDK automatically injects Hub metadata (storyrun-name, storyrun-namespace, current-step-id)
// from the execution context if available, enabling interop with the bobravoz Hub.
//
// The function implements:
//   - Automatic reconnection on transient failures (Unavailable, ResourceExhausted, Aborted, DeadlineExceeded)
//   - Exponential backoff with jitter (configurable via BUBU_GRPC_RECONNECT_BASE_BACKOFF and _MAX_BACKOFF)
//   - Transparent heartbeat sending/filtering to detect connection hangs (BUBU_GRPC_HANG_TIMEOUT)
//   - Optional TLS via BUBU_GRPC_CA_FILE (custom CA) or BUBU_GRPC_CLIENT_TLS=true (system roots)
//   - Backpressure handling with timeouts (BUBU_GRPC_CHANNEL_SEND_TIMEOUT or BUBU_GRPC_MESSAGE_TIMEOUT)
//   - Configurable message size limits (BUBU_GRPC_CLIENT_MAX_RECV_BYTES, BUBU_GRPC_CLIENT_MAX_SEND_BYTES)
//
// Blocks until the input channel is closed, context is canceled, or a permanent error occurs.
// Respects context cancellation for graceful shutdown.
//
// Example:
//
//	in := make(chan engram.StreamMessage, 16)
//	out := make(chan engram.StreamMessage, 16)
//
//	go func() {
//	    defer close(in)
//	    in <- engram.StreamMessage{
//	        Metadata: map[string]string{"trace-id": "abc123"},
//	        Payload:  []byte(`{"key": "value"}`),
//	        Inputs:   []byte(`{"configKey": "configValue"}`),
//	    }
//	}()
//
//	go func() {
//	    for msg := range out {
//	        log.Printf("Received: %s (trace: %s)", msg.Payload, msg.Metadata["trace-id"])
//	    }
//	}()
//
//	if err := sdk.StreamToWithMetadata(ctx, "downstream:50051", in, out); err != nil {
//	    return fmt.Errorf("streaming failed: %w", err)
//	}
func StreamToWithMetadata(ctx context.Context, target string, in <-chan engram.StreamMessage, out chan<- engram.StreamMessage) error {
	// Dial options
	var dialOpts []grpc.DialOption

	// Message size limits (bytes)
	maxRecv := getEnvBytes("BUBU_GRPC_CLIENT_MAX_RECV_BYTES", DefaultMaxMessageSize)
	if maxRecv > 0 {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecv)))
	}
	maxSend := getEnvBytes("BUBU_GRPC_CLIENT_MAX_SEND_BYTES", DefaultMaxMessageSize)
	if maxSend > 0 {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxSend)))
	}

	// TLS posture: if a CA file is provided, automatically enable TLS.
	// Otherwise, use insecure only if BUBU_GRPC_CLIENT_TLS explicitly == "false".
	if caFile := os.Getenv("BUBU_GRPC_CA_FILE"); caFile != "" {
		var tlsConf tls.Config
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return fmt.Errorf("failed to read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return fmt.Errorf("failed to append CA certs from %s", caFile)
		}
		tlsConf.RootCAs = pool
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tlsConf)))
	} else if os.Getenv("BUBU_GRPC_CLIENT_TLS") == "true" {
		// TLS enabled with system roots
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	} else {
		// Insecure only if explicitly disabled; log a warning via context logger
		LoggerFromContext(ctx).Warn("gRPC client using insecure transport; set BUBU_GRPC_CLIENT_TLS=true or provide BUBU_GRPC_CA_FILE")
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Apply a client-side dial timeout separate from rpc timeouts
	if v := os.Getenv("BUBU_GRPC_DIAL_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
	}

	// Reconnection settings
	maxBackoff := 30 * time.Second
	if v := os.Getenv("BUBU_GRPC_RECONNECT_MAX_BACKOFF"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			maxBackoff = d
		}
	}
	baseBackoff := 500 * time.Millisecond
	if v := os.Getenv("BUBU_GRPC_RECONNECT_BASE_BACKOFF"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			baseBackoff = d
		}
	}
	// Limit dial attempts during a single outage
	maxRetries := 10 // Reasonable default for transient failures; set to 0 for infinite retry
	if v := os.Getenv("BUBU_GRPC_RECONNECT_MAX_RETRIES"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i >= 0 {
			maxRetries = i
		}
	}

	// Attach required Hub metadata for interop if available.
	var mdPairs []string
	if execCtxData, err := runtime.LoadExecutionContextData(); err == nil {
		storyRunName := execCtxData.StoryInfo.StoryRunID
		stepID := execCtxData.StoryInfo.StepName
		ns := k8s.ResolvePodNamespace()
		if storyRunName != "" && stepID != "" && ns != "" {
			mdPairs = []string{
				"storyrun-name", storyRunName,
				"storyrun-namespace", ns,
				"current-step-id", stepID,
			}
		}
	}

	// Reconnect loop with separate retry budgets for dial vs stream failures
	dialAttempt := 0
	streamAttempt := 0
	for {
		// Check context before attempting
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Execute attempt and capture result without panic-based flow control
		result := func() attemptResult {
			conn, err := grpc.DialContext(ctx, target, dialOpts...)
			if err != nil {
				dialAttempt++
				if !shouldRetry(err) || exceeded(dialAttempt, maxRetries) {
					return attemptResult{err: fmt.Errorf("failed to dial gRPC target '%s': %w", target, err), permanent: true}
				}
				// Retryable dial error
				return attemptResult{err: err}
			}
			defer conn.Close()

			// Outgoing context with metadata if available
			callCtx := ctx
			if len(mdPairs) > 0 {
				callCtx = metadata.NewOutgoingContext(callCtx, metadata.Pairs(mdPairs...))
			}

			// Create a cancelable RPC context per attempt
			rpcCtx, rpcCancel := context.WithCancel(callCtx)
			defer rpcCancel()

			client := bobravozgrpcproto.NewHubClient(conn)
			stream, err := client.Process(rpcCtx)
			if err != nil {
				// Decide whether to retry based on error code
				streamAttempt++
				if !shouldRetry(err) || exceeded(streamAttempt, maxRetries) {
					return attemptResult{err: fmt.Errorf("failed to open Hub.Process stream to '%s': %w", target, err), permanent: true}
				}
				// Retryable stream open error
				return attemptResult{err: err}
			}

			g, gCtx := errgroup.WithContext(rpcCtx)

			g.Go(func() error {
				<-gCtx.Done()
				rpcCancel()
				return nil
			})

			// Heartbeat sender: keep connection alive
			g.Go(func() error {
				interval := getHeartbeatInterval()
				ticker := time.NewTicker(interval)
				defer ticker.Stop()

				for {
					select {
					case <-gCtx.Done():
						return gCtx.Err()
					case <-ticker.C:
						heartbeat := createHeartbeat()
						select {
						case <-gCtx.Done():
							return gCtx.Err()
						default:
							if err := stream.Send(heartbeat); err != nil {
								return fmt.Errorf("heartbeat send failed: %w", err)
							}
						}
					}
				}
			})

			// Sender: from user → gRPC (simple, no timeout goroutines!)
			g.Go(func() error {
				for {
					select {
					case <-gCtx.Done():
						return stream.CloseSend()
					case msg, ok := <-in:
						if !ok {
							return stream.CloseSend()
						}

						// Convert StreamMessage to DataPacket
						payload := &structpb.Struct{}
						if err := protojson.Unmarshal(msg.Payload, payload); err != nil {
							return fmt.Errorf("error unmarshaling payload to gRPC stream: %w", err)
						}
						var inputs *structpb.Struct
						if len(msg.Inputs) > 0 {
							inputs = &structpb.Struct{}
							if err := protojson.Unmarshal(msg.Inputs, inputs); err != nil {
								return fmt.Errorf("error unmarshaling inputs to gRPC stream: %w", err)
							}
						}

						packet := &bobravozgrpcproto.DataPacket{
							Metadata: msg.Metadata,
							Payload:  payload,
							Inputs:   inputs,
						}

						// Simple blocking Send - heartbeats keep connection alive
						if err := stream.Send(packet); err != nil {
							return fmt.Errorf("error sending data on stream: %w", err)
						}
					}
				}
			})

			// Receiver: from gRPC → user (with heartbeat filtering and hang detection)
			g.Go(func() error {
				// Use atomic to avoid race between read and write
				var lastRecvNano atomic.Int64
				lastRecvNano.Store(time.Now().UnixNano())
				hangTimeout := getHangTimeout()

				for {
					// Check for connection hang
					lastNano := lastRecvNano.Load()
					if time.Since(time.Unix(0, lastNano)) > hangTimeout {
						return fmt.Errorf("connection hang detected: no messages for %v", hangTimeout)
					}

					// Simple blocking Recv - no timeout goroutine needed!
					resp, err := stream.Recv()
					if err == io.EOF {
						return nil
					}
					if err != nil {
						return fmt.Errorf("error receiving from gRPC stream: %w", err)
					}

					lastRecvNano.Store(time.Now().UnixNano())

					// Filter heartbeats - don't pass to user's out channel
					if isHeartbeat(resp.Metadata) {
						continue
					}

					// Convert to StreamMessage
					payloadBytes, err := protojson.Marshal(resp.Payload)
					if err != nil {
						return fmt.Errorf("error marshaling payload from gRPC stream: %w", err)
					}
					var inputsBytes []byte
					if resp.Inputs != nil {
						inputsBytes, err = protojson.Marshal(resp.Inputs)
						if err != nil {
							return fmt.Errorf("error marshaling inputs from gRPC stream: %w", err)
						}
					}

					msg := engram.StreamMessage{
						Metadata: resp.Metadata,
						Payload:  payloadBytes,
						Inputs:   inputsBytes,
					}

					// Attempt non-blocking send; on backpressure, wait up to timeout
					select {
					case <-gCtx.Done():
						return gCtx.Err()
					case out <- msg:
						// sent immediately
					default:
						timer := time.NewTimer(getBackpressureTimeout())
						select {
						case <-gCtx.Done():
							if !timer.Stop() {
								<-timer.C
							}
							return gCtx.Err()
						case out <- msg:
							if !timer.Stop() {
								<-timer.C
							}
							// sent after wait
						case <-timer.C:
							return fmt.Errorf("timeout delivering message to caller: output channel not drained")
						}
					}
				}
			})

			if err := g.Wait(); err != nil {
				// Decide retry vs permanent error
				streamAttempt++
				if !shouldRetry(err) || exceeded(streamAttempt, maxRetries) {
					return attemptResult{err: err, permanent: true}
				}
				// Retryable streaming error
				return attemptResult{err: err}
			}

			// Graceful completion (e.g., input closed)
			return attemptResult{success: true}
		}()

		// Handle attempt result
		if result.success {
			return nil // Successful completion, no reconnect
		}

		if result.permanent {
			return result.err // Permanent error, do not retry
		}

		// Retryable error: check context before backoff
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Apply backoff and retry
		// Use max of both counters for backoff calculation to avoid resetting backoff
		totalAttempts := dialAttempt
		if streamAttempt > totalAttempts {
			totalAttempts = streamAttempt
		}
		backoffSleep(ctx, totalAttempts, baseBackoff, maxBackoff)
	}
}

// shouldRetry determines whether an error is transient and warrants a reconnect.
func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		// Non-gRPC error: assume retryable for common network errors
		msg := err.Error()
		return strings.Contains(msg, "connection refused") || strings.Contains(msg, "transport is closing") || strings.Contains(msg, "deadline exceeded")
	}
	switch st.Code() {
	case codes.Unavailable, codes.ResourceExhausted, codes.Aborted, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

func exceeded(attempt, maxRetries int) bool {
	if maxRetries == 0 {
		return false
	}
	return attempt >= maxRetries
}

func backoffSleep(ctx context.Context, attempt int, base, max time.Duration) {
	// Exponential backoff with jitter, capped to prevent overflow
	// Cap attempt to prevent integer overflow in bit shift operation
	// (500ms << 30 would overflow int64)
	safeAttempt := attempt
	if safeAttempt > 30 {
		safeAttempt = 30
	}
	d := base << safeAttempt
	if d > max || d < 0 { // Also check for negative (overflow wrap-around)
		d = max
	}
	jitter := time.Duration(int64(d) / 5)
	sleep := d - jitter + time.Duration(randInt63n(int64(2*jitter)))
	t := time.NewTimer(sleep)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return
	case <-t.C:
		return
	}
}

// randInt63n is a concurrency-safe pseudo-random helper for backoff jitter.
// Uses math/rand with a mutex-protected source to ensure proper randomness
// even when multiple goroutines reconnect simultaneously.
func randInt63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	rngMu.Lock()
	defer rngMu.Unlock()
	return rng.Int63n(n)
}
