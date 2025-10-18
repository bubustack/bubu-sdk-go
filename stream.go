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

	bobravozgrpcproto "github.com/bubustack/bobravoz-grpc/proto/v1"
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
	pbproto "google.golang.org/protobuf/proto"
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

	// Client buffer defaults (bounded)
	DefaultClientBufferMaxMessages = 100
	DefaultClientBufferMaxBytes    = 10 * 1024 * 1024 // 10 MiB
)

func getEnvInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			return i
		}
	}
	return def
}

// nolint:unparam // def is intentionally configurable for future call sites
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
func isHeartbeat(meta map[string]string) bool {
	const heartbeatTrue = "true"
	return meta != nil && meta["bubu-heartbeat"] == heartbeatTrue
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
	bobravozgrpcproto.UnimplementedHubServiceServer
	handler       func(ctx context.Context, in <-chan engram.StreamMessage, out chan<- engram.StreamMessage) error
	activeStreams sync.WaitGroup // Tracks active Process() handlers for graceful shutdown coordination
}

// clientMessageBuffer is a bounded in-memory buffer of DataPackets for retry after transient send failures.
// It mirrors hub-side semantics: bounded by message count and total bytes; drops on overflow with reason.
type clientMessageBuffer struct {
	mu         sync.Mutex
	messages   []*bobravozgrpcproto.DataPacket
	totalBytes int
	maxMsgs    int
	maxBytes   int
	// metrics context
	ctx context.Context
}

func newClientMessageBufferWithContext(ctx context.Context) *clientMessageBuffer {
	maxMsgs := getEnvInt("BUBU_GRPC_CLIENT_BUFFER_MAX_MESSAGES", DefaultClientBufferMaxMessages)
	if maxMsgs <= 0 {
		maxMsgs = DefaultClientBufferMaxMessages
	}
	maxBytes := getEnvBytes("BUBU_GRPC_CLIENT_BUFFER_MAX_BYTES", DefaultClientBufferMaxBytes)
	if maxBytes <= 0 {
		maxBytes = DefaultClientBufferMaxBytes
	}
	b := &clientMessageBuffer{
		messages: make([]*bobravozgrpcproto.DataPacket, 0, maxMsgs),
		maxMsgs:  maxMsgs,
		maxBytes: maxBytes,
		ctx:      ctx,
	}
	// Register gauges: provide safe callbacks that lock and read size/bytes
	metrics.RegisterClientBufferGauges(
		func() float64 {
			b.mu.Lock()
			defer b.mu.Unlock()
			return float64(len(b.messages))
		},
		func() float64 {
			b.mu.Lock()
			defer b.mu.Unlock()
			return float64(b.totalBytes)
		},
	)
	return b
}

// newClientMessageBuffer is retained for backward compatibility in tests.
// It defaults metrics context to Background; production paths should prefer newClientMessageBufferWithContext.
func newClientMessageBuffer() *clientMessageBuffer {
	return newClientMessageBufferWithContext(context.Background())
}

// add attempts to add; returns true if added, false if dropped (overflow or oversize)
func (b *clientMessageBuffer) add(p *bobravozgrpcproto.DataPacket) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	size := pbproto.Size(p)
	if size > b.maxBytes {
		metrics.RecordClientBufferDrop(b.ctx, "oversize")
		return false
	}
	if len(b.messages) >= b.maxMsgs || b.totalBytes+size > b.maxBytes {
		metrics.RecordClientBufferDrop(b.ctx, "buffer_full")
		return false
	}
	b.messages = append(b.messages, p)
	b.totalBytes += size
	return true
}

// flush tries to send all buffered messages using the provided send function; keeps failed tail
func (b *clientMessageBuffer) flush(ctx context.Context, send func(*bobravozgrpcproto.DataPacket) error) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.messages) == 0 {
		return 0
	}
	flushed := 0
	remaining := make([]*bobravozgrpcproto.DataPacket, 0, len(b.messages))
	for _, msg := range b.messages {
		if ctx.Err() != nil {
			remaining = append(remaining, msg)
			continue
		}
		if err := send(msg); err != nil {
			remaining = append(remaining, msg)
			break
		}
		flushed++
		b.totalBytes -= pbproto.Size(msg)
	}
	b.messages = remaining
	return flushed
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
func (s *server) Process(stream bobravozgrpcproto.HubService_ProcessServer) error {
	// Track this stream for graceful shutdown coordination
	s.activeStreams.Add(1)
	defer s.activeStreams.Done()

	streamCtx := stream.Context()
	ctx, cancelCtx := context.WithCancel(streamCtx)
	defer cancelCtx()

	// Serialize all writes to the gRPC stream to avoid concurrent Send hazards.
	// gRPC streams are not safe for concurrent Send() from multiple goroutines.
	var sendMu sync.Mutex

	// Buffer channels to reduce risk of blocking when one side stops first
	bufSize := getEnvInt("BUBU_GRPC_CHANNEL_BUFFER_SIZE", DefaultChannelBufferSize)
	in := make(chan engram.StreamMessage, bufSize)
	out := make(chan engram.StreamMessage, bufSize)

	g, gctx := errgroup.WithContext(ctx)

	// Graceful shutdown coordination
	// shutdownInitiated channel removed; drainOnShutdown handles coordination
	handlerDone := make(chan struct{})
	shutdownTimeout := getGracefulShutdownTimeout()

	// Heartbeat sender: sends periodic heartbeats to keep connection alive
	// and allow remote side to detect if we're hung
	g.Go(func() error { return heartbeatLoop(gctx, stream, &sendMu) })

	// Reader: from gRPC → user handler (with heartbeat filtering and hang detection)
	g.Go(func() error { return readLoop(gctx, stream, in) })

	// Writer: from user handler → gRPC (simple, no timeout goroutines!)
	g.Go(func() error { return writeLoop(gctx, stream, out, &sendMu) })

	// User handler - unchanged API
	g.Go(func() error {
		defer close(out)
		defer close(handlerDone)
		err := s.handler(gctx, in, out)
		return err
	})

	// Graceful shutdown coordinator: on context cancellation, drain pending messages
	g.Go(func() error { return drainOnShutdown(gctx, out, stream, &sendMu, shutdownTimeout, handlerDone) })

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
	bobravozgrpcproto.RegisterHubServiceServer(grpcServer, svcImpl)

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
func StreamTo(
	ctx context.Context,
	target string,
	in <-chan []byte,
	out chan<- []byte,
) error {
	// Allow operator-provided env-based wiring when target is empty
	if target == "" {
		if v := os.Getenv("DOWNSTREAM_HOST"); v != "" {
			target = v
		} else if v := os.Getenv("UPSTREAM_HOST"); v != "" {
			target = v
		}
	}
	// Convert []byte channels to StreamMessage channels for backward compatibility
	inMsg := make(chan engram.StreamMessage, DefaultChannelBufferSize)
	outMsg := make(chan engram.StreamMessage, DefaultChannelBufferSize)

	// Use a derived context to coordinate converter goroutines and stream lifecycle
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	spawnBytesToMessageConverter(streamCtx, in, inMsg)
	spawnMessageToBytesConverter(streamCtx, outMsg, out)

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
	err     error
	success bool // true if attempt completed successfully
}

// clientRunConfig captures shared parameters for a single streaming attempt.
type clientRunConfig struct {
	ctx      context.Context
	target   string
	dialOpts []grpc.DialOption
	mdPairs  []string
	buffer   *clientMessageBuffer
	in       <-chan engram.StreamMessage
	out      chan<- engram.StreamMessage
}

// makeRPCContext constructs a per-attempt RPC context using env-configured timeouts.
func makeRPCContext(callCtx context.Context) (context.Context, context.CancelFunc) {
	if v := os.Getenv("BUBU_GRPC_STREAM_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return context.WithTimeout(callCtx, d)
		}
		return context.WithCancel(callCtx)
	}
	defaultD := 30 * time.Second
	if v := os.Getenv("BUBU_HUB_PER_MESSAGE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			defaultD = d
		}
	}
	return context.WithTimeout(callCtx, defaultD)
}

// flushClientBuffer attempts to flush any buffered messages before normal send path resumes.
func flushClientBuffer(
	ctx context.Context,
	cfg *clientRunConfig,
	stream bobravozgrpcproto.HubService_ProcessClient,
	sendMu *sync.Mutex,
) {
	if flushed := cfg.buffer.flush(ctx, func(p *bobravozgrpcproto.DataPacket) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(&bobravozgrpcproto.ProcessRequest{Packet: p})
	}); flushed > 0 {
		metrics.RecordClientBufferFlush(ctx, flushed)
	}
}

// clientHeartbeatLoop periodically sends heartbeats on the client stream.
func clientHeartbeatLoop(
	gCtx context.Context,
	stream bobravozgrpcproto.HubService_ProcessClient,
	sendMu *sync.Mutex,
) error {
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
				sendMu.Lock()
				err := stream.Send(&bobravozgrpcproto.ProcessRequest{Packet: heartbeat})
				sendMu.Unlock()
				if err != nil {
					return fmt.Errorf("heartbeat send failed: %w", err)
				}
			}
		}
	}
}

// clientSenderLoop forwards messages from cfg.in to the gRPC stream with buffering semantics.
func clientSenderLoop(
	gCtx context.Context,
	cfg *clientRunConfig,
	stream bobravozgrpcproto.HubService_ProcessClient,
	sendMu *sync.Mutex,
) error {
	for {
		select {
		case <-gCtx.Done():
			return stream.CloseSend()
		case msg, ok := <-cfg.in:
			if !ok {
				return stream.CloseSend()
			}
			packet, err := buildPacketFromMsg(msg)
			if err != nil {
				return fmt.Errorf("convert message: %w", err)
			}
			if cfg.buffer.flush(gCtx, func(p *bobravozgrpcproto.DataPacket) error {
				sendMu.Lock()
				defer sendMu.Unlock()
				return stream.Send(&bobravozgrpcproto.ProcessRequest{Packet: p})
			}) > 0 {
				// Preserve existing metric behavior
				metrics.RecordClientBufferFlush(gCtx, 0)
			}
			sendMu.Lock()
			err = stream.Send(&bobravozgrpcproto.ProcessRequest{Packet: packet})
			sendMu.Unlock()
			if err != nil {
				if cfg.buffer.add(packet) {
					metrics.RecordClientBufferAdded(gCtx, "transient_error")
				}
				return fmt.Errorf("send on stream: %w", err)
			}
		}
	}
}

// buildMessageFromPacket converts a DataPacket to StreamMessage bytes.
func buildMessageFromPacket(p *bobravozgrpcproto.DataPacket) (engram.StreamMessage, error) {
	payloadBytes, err := protojson.Marshal(p.Payload)
	if err != nil {
		return engram.StreamMessage{}, fmt.Errorf("marshal payload from stream: %w", err)
	}
	var inputsBytes []byte
	if p.Inputs != nil {
		inputsBytes, err = protojson.Marshal(p.Inputs)
		if err != nil {
			return engram.StreamMessage{}, fmt.Errorf("marshal inputs from stream: %w", err)
		}
	}
	return engram.StreamMessage{Metadata: p.Metadata, Payload: payloadBytes, Inputs: inputsBytes}, nil
}

// offerStreamMessageWithBackpressure tries to send with a timeout to avoid deadlocks.
func offerStreamMessageWithBackpressure(
	gCtx context.Context,
	ch chan<- engram.StreamMessage,
	msg engram.StreamMessage,
) error {
	select {
	case <-gCtx.Done():
		return gCtx.Err()
	case ch <- msg:
		return nil
	default:
		timer := time.NewTimer(getBackpressureTimeout())
		defer func() {
			if !timer.Stop() {
				<-timer.C
			}
		}()
		select {
		case <-gCtx.Done():
			return gCtx.Err()
		case ch <- msg:
			return nil
		case <-timer.C:
			return fmt.Errorf("timeout delivering message to caller: output channel not drained")
		}
	}
}

// clientReceiverLoop reads from gRPC and forwards to cfg.out, filtering heartbeats and monitoring hangs.
func clientReceiverLoop(
	gCtx context.Context,
	cfg *clientRunConfig,
	stream bobravozgrpcproto.HubService_ProcessClient,
) error {
	var lastRecvNano atomic.Int64
	lastRecvNano.Store(time.Now().UnixNano())
	hangTimeout := getHangTimeout()
	for {
		lastNano := lastRecvNano.Load()
		if time.Since(time.Unix(0, lastNano)) > hangTimeout {
			return fmt.Errorf("connection hang detected: no messages for %v", hangTimeout)
		}
		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("recv from stream: %w", err)
		}
		lastRecvNano.Store(time.Now().UnixNano())
		if isHeartbeat(resp.Packet.Metadata) {
			continue
		}
		msg, err := buildMessageFromPacket(resp.Packet)
		if err != nil {
			return err
		}
		// Backpressure-aware deliver
		if err := offerStreamMessageWithBackpressure(gCtx, cfg.out, msg); err != nil {
			return err
		}
	}
}

func runClientAttempt(cfg *clientRunConfig) attemptResult {
	conn, err := grpc.NewClient(cfg.target, cfg.dialOpts...)
	if err != nil {
		return attemptResult{err: err}
	}
	defer func() { _ = conn.Close() }()

	// Outgoing context with metadata if available
	callCtx := cfg.ctx
	if len(cfg.mdPairs) > 0 {
		callCtx = metadata.NewOutgoingContext(callCtx, metadata.Pairs(cfg.mdPairs...))
	}

	// RPC context per attempt with optional deadline
	rpcCtx, rpcCancel := makeRPCContext(callCtx)
	defer rpcCancel()

	client := bobravozgrpcproto.NewHubServiceClient(conn)
	stream, err := client.Process(rpcCtx)
	if err != nil {
		return attemptResult{err: err}
	}

	g, gCtx := errgroup.WithContext(rpcCtx)
	var sendMu sync.Mutex

	g.Go(func() error {
		<-gCtx.Done()
		rpcCancel()
		return nil
	})

	// Flush buffered messages accumulated from previous attempts
	flushClientBuffer(rpcCtx, cfg, stream, &sendMu)

	// Heartbeat sender
	g.Go(func() error { return clientHeartbeatLoop(gCtx, stream, &sendMu) })

	// Sender: from user → gRPC
	g.Go(func() error { return clientSenderLoop(gCtx, cfg, stream, &sendMu) })

	// Receiver: gRPC → user
	g.Go(func() error { return clientReceiverLoop(gCtx, cfg, stream) })

	if err := g.Wait(); err != nil {
		return attemptResult{err: err}
	}
	return attemptResult{success: true}
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
func StreamToWithMetadata(
	ctx context.Context,
	target string,
	in <-chan engram.StreamMessage,
	out chan<- engram.StreamMessage,
) error {
	// Allow operator-provided env-based wiring when target is empty
	if target == "" {
		if v := os.Getenv("DOWNSTREAM_HOST"); v != "" {
			target = v
		} else if v := os.Getenv("UPSTREAM_HOST"); v != "" {
			target = v
		}
	}
	// Dial options (sizes and TLS)
	dialOpts, err := buildDialOptionsFromEnv(ctx)
	if err != nil {
		return err
	}

	// Reconnection settings
	baseBackoff, maxBackoff, maxRetries := parseReconnectSettingsFromEnv()

	// Attach required Hub metadata for interop if available.
	mdPairs := attachHubMetadataPairs()

	// Bounded client buffer that persists across reconnect attempts
	buffer := newClientMessageBufferWithContext(ctx)

	// Reconnect loop with a single attempt counter for backoff
	attempt := 0
	for {
		// Check context before attempting
		if ctx.Err() != nil {
			return ctx.Err()
		}

		cfg := &clientRunConfig{
			ctx:      ctx,
			target:   target,
			dialOpts: dialOpts,
			mdPairs:  mdPairs,
			buffer:   buffer,
			in:       in,
			out:      out,
		}
		// Record a reconnect attempt for any iteration beyond the first attempt
		if attempt > 0 {
			metrics.RecordStreamReconnectAttempt(ctx)
		}
		result := runClientAttempt(cfg)

		// Handle attempt result
		if result.success {
			return nil // Successful completion, no reconnect
		}

		if result.err != nil && (!shouldRetry(result.err) || exceeded(attempt, maxRetries)) {
			// Terminal failure after attempts
			metrics.RecordStreamReconnectFailure(ctx)
			return result.err
		}

		// Retryable error: check context before backoff
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Apply backoff and retry
		attempt++
		backoffSleep(ctx, attempt, baseBackoff, maxBackoff)
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
		return strings.Contains(msg, "connection refused") ||
			strings.Contains(msg, "transport is closing") ||
			strings.Contains(msg, "deadline exceeded")
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

// buildPacketFromMsg converts a StreamMessage to the gRPC DataPacket.
func buildPacketFromMsg(msg engram.StreamMessage) (*bobravozgrpcproto.DataPacket, error) {
	payload := &structpb.Struct{}
	if err := protojson.Unmarshal(msg.Payload, payload); err != nil {
		return nil, fmt.Errorf("unmarshal payload: %w", err)
	}
	var inputs *structpb.Struct
	if len(msg.Inputs) > 0 {
		inputs = &structpb.Struct{}
		if err := protojson.Unmarshal(msg.Inputs, inputs); err != nil {
			return nil, fmt.Errorf("unmarshal inputs: %w", err)
		}
	}
	return &bobravozgrpcproto.DataPacket{
		Metadata: msg.Metadata,
		Payload:  payload,
		Inputs:   inputs,
	}, nil
}

// spawnBytesToMessageConverter launches a goroutine that converts []byte to StreamMessage.
func spawnBytesToMessageConverter(ctx context.Context, in <-chan []byte, inMsg chan<- engram.StreamMessage) {
	go func() {
		defer close(inMsg)
		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-in:
				if !ok {
					return
				}
				msg := engram.StreamMessage{Metadata: map[string]string{}, Payload: data, Inputs: nil}
				select {
				case <-ctx.Done():
					return
				case inMsg <- msg:
				}
			}
		}
	}()
}

// spawnMessageToBytesConverter launches a goroutine that converts StreamMessage to []byte.
func spawnMessageToBytesConverter(ctx context.Context, outMsg <-chan engram.StreamMessage, out chan<- []byte) {
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-outMsg:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case out <- msg.Payload:
				}
			}
		}
	}()
}

// buildDialOptionsFromEnv constructs grpc.DialOptions including message sizes and TLS based on environment variables.
func buildDialOptionsFromEnv(ctx context.Context) ([]grpc.DialOption, error) {
	var dialOpts []grpc.DialOption
	maxRecv := getEnvBytes("BUBU_GRPC_CLIENT_MAX_RECV_BYTES", DefaultMaxMessageSize)
	if maxRecv > 0 {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecv)))
	}
	maxSend := getEnvBytes("BUBU_GRPC_CLIENT_MAX_SEND_BYTES", DefaultMaxMessageSize)
	if maxSend > 0 {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxSend)))
	}

	var tlsConf *tls.Config
	if caFile := os.Getenv("BUBU_GRPC_CA_FILE"); caFile != "" {
		tlsConf = &tls.Config{}
		if pem, err := os.ReadFile(caFile); err == nil {
			pool := x509.NewCertPool()
			if pool.AppendCertsFromPEM(pem) {
				tlsConf.RootCAs = pool
			} else {
				LoggerFromContext(ctx).Warn("Failed to append CA certs; falling back to system roots", "caFile", caFile)
			}
		} else {
			LoggerFromContext(ctx).Warn("Failed to read BUBU_GRPC_CA_FILE; falling back to system roots", "error", err)
		}
	}
	certFile := os.Getenv("BUBU_GRPC_CLIENT_CERT_FILE")
	keyFile := os.Getenv("BUBU_GRPC_CLIENT_KEY_FILE")
	if certFile != "" && keyFile != "" {
		if tlsConf == nil {
			tlsConf = &tls.Config{}
		}
		if cert, err := tls.LoadX509KeyPair(certFile, keyFile); err == nil {
			tlsConf.Certificates = []tls.Certificate{cert}
		} else {
			LoggerFromContext(ctx).Warn("Failed to load client certificate; proceeding without mTLS", "error", err)
		}
	}
	if tlsConf != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConf)))
	} else if os.Getenv("BUBU_GRPC_CLIENT_TLS") == "true" {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	} else {
		if os.Getenv("BUBU_GRPC_REQUIRE_TLS") == "true" {
			return nil, fmt.Errorf(
				"TLS required by BUBU_GRPC_REQUIRE_TLS; " +
					"set BUBU_GRPC_CLIENT_TLS=true or provide BUBU_GRPC_CA_FILE/BUBU_GRPC_CLIENT_CERT_FILE",
			)
		}
		LoggerFromContext(ctx).Warn(
			"gRPC client using insecure transport; " +
				"set BUBU_GRPC_CLIENT_TLS=true or provide BUBU_GRPC_CA_FILE",
		)
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	return dialOpts, nil
}

// parseReconnectSettingsFromEnv parses backoff and retry settings for reconnect loop.
func parseReconnectSettingsFromEnv() (time.Duration, time.Duration, int) {
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
	maxRetries := 10
	if v := os.Getenv("BUBU_GRPC_RECONNECT_MAX_RETRIES"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i >= 0 {
			maxRetries = i
		}
	}
	return baseBackoff, maxBackoff, maxRetries
}

// attachHubMetadataPairs collects standard hub metadata from the current execution context.
func attachHubMetadataPairs() []string {
	if execCtxData, err := runtime.LoadExecutionContextData(); err == nil {
		storyRunName := execCtxData.StoryInfo.StoryRunID
		stepID := execCtxData.StoryInfo.StepName
		ns := k8s.ResolvePodNamespace()
		if storyRunName != "" && stepID != "" && ns != "" {
			return []string{
				"storyrun-name", storyRunName,
				"storyrun-namespace", ns,
				"current-step-id", stepID,
			}
		}
	}
	return nil
}

// heartbeatLoop periodically sends heartbeats on the stream; returns error when send fails.
func heartbeatLoop(ctx context.Context, stream bobravozgrpcproto.HubService_ProcessServer, sendMu *sync.Mutex) error {
	interval := getHeartbeatInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			heartbeat := createHeartbeat()
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				sendMu.Lock()
				err := stream.Send(&bobravozgrpcproto.ProcessResponse{Packet: heartbeat})
				sendMu.Unlock()
				if err != nil {
					return fmt.Errorf("heartbeat send failed (connection dead): %w", err)
				}
			}
		}
	}
}

// readLoop reads from gRPC stream, filters heartbeats, forwards to input channel with backpressure, and monitors hangs.
func readLoop(
	gctx context.Context,
	stream bobravozgrpcproto.HubService_ProcessServer,
	in chan<- engram.StreamMessage,
) error {
	defer close(in)

	var lastRecvNano atomic.Int64
	lastRecvNano.Store(time.Now().UnixNano())
	hangTimeout := getHangTimeout()

	for {
		select {
		case <-gctx.Done():
			return gctx.Err()
		default:
		}

		// Hang detection window
		lastNano := lastRecvNano.Load()
		if time.Since(time.Unix(0, lastNano)) > hangTimeout {
			ago := time.Since(time.Unix(0, lastNano))
			return fmt.Errorf(
				"connection hang detected: no messages for %v (last recv: %v ago)",
				hangTimeout,
				ago,
			)
		}

		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("error receiving from gRPC stream: %w", err)
		}

		lastRecvNano.Store(time.Now().UnixNano())
		if isHeartbeat(req.Packet.Metadata) {
			continue
		}

		msg, err := buildMessageFromPacket(req.Packet)
		if err != nil {
			return fmt.Errorf("error marshaling message from gRPC stream: %w", err)
		}
		metrics.RecordStreamMessage(gctx, "received")

		if err := offerStreamMessageWithBackpressure(gctx, in, msg); err != nil {
			return fmt.Errorf("timeout sending to handler: %w", err)
		}
	}
}

// writeLoop converts messages from out and sends them to gRPC stream with serialization and metrics.
func writeLoop(
	gctx context.Context,
	stream bobravozgrpcproto.HubService_ProcessServer,
	out <-chan engram.StreamMessage,
	sendMu *sync.Mutex,
) error {
	for {
		select {
		case <-gctx.Done():
			return gctx.Err()
		case msg, ok := <-out:
			if !ok {
				return nil
			}
			packet, err := buildPacketFromMsg(msg)
			if err != nil {
				return fmt.Errorf("error converting StreamMessage to DataPacket: %w", err)
			}
			metrics.RecordStreamMessage(gctx, "sent")
			sendMu.Lock()
			sendErr := stream.Send(&bobravozgrpcproto.ProcessResponse{Packet: packet})
			sendMu.Unlock()
			if sendErr != nil {
				return fmt.Errorf("error sending data to gRPC stream: %w", sendErr)
			}
		}
	}
}

// startServerDrain launches a background drain of remaining messages on server shutdown.
func startServerDrain(
	gctx context.Context,
	out <-chan engram.StreamMessage,
	stream bobravozgrpcproto.HubService_ProcessServer,
	sendMu *sync.Mutex,
) <-chan struct{} {
	drainComplete := make(chan struct{})
	logger := LoggerFromContext(gctx)
	go func() {
		defer close(drainComplete)
		drainCount := 0
		drainTimer := time.NewTimer(5 * time.Second)
		defer drainTimer.Stop()
		for {
			select {
			case msg, ok := <-out:
				if !ok {
					logger.Info("Drain complete", "messagesDrained", drainCount)
					return
				}
				packet, err := buildPacketFromMsg(msg)
				if err != nil {
					logger.Error("Failed to convert message during drain, dropping", "error", err)
					continue
				}
				sendMu.Lock()
				sendErr := stream.Send(&bobravozgrpcproto.ProcessResponse{Packet: packet})
				sendMu.Unlock()
				if sendErr != nil {
					logger.Error("Failed to send message during drain", "error", sendErr)
					return
				}
				drainCount++
			case <-drainTimer.C:
				logger.Warn("Drain timeout exceeded, aborting", "drainedCount", drainCount)
				return
			}
		}
	}()
	return drainComplete
}

// waitForDrainWithGrace waits for drain completion with an additional grace period.
func waitForDrainWithGrace(
	gctx context.Context,
	drainComplete <-chan struct{},
	grace time.Duration,
	reason string,
) {
	logger := LoggerFromContext(gctx)
	select {
	case <-drainComplete:
		logger.Info("Graceful shutdown complete", "reason", reason)
	case <-time.After(grace):
		logger.Warn("Drain did not complete within grace period", "grace", grace, "reason", reason)
	}
}

// drainOnShutdown coordinates graceful drain after context cancellation.
func drainOnShutdown(
	gctx context.Context,
	out <-chan engram.StreamMessage,
	stream bobravozgrpcproto.HubService_ProcessServer,
	sendMu *sync.Mutex,
	shutdownTimeout time.Duration,
	handlerDone <-chan struct{},
) error {
	select {
	case <-gctx.Done():
		LoggerFromContext(gctx).Info("Graceful shutdown initiated, draining stream")
		drainComplete := startServerDrain(gctx, out, stream, sendMu)

		handlerTimer := time.NewTimer(shutdownTimeout)
		defer handlerTimer.Stop()

		select {
		case <-drainComplete:
			LoggerFromContext(gctx).Info("Graceful shutdown complete (drain finished)")
			return nil
		case <-handlerDone:
			LoggerFromContext(gctx).Info("User handler completed, waiting for drain")
			waitForDrainWithGrace(gctx, drainComplete, 5*time.Second, "handler exit")
			return nil
		case <-handlerTimer.C:
			LoggerFromContext(gctx).Warn(
				"User handler did not complete within timeout, waiting for drain",
				"timeout", shutdownTimeout,
			)
			waitForDrainWithGrace(gctx, drainComplete, 5*time.Second, "handler timeout")
			return nil
		}
	case <-handlerDone:
		return nil
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
