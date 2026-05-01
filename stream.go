package sdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	randv2 "math/rand/v2"
	"mime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/runtime"
	"github.com/bubustack/core/contracts"
	coretransport "github.com/bubustack/core/runtime/transport"
	transportconnector "github.com/bubustack/core/runtime/transport/connector"
	"github.com/bubustack/tractatus/envelope"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	tractatusvalidation "github.com/bubustack/tractatus/validation"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// DefaultChannelBufferSize is the in-memory buffer used for Engram stream channels.
	DefaultChannelBufferSize = 16
	// DefaultMaxMessageSize caps gRPC message sizes when talking to connectors.
	DefaultMaxMessageSize         = 10 * 1024 * 1024
	defaultMessageTimeout         = 30 * time.Second
	defaultTimedSendCleanupWait   = 2 * time.Second
	defaultPacketDedupeEntries    = 4096
	octetStreamCodec              = "application/octet-stream"
	reconnectJitterMinMultiplier  = 0.8
	reconnectJitterSpanMultiplier = 0.4
	downstreamDeliveryReceiptType = "downstream.delivered"
	deliveryReceiptStreamIDKey    = "stream_id"
	deliveryReceiptSequenceKey    = "sequence"
	deliveryReceiptPartitionKey   = "partition"
	deliveryReceiptSizeBytesKey   = "size_bytes"
)

var errPacketDeduperPendingOverflow = errors.New("packet dedupe pending capacity exceeded")
var errTimedSendCleanupTimeout = errors.New("timed send worker cleanup exceeded timeout")
var errControlStartupHandshakeTimeout = errors.New("control startup handshake timed out")

func validateTransportMessage(kind string, msg proto.Message) error {
	if msg == nil {
		return nil
	}
	if err := tractatusvalidation.Validate(msg); err != nil {
		return fmt.Errorf("transport %s invalid: %w", kind, err)
	}
	return nil
}

type reconnectPolicy struct {
	base       time.Duration
	max        time.Duration
	maxRetries int
}

type streamRuntimeOptions struct {
	messageTimeout           time.Duration
	publishHeartbeatInterval time.Duration
	channelSendTimeout       time.Duration
	controlHeartbeatInterval time.Duration
	hangWatcher              *hangWatcher
	packetDeduper            *packetDeduper
	controlRequests          chan *transportpb.ControlRequest
	sendTracker              *timedSendTracker
	startupHandshake         *controlStartupHandshake
}

type transportSessionRunner[C any] func(
	context.Context, string, bindingReference,
	engram.StreamingEngram[C], envResolver,
) error
type reconnectSleepFunc func(context.Context, time.Duration) error
type reconnectDelayFunc func(time.Duration, time.Duration) time.Duration

type packetDeduperContextKey struct{}
type controlRequestQueueContextKey struct{}

var reconnectJitterFloat64 = func() float64 {
	return randv2.Float64()
}

func resolveReconnectPolicy(env envResolver) reconnectPolicy {
	base := defaultReconnectBaseBackoff
	if d := parsePositiveDuration(env.lookup(contracts.GRPCReconnectBaseBackoffEnv)); d > 0 {
		base = d
	}

	max := defaultReconnectMaxBackoff
	if d := parsePositiveDuration(env.lookup(contracts.GRPCReconnectMaxBackoffEnv)); d > 0 {
		max = d
	}

	maxRetries := defaultReconnectMaxRetries
	if raw := strings.TrimSpace(env.lookup(contracts.GRPCReconnectMaxRetriesEnv)); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			slog.Default().Warn("ignoring invalid reconnect max retries env var",
				"key", contracts.GRPCReconnectMaxRetriesEnv, "value", raw)
		} else if n < 0 {
			maxRetries = -1
		} else {
			maxRetries = n
		}
	}

	return reconnectPolicy{
		base:       base,
		max:        max,
		maxRetries: maxRetries,
	}
}

func parsePositiveDuration(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0
	}
	return d
}

const (
	controlBindingMetadataKey       = "bubu.transport.binding"
	defaultControlHeartbeatInterval = 30 * time.Second
	// defaultPublishHeartbeatInterval keeps the publish stream alive when the
	// engram has nothing to output.  Must be shorter than the connector's
	// MessageTimeout (default 30 s) so the server-side RecvWithTimeout never
	// fires DeadlineExceeded on an idle-but-healthy publish stream.
	defaultPublishHeartbeatInterval = 10 * time.Second
)

// StartStreamServer boots a StreamingEngram using the new transport connector contract.
// The Engram must have BUBU_TRANSPORT_BINDING set; no other transport modes are supported.
func StartStreamServer[C any](ctx context.Context, e engram.StreamingEngram[C]) error {
	ctx, _ = withDefaultLogger(ctx)
	defer publishCapturedLogs(ctx)
	logger := LoggerFromContext(ctx)
	logger.Info("Initializing Bubu SDK for streaming execution")

	config, secrets, err := loadStreamingExecutionContext[C](ctx)
	if err != nil {
		return err
	}

	if err := callWithPanicRecoveryNoValue("streaming engram Init", func() error {
		return e.Init(ctx, config, secrets)
	}); err != nil {
		return fmt.Errorf("streaming engram initialization failed: %w", err)
	}

	ref, err := bindingReferenceFromEnv()
	if err != nil {
		return err
	}
	if ref.Info == nil {
		return fmt.Errorf("transport binding missing inline payload")
	}
	endpoint := strings.TrimSpace(ref.endpoint())
	if endpoint == "" {
		return fmt.Errorf("transport binding missing endpoint")
	}

	logger.Info("Connecting to transport connector", "endpoint", endpoint, "driver", normalizedDriver(ref))
	return runTransportConnectorStream(ctx, endpoint, ref, e)
}

func runTransportConnectorStream[C any](
	ctx context.Context,
	endpoint string,
	ref bindingReference,
	e engram.StreamingEngram[C],
) error {
	env := newEnvResolver(ref.envOverrides())
	return runTransportConnectorStreamWithDeps(
		ctx,
		endpoint,
		ref,
		e,
		env,
		runTransportSession[C],
		sleepWithContext,
		jitterReconnectDelay,
	)
}

func runTransportConnectorStreamWithDeps[C any](
	ctx context.Context,
	endpoint string,
	ref bindingReference,
	e engram.StreamingEngram[C],
	env envResolver,
	sessionFn transportSessionRunner[C],
	sleepFn reconnectSleepFunc,
	delayFn reconnectDelayFunc,
) error {
	if sessionFn == nil {
		sessionFn = runTransportSession[C]
	}
	if sleepFn == nil {
		sleepFn = sleepWithContext
	}
	if delayFn == nil {
		delayFn = jitterReconnectDelay
	}

	bufferSize := resolveChannelBufferSize(env)
	baseSessionCtx := withPacketDeduperContext(ctx, newPacketDeduper(defaultPacketDedupeEntries))
	policy := resolveReconnectPolicy(env)
	backoff := policy.base
	if backoff <= 0 {
		backoff = time.Second
	}
	retriesRemaining := policy.maxRetries
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Use a fresh internal control queue per reconnect attempt so stale
		// requests from a failed session cannot leak into the next session.
		sessionCtx := withControlRequestQueueContext(baseSessionCtx, make(chan *transportpb.ControlRequest, bufferSize))
		err := sessionFn(sessionCtx, endpoint, ref, e, env)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if !isRetriableTransportSessionError(err) {
			return err
		}

		LoggerFromContext(ctx).Error("Transport connector session ended; retrying", "error", err)

		if retriesRemaining >= 0 {
			retriesRemaining--
			if retriesRemaining < 0 {
				return fmt.Errorf("transport connector retries exhausted: %w", err)
			}
		}

		wait := delayFn(backoff, policy.max)
		if err := sleepFn(ctx, wait); err != nil {
			return err
		}

		backoff = nextReconnectBackoff(backoff, policy.max)
	}
}

func runTransportSession[C any]( //nolint:gocyclo
	ctx context.Context,
	endpoint string,
	ref bindingReference,
	e engram.StreamingEngram[C],
	env envResolver,
) (err error) {
	sessionCtx, cancel := context.WithCancel(ctx)

	logger := LoggerFromContext(ctx)
	sendTracker := newTimedSendTracker()
	defer func() {
		if cleanupErr := waitForTimedSendCleanup(logger, sendTracker, defaultTimedSendCleanupWait); cleanupErr != nil {
			if err == nil {
				err = cleanupErr
				return
			}
			err = errors.Join(err, cleanupErr)
		}
	}()
	defer cancel()

	conn, err := connectorDial(sessionCtx, endpoint, env)
	if err != nil {
		return err
	}
	if isDebugEnabled() {
		logger.Debug("Transport connector session established",
			slog.String("endpoint", endpoint),
			slog.String("driver", normalizedDriver(ref)),
		)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			logger.Warn("Failed to close transport connector", "error", closeErr)
		}
	}()

	bufferSize := resolveChannelBufferSize(env)
	in := make(chan engram.InboundMessage, bufferSize)
	out := make(chan engram.StreamMessage, bufferSize)
	controlRequests := controlRequestQueueFromContext(sessionCtx)
	if controlRequests == nil {
		controlRequests = make(chan *transportpb.ControlRequest, bufferSize)
	}

	messageTimeout := resolveMessageTimeout(env)
	opts := streamRuntimeOptions{
		messageTimeout:           messageTimeout,
		publishHeartbeatInterval: resolvePublishHeartbeatInterval(env, messageTimeout),
		channelSendTimeout:       resolveChannelSendTimeout(env),
		controlHeartbeatInterval: resolveControlHeartbeatInterval(env),
		packetDeduper:            packetDeduperFromContext(sessionCtx),
		controlRequests:          controlRequests,
		sendTracker:              sendTracker,
		startupHandshake:         newControlStartupHandshake(),
	}
	if hangTimeout := resolveHangTimeout(env); hangTimeout > 0 {
		opts.hangWatcher = newHangWatcher(sessionCtx, hangTimeout, cancel)
		defer opts.hangWatcher.Stop()
	}

	// Open a single bidirectional Data stream for both sending and receiving.
	dataCtx, dataCancel := context.WithCancel(sessionCtx)
	defer dataCancel()
	dataCtx = metadata.NewOutgoingContext(dataCtx, metadata.Pairs(
		coretransport.ProtocolMetadataKey, coretransport.ProtocolVersion,
	))
	dataStream, err := conn.Client().Data(dataCtx)
	if err != nil {
		return fmt.Errorf("data stream open failed: %w", err)
	}

	g, gctx := errgroup.WithContext(sessionCtx)
	var streamCompletedGracefully atomic.Bool
	var primarySessionErr struct {
		sync.Mutex
		err error
	}
	recordPrimarySessionErr := func(candidate error) {
		if candidate == nil {
			return
		}
		if errors.Is(candidate, context.Canceled) && !errors.Is(candidate, io.EOF) &&
			!errors.Is(candidate, errTimedSendCleanupTimeout) &&
			!errors.Is(candidate, errControlStartupHandshakeTimeout) {
			return
		}
		if errors.Is(candidate, context.DeadlineExceeded) {
			return
		}
		primarySessionErr.Lock()
		defer primarySessionErr.Unlock()
		if primarySessionErr.err == nil {
			primarySessionErr.err = candidate
		}
	}
	loadPrimarySessionErr := func() error {
		primarySessionErr.Lock()
		defer primarySessionErr.Unlock()
		return primarySessionErr.err
	}
	handler := controlDirectiveHandler(e)
	g.Go(func() error {
		logger.Debug("SDK: Starting Control loop goroutine")
		err := connectorControlLoop(gctx, conn.Client(), ref, handler, opts)
		recordPrimarySessionErr(err)
		cancel()
		return err
	})
	if err := opts.startupHandshake.Wait(sessionCtx, messageTimeout); err != nil {
		cancel()
		groupErr := g.Wait()
		if groupErr != nil && !errors.Is(groupErr, context.Canceled) {
			return groupErr
		}
		return err
	}
	g.Go(func() error {
		defer close(in)
		logger.Debug("SDK: Starting Data recv loop goroutine")
		recvErr := connectorDataRecvLoop(gctx, dataStream, in, opts)
		if recvErr == nil {
			// A closed Data recv stream with no session cancellation leaves the
			// session half-open (control/send loops still running). Surface this
			// as a retriable session failure so reconnect logic can recover.
			if err := gctx.Err(); err != nil {
				cancel()
				return err
			}
			cancel()
			err := fmt.Errorf("data stream recv failed: %w", io.EOF)
			recordPrimarySessionErr(err)
			return err
		}
		recordPrimarySessionErr(recvErr)
		cancel()
		return recvErr
	})
	g.Go(func() error {
		logger.Debug("SDK: Starting Data send loop goroutine")
		err := connectorDataSendLoop(gctx, dataStream, out, opts)
		recordPrimarySessionErr(err)
		if err != nil {
			cancel()
		}
		return err
	})
	g.Go(func() error {
		defer close(out)
		logger.Debug("SDK: Starting Engram Stream goroutine - calling e.Stream()")
		err := callWithPanicRecoveryNoValue("streaming engram Stream", func() error {
			return e.Stream(gctx, in, out)
		})
		recordPrimarySessionErr(err)
		if err == nil {
			streamCompletedGracefully.Store(true)
		}
		cancel()
		logger.Debug("SDK: Engram Stream goroutine exited", "error", err)
		return err
	})

	logger.Debug("SDK: All goroutines started, waiting for completion")
	err = g.Wait()
	logger.Debug("SDK: All goroutines completed", "error", err)
	if streamCompletedGracefully.Load() && errors.Is(err, context.Canceled) {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		if primary := loadPrimarySessionErr(); primary != nil {
			return primary
		}
	}
	return err
}

func connectorDataRecvLoop( //nolint:gocyclo
	ctx context.Context,
	stream transportpb.TransportConnectorService_DataClient,
	in chan<- engram.InboundMessage,
	opts streamRuntimeOptions,
) error {
	logger := LoggerFromContext(ctx)
	logger.Debug("SDK: Data recv loop started, waiting for packets from connector")
	reassembler := newDataChunkReassembler(defaultChunkReassemblyTTL, 0, 0)
	defer reassembler.Stop()
	deduper := opts.packetDeduper
	if deduper == nil {
		deduper = newPacketDeduper(defaultPacketDedupeEntries)
	}
	deduper.StartSession()
	packetCount := 0
	for {
		// Do NOT use RecvWithTimeout here — Data streams can be idle for long
		// stretches. A hard timeout kills the stream and cascades cancellations.
		// Liveness is handled by gRPC keepalive and the optional hang watcher.
		resp, err := stream.Recv()
		if err == io.EOF {
			logger.Debug("SDK: Data stream closed by connector", "packetsReceived", packetCount)
			return nil
		}
		if err != nil {
			logger.Error("SDK: Data receive failed", "error", err, "packetsReceived", packetCount)
			return fmt.Errorf("data receive failed: %w", err)
		}
		// Heartbeat/keepalive packets have no frame — skip them.
		if resp.GetFrame() == nil {
			queueDownstreamDeliveryReceipt(ctx, opts, dataResponseToPublishRequest(resp))
			if opts.hangWatcher != nil {
				opts.hangWatcher.Touch()
			}
			continue
		}
		assembled, complete, err := reassembler.Add(dataResponseToPublishRequest(resp))
		if err != nil {
			logger.Error("SDK: Chunk reassembly failed", "error", err)
			return err
		}
		if !complete {
			if opts.hangWatcher != nil {
				opts.hangWatcher.Touch()
			}
			continue
		}
		status, key, generation := deduper.Begin(assembled) //nolint:revive
		switch status {
		case packetDuplicatePending:
			logger.Debug("SDK: Dropping duplicate packet still pending processing", "key", key)
			if opts.hangWatcher != nil {
				opts.hangWatcher.Touch()
			}
			continue
		case packetDuplicateCompleted:
			logger.Debug("SDK: Dropping completed duplicate packet from Data stream", "key", key)
			queueDownstreamDeliveryReceipt(ctx, opts, assembled)
			if opts.hangWatcher != nil {
				opts.hangWatcher.Touch()
			}
			continue
		case packetPendingOverflow:
			return fmt.Errorf("%w: limit=%d", errPacketDeduperPendingOverflow, deduper.maxEntries)
		}
		packetCount++
		logger.Debug("SDK: Received packet from connector Data stream", "packetCount", packetCount)
		msg, err := publishRequestToStreamMessage(assembled)
		if err != nil {
			logger.Error("SDK: Failed to translate packet", "error", err)
			return err
		}
		if drop, reason := shouldDropSubscribeMessage(msg); drop {
			logger.Debug("SDK: Dropping packet from Data stream", "reason", reason)
			receipt := downstreamDeliveryReceiptControlRequest(assembled)
			if receipt == nil || queueControlRequest(ctx, opts.controlRequests, receipt) {
				deduper.Complete(key, generation)
			} else {
				deduper.Release(key, generation)
			}
			if opts.hangWatcher != nil {
				opts.hangWatcher.Touch()
			}
			continue
		}
		logger.Debug("SDK: Translated packet, enqueueing for engram", "packetCount", packetCount)
		if opts.hangWatcher != nil {
			opts.hangWatcher.Touch()
		}
		inbound := attachDownstreamProcessingReceipt(ctx, opts, engram.NewInboundMessage(msg),
			assembled, deduper, key, generation)
		delivered, err := enqueueStreamMessage(ctx, in, inbound, opts.channelSendTimeout)
		if err != nil {
			logger.Error("SDK: Failed to enqueue packet for engram", "error", err)
			return err
		}
		if !delivered {
			deduper.Release(key, generation)
			logger.Debug("SDK: Dropped packet before engram consumption", "packetCount", packetCount)
			continue
		}
		logger.Debug("SDK: Packet enqueued for engram successfully", "packetCount", packetCount)
	}
}

//nolint:lll,unparam
func queueDownstreamDeliveryReceipt(ctx context.Context, opts streamRuntimeOptions, req *transportpb.PublishRequest) bool {
	return queueControlRequest(ctx, opts.controlRequests, downstreamDeliveryReceiptControlRequest(req))
}

func attachDownstreamProcessingReceipt(
	ctx context.Context,
	opts streamRuntimeOptions,
	msg engram.InboundMessage,
	req *transportpb.PublishRequest,
	deduper *packetDeduper,
	key string,
	generation uint64,
) engram.InboundMessage {
	receipt := downstreamDeliveryReceiptControlRequest(req)
	if receipt == nil && key == "" {
		return msg
	}
	return engram.BindProcessingReceipt(msg, func() {
		if receipt == nil {
			deduper.Complete(key, generation)
			return
		}
		if queueControlRequest(ctx, opts.controlRequests, receipt) {
			deduper.Complete(key, generation)
			return
		}
		// If the receipt cannot be queued, release the pending entry so a later
		// replay can be delivered instead of being suppressed as already handled.
		deduper.Release(key, generation)
	})
}

func queueControlRequest(ctx context.Context, controlRequests chan *transportpb.ControlRequest, req *transportpb.ControlRequest) bool { //nolint:lll
	if controlRequests == nil || req == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return false
	default:
	}
	if safeSendControlRequest(ctx, controlRequests, req) {
		return true
	}
	if ctx.Err() == nil {
		LoggerFromContext(ctx).Debug("Dropping internal control request because control queue is unavailable",
			"action", req.GetCustomAction())
	}
	return false
}

func safeSendControlRequest(ctx context.Context, controlRequests chan *transportpb.ControlRequest, req *transportpb.ControlRequest) (sent bool) { //nolint:lll
	defer func() {
		if recover() != nil {
			sent = false
		}
	}()
	select {
	case controlRequests <- req:
		return true
	case <-ctx.Done():
		return false
	default:
		return false
	}
}

func downstreamDeliveryReceiptControlRequest(req *transportpb.PublishRequest) *transportpb.ControlRequest {
	if req == nil {
		return nil
	}
	env := req.GetEnvelope()
	if env == nil || env.GetSequence() == 0 {
		return nil
	}
	streamID := strings.TrimSpace(env.GetStreamId())
	if streamID == "" {
		return nil
	}
	dataReq := publishRequestToDataRequest(req)
	if dataReq == nil {
		return nil
	}
	return &transportpb.ControlRequest{
		CustomAction: downstreamDeliveryReceiptType,
		Metadata: map[string]string{
			deliveryReceiptStreamIDKey:  streamID,
			deliveryReceiptSequenceKey:  strconv.FormatUint(env.GetSequence(), 10),
			deliveryReceiptPartitionKey: strings.TrimSpace(env.GetPartition()),
			deliveryReceiptSizeBytesKey: strconv.Itoa(proto.Size(dataReq)),
		},
	}
}

type packetDeduper struct {
	mu         sync.Mutex
	maxEntries int
	order      []string
	completed  map[string]struct{}
	pending    map[string]uint64
	generation uint64
}

func newPacketDeduper(maxEntries int) *packetDeduper {
	if maxEntries <= 0 {
		maxEntries = defaultPacketDedupeEntries
	}
	return &packetDeduper{
		maxEntries: maxEntries,
		order:      make([]string, 0, maxEntries),
		completed:  make(map[string]struct{}, maxEntries),
		pending:    make(map[string]uint64, maxEntries),
	}
}

func withPacketDeduperContext(ctx context.Context, deduper *packetDeduper) context.Context {
	if ctx == nil || deduper == nil {
		return ctx
	}
	return context.WithValue(ctx, packetDeduperContextKey{}, deduper)
}

func packetDeduperFromContext(ctx context.Context) *packetDeduper {
	if ctx == nil {
		return nil
	}
	deduper, _ := ctx.Value(packetDeduperContextKey{}).(*packetDeduper)
	return deduper
}

func withControlRequestQueueContext(ctx context.Context, queue chan *transportpb.ControlRequest) context.Context {
	if ctx == nil || queue == nil {
		return ctx
	}
	return context.WithValue(ctx, controlRequestQueueContextKey{}, queue)
}

func controlRequestQueueFromContext(ctx context.Context) chan *transportpb.ControlRequest {
	if ctx == nil {
		return nil
	}
	queue, _ := ctx.Value(controlRequestQueueContextKey{}).(chan *transportpb.ControlRequest)
	return queue
}

type packetDuplicateStatus int

const (
	packetNew packetDuplicateStatus = iota
	packetDuplicatePending
	packetDuplicateCompleted
	packetPendingOverflow
)

func (d *packetDeduper) StartSession() {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.generation++
	clear(d.pending)
}

func (d *packetDeduper) Begin(req *transportpb.PublishRequest) (packetDuplicateStatus, string, uint64) {
	if d == nil {
		return packetNew, "", 0
	}
	key := dedupeKeyForPacket(req)
	if key == "" {
		return packetNew, "", 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	generation := d.generation
	if pendingGeneration, ok := d.pending[key]; ok && pendingGeneration == generation {
		return packetDuplicatePending, key, generation
	}
	if _, ok := d.completed[key]; ok {
		return packetDuplicateCompleted, key, generation
	}
	if d.maxEntries > 0 && len(d.pending) >= d.maxEntries {
		return packetPendingOverflow, key, generation
	}
	d.pending[key] = generation
	return packetNew, key, generation
}

func (d *packetDeduper) Complete(key string, generation uint64) {
	if d == nil || key == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if pendingGeneration, ok := d.pending[key]; !ok || pendingGeneration != generation {
		return
	}
	delete(d.pending, key)
	if _, ok := d.completed[key]; ok {
		return
	}
	if d.maxEntries > 0 && len(d.order) >= d.maxEntries {
		evicted := d.order[0]
		d.order = d.order[1:]
		delete(d.completed, evicted)
	}
	d.order = append(d.order, key)
	d.completed[key] = struct{}{}
}

func (d *packetDeduper) Release(key string, generation uint64) {
	if d == nil || key == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if pendingGeneration, ok := d.pending[key]; ok && pendingGeneration == generation {
		delete(d.pending, key)
	}
}

func dedupeKeyForPacket(req *transportpb.PublishRequest) string {
	if req == nil {
		return ""
	}
	if key := dedupeKeyForEnvelope(req.GetEnvelope()); key != "" {
		return "env|" + key
	}
	if key := dedupeKeyForMessageID(req.GetMetadata()); key != "" {
		return "msg|" + key
	}
	return ""
}

func dedupeKeyForEnvelope(env *transportpb.StreamEnvelope) string {
	if env == nil {
		return ""
	}
	streamID := strings.TrimSpace(env.GetStreamId())
	if streamID == "" || env.GetSequence() == 0 {
		return ""
	}
	return streamID + "|" + strings.TrimSpace(env.GetPartition()) + "|" + strconv.FormatUint(env.GetSequence(), 10)
}

func dedupeKeyForMessageID(metadata map[string]string) string { //nolint:revive
	if len(metadata) == 0 {
		return ""
	}
	return strings.TrimSpace(metadata[metadataEnvelopeMessageIDKey])
}

func shouldDropSubscribeMessage(msg engram.StreamMessage) (bool, string) {
	if isMessageHeartbeat(msg) {
		return true, "heartbeat" //nolint:goconst
	}
	if isMessageNoop(msg) {
		return true, "noop" //nolint:goconst
	}
	if isMessageEmpty(msg) {
		return true, "empty"
	}
	return false, ""
}

func isMessageHeartbeat(msg engram.StreamMessage) bool {
	if msg.Metadata != nil && strings.EqualFold(strings.TrimSpace(msg.Metadata["bubu-heartbeat"]), "true") {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(msg.Kind), engram.StreamMessageKindHeartbeat)
}

func isMessageNoop(msg engram.StreamMessage) bool {
	return strings.EqualFold(strings.TrimSpace(msg.Kind), engram.StreamMessageKindNoop)
}

func isMessageEmpty(msg engram.StreamMessage) bool {
	if msg.Audio != nil && len(msg.Audio.PCM) > 0 {
		return false
	}
	if msg.Video != nil && len(msg.Video.Payload) > 0 {
		return false
	}
	if msg.Binary != nil && len(msg.Binary.Payload) > 0 {
		return false
	}
	if len(msg.Payload) > 0 || len(msg.Inputs) > 0 {
		return false
	}
	if strings.TrimSpace(msg.Kind) != "" || strings.TrimSpace(msg.MessageID) != "" || !msg.Timestamp.IsZero() {
		return false
	}
	if len(msg.Transports) > 0 || msg.Envelope != nil {
		return false
	}
	return true
}

func connectorDataSendLoop(
	ctx context.Context,
	stream transportpb.TransportConnectorService_DataClient,
	out <-chan engram.StreamMessage,
	opts streamRuntimeOptions,
) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Heartbeat ticker keeps the data stream alive when the engram has
	// nothing to output (e.g. VAD detected no speech, STT has no transcript).
	// Without this the connector's RecvWithTimeout fires DeadlineExceeded
	// after MessageTimeout (default 30 s), tearing down all streams.
	heartbeatInterval := opts.publishHeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = defaultPublishHeartbeatInterval
	}
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-streamCtx.Done():
			bestEffortCloseSend(streamCtx, "data", stream.CloseSend)
			// Best-effort drain any buffered messages without waiting for the
			// producer to close the channel. Cancellation must not block
			// forever when an engram ignores context shutdown.
			drainStreamMessages(out)
			return streamCtx.Err()
		case msg, ok := <-out:
			if !ok {
				if err := stream.CloseSend(); err != nil {
					return fmt.Errorf("data stream close send failed: %w", err)
				}
				return nil
			}
			injectTraceContext(ctx, &msg)
			req, err := streamMessageToPublishRequest(msg)
			if err != nil {
				return err
			}
			dataReq := publishRequestToDataRequest(req)
			if err := validateTransportMessage("data request", dataReq); err != nil {
				return err
			}
			if err := callSendWithTimeout(streamCtx, opts.messageTimeout, cancel, "data send", opts.sendTracker, func() error {
				return stream.Send(dataReq)
			}); err != nil {
				return fmt.Errorf("data stream send failed: %w", err)
			}
			if opts.hangWatcher != nil {
				opts.hangWatcher.Touch()
			}
			ticker.Reset(heartbeatInterval)
		case <-ticker.C:
			hb := &transportpb.DataRequest{
				Metadata: map[string]string{"bubu-heartbeat": "true"},
			}
			if err := validateTransportMessage("data request", hb); err != nil {
				return err
			}
			if err := callSendWithTimeout(streamCtx, opts.messageTimeout, cancel, "data heartbeat", opts.sendTracker, func() error { //nolint:lll
				return stream.Send(hb)
			}); err != nil {
				return fmt.Errorf("data heartbeat send failed: %w", err)
			}
			if opts.hangWatcher != nil {
				opts.hangWatcher.Touch()
			}
		}
	}
}

func drainStreamMessages(out <-chan engram.StreamMessage) {
	for {
		select {
		case _, ok := <-out:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

func connectorControlLoop( //nolint:gocyclo
	ctx context.Context,
	client transportpb.TransportConnectorServiceClient,
	ref bindingReference,
	handler engram.ControlDirectiveHandler,
	opts streamRuntimeOptions,
) error {
	controlCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	mdPairs := []string{
		coretransport.ProtocolMetadataKey, coretransport.ProtocolVersion,
	}
	if bindingID := controlBindingMetadataValue(ref); bindingID != "" {
		mdPairs = append(mdPairs, controlBindingMetadataKey, bindingID)
	}
	controlCtx = metadata.NewOutgoingContext(controlCtx, metadata.Pairs(mdPairs...))
	stream, err := client.Control(controlCtx)
	if err != nil {
		return fmt.Errorf("control stream open failed: %w", err)
	}
	heartbeatInterval := opts.controlHeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = defaultControlHeartbeatInterval
	}
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	recvCh := startControlReceiver(controlCtx, stream)
	controlRequests := opts.controlRequests

	for {
		select {
		case <-controlCtx.Done():
			bestEffortCloseSend(controlCtx, "control", stream.CloseSend)
			return controlCtx.Err()
		case req, ok := <-controlRequests:
			if !ok {
				controlRequests = nil
				continue
			}
			if req == nil {
				continue
			}
			if err := validateTransportMessage("control request", req); err != nil {
				return err
			}
			if err := callSendWithTimeout(controlCtx, opts.messageTimeout, cancel, "control internal send", opts.sendTracker, func() error { //nolint:lll
				return stream.Send(req)
			}); err != nil {
				return fmt.Errorf("control stream send failed: %w", err)
			}
			if opts.hangWatcher != nil {
				opts.hangWatcher.Touch()
			}
		case msg, ok := <-recvCh:
			if !ok {
				if err := controlCtx.Err(); err != nil {
					return err
				}
				return fmt.Errorf("control stream recv failed: %w", io.EOF)
			}
			if err := opts.startupHandshake.Observe(msg.response); err != nil {
				return err
			}
			if opts.hangWatcher != nil {
				opts.hangWatcher.Touch()
			}
			if err := processControlMessage(controlCtx, stream, handler, msg, opts.messageTimeout, cancel, opts.sendTracker); err != nil { //nolint:lll
				if err == io.EOF {
					if cerr := controlCtx.Err(); cerr != nil {
						return cerr
					}
					return fmt.Errorf("control stream recv failed: %w", err)
				}
				return err
			}
		case <-ticker.C:
			if err := sendControlHeartbeat(controlCtx, stream, opts.messageTimeout, cancel, opts.sendTracker); err != nil {
				return err
			}
			if opts.hangWatcher != nil {
				opts.hangWatcher.Touch()
			}
		}
	}
}

func bestEffortCloseSend(ctx context.Context, streamName string, closeSend func() error) {
	if closeSend == nil {
		return
	}
	if err := closeSend(); err != nil {
		LoggerFromContext(ctx).Debug("best-effort close send failed during cancellation",
			"stream", streamName,
			"error", err)
	}
}

type controlMessage struct {
	response *transportpb.ControlResponse
	err      error
}

type controlStartupHandshake struct {
	mu                    sync.Mutex
	doneCh                chan struct{}
	readySeen             bool
	capabilityRequired    bool
	startupCapabilitySeen bool
	err                   error
	once                  sync.Once
}

func newControlStartupHandshake() *controlStartupHandshake {
	return &controlStartupHandshake{doneCh: make(chan struct{})}
}

func (h *controlStartupHandshake) Observe(resp *transportpb.ControlResponse) error {
	if h == nil || resp == nil {
		return nil
	}
	typ := normalizeControlType(protoControlActionToType(resp.GetAction(), resp.GetCustomAction()))
	h.mu.Lock()
	defer h.mu.Unlock()
	switch typ {
	case "connector.ready": //nolint:goconst
		h.readySeen = true
		mode, err := coretransport.NormalizeStartupCapabilitiesMode(resp.GetMetadata()[coretransport.StartupCapabilitiesMetadataKey]) //nolint:lll
		if err != nil {
			h.err = fmt.Errorf("invalid connector.ready startup metadata: %w", err)
			h.finishLocked()
			return h.err
		}
		h.capabilityRequired = mode == coretransport.StartupCapabilitiesRequired
		h.finishLocked()
	case "connector.capabilities": //nolint:goconst
		h.startupCapabilitySeen = true
		h.finishLocked()
	}
	return nil
}

func (h *controlStartupHandshake) finishLocked() {
	if h.err != nil {
		h.once.Do(func() {
			close(h.doneCh)
		})
		return
	}
	if !h.readySeen {
		return
	}
	if h.capabilityRequired && !h.startupCapabilitySeen {
		return
	}
	h.once.Do(func() {
		close(h.doneCh)
	})
}

func (h *controlStartupHandshake) Wait(ctx context.Context, timeout time.Duration) error {
	if h == nil {
		return nil
	}
	if timeout <= 0 {
		timeout = defaultMessageTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-h.doneCh:
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.err
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("%w: timeout=%s", errControlStartupHandshakeTimeout, timeout)
	}
}

func startControlReceiver(
	ctx context.Context,
	stream transportpb.TransportConnectorService_ControlClient,
) <-chan controlMessage {
	recvCh := make(chan controlMessage, 1)
	go func() {
		defer close(recvCh)
		for {
			// Control streams can be idle for long stretches. Do NOT enforce a
			// recv timeout here; gRPC keepalive + optional hang watcher handle liveness.
			response, recvErr := stream.Recv()
			if recvErr != nil {
				sendControlMessage(ctx, recvCh, controlMessage{err: recvErr})
				return
			}
			if !sendControlMessage(ctx, recvCh, controlMessage{response: response}) {
				return
			}
		}
	}()
	return recvCh
}

func sendControlMessage(ctx context.Context, recvCh chan<- controlMessage, msg controlMessage) bool {
	select {
	case recvCh <- msg:
		return true
	case <-ctx.Done():
		return false
	}
}

func resolveControlHeartbeatInterval(env envResolver) time.Duration {
	if d := parsePositiveDuration(env.lookup(contracts.TransportHeartbeatIntervalEnv)); d > 0 {
		return d
	}
	return defaultControlHeartbeatInterval
}

func resolvePublishHeartbeatInterval(env envResolver, messageTimeout time.Duration) time.Duration {
	interval := defaultPublishHeartbeatInterval
	if d := parsePositiveDuration(env.lookup(contracts.GRPCHeartbeatIntervalEnv)); d > 0 {
		interval = d
	}
	if messageTimeout > 0 && interval >= messageTimeout {
		clamped := messageTimeout - time.Nanosecond
		if clamped <= 0 {
			clamped = time.Nanosecond
		}
		slog.Default().Warn("clamping publish heartbeat interval below message timeout",
			"heartbeatInterval", interval,
			"messageTimeout", messageTimeout,
			"clamped", clamped,
			"key", contracts.GRPCHeartbeatIntervalEnv,
		)
		return clamped
	}
	return interval
}

func resolveChannelBufferSize(env envResolver) int {
	return transportconnector.ChannelBufferSize(env, transportconnector.DefaultChannelBufferSize)
}

func resolveMessageTimeout(env envResolver) time.Duration {
	return transportconnector.MessageTimeout(env, defaultMessageTimeout)
}

func resolveChannelSendTimeout(env envResolver) time.Duration {
	return transportconnector.ChannelSendTimeout(env)
}

func resolveHangTimeout(env envResolver) time.Duration {
	return transportconnector.HangTimeout(env)
}

func processControlMessage(
	ctx context.Context,
	stream transportpb.TransportConnectorService_ControlClient,
	handler engram.ControlDirectiveHandler,
	msg controlMessage,
	timeout time.Duration,
	cancel context.CancelFunc,
	tracker *timedSendTracker,
) error {
	if msg.err != nil {
		if msg.err == io.EOF {
			return io.EOF
		}
		return fmt.Errorf("control stream recv failed: %w", msg.err)
	}
	if err := validateTransportMessage("control response", msg.response); err != nil {
		return err
	}

	response, handleErr := handler.HandleControlDirective(ctx, protoToControlDirective(msg.response))
	if handleErr != nil {
		response = &engram.ControlDirective{
			Type: "error",
			Metadata: map[string]string{
				"message": handleErr.Error(),
			},
		}
	}
	if response == nil {
		return nil
	}
	req := controlDirectiveToProto(response)
	if err := validateTransportMessage("control request", req); err != nil {
		return err
	}
	if err := callSendWithTimeout(ctx, timeout, cancel, "control send", tracker, func() error {
		return stream.Send(req)
	}); err != nil {
		return fmt.Errorf("control stream send failed: %w", err)
	}
	return nil
}

func sendControlHeartbeat(
	ctx context.Context,
	stream transportpb.TransportConnectorService_ControlClient,
	timeout time.Duration,
	cancel context.CancelFunc,
	tracker *timedSendTracker,
) error {
	req := connectorHeartbeatDirective()
	if err := validateTransportMessage("control request", req); err != nil {
		return err
	}
	if err := callSendWithTimeout(ctx, timeout, cancel, "control heartbeat", tracker, func() error {
		return stream.Send(req)
	}); err != nil {
		return fmt.Errorf("control stream heartbeat failed: %w", err)
	}
	return nil
}

func connectorHeartbeatDirective() *transportpb.ControlRequest {
	return controlDirectiveToProto(&engram.ControlDirective{
		Type: "heartbeat",
		Metadata: map[string]string{
			"ts": strconv.FormatInt(time.Now().UnixMilli(), 10),
		},
	})
}

func controlDirectiveHandler[C any](e engram.StreamingEngram[C]) engram.ControlDirectiveHandler {
	if handler, ok := any(e).(engram.ControlDirectiveHandler); ok {
		return handler
	}
	return defaultControlDirectiveHandler{}
}

type defaultControlDirectiveHandler struct{}

func (defaultControlDirectiveHandler) HandleControlDirective(
	ctx context.Context,
	directive engram.ControlDirective,
) (*engram.ControlDirective, error) {
	logger := LoggerFromContext(ctx)
	typ := strings.ToLower(strings.TrimSpace(directive.Type))
	switch typ {
	case "", "noop":
		logger.Debug("Control directive noop; acknowledging", "type", directive.Type)
		return ackControlDirective(directive.Type, false, "noop"), nil
	case "connector.ready":
		logger.Info("Connector startup ready received", "metadata", directive.Metadata)
		return ackControlDirective(directive.Type, true, "startup"), nil
	case "connector.capabilities":
		logger.Info("Connector capability update received", "metadata", directive.Metadata)
		return ackControlDirective(directive.Type, true, "capabilities"), nil
	case "handoff.draining", "handoff.cutover", "handoff.ready": //nolint:goconst
		logger.Info("Handoff directive received", "type", directive.Type, "metadata", directive.Metadata)
		return ackControlDirective(directive.Type, true, "handoff"), nil
	case "ack": //nolint:goconst
		logger.Debug("Control acknowledgement received", "metadata", directive.Metadata)
		return nil, nil
	default:
		logger.Debug("Control directive received with no handler registered; acknowledging as noop", "type", directive.Type)
		return ackControlDirective(directive.Type, false, "no_control_handler"), nil
	}
}

func ackControlDirective(originalType string, handled bool, reason string) *engram.ControlDirective {
	return &engram.ControlDirective{
		Type: "ack",
		Metadata: map[string]string{
			"handled": strconv.FormatBool(handled),
			"reason":  reason,
			"type":    strings.TrimSpace(originalType),
		},
	}
}

func normalizeControlType(val string) string {
	typ := strings.ToLower(strings.TrimSpace(val))
	if typ == "" {
		return "unknown"
	}
	return typ
}

func protoToControlDirective(d *transportpb.ControlResponse) engram.ControlDirective {
	if d == nil {
		return engram.ControlDirective{}
	}
	return engram.ControlDirective{
		Type:     protoControlActionToType(d.GetAction(), d.GetCustomAction()),
		Metadata: cloneStringMap(d.GetMetadata()),
	}
}

func controlDirectiveToProto(d *engram.ControlDirective) *transportpb.ControlRequest {
	if d == nil {
		return nil
	}
	action, custom := controlTypeToProto(d.Type)
	return &transportpb.ControlRequest{
		Action:       action,
		CustomAction: custom,
		Metadata:     cloneStringMap(d.Metadata),
	}
}

func controlTypeToProto(typ string) (transportpb.ControlAction, string) { //nolint:gocyclo
	switch normalizeControlType(typ) {
	case "", "noop":
		return transportpb.ControlAction_CONTROL_ACTION_NOOP, ""
	case "start":
		return transportpb.ControlAction_CONTROL_ACTION_START, ""
	case "stop":
		return transportpb.ControlAction_CONTROL_ACTION_STOP, ""
	case "start-upstream":
		return transportpb.ControlAction_CONTROL_ACTION_START_UPSTREAM, ""
	case "stop-upstream":
		return transportpb.ControlAction_CONTROL_ACTION_STOP_UPSTREAM, ""
	case "start-downstream":
		return transportpb.ControlAction_CONTROL_ACTION_START_DOWNSTREAM, ""
	case "stop-downstream":
		return transportpb.ControlAction_CONTROL_ACTION_STOP_DOWNSTREAM, ""
	case "heartbeat":
		return transportpb.ControlAction_CONTROL_ACTION_HEARTBEAT, ""
	case "connector.ready":
		return transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_READY, ""
	case "connector.capabilities":
		return transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_CAPABILITIES, ""
	case "handoff.draining":
		return transportpb.ControlAction_CONTROL_ACTION_HANDOFF_DRAINING, ""
	case "handoff.cutover":
		return transportpb.ControlAction_CONTROL_ACTION_HANDOFF_CUTOVER, ""
	case "handoff.ready":
		return transportpb.ControlAction_CONTROL_ACTION_HANDOFF_READY, ""
	case "ack":
		return transportpb.ControlAction_CONTROL_ACTION_ACK, ""
	case "error":
		return transportpb.ControlAction_CONTROL_ACTION_ERROR, ""
	case "codec-select":
		return transportpb.ControlAction_CONTROL_ACTION_CODEC_SELECT, ""
	default:
		return transportpb.ControlAction_CONTROL_ACTION_UNSPECIFIED, strings.TrimSpace(typ)
	}
}

func protoControlActionToType(action transportpb.ControlAction, custom string) string { //nolint:gocyclo
	if strings.TrimSpace(custom) != "" {
		return strings.TrimSpace(custom)
	}
	switch action {
	case transportpb.ControlAction_CONTROL_ACTION_NOOP:
		return "noop"
	case transportpb.ControlAction_CONTROL_ACTION_START:
		return "start"
	case transportpb.ControlAction_CONTROL_ACTION_STOP:
		return "stop"
	case transportpb.ControlAction_CONTROL_ACTION_START_UPSTREAM:
		return "start-upstream"
	case transportpb.ControlAction_CONTROL_ACTION_STOP_UPSTREAM:
		return "stop-upstream"
	case transportpb.ControlAction_CONTROL_ACTION_START_DOWNSTREAM:
		return "start-downstream"
	case transportpb.ControlAction_CONTROL_ACTION_STOP_DOWNSTREAM:
		return "stop-downstream"
	case transportpb.ControlAction_CONTROL_ACTION_HEARTBEAT:
		return "heartbeat"
	case transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_READY:
		return "connector.ready"
	case transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_CAPABILITIES:
		return "connector.capabilities"
	case transportpb.ControlAction_CONTROL_ACTION_HANDOFF_DRAINING:
		return "handoff.draining"
	case transportpb.ControlAction_CONTROL_ACTION_HANDOFF_CUTOVER:
		return "handoff.cutover"
	case transportpb.ControlAction_CONTROL_ACTION_HANDOFF_READY:
		return "handoff.ready"
	case transportpb.ControlAction_CONTROL_ACTION_ACK:
		return "ack"
	case transportpb.ControlAction_CONTROL_ACTION_ERROR:
		return "error"
	case transportpb.ControlAction_CONTROL_ACTION_CODEC_SELECT:
		return "codec-select"
	default:
		return ""
	}
}

func shouldSubscribeBinary(ref bindingReference) bool {
	if ref.Info == nil {
		return true
	}
	if len(ref.Info.GetAudioCodecs()) == 0 && len(ref.Info.GetVideoCodecs()) == 0 && len(ref.Info.GetBinaryTypes()) == 0 {
		return true
	}
	return len(ref.Info.GetBinaryTypes()) > 0
}

func streamMessageToPublishRequest(msg engram.StreamMessage) (*transportpb.PublishRequest, error) { //nolint:gocyclo
	if err := msg.Validate(); err != nil {
		return nil, err
	}
	if msg.Audio != nil {
		req := &transportpb.PublishRequest{
			Frame: &transportpb.PublishRequest_Audio{
				Audio: &transportpb.AudioFrame{
					Pcm:          append([]byte(nil), msg.Audio.PCM...),
					SampleRateHz: msg.Audio.SampleRateHz,
					Channels:     msg.Audio.Channels,
					Codec:        strings.TrimSpace(msg.Audio.Codec),
					TimestampMs:  durationToMillis(msg.Audio.Timestamp),
				},
			},
		}
		if err := applyStreamContextToPublishRequest(req, msg); err != nil {
			return nil, err
		}
		return validatedPublishRequest(req)
	}
	if msg.Video != nil {
		req := &transportpb.PublishRequest{
			Frame: &transportpb.PublishRequest_Video{
				Video: &transportpb.VideoFrame{
					Payload:     append([]byte(nil), msg.Video.Payload...),
					Codec:       strings.TrimSpace(msg.Video.Codec),
					Width:       msg.Video.Width,
					Height:      msg.Video.Height,
					TimestampMs: durationToMillis(msg.Video.Timestamp),
					Raw:         msg.Video.Raw,
				},
			},
		}
		if err := applyStreamContextToPublishRequest(req, msg); err != nil {
			return nil, err
		}
		return validatedPublishRequest(req)
	}
	if !shouldBypassEnvelopeForRawBinary(msg) {
		env := streamMessageEnvelope(msg)
		if env != nil && (env.Kind != "" || len(env.Payload) > 0 || len(env.Inputs) > 0 || len(env.Transports) > 0) {
			frame, err := envelope.ToBinaryFrame(env)
			if err != nil {
				return nil, fmt.Errorf("envelope marshal failed: %w", err)
			}
			if msg.Binary != nil {
				frame.TimestampMs = durationToMillis(msg.Binary.Timestamp)
			}
			req := &transportpb.PublishRequest{
				Frame: &transportpb.PublishRequest_Binary{Binary: frame},
			}
			if err := applyStreamContextToPublishRequest(req, msg); err != nil {
				return nil, err
			}
			return validatedPublishRequest(req)
		}
	}
	if msg.Binary != nil {
		mimeType := strings.TrimSpace(msg.Binary.MimeType)
		if mimeType == "" {
			mimeType = octetStreamCodec
		}
		req := &transportpb.PublishRequest{
			Frame: &transportpb.PublishRequest_Binary{
				Binary: &transportpb.BinaryFrame{
					Payload:     copyBytes(msg.Binary.Payload),
					MimeType:    mimeType,
					TimestampMs: durationToMillis(msg.Binary.Timestamp),
				},
			},
		}
		if err := applyStreamContextToPublishRequest(req, msg); err != nil {
			return nil, err
		}
		return validatedPublishRequest(req)
	}
	return nil, fmt.Errorf("stream message missing audio, video, or binary payload")
}

func validatedPublishRequest(req *transportpb.PublishRequest) (*transportpb.PublishRequest, error) {
	if err := validateTransportMessage("publish request", req); err != nil {
		return nil, err
	}
	return req, nil
}

func shouldBypassEnvelopeForRawBinary(msg engram.StreamMessage) bool {
	if msg.Binary == nil {
		return false
	}
	if isReservedEnvelopeMimeType(msg.Binary.MimeType) {
		return false
	}
	if len(msg.Inputs) > 0 || len(msg.Transports) > 0 || msg.Envelope != nil {
		return false
	}
	if len(msg.Payload) == 0 {
		return true
	}
	if isStructuredJSONMimeType(msg.Binary.MimeType) {
		return false
	}
	return bytes.Equal(msg.Payload, msg.Binary.Payload)
}

func isReservedEnvelopeMimeType(mimeType string) bool {
	canonical, ok := canonicalBinaryMimeType(mimeType)
	return ok && canonical == envelope.MIMEType
}

func isStructuredJSONMimeType(mimeType string) bool {
	canonical, ok := canonicalBinaryMimeType(mimeType)
	return ok && canonical == "application/json"
}

func canonicalBinaryMimeType(mimeType string) (string, bool) {
	mimeType = strings.TrimSpace(mimeType)
	if mimeType == "" {
		return "", false
	}
	parsed, _, err := mime.ParseMediaType(mimeType)
	if err != nil {
		return mimeType, false
	}
	return strings.ToLower(parsed), true
}

func publishRequestToStreamMessage(req *transportpb.PublishRequest) (engram.StreamMessage, error) {
	if err := validateTransportMessage("publish request", req); err != nil {
		return engram.StreamMessage{}, err
	}
	var msg engram.StreamMessage
	switch frame := req.GetFrame().(type) {
	case *transportpb.PublishRequest_Audio:
		audio := frame.Audio
		if audio == nil {
			return engram.StreamMessage{}, fmt.Errorf("audio frame missing payload")
		}
		payload := append([]byte(nil), audio.GetPcm()...)
		msg = engram.StreamMessage{
			Audio: &engram.AudioFrame{
				PCM:          payload,
				SampleRateHz: audio.GetSampleRateHz(),
				Channels:     audio.GetChannels(),
				Codec:        audio.GetCodec(),
				Timestamp:    durationFromMillis(audio.GetTimestampMs()),
			},
		}
	case *transportpb.PublishRequest_Video:
		video := frame.Video
		if video == nil {
			return engram.StreamMessage{}, fmt.Errorf("video frame missing payload")
		}
		msg = engram.StreamMessage{
			Video: &engram.VideoFrame{
				Payload:   append([]byte(nil), video.GetPayload()...),
				Codec:     video.GetCodec(),
				Width:     video.GetWidth(),
				Height:    video.GetHeight(),
				Timestamp: durationFromMillis(video.GetTimestampMs()),
				Raw:       video.GetRaw(),
			},
		}
	case *transportpb.PublishRequest_Binary:
		binary := frame.Binary
		if binary == nil {
			return engram.StreamMessage{}, fmt.Errorf("binary frame missing payload")
		}
		if isReservedEnvelopeMimeType(binary.GetMimeType()) {
			env, err := envelope.FromBinaryFrame(&transportpb.BinaryFrame{
				Payload:     binary.GetPayload(),
				MimeType:    envelope.MIMEType,
				TimestampMs: binary.GetTimestampMs(),
			})
			if err != nil {
				return engram.StreamMessage{}, fmt.Errorf("envelope decode failed: %w", err)
			}
			populateMessageFromEnvelope(&msg, env)
			if err := mergeRequestContextIntoStreamMessage(req, &msg); err != nil {
				return engram.StreamMessage{}, err
			}
			if err := msg.Validate(); err != nil {
				return engram.StreamMessage{}, fmt.Errorf("decoded stream message invalid: %w", err)
			}
			return msg, nil
		}
		payload := append([]byte(nil), binary.GetPayload()...)
		mimeType := strings.TrimSpace(binary.GetMimeType())
		if mimeType == "" {
			mimeType = octetStreamCodec
		}
		msg = engram.StreamMessage{
			Payload: payload,
			Binary: &engram.BinaryFrame{
				// Keep payload and binary payload mirrored without a second full copy
				// on the binary passthrough hot path.
				Payload:   payload,
				MimeType:  mimeType,
				Timestamp: durationFromMillis(binary.GetTimestampMs()),
			},
		}
	default:
		return engram.StreamMessage{}, fmt.Errorf("publish request missing frame payload")
	}
	if err := mergeRequestContextIntoStreamMessage(req, &msg); err != nil {
		return engram.StreamMessage{}, err
	}
	if err := msg.Validate(); err != nil {
		return engram.StreamMessage{}, fmt.Errorf("decoded stream message invalid: %w", err)
	}
	return msg, nil
}

func durationToMillis(d time.Duration) uint64 {
	if d <= 0 {
		return 0
	}
	return uint64(d / time.Millisecond)
}

func durationFromMillis(ms uint64) time.Duration {
	return time.Duration(ms) * time.Millisecond
}

func enqueueStreamMessage(
	ctx context.Context,
	dst chan<- engram.InboundMessage,
	msg engram.InboundMessage,
	timeout time.Duration,
) (bool, error) {
	if timeout <= 0 {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case dst <- msg:
			return true, nil
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case dst <- msg:
		return true, nil
	case <-timer.C:
		// channelSendTimeout > 0 is a lossy configuration: drop the message and
		// return a non-delivered result so the caller can release any pending
		// dedupe state without emitting downstream delivery receipts.
		LoggerFromContext(ctx).Warn("stream channel send timeout; dropping message",
			"timeout", timeout)
		return false, nil
	}
}

func callSendWithTimeout(
	ctx context.Context,
	timeout time.Duration,
	cancel context.CancelFunc,
	opName string,
	tracker *timedSendTracker,
	fn func() error,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	errCh := make(chan error, 1)
	if err := startTimedSend(tracker, func() {
		err := fn()
		// Non-blocking send: if the caller already timed out and nobody reads
		// errCh, the buffered channel absorbs this single write and the
		// goroutine exits immediately instead of leaking.
		select {
		case errCh <- err:
		default:
		}
	}); err != nil {
		return fmt.Errorf("%s rejected: %w", opName, err)
	}
	if timeout <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return err
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	case <-timer.C:
		if cancel != nil {
			cancel()
		}
		return fmt.Errorf("%s timed out after %s", opName, timeout)
	}
}

const maxTimedSendGoroutines = 16

type timedSendTracker struct {
	mu     sync.Mutex
	active int
	done   chan struct{}
}

func newTimedSendTracker() *timedSendTracker {
	done := make(chan struct{})
	close(done)
	return &timedSendTracker{done: done}
}

func startTimedSend(tracker *timedSendTracker, fn func()) error {
	if fn == nil {
		return nil
	}
	if tracker == nil {
		go fn()
		return nil
	}
	tracker.mu.Lock()
	if tracker.active >= maxTimedSendGoroutines {
		tracker.mu.Unlock()
		return fmt.Errorf(
			"too many in-flight send goroutines (%d)",
			maxTimedSendGoroutines,
		)
	}
	tracker.mu.Unlock()
	tracker.begin()
	go func() {
		defer tracker.end()
		fn()
	}()
	return nil
}

func (t *timedSendTracker) Wait(timeout time.Duration) bool {
	if t == nil {
		return true
	}
	done := t.currentDone()
	if timeout <= 0 {
		<-done
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func (t *timedSendTracker) begin() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active == 0 {
		t.done = make(chan struct{})
	}
	t.active++
}

func (t *timedSendTracker) end() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active <= 0 {
		return
	}
	t.active--
	if t.active == 0 {
		close(t.done)
	}
}

func (t *timedSendTracker) currentDone() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done == nil {
		done := make(chan struct{})
		close(done)
		t.done = done
	}
	return t.done
}

func waitForTimedSendCleanup(logger *slog.Logger, tracker *timedSendTracker, timeout time.Duration) error {
	if tracker == nil {
		return nil
	}
	if tracker.Wait(timeout) {
		return nil
	}
	if logger != nil {
		logger.Warn("Timed send worker cleanup exceeded timeout", "timeout", timeout)
	}
	return fmt.Errorf("%w: timeout=%s", errTimedSendCleanupTimeout, timeout)
}

func nextReconnectBackoff(current time.Duration, max time.Duration) time.Duration {
	backoff := current
	if backoff <= 0 {
		backoff = time.Second
	}
	if max > 0 {
		backoff *= 2
		if backoff > max {
			return max
		}
		return backoff
	}
	if backoff < defaultReconnectMaxBackoff {
		backoff *= 2
		if backoff > defaultReconnectMaxBackoff {
			return defaultReconnectMaxBackoff
		}
	}
	return backoff
}

func jitterReconnectDelay(wait time.Duration, max time.Duration) time.Duration {
	if wait <= 0 {
		return 0
	}
	jittered := time.Duration(float64(wait) * (reconnectJitterMinMultiplier + reconnectJitterFloat64()*reconnectJitterSpanMultiplier)) //nolint:lll
	if jittered <= 0 {
		jittered = time.Millisecond
	}
	if max > 0 && jittered > max {
		return max
	}
	return jittered
}

func sleepWithContext(ctx context.Context, wait time.Duration) error {
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isRetriableTransportSessionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errTimedSendCleanupTimeout) {
		return false
	}
	if errors.Is(err, errControlStartupHandshakeTimeout) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	switch status.Code(err) {
	case codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated, codes.Unimplemented, codes.FailedPrecondition: //nolint:lll
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"transport binding missing",
		"transport connector endpoint empty",
		"transport binding missing endpoint",
		"protocol mismatch",
	} {
		if strings.Contains(msg, marker) {
			return false
		}
	}
	return true
}

type hangWatcher struct {
	timeout time.Duration
	cancel  context.CancelFunc
	resetCh chan struct{}
	stopCh  chan struct{}
	once    sync.Once
	logger  *slog.Logger
}

func newHangWatcher(ctx context.Context, timeout time.Duration, cancel context.CancelFunc) *hangWatcher {
	if timeout <= 0 || cancel == nil {
		return nil
	}
	hw := &hangWatcher{
		timeout: timeout,
		cancel:  cancel,
		resetCh: make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
		logger:  LoggerFromContext(ctx),
	}
	go hw.loop()
	return hw
}

func (w *hangWatcher) loop() {
	timer := time.NewTimer(w.timeout)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if w.logger != nil {
				w.logger.Error("Transport hang timeout triggered", "timeout", w.timeout)
			}
			w.cancel()
			return
		case <-w.resetCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(w.timeout)
		case <-w.stopCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
	}
}

func (w *hangWatcher) Touch() {
	if w == nil {
		return
	}
	// Drain any pending signal so the next send always succeeds.
	// This prevents burst Touch() calls from silently dropping resets.
	select {
	case <-w.resetCh:
	default:
	}
	select {
	case <-w.stopCh:
	case w.resetCh <- struct{}{}:
	default:
	}
}

func (w *hangWatcher) Stop() {
	if w == nil {
		return
	}
	w.once.Do(func() {
		close(w.stopCh)
	})
}

func loadStreamingExecutionContext[C any](ctx context.Context) (C, *engram.Secrets, error) {
	var zeroConfig C

	execCtxData, err := runtime.LoadExecutionContextData()
	if err != nil {
		return zeroConfig, nil, fmt.Errorf("failed to load execution context: %w", err)
	}
	logExecutionContextDebug(LoggerFromContext(ctx), execCtxData)

	sm, err := storage.SharedManager(ctx)
	if err != nil {
		return zeroConfig, nil, fmt.Errorf("failed to create storage manager: %w", err)
	}
	configMap, err := hydrateConfig(ctx, sm, execCtxData.Config, execCtxData.CELContext)
	if err != nil {
		return zeroConfig, nil, fmt.Errorf("failed to hydrate config: %w", err)
	}
	config, err := runtime.UnmarshalFromMap[C](configMap)
	if err != nil {
		return zeroConfig, nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	secrets, err := engram.NewSecretsWithError(ctx, execCtxData.Secrets)
	if err != nil {
		return zeroConfig, nil, fmt.Errorf("failed to expand secrets: %w", err)
	}
	return config, secrets, nil
}

func normalizedDriver(ref bindingReference) string {
	if ref.Info == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(ref.Info.GetDriver()))
}

func controlBindingMetadataValue(ref bindingReference) string {
	name := strings.TrimSpace(ref.Name)
	if name == "" {
		return ""
	}
	namespace := strings.TrimSpace(ref.Namespace)
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}

// publishRequestToDataRequest converts the internal PublishRequest representation
// into a DataRequest for the Data bidirectional stream.
func publishRequestToDataRequest(req *transportpb.PublishRequest) *transportpb.DataRequest {
	if req == nil {
		return nil
	}
	dr := &transportpb.DataRequest{
		Metadata:   req.GetMetadata(),
		Payload:    req.GetPayload(),
		Inputs:     req.GetInputs(),
		Transports: req.GetTransports(),
		Envelope:   req.GetEnvelope(),
	}
	switch f := req.GetFrame().(type) {
	case *transportpb.PublishRequest_Audio:
		dr.Frame = &transportpb.DataRequest_Audio{Audio: f.Audio}
	case *transportpb.PublishRequest_Video:
		dr.Frame = &transportpb.DataRequest_Video{Video: f.Video}
	case *transportpb.PublishRequest_Binary:
		dr.Frame = &transportpb.DataRequest_Binary{Binary: f.Binary}
	}
	return dr
}

// dataResponseToPublishRequest converts a DataResponse from the Data stream
// into a PublishRequest so the existing chunk reassembler and message
// translation pipeline can be reused without modification.
func dataResponseToPublishRequest(resp *transportpb.DataResponse) *transportpb.PublishRequest {
	if resp == nil {
		return nil
	}
	pr := &transportpb.PublishRequest{
		Metadata:   resp.GetMetadata(),
		Payload:    resp.GetPayload(),
		Inputs:     resp.GetInputs(),
		Transports: resp.GetTransports(),
		Envelope:   resp.GetEnvelope(),
	}
	switch f := resp.GetFrame().(type) {
	case *transportpb.DataResponse_Audio:
		pr.Frame = &transportpb.PublishRequest_Audio{Audio: f.Audio}
	case *transportpb.DataResponse_Video:
		pr.Frame = &transportpb.PublishRequest_Video{Video: f.Video}
	case *transportpb.DataResponse_Binary:
		pr.Frame = &transportpb.PublishRequest_Binary{Binary: f.Binary}
	}
	return pr
}

// newDataChunkReassembler is an alias for newPublishChunkReassembler.
// The reassembler operates on PublishRequest (the internal representation)
// regardless of the wire type.
var newDataChunkReassembler = newPublishChunkReassembler
