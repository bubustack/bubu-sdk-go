package sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bubustack/bobrapet/pkg/contracts"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/runtime"
	"github.com/bubustack/tractatus/envelope"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
)

const (
	// DefaultChannelBufferSize is the in-memory buffer used for Engram stream channels.
	DefaultChannelBufferSize = 16
	// DefaultMaxMessageSize caps gRPC message sizes when talking to connectors.
	DefaultMaxMessageSize = 10 * 1024 * 1024
	defaultMessageTimeout = 30 * time.Second
	octetStreamCodec      = "application/octet-stream"
)

type reconnectPolicy struct {
	base       time.Duration
	max        time.Duration
	maxRetries int
}

type streamRuntimeOptions struct {
	messageTimeout           time.Duration
	channelSendTimeout       time.Duration
	controlHeartbeatInterval time.Duration
	hangWatcher              *hangWatcher
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
		if n, err := strconv.Atoi(raw); err == nil {
			if n < 0 {
				maxRetries = -1
			} else {
				maxRetries = n
			}
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
)

// StartStreamServer boots a StreamingEngram using the new transport connector contract.
// The Engram must have BUBU_TRANSPORT_BINDING set; no other transport modes are supported.
func StartStreamServer[C any](ctx context.Context, e engram.StreamingEngram[C]) error {
	logger := LoggerFromContext(ctx)
	logger.Info("Initializing Bubu SDK for streaming execution")

	config, secrets, err := loadStreamingExecutionContext[C](ctx)
	if err != nil {
		return err
	}

	if err := e.Init(ctx, config, secrets); err != nil {
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

		err := runTransportSession(ctx, endpoint, ref, e, env)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		LoggerFromContext(ctx).Error("Transport connector session ended; retrying", "error", err)

		if retriesRemaining >= 0 {
			retriesRemaining--
			if retriesRemaining < 0 {
				return fmt.Errorf("transport connector retries exhausted: %w", err)
			}
		}

		wait := backoff
		if policy.max > 0 && wait > policy.max {
			wait = policy.max
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}

		if policy.max > 0 {
			backoff *= 2
			if backoff > policy.max {
				backoff = policy.max
			}
		} else if backoff < defaultReconnectMaxBackoff {
			backoff *= 2
			if backoff > defaultReconnectMaxBackoff {
				backoff = defaultReconnectMaxBackoff
			}
		}
	}
}

func runTransportSession[C any](
	ctx context.Context,
	endpoint string,
	ref bindingReference,
	e engram.StreamingEngram[C],
	env envResolver,
) error {
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	logger := LoggerFromContext(ctx)

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
	in := make(chan engram.StreamMessage, bufferSize)
	out := make(chan engram.StreamMessage, bufferSize)

	opts := streamRuntimeOptions{
		messageTimeout:           resolveMessageTimeout(env),
		channelSendTimeout:       resolveChannelSendTimeout(env),
		controlHeartbeatInterval: resolveControlHeartbeatInterval(env),
	}
	if hangTimeout := resolveHangTimeout(env); hangTimeout > 0 {
		opts.hangWatcher = newHangWatcher(sessionCtx, hangTimeout, cancel)
		defer opts.hangWatcher.Stop()
	}

	g, gctx := errgroup.WithContext(sessionCtx)
	g.Go(func() error {
		defer close(in)
		return connectorSubscribeLoop(gctx, conn.Client(), ref, in, opts)
	})
	g.Go(func() error {
		return connectorPublishLoop(gctx, conn.Client(), out, opts)
	})
	handler := controlDirectiveHandler(e)
	g.Go(func() error {
		return connectorControlLoop(gctx, conn.Client(), ref, handler, opts)
	})
	g.Go(func() error {
		defer close(out)
		return e.Stream(gctx, in, out)
	})

	return g.Wait()
}

func connectorSubscribeLoop(
	ctx context.Context,
	client transportpb.TransportConnectorClient,
	ref bindingReference,
	in chan<- engram.StreamMessage,
	opts streamRuntimeOptions,
) error {
	subReq := &transportpb.SubscribeRequest{
		Audio:  shouldSubscribeAudio(ref),
		Video:  shouldSubscribeVideo(ref),
		Binary: shouldSubscribeBinary(ref),
	}
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Subscribe(streamCtx, subReq)
	if err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}
	for {
		frame, err := recvWithTimeout(streamCtx, opts.messageTimeout, cancel, "subscribe recv", stream.Recv)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("subscribe receive failed: %w", err)
		}
		msg, err := publishRequestToStreamMessage(frame)
		if err != nil {
			return err
		}
		if opts.hangWatcher != nil {
			opts.hangWatcher.Touch()
		}
		if err := enqueueStreamMessage(streamCtx, in, msg, opts.channelSendTimeout); err != nil {
			return err
		}
	}
}

func connectorPublishLoop(
	ctx context.Context,
	client transportpb.TransportConnectorClient,
	out <-chan engram.StreamMessage,
	opts streamRuntimeOptions,
) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Publish(streamCtx)
	if err != nil {
		return fmt.Errorf("publish stream open failed: %w", err)
	}
	for {
		select {
		case <-streamCtx.Done():
			return streamCtx.Err()
		case msg, ok := <-out:
			if !ok {
				_, err := recvWithTimeout(streamCtx, opts.messageTimeout, cancel, "publish close", stream.CloseAndRecv)
				return err
			}
			injectTraceContext(ctx, &msg)
			req, err := streamMessageToPublishRequest(msg)
			if err != nil {
				return err
			}
			if err := callSendWithTimeout(streamCtx, opts.messageTimeout, cancel, "publish send", func() error {
				return stream.Send(req)
			}); err != nil {
				return fmt.Errorf("publish stream send failed: %w", err)
			}
			if opts.hangWatcher != nil {
				opts.hangWatcher.Touch()
			}
		}
	}
}

func connectorControlLoop(
	ctx context.Context,
	client transportpb.TransportConnectorClient,
	ref bindingReference,
	handler engram.ControlDirectiveHandler,
	opts streamRuntimeOptions,
) error {
	controlCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if strings.TrimSpace(ref.Raw) != "" {
		controlCtx = metadata.NewOutgoingContext(controlCtx, metadata.Pairs(controlBindingMetadataKey, ref.Raw))
	}
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

	recvCh := startControlReceiver(controlCtx, stream, opts.messageTimeout, cancel)

	for {
		select {
		case <-controlCtx.Done():
			return controlCtx.Err()
		case msg, ok := <-recvCh:
			if !ok {
				return nil
			}
			if opts.hangWatcher != nil {
				opts.hangWatcher.Touch()
			}
			if err := processControlMessage(controlCtx, stream, handler, msg, opts.messageTimeout, cancel); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
		case <-ticker.C:
			if err := sendControlHeartbeat(controlCtx, stream, opts.messageTimeout, cancel); err != nil {
				return err
			}
			if opts.hangWatcher != nil {
				opts.hangWatcher.Touch()
			}
		}
	}
}

type controlMessage struct {
	directive *transportpb.ControlDirective
	err       error
}

func startControlReceiver(
	ctx context.Context,
	stream transportpb.TransportConnector_ControlClient,
	timeout time.Duration,
	cancel context.CancelFunc,
) <-chan controlMessage {
	recvCh := make(chan controlMessage, 1)
	go func() {
		defer close(recvCh)
		for {
			directive, recvErr := recvWithTimeout(ctx, timeout, cancel, "control recv", stream.Recv)
			if recvErr != nil {
				recvCh <- controlMessage{err: recvErr}
				return
			}
			select {
			case recvCh <- controlMessage{directive: directive}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return recvCh
}

func resolveControlHeartbeatInterval(env envResolver) time.Duration {
	if d := parsePositiveDuration(env.lookup(contracts.TransportHeartbeatIntervalEnv)); d > 0 {
		return d
	}
	return defaultControlHeartbeatInterval
}

func resolveChannelBufferSize(env envResolver) int {
	if size, err := strconv.Atoi(strings.TrimSpace(env.lookup(contracts.GRPCChannelBufferSizeEnv))); err == nil && size > 0 {
		return size
	}
	return DefaultChannelBufferSize
}

func resolveMessageTimeout(env envResolver) time.Duration {
	if d := parsePositiveDuration(env.lookup(contracts.GRPCMessageTimeoutEnv)); d > 0 {
		return d
	}
	return defaultMessageTimeout
}

func resolveChannelSendTimeout(env envResolver) time.Duration {
	return parsePositiveDuration(env.lookup(contracts.GRPCChannelSendTimeoutEnv))
}

func resolveHangTimeout(env envResolver) time.Duration {
	return parsePositiveDuration(env.lookup(contracts.GRPCHangTimeoutEnv))
}

func processControlMessage(
	ctx context.Context,
	stream transportpb.TransportConnector_ControlClient,
	handler engram.ControlDirectiveHandler,
	msg controlMessage,
	timeout time.Duration,
	cancel context.CancelFunc,
) error {
	if msg.err != nil {
		if msg.err == io.EOF {
			return io.EOF
		}
		return fmt.Errorf("control stream recv failed: %w", msg.err)
	}

	response, handleErr := handler.HandleControlDirective(ctx, protoToControlDirective(msg.directive))
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
	if err := callSendWithTimeout(ctx, timeout, cancel, "control send", func() error {
		return stream.Send(controlDirectiveToProto(response))
	}); err != nil {
		return fmt.Errorf("control stream send failed: %w", err)
	}
	return nil
}

func sendControlHeartbeat(
	ctx context.Context,
	stream transportpb.TransportConnector_ControlClient,
	timeout time.Duration,
	cancel context.CancelFunc,
) error {
	if err := callSendWithTimeout(ctx, timeout, cancel, "control heartbeat", func() error {
		return stream.Send(connectorHeartbeatDirective())
	}); err != nil {
		return fmt.Errorf("control stream heartbeat failed: %w", err)
	}
	return nil
}

func connectorHeartbeatDirective() *transportpb.ControlDirective {
	return &transportpb.ControlDirective{
		Type: "heartbeat",
		Metadata: map[string]string{
			"ts": strconv.FormatInt(time.Now().UnixMilli(), 10),
		},
	}
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
	case "connector.capabilities":
		logger.Info("Connector capability update received", "metadata", directive.Metadata)
		return ackControlDirective(directive.Type, true, "capabilities"), nil
	case "ack":
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

func protoToControlDirective(d *transportpb.ControlDirective) engram.ControlDirective {
	if d == nil {
		return engram.ControlDirective{}
	}
	return engram.ControlDirective{
		Type:     strings.TrimSpace(d.GetType()),
		Metadata: cloneStringMap(d.GetMetadata()),
	}
}

func controlDirectiveToProto(d *engram.ControlDirective) *transportpb.ControlDirective {
	if d == nil {
		return nil
	}
	return &transportpb.ControlDirective{
		Type:     strings.TrimSpace(d.Type),
		Metadata: cloneStringMap(d.Metadata),
	}
}

func shouldSubscribeAudio(ref bindingReference) bool {
	if ref.Info == nil {
		return true
	}
	if len(ref.Info.GetAudioCodecs()) == 0 && len(ref.Info.GetVideoCodecs()) == 0 {
		return true
	}
	return len(ref.Info.GetAudioCodecs()) > 0
}

func shouldSubscribeVideo(ref bindingReference) bool {
	if ref.Info == nil {
		return false
	}
	return len(ref.Info.GetVideoCodecs()) > 0
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

func streamMessageToPublishRequest(msg engram.StreamMessage) (*transportpb.PublishRequest, error) {
	if msg.Audio != nil {
		return &transportpb.PublishRequest{
			Frame: &transportpb.PublishRequest_Audio{
				Audio: &transportpb.AudioFrame{
					Pcm:          append([]byte(nil), msg.Audio.PCM...),
					SampleRateHz: msg.Audio.SampleRateHz,
					Channels:     msg.Audio.Channels,
					Codec:        strings.TrimSpace(msg.Audio.Codec),
					TimestampMs:  durationToMillis(msg.Audio.Timestamp),
				},
			},
		}, nil
	}
	if msg.Video != nil {
		return &transportpb.PublishRequest{
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
		}, nil
	}
	if env := streamMessageEnvelope(msg); env != nil {
		frame, err := envelope.ToBinaryFrame(env)
		if err != nil {
			return nil, fmt.Errorf("envelope marshal failed: %w", err)
		}
		if msg.Binary != nil {
			frame.TimestampMs = durationToMillis(msg.Binary.Timestamp)
		}
		return &transportpb.PublishRequest{
			Frame: &transportpb.PublishRequest_Binary{Binary: frame},
		}, nil
	}
	if msg.Binary != nil {
		mimeType := strings.TrimSpace(msg.Binary.MimeType)
		if mimeType == "" {
			mimeType = octetStreamCodec
		}
		return &transportpb.PublishRequest{
			Frame: &transportpb.PublishRequest_Binary{
				Binary: &transportpb.BinaryFrame{
					Payload:     copyBytes(msg.Binary.Payload),
					MimeType:    mimeType,
					TimestampMs: durationToMillis(msg.Binary.Timestamp),
				},
			},
		}, nil
	}
	return nil, fmt.Errorf("stream message missing audio, video, or binary payload")
}

func publishRequestToStreamMessage(req *transportpb.PublishRequest) (engram.StreamMessage, error) {
	switch frame := req.GetFrame().(type) {
	case *transportpb.PublishRequest_Audio:
		audio := frame.Audio
		if audio == nil {
			return engram.StreamMessage{}, fmt.Errorf("audio frame missing payload")
		}
		payload := append([]byte(nil), audio.GetPcm()...)
		return engram.StreamMessage{
			Metadata: map[string]string{},
			Payload:  payload,
			Audio: &engram.AudioFrame{
				PCM:          payload,
				SampleRateHz: audio.GetSampleRateHz(),
				Channels:     audio.GetChannels(),
				Codec:        audio.GetCodec(),
				Timestamp:    durationFromMillis(audio.GetTimestampMs()),
			},
		}, nil
	case *transportpb.PublishRequest_Video:
		video := frame.Video
		if video == nil {
			return engram.StreamMessage{}, fmt.Errorf("video frame missing payload")
		}
		return engram.StreamMessage{
			Metadata: map[string]string{},
			Video: &engram.VideoFrame{
				Payload:   append([]byte(nil), video.GetPayload()...),
				Codec:     video.GetCodec(),
				Width:     video.GetWidth(),
				Height:    video.GetHeight(),
				Timestamp: durationFromMillis(video.GetTimestampMs()),
				Raw:       video.GetRaw(),
			},
		}, nil
	case *transportpb.PublishRequest_Binary:
		binary := frame.Binary
		if binary == nil {
			return engram.StreamMessage{}, fmt.Errorf("binary frame missing payload")
		}
		if binary.GetMimeType() == envelope.MIMEType {
			env, err := envelope.FromBinaryFrame(binary)
			if err != nil {
				return engram.StreamMessage{}, fmt.Errorf("envelope decode failed: %w", err)
			}
			var msg engram.StreamMessage
			populateMessageFromEnvelope(&msg, env)
			return msg, nil
		}
		payload := append([]byte(nil), binary.GetPayload()...)
		return engram.StreamMessage{
			Payload: payload,
			Binary: &engram.BinaryFrame{
				Payload:   payload,
				MimeType:  binary.GetMimeType(),
				Timestamp: durationFromMillis(binary.GetTimestampMs()),
			},
		}, nil
	default:
		return engram.StreamMessage{}, fmt.Errorf("publish request missing frame payload")
	}
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
	dst chan<- engram.StreamMessage,
	msg engram.StreamMessage,
	timeout time.Duration,
) error {
	if timeout <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case dst <- msg:
			return nil
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case dst <- msg:
		return nil
	case <-timer.C:
		return fmt.Errorf("stream channel send timed out after %s", timeout)
	}
}

type callResult[T any] struct {
	value T
	err   error
}

func recvWithTimeout[T any](
	ctx context.Context,
	timeout time.Duration,
	cancel context.CancelFunc,
	opName string,
	fn func() (T, error),
) (T, error) {
	var zero T
	if timeout <= 0 {
		return fn()
	}
	resultCh := make(chan callResult[T], 1)
	go func() {
		val, err := fn()
		resultCh <- callResult[T]{value: val, err: err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case res := <-resultCh:
		return res.value, res.err
	case <-timer.C:
		if cancel != nil {
			cancel()
		}
		return zero, fmt.Errorf("%s timed out after %s", opName, timeout)
	}
}

func callSendWithTimeout(
	ctx context.Context,
	timeout time.Duration,
	cancel context.CancelFunc,
	opName string,
	fn func() error,
) error {
	if timeout <= 0 {
		return fn()
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- fn()
	}()
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

	config, err := runtime.UnmarshalFromMap[C](execCtxData.Config)
	if err != nil {
		return zeroConfig, nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	secrets := engram.NewSecrets(ctx, execCtxData.Secrets)
	return config, secrets, nil
}

func normalizedDriver(ref bindingReference) string {
	if ref.Info == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(ref.Info.GetDriver()))
}
