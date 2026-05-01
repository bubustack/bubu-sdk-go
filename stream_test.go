package sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/core/contracts"
	coretransport "github.com/bubustack/core/runtime/transport"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestResolveChannelBufferSizeFromEnv(t *testing.T) {
	env := newEnvResolver(map[string]string{contracts.GRPCChannelBufferSizeEnv: "64"})
	if size := resolveChannelBufferSize(env); size != 64 {
		t.Fatalf("expected buffer size 64, got %d", size)
	}
}

func TestResolveChannelBufferSizeDefault(t *testing.T) {
	env := newEnvResolver(map[string]string{contracts.GRPCChannelBufferSizeEnv: "invalid"})
	if size := resolveChannelBufferSize(env); size != DefaultChannelBufferSize {
		t.Fatalf("expected default buffer size %d, got %d", DefaultChannelBufferSize, size)
	}
}

func TestResolveReconnectPolicyDefaults(t *testing.T) {
	env := newEnvResolver(nil)
	policy := resolveReconnectPolicy(env)
	if policy.base != defaultReconnectBaseBackoff {
		t.Fatalf("expected base backoff %s, got %s", defaultReconnectBaseBackoff, policy.base)
	}
	if policy.max != defaultReconnectMaxBackoff {
		t.Fatalf("expected max backoff %s, got %s", defaultReconnectMaxBackoff, policy.max)
	}
	if policy.maxRetries != defaultReconnectMaxRetries {
		t.Fatalf("expected max retries %d, got %d", defaultReconnectMaxRetries, policy.maxRetries)
	}
}

func TestResolveReconnectPolicyFromEnv(t *testing.T) {
	env := newEnvResolver(map[string]string{
		contracts.GRPCReconnectBaseBackoffEnv: "750ms",
		contracts.GRPCReconnectMaxBackoffEnv:  "45s",
		contracts.GRPCReconnectMaxRetriesEnv:  "5",
	})

	policy := resolveReconnectPolicy(env)
	if policy.base != 750*time.Millisecond {
		t.Fatalf("expected base backoff 750ms, got %s", policy.base)
	}
	if policy.max != 45*time.Second {
		t.Fatalf("expected max backoff 45s, got %s", policy.max)
	}
	if policy.maxRetries != 5 {
		t.Fatalf("expected max retries 5, got %d", policy.maxRetries)
	}
}

func TestResolveMessageTimeoutDefault(t *testing.T) {
	env := newEnvResolver(nil)
	if timeout := resolveMessageTimeout(env); timeout != defaultMessageTimeout {
		t.Fatalf("expected default message timeout %s, got %s", defaultMessageTimeout, timeout)
	}
}

func TestResolveMessageTimeoutOverride(t *testing.T) {
	env := newEnvResolver(map[string]string{contracts.GRPCMessageTimeoutEnv: "45s"})
	if timeout := resolveMessageTimeout(env); timeout != 45*time.Second {
		t.Fatalf("expected override timeout 45s, got %s", timeout)
	}
}

func TestResolvePublishHeartbeatIntervalDefault(t *testing.T) {
	env := newEnvResolver(nil)
	if interval := resolvePublishHeartbeatInterval(env, defaultMessageTimeout); interval != defaultPublishHeartbeatInterval { //nolint:lll
		t.Fatalf("expected default publish heartbeat %s, got %s", defaultPublishHeartbeatInterval, interval)
	}
}

func TestResolvePublishHeartbeatIntervalFromEnv(t *testing.T) {
	env := newEnvResolver(map[string]string{contracts.GRPCHeartbeatIntervalEnv: "7s"})
	if interval := resolvePublishHeartbeatInterval(env, defaultMessageTimeout); interval != 7*time.Second {
		t.Fatalf("expected publish heartbeat override 7s, got %s", interval)
	}
}

func TestResolvePublishHeartbeatIntervalClampsBelowMessageTimeout(t *testing.T) {
	env := newEnvResolver(map[string]string{contracts.GRPCHeartbeatIntervalEnv: "45s"})
	want := 30*time.Second - time.Nanosecond
	if interval := resolvePublishHeartbeatInterval(env, 30*time.Second); interval != want {
		t.Fatalf("expected clamped publish heartbeat %s, got %s", want, interval)
	}
}

func TestResolveChannelSendTimeout(t *testing.T) {
	env := newEnvResolver(map[string]string{contracts.GRPCChannelSendTimeoutEnv: "5s"})
	if timeout := resolveChannelSendTimeout(env); timeout != 5*time.Second {
		t.Fatalf("expected channel send timeout 5s, got %s", timeout)
	}
}

func TestResolveHangTimeout(t *testing.T) {
	env := newEnvResolver(map[string]string{contracts.GRPCHangTimeoutEnv: "0"})
	if timeout := resolveHangTimeout(env); timeout != 0 {
		t.Fatalf("expected zero timeout when env is 0, got %s", timeout)
	}
	env = newEnvResolver(map[string]string{contracts.GRPCHangTimeoutEnv: "20s"})
	if timeout := resolveHangTimeout(env); timeout != 20*time.Second {
		t.Fatalf("expected hang timeout 20s, got %s", timeout)
	}
}

func TestDrainStreamMessagesDrainsBufferedValues(t *testing.T) {
	out := make(chan engram.StreamMessage, 2)
	out <- engram.StreamMessage{Kind: "one"}
	out <- engram.StreamMessage{Kind: "two"}

	drainStreamMessages(out)

	select {
	case msg := <-out:
		t.Fatalf("expected buffered messages to be drained, got %+v", msg)
	default:
	}
}

func TestDrainStreamMessagesDoesNotBlockOnOpenChannel(t *testing.T) {
	out := make(chan engram.StreamMessage)
	done := make(chan struct{})

	go func() {
		drainStreamMessages(out)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("drainStreamMessages blocked on an open channel")
	}
}

func TestSendControlMessageRespectsCanceledContextWhenChannelFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	recvCh := make(chan controlMessage, 1)
	recvCh <- controlMessage{response: nil}
	cancel()

	done := make(chan bool, 1)
	go func() {
		done <- sendControlMessage(ctx, recvCh, controlMessage{err: context.Canceled})
	}()

	select {
	case delivered := <-done:
		if delivered {
			t.Fatal("expected sendControlMessage to stop when context is canceled")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("sendControlMessage blocked on a full channel after cancellation")
	}
}

func TestQueueControlRequestClosedChannelDoesNotPanic(t *testing.T) {
	controlRequests := make(chan *transportpb.ControlRequest)
	close(controlRequests)
	req := &transportpb.ControlRequest{CustomAction: "test"}

	done := make(chan struct{}, 1)
	go func() {
		queueControlRequest(context.Background(), controlRequests, req)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("queueControlRequest blocked on closed channel")
	}
}

func TestQueueControlRequestCanceledContextSkipsSendWhenQueueAvailable(t *testing.T) {
	controlRequests := make(chan *transportpb.ControlRequest, 1)
	req := &transportpb.ControlRequest{CustomAction: "test"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	queueControlRequest(ctx, controlRequests, req)

	select {
	case got := <-controlRequests:
		t.Fatalf("expected canceled context to skip send, got %+v", got)
	default:
	}
}

func TestQueueControlRequestCanceledContextReturnsWhenQueueFull(t *testing.T) {
	controlRequests := make(chan *transportpb.ControlRequest, 1)
	controlRequests <- &transportpb.ControlRequest{CustomAction: "existing"}
	req := &transportpb.ControlRequest{CustomAction: "test"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{}, 1)
	go func() {
		queueControlRequest(ctx, controlRequests, req)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("queueControlRequest blocked with canceled context and full queue")
	}
	if got := len(controlRequests); got != 1 {
		t.Fatalf("expected queue length 1, got %d", got)
	}
}

func TestQueueControlRequestFullQueueReturnsWithoutBlocking(t *testing.T) {
	controlRequests := make(chan *transportpb.ControlRequest, 1)
	first := &transportpb.ControlRequest{CustomAction: "first"}
	second := &transportpb.ControlRequest{CustomAction: "second"}
	controlRequests <- first

	done := make(chan struct{})
	go func() {
		queueControlRequest(context.Background(), controlRequests, second)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("queueControlRequest blocked on a full queue with live context")
	}

	got := <-controlRequests
	if got.GetCustomAction() != "first" {
		t.Fatalf("expected existing request to remain queued, got %q", got.GetCustomAction())
	}
}

func TestQueueControlRequestCanceledContextStillWinsWhenQueueFull(t *testing.T) {
	controlRequests := make(chan *transportpb.ControlRequest, 1)
	controlRequests <- &transportpb.ControlRequest{CustomAction: "existing"}
	req := &transportpb.ControlRequest{CustomAction: "blocked"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		queueControlRequest(ctx, controlRequests, req)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("queueControlRequest blocked with canceled context and full queue")
	}

	got := <-controlRequests
	if got.GetCustomAction() != "existing" {
		t.Fatalf("expected existing request to remain queued, got %q", got.GetCustomAction())
	}
}

func TestQueueDownstreamDeliveryReceiptSkipsNonSequencedRequest(t *testing.T) {
	controlRequests := make(chan *transportpb.ControlRequest, 1)
	opts := streamRuntimeOptions{controlRequests: controlRequests}
	req := &transportpb.PublishRequest{
		Envelope: &transportpb.StreamEnvelope{
			StreamId:  "step",
			Partition: "p0",
			Sequence:  0,
		},
	}

	queueDownstreamDeliveryReceipt(context.Background(), opts, req)

	select {
	case got := <-controlRequests:
		t.Fatalf("expected no receipt for non-sequenced request, got %+v", got)
	default:
	}
}

func TestQueueDownstreamDeliveryReceiptSkipsRequestWithoutStreamID(t *testing.T) {
	controlRequests := make(chan *transportpb.ControlRequest, 1)
	opts := streamRuntimeOptions{controlRequests: controlRequests}
	req := &transportpb.PublishRequest{
		Envelope: &transportpb.StreamEnvelope{
			Sequence:  9,
			Partition: "p0",
		},
	}

	queueDownstreamDeliveryReceipt(context.Background(), opts, req)

	select {
	case got := <-controlRequests:
		t.Fatalf("expected no receipt for request without stream id, got %+v", got)
	default:
	}
}

func TestEnqueueStreamMessageTimeoutDropsWhenChannelFull(t *testing.T) {
	dst := make(chan engram.InboundMessage, 1)
	dst <- engram.NewInboundMessage(engram.StreamMessage{Kind: "existing"})
	msg := engram.NewInboundMessage(engram.StreamMessage{Kind: "new"})

	start := time.Now()
	delivered, err := enqueueStreamMessage(context.Background(), dst, msg, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("expected nil error when dropping on timeout, got %v", err)
	}
	if delivered {
		t.Fatal("expected enqueue timeout drop to report non-delivery")
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("enqueueStreamMessage took too long to drop message: %s", elapsed)
	}
	if got := len(dst); got != 1 {
		t.Fatalf("expected full queue to remain unchanged after drop, got len=%d", got)
	}
}

func TestEnqueueStreamMessageTimeoutRespectsCanceledContext(t *testing.T) {
	dst := make(chan engram.InboundMessage, 1)
	dst <- engram.NewInboundMessage(engram.StreamMessage{Kind: "occupied"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	delivered, err := enqueueStreamMessage(ctx, dst, engram.NewInboundMessage(engram.StreamMessage{Kind: "new"}), 50*time.Millisecond) //nolint:lll
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if delivered {
		t.Fatal("expected canceled enqueue to report non-delivery")
	}
	if got := len(dst); got != 1 {
		t.Fatalf("expected canceled enqueue to preserve existing queue item, got len=%d", got)
	}
}

func TestLossyEnqueueKeepsPendingPacketWithoutEmittingReceipt(t *testing.T) {
	deduper := newPacketDeduper(defaultPacketDedupeEntries)
	deduper.StartSession()
	req := &transportpb.PublishRequest{
		Envelope: &transportpb.StreamEnvelope{
			StreamId:  "downstream-step",
			Sequence:  7,
			Partition: "p0",
		},
	}
	status, key, generation := deduper.Begin(req) //nolint:revive
	if status != packetNew {
		t.Fatalf("expected packetNew, got %v", status)
	}

	receipts := make(chan *transportpb.ControlRequest, 1)
	inbound := attachDownstreamProcessingReceipt(
		context.Background(),
		streamRuntimeOptions{controlRequests: receipts},
		engram.NewInboundMessage(engram.StreamMessage{Envelope: req.GetEnvelope()}),
		req,
		deduper,
		key,
		generation,
	)

	dst := make(chan engram.InboundMessage, 1)
	dst <- engram.NewInboundMessage(engram.StreamMessage{Kind: "occupied"})
	delivered, err := enqueueStreamMessage(context.Background(), dst, inbound, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("expected nil error for lossy drop, got %v", err)
	}
	if delivered {
		t.Fatal("expected lossy drop to report non-delivery")
	}
	select {
	case receipt := <-receipts:
		t.Fatalf("expected no receipt emission on dropped message, got %+v", receipt)
	default:
	}

	statusAfterDrop, _, _ := deduper.Begin(req)
	if statusAfterDrop != packetDuplicatePending {
		t.Fatalf("expected dropped packet to remain pending at helper level, got %v", statusAfterDrop)
	}
}

func TestPublishRequestToStreamMessageBinaryPassthroughMirrorsSinglePayloadCopy(t *testing.T) {
	src := []byte("binary")
	req := &transportpb.PublishRequest{
		Frame: &transportpb.PublishRequest_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:     src,
				MimeType:    "application/octet-stream",
				TimestampMs: 11,
			},
		},
	}

	msg, err := publishRequestToStreamMessage(req)
	if err != nil {
		t.Fatalf("publishRequestToStreamMessage() failed: %v", err)
	}
	if msg.Binary == nil {
		t.Fatal("expected binary frame in decoded stream message")
	}
	if string(msg.Payload) != "binary" { //nolint:goconst
		t.Fatalf("unexpected payload: %q", string(msg.Payload))
	}
	if string(msg.Binary.Payload) != "binary" {
		t.Fatalf("unexpected binary payload: %q", string(msg.Binary.Payload))
	}

	// Ensure decoded payload remains isolated from source request mutation.
	src[0] = 'X'
	if string(msg.Payload) != "binary" {
		t.Fatalf("decoded payload should not alias source request payload, got %q", string(msg.Payload))
	}

	// Binary passthrough keeps payload and binary payload mirrored.
	msg.Payload[0] = 'z'
	if string(msg.Binary.Payload) != "zinary" {
		t.Fatalf("expected mirrored payload/binary payload, got %q", string(msg.Binary.Payload))
	}
}

func TestNewPacketDeduperDefaultsMaxEntriesWhenNonPositive(t *testing.T) {
	deduper := newPacketDeduper(0)
	if deduper == nil {
		t.Fatal("expected non-nil deduper")
	}
	if deduper.maxEntries != defaultPacketDedupeEntries {
		t.Fatalf("expected default max entries %d, got %d", defaultPacketDedupeEntries, deduper.maxEntries)
	}
	if deduper.pending == nil || deduper.completed == nil {
		t.Fatal("expected pending/completed maps to be initialized")
	}
}

func TestPacketDeduperBeginRepeatedKeepsPendingStateBounded(t *testing.T) {
	deduper := newPacketDeduper(defaultPacketDedupeEntries)
	deduper.StartSession()
	req := &transportpb.PublishRequest{
		Envelope: &transportpb.StreamEnvelope{
			StreamId:  "step-a",
			Sequence:  11,
			Partition: "p0",
		},
	}

	for i := range 25 {
		status, _, _ := deduper.Begin(req) //nolint:revive
		if i == 0 && status != packetNew {
			t.Fatalf("first Begin expected packetNew, got %v", status)
		}
		if i > 0 && status != packetDuplicatePending {
			t.Fatalf("repeated Begin expected packetDuplicatePending, got %v", status)
		}
	}

	if got := len(deduper.pending); got != 1 {
		t.Fatalf("expected pending state to stay bounded at 1 for repeated Begin, got %d", got)
	}
}

func TestPacketDeduperBeginReturnsOverflowWhenPendingCapacityExceeded(t *testing.T) {
	deduper := newPacketDeduper(2)
	deduper.StartSession()

	reqs := []*transportpb.PublishRequest{
		{Envelope: &transportpb.StreamEnvelope{StreamId: "step-a", Sequence: 1, Partition: "p0"}},
		{Envelope: &transportpb.StreamEnvelope{StreamId: "step-a", Sequence: 2, Partition: "p0"}},
		{Envelope: &transportpb.StreamEnvelope{StreamId: "step-a", Sequence: 3, Partition: "p0"}},
	}

	for i, req := range reqs[:2] {
		status, _, _ := deduper.Begin(req) //nolint:revive
		if status != packetNew {
			t.Fatalf("begin %d expected packetNew, got %v", i, status)
		}
	}

	status, _, _ := deduper.Begin(reqs[2]) //nolint:revive
	if status != packetPendingOverflow {
		t.Fatalf("expected packetPendingOverflow, got %v", status)
	}
	if got := len(deduper.pending); got != 2 {
		t.Fatalf("expected pending entries to remain capped at 2, got %d", got)
	}

	status, _, _ = deduper.Begin(reqs[0])
	if status != packetDuplicatePending {
		t.Fatalf("expected oldest in-flight packet to remain pending, got %v", status)
	}
}

func TestPacketDeduperStartSessionClearsPendingState(t *testing.T) {
	deduper := newPacketDeduper(defaultPacketDedupeEntries)
	deduper.StartSession()

	req1 := &transportpb.PublishRequest{
		Envelope: &transportpb.StreamEnvelope{
			StreamId:  "step-a",
			Sequence:  1,
			Partition: "p0",
		},
	}
	req2 := &transportpb.PublishRequest{
		Envelope: &transportpb.StreamEnvelope{
			StreamId:  "step-a",
			Sequence:  2,
			Partition: "p0",
		},
	}

	if status, _, _ := deduper.Begin(req1); status != packetNew { //nolint:revive
		t.Fatalf("expected req1 to be new, got %v", status)
	}
	if status, _, _ := deduper.Begin(req2); status != packetNew { //nolint:revive
		t.Fatalf("expected req2 to be new, got %v", status)
	}
	if got := len(deduper.pending); got != 2 {
		t.Fatalf("expected 2 pending entries before session restart, got %d", got)
	}

	deduper.StartSession()
	if got := len(deduper.pending); got != 0 {
		t.Fatalf("expected pending state to be cleared on StartSession, got %d", got)
	}
	if status, _, _ := deduper.Begin(req1); status != packetNew { //nolint:revive
		t.Fatalf("expected req1 to be new after pending cleanup, got %v", status)
	}
}

func TestPacketDeduperReleaseStaleGenerationDoesNotClearCurrentPending(t *testing.T) {
	deduper := newPacketDeduper(defaultPacketDedupeEntries)
	deduper.StartSession()

	req := &transportpb.PublishRequest{
		Envelope: &transportpb.StreamEnvelope{
			StreamId:  "step-a",
			Sequence:  3,
			Partition: "p0",
		},
	}

	_, key, staleGeneration := deduper.Begin(req)
	deduper.StartSession()                             // increments generation and clears previous pending state
	status, _, currentGeneration := deduper.Begin(req) //nolint:revive
	if status != packetNew {
		t.Fatalf("expected packetNew in new generation, got %v", status)
	}
	if currentGeneration == staleGeneration {
		t.Fatal("expected generation to advance after StartSession")
	}

	deduper.Release(key, staleGeneration)
	statusAfterStaleRelease, _, _ := deduper.Begin(req)
	if statusAfterStaleRelease != packetDuplicatePending {
		t.Fatalf("expected current pending entry to survive stale-generation release, got %v", statusAfterStaleRelease)
	}
}

type noopStreamingEngram struct{}

func (noopStreamingEngram) Init(context.Context, struct{}, *engram.Secrets) error { return nil }

func (noopStreamingEngram) Stream(context.Context, <-chan engram.InboundMessage, chan<- engram.StreamMessage) error {
	return nil
}

type blockingStreamingEngram struct{}

func (blockingStreamingEngram) Init(context.Context, struct{}, *engram.Secrets) error { return nil }

func (blockingStreamingEngram) Stream(ctx context.Context, _ <-chan engram.InboundMessage, _ chan<- engram.StreamMessage) error { //nolint:lll
	<-ctx.Done()
	return ctx.Err()
}

type singleMessageStreamingEngram struct {
	msg engram.StreamMessage
}

func (singleMessageStreamingEngram) Init(context.Context, struct{}, *engram.Secrets) error {
	return nil
}

func (e singleMessageStreamingEngram) Stream(ctx context.Context,
	_ <-chan engram.InboundMessage, out chan<- engram.StreamMessage) error {
	select {
	case out <- e.msg:
	case <-ctx.Done():
		return ctx.Err()
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestCallSendWithTimeoutTracksTimedOutWorkerUntilReleased(t *testing.T) {
	tracker := newTimedSendTracker()
	blocked := make(chan struct{})
	release := make(chan struct{})

	err := callSendWithTimeout(context.Background(), 10*time.Millisecond, nil, "data send", tracker, func() error {
		close(blocked)
		<-release
		return nil
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	<-blocked

	if tracker.Wait(10 * time.Millisecond) {
		t.Fatal("expected timed send worker to remain tracked while blocked")
	}

	close(release)
	if !tracker.Wait(100 * time.Millisecond) {
		t.Fatal("expected timed send worker to finish after release")
	}
}

func TestTimedSendTrackerWaitDoesNotAllocatePerWaiterGoroutine(t *testing.T) {
	tracker := newTimedSendTracker()
	release := make(chan struct{})
	started := make(chan struct{})

	if err := startTimedSend(tracker, func() {
		close(started)
		<-release
	}); err != nil {
		t.Fatalf("startTimedSend: %v", err)
	}
	<-started

	// Establish a conservative baseline before issuing repeated timed waits.
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	const waitCalls = 200
	for i := range waitCalls {
		if tracker.Wait(1 * time.Millisecond) {
			t.Fatalf("expected Wait call %d to time out while send remains blocked", i)
		}
	}

	afterWaits := runtime.NumGoroutine()
	delta := afterWaits - baseline
	// Guard against per-Wait goroutine allocation: repeated timed waits should not
	// leave one blocked waiter goroutine per call while work is still pending.
	if delta > 25 {
		t.Fatalf("expected bounded waiter goroutines after repeated Wait calls, baseline=%d after=%d delta=%d", baseline, afterWaits, delta) //nolint:lll
	}

	close(release)
	if !tracker.Wait(100 * time.Millisecond) {
		t.Fatal("expected tracker to drain after releasing blocked send")
	}
}

func TestWaitForTimedSendCleanupReturnsTimeoutError(t *testing.T) {
	tracker := newTimedSendTracker()
	release := make(chan struct{})
	started := make(chan struct{})

	if err := startTimedSend(tracker, func() {
		close(started)
		<-release
	}); err != nil {
		t.Fatalf("startTimedSend: %v", err)
	}
	<-started

	err := waitForTimedSendCleanup(nil, tracker, 10*time.Millisecond)
	if !errors.Is(err, errTimedSendCleanupTimeout) {
		t.Fatalf("expected timed send cleanup timeout, got %v", err)
	}

	close(release)
	if !tracker.Wait(100 * time.Millisecond) {
		t.Fatal("expected tracker to drain after releasing blocked send")
	}
}

func TestCallSendWithTimeoutHonorsCanceledContextWhenTimeoutDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	err := callSendWithTimeout(ctx, 0, nil, "data send", nil, func() error {
		called = true
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if called {
		t.Fatal("expected fn not to be called when context is already canceled")
	}
}

func TestCallSendWithTimeoutTimeoutDisabledReturnsOnCancellationAndDrainsTrackedWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tracker := newTimedSendTracker()
	started := make(chan struct{})
	release := make(chan struct{})

	errCh := make(chan error, 1)
	go func() {
		errCh <- callSendWithTimeout(ctx, 0, nil, "data send", tracker, func() error {
			close(started)
			<-release
			return nil
		})
	}()

	select {
	case <-started:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected timeout-disabled send worker to start")
	}

	if tracker.Wait(10 * time.Millisecond) {
		t.Fatal("expected tracked timeout-disabled send worker to remain pending while blocked")
	}

	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context canceled, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected callSendWithTimeout to return on context cancellation")
	}

	if tracker.Wait(10 * time.Millisecond) {
		t.Fatal("expected tracked worker to remain pending until send is released")
	}

	close(release)

	if !tracker.Wait(100 * time.Millisecond) {
		t.Fatal("expected tracked timeout-disabled send worker to drain after release")
	}
}

func TestCallSendWithTimeoutHonorsCanceledContextWhenTimeoutEnabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	err := callSendWithTimeout(ctx, 100*time.Millisecond, nil, "data send", nil, func() error {
		called = true
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if called {
		t.Fatal("expected fn not to be called when context is already canceled")
	}
}

func TestRunTransportConnectorStreamDoesNotRetryNonRetriableErrors(t *testing.T) {
	attempts := 0
	sleeps := 0
	sessionErr := status.Error(codes.InvalidArgument, "bad connector config")

	err := runTransportConnectorStreamWithDeps(
		context.Background(),
		"connector:9000",
		bindingReference{},
		noopStreamingEngram{},
		newEnvResolver(map[string]string{contracts.GRPCReconnectMaxRetriesEnv: "3"}),
		func(context.Context, string, bindingReference, engram.StreamingEngram[struct{}], envResolver) error {
			attempts++
			return sessionErr
		},
		func(context.Context, time.Duration) error {
			sleeps++
			return nil
		},
		func(wait time.Duration, max time.Duration) time.Duration {
			return wait
		},
	)
	if !errors.Is(err, sessionErr) {
		t.Fatalf("expected original non-retriable error, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected one attempt, got %d", attempts)
	}
	if sleeps != 0 {
		t.Fatalf("expected no reconnect sleeps, got %d", sleeps)
	}
}

func TestIsRetriableTransportSessionError_TimedSendCleanupTimeoutNotRetriable(t *testing.T) {
	cleanupTimeoutErr := fmt.Errorf("%w: timeout=%s", errTimedSendCleanupTimeout, time.Second)
	if isRetriableTransportSessionError(cleanupTimeoutErr) {
		t.Fatalf("expected timed send cleanup timeout to be non-retriable, got retriable")
	}
}

func TestRunTransportConnectorStreamDoesNotRetryTimedSendCleanupTimeout(t *testing.T) {
	attempts := 0
	sleeps := 0
	sessionErr := errors.Join(errors.New("temporary transport failure"), errTimedSendCleanupTimeout)

	err := runTransportConnectorStreamWithDeps(
		context.Background(),
		"connector:9000",
		bindingReference{},
		noopStreamingEngram{},
		newEnvResolver(map[string]string{contracts.GRPCReconnectMaxRetriesEnv: "3"}),
		func(context.Context, string, bindingReference, engram.StreamingEngram[struct{}], envResolver) error {
			attempts++
			return sessionErr
		},
		func(context.Context, time.Duration) error {
			sleeps++
			return nil
		},
		func(wait time.Duration, max time.Duration) time.Duration {
			return wait
		},
	)
	if !errors.Is(err, errTimedSendCleanupTimeout) {
		t.Fatalf("expected timed send cleanup timeout, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected one attempt for cleanup-timeout error, got %d", attempts)
	}
	if sleeps != 0 {
		t.Fatalf("expected no reconnect sleeps on cleanup-timeout error, got %d", sleeps)
	}
}

func TestRunTransportConnectorStreamDoesNotRetryControlStartupHandshakeTimeout(t *testing.T) {
	attempts := 0
	sleeps := 0
	sessionErr := fmt.Errorf("%w: timeout=%s", errControlStartupHandshakeTimeout, time.Second)

	err := runTransportConnectorStreamWithDeps(
		context.Background(),
		"connector:9000",
		bindingReference{},
		noopStreamingEngram{},
		newEnvResolver(map[string]string{contracts.GRPCReconnectMaxRetriesEnv: "3"}),
		func(context.Context, string, bindingReference, engram.StreamingEngram[struct{}], envResolver) error {
			attempts++
			return sessionErr
		},
		func(context.Context, time.Duration) error {
			sleeps++
			return nil
		},
		func(wait time.Duration, max time.Duration) time.Duration {
			return wait
		},
	)
	if !errors.Is(err, errControlStartupHandshakeTimeout) {
		t.Fatalf("expected control startup handshake timeout, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected one attempt for startup-handshake error, got %d", attempts)
	}
	if sleeps != 0 {
		t.Fatalf("expected no reconnect sleeps on startup-handshake error, got %d", sleeps)
	}
}

func TestRunTransportConnectorStreamDoesNotRetryBlockedDataSendCleanupTimeout(t *testing.T) {
	prevDial := connectorDial
	t.Cleanup(func() {
		connectorDial = prevDial
	})

	sendStarted := make(chan struct{})
	sendRelease := make(chan struct{})
	client := &fakeSessionTransportClient{
		dataStream: &blockingDataSessionStream{
			sendStarted: sendStarted,
			sendBlock:   sendRelease,
		},
		controlStream: &blockingControlSessionStream{},
	}
	attempts := 0
	connectorDial = func(context.Context, string, envResolver, ...grpc.DialOption) (*TransportConnectorClient, error) {
		attempts++
		return &TransportConnectorClient{client: client}, nil
	}

	sleeps := 0
	errCh := make(chan error, 1)
	go func() {
		errCh <- runTransportConnectorStreamWithDeps(
			context.Background(),
			"connector:9000",
			bindingReference{},
			singleMessageStreamingEngram{msg: engram.StreamMessage{Payload: []byte(`{"ok":true}`)}},
			newEnvResolver(map[string]string{
				contracts.GRPCMessageTimeoutEnv:      "15ms",
				contracts.GRPCReconnectMaxRetriesEnv: "3",
			}),
			nil,
			func(context.Context, time.Duration) error {
				sleeps++
				return nil
			},
			func(wait time.Duration, max time.Duration) time.Duration {
				return wait
			},
		)
	}()

	select {
	case <-sendStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected data send loop to enter blocked Send path")
	}

	var err error
	select {
	case err = <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("expected blocked send cleanup timeout to terminate outer reconnect loop")
	}
	close(sendRelease)

	if !errors.Is(err, errTimedSendCleanupTimeout) {
		t.Fatalf("expected timed send cleanup timeout, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected one connector dial attempt, got %d", attempts)
	}
	if sleeps != 0 {
		t.Fatalf("expected no reconnect sleeps after blocked send cleanup timeout, got %d", sleeps)
	}
}

func TestRunTransportConnectorStreamRetriesTransientErrorsWithBackoff(t *testing.T) {
	attempts := 0
	var sleeps []time.Duration

	err := runTransportConnectorStreamWithDeps(
		context.Background(),
		"connector:9000",
		bindingReference{},
		noopStreamingEngram{},
		newEnvResolver(map[string]string{
			contracts.GRPCReconnectBaseBackoffEnv: "100ms",
			contracts.GRPCReconnectMaxBackoffEnv:  "200ms",
			contracts.GRPCReconnectMaxRetriesEnv:  "3",
		}),
		func(context.Context, string, bindingReference, engram.StreamingEngram[struct{}], envResolver) error {
			attempts++
			if attempts < 3 {
				return errors.New("temporary transport failure")
			}
			return nil
		},
		func(_ context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			return nil
		},
		func(wait time.Duration, max time.Duration) time.Duration {
			return wait
		},
	)
	if err != nil {
		t.Fatalf("expected retries to recover, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
	if len(sleeps) != 2 {
		t.Fatalf("expected 2 reconnect sleeps, got %d", len(sleeps))
	}
	if sleeps[0] != 100*time.Millisecond || sleeps[1] != 200*time.Millisecond {
		t.Fatalf("unexpected reconnect sleeps: %#v", sleeps)
	}
}

func TestRunTransportConnectorStreamReusesPacketDeduperAcrossRetries(t *testing.T) {
	attempts := 0
	var dedupers []*packetDeduper

	err := runTransportConnectorStreamWithDeps(
		context.Background(),
		"connector:9000",
		bindingReference{},
		noopStreamingEngram{},
		newEnvResolver(map[string]string{contracts.GRPCReconnectMaxRetriesEnv: "2"}),
		func(ctx context.Context, _ string, _ bindingReference, _ engram.StreamingEngram[struct{}], _ envResolver) error {
			attempts++
			deduper := packetDeduperFromContext(ctx)
			if deduper == nil {
				t.Fatal("expected packet deduper in transport session context")
			}
			dedupers = append(dedupers, deduper)
			if attempts < 2 {
				return errors.New("temporary transport failure")
			}
			return nil
		},
		func(context.Context, time.Duration) error {
			return nil
		},
		func(wait time.Duration, max time.Duration) time.Duration {
			return wait
		},
	)
	if err != nil {
		t.Fatalf("expected retries to recover, got %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
	if len(dedupers) != 2 {
		t.Fatalf("expected 2 deduper captures, got %d", len(dedupers))
	}
	if dedupers[0] != dedupers[1] {
		t.Fatal("expected reconnect attempts to reuse the same packet deduper")
	}
}

func TestRunTransportConnectorStreamResetsControlRequestQueueAcrossRetries(t *testing.T) {
	attempts := 0
	var queues []chan *transportpb.ControlRequest

	err := runTransportConnectorStreamWithDeps(
		context.Background(),
		"connector:9000",
		bindingReference{},
		noopStreamingEngram{},
		newEnvResolver(map[string]string{contracts.GRPCReconnectMaxRetriesEnv: "2"}),
		func(ctx context.Context, _ string, _ bindingReference, _ engram.StreamingEngram[struct{}], _ envResolver) error {
			attempts++
			queue := controlRequestQueueFromContext(ctx)
			if queue == nil {
				t.Fatal("expected control request queue in transport session context")
			}
			queues = append(queues, queue)
			if attempts == 1 {
				queue <- &transportpb.ControlRequest{CustomAction: downstreamDeliveryReceiptType}
				return errors.New("temporary transport failure")
			}
			select {
			case req := <-queue:
				t.Fatalf("unexpected stale control request carried across retry: %+v", req)
			default:
			}
			return nil
		},
		func(context.Context, time.Duration) error {
			return nil
		},
		func(wait time.Duration, max time.Duration) time.Duration {
			return wait
		},
	)
	if err != nil {
		t.Fatalf("expected retries to recover, got %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
	if len(queues) != 2 {
		t.Fatalf("expected 2 queue captures, got %d", len(queues))
	}
	if queues[0] == queues[1] {
		t.Fatal("expected reconnect attempts to use a fresh control request queue")
	}
}

func TestRunTransportSessionReturnsRetriableEOFWhenDataRecvClosesUnexpectedly(t *testing.T) {
	prevDial := connectorDial
	t.Cleanup(func() {
		connectorDial = prevDial
	})

	client := &fakeSessionTransportClient{
		dataStream:    &fakeDataSendLoopStream{},
		controlStream: &blockingControlSessionStream{},
	}
	connectorDial = func(context.Context, string, envResolver, ...grpc.DialOption) (*TransportConnectorClient, error) {
		return &TransportConnectorClient{client: client}, nil
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- runTransportSession(
			context.Background(),
			"connector:9000",
			bindingReference{},
			blockingStreamingEngram{},
			newEnvResolver(nil),
		)
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected data recv EOF session error, got %v", err)
		}
		if !isRetriableTransportSessionError(err) {
			t.Fatalf("expected unexpected data recv EOF to be retriable, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected session to terminate when data recv stream closes unexpectedly")
	}
}

func TestRunTransportSessionReturnsNilWhenEngramStreamEndsGracefully(t *testing.T) {
	prevDial := connectorDial
	t.Cleanup(func() {
		connectorDial = prevDial
	})

	client := &fakeSessionTransportClient{
		dataStream:    &blockingDataSessionStream{},
		controlStream: &blockingControlSessionStream{},
	}
	connectorDial = func(context.Context, string, envResolver, ...grpc.DialOption) (*TransportConnectorClient, error) {
		return &TransportConnectorClient{client: client}, nil
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- runTransportSession(
			context.Background(),
			"connector:9000",
			bindingReference{},
			noopStreamingEngram{},
			newEnvResolver(nil),
		)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected graceful stream completion to end session cleanly, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected session to terminate when engram stream exits cleanly")
	}
}

func TestJitterReconnectDelayClampsToMax(t *testing.T) {
	prev := reconnectJitterFloat64
	reconnectJitterFloat64 = func() float64 { return 1.0 }
	t.Cleanup(func() {
		reconnectJitterFloat64 = prev
	})

	delay := jitterReconnectDelay(100*time.Millisecond, 110*time.Millisecond)
	if delay != 110*time.Millisecond {
		t.Fatalf("expected jittered delay to clamp to max, got %s", delay)
	}
}

type fakeDataSendLoopStream struct {
	closeSendErr   error
	closeSendCalls int
	sendCalls      int
}

func (f *fakeDataSendLoopStream) Send(*transportpb.DataRequest) error {
	f.sendCalls++
	return nil
}
func (f *fakeDataSendLoopStream) Recv() (*transportpb.DataResponse, error) {
	return nil, io.EOF
}
func (f *fakeDataSendLoopStream) CloseSend() error {
	f.closeSendCalls++
	return f.closeSendErr
}
func (f *fakeDataSendLoopStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (f *fakeDataSendLoopStream) Trailer() metadata.MD         { return metadata.MD{} }
func (f *fakeDataSendLoopStream) Context() context.Context     { return context.Background() }
func (f *fakeDataSendLoopStream) SendMsg(any) error            { return nil }
func (f *fakeDataSendLoopStream) RecvMsg(any) error            { return nil }

type fakeControlLoopStream struct {
	closeSendErr   error
	closeSendCalls int
	sendErr        error
	recvErr        error
	sendStarted    chan struct{}
	sendBlock      <-chan struct{}
	recvWait       <-chan struct{}
	sendOnce       sync.Once
}

func (f *fakeControlLoopStream) Send(*transportpb.ControlRequest) error {
	if f.sendStarted != nil {
		f.sendOnce.Do(func() { close(f.sendStarted) })
	}
	if f.sendBlock != nil {
		<-f.sendBlock
	}
	return f.sendErr
}
func (f *fakeControlLoopStream) Recv() (*transportpb.ControlResponse, error) {
	if f.recvWait != nil {
		<-f.recvWait
	}
	if f.recvErr != nil {
		return nil, f.recvErr
	}
	return nil, io.EOF
}
func (f *fakeControlLoopStream) CloseSend() error {
	f.closeSendCalls++
	return f.closeSendErr
}
func (f *fakeControlLoopStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (f *fakeControlLoopStream) Trailer() metadata.MD         { return metadata.MD{} }
func (f *fakeControlLoopStream) Context() context.Context     { return context.Background() }
func (f *fakeControlLoopStream) SendMsg(any) error            { return nil }
func (f *fakeControlLoopStream) RecvMsg(any) error            { return nil }

type fakeControlLoopClient struct {
	stream transportpb.TransportConnectorService_ControlClient
}

func (f *fakeControlLoopClient) Data(context.Context, ...grpc.CallOption) (transportpb.TransportConnectorService_DataClient, error) { //nolint:lll
	return nil, errors.New("unexpected Data call in control loop test")
}

func (f *fakeControlLoopClient) Control(context.Context, ...grpc.CallOption) (transportpb.TransportConnectorService_ControlClient, error) { //nolint:lll
	return f.stream, nil
}

func (f *fakeControlLoopClient) HubPush(context.Context, ...grpc.CallOption) (transportpb.TransportConnectorService_HubPushClient, error) { //nolint:lll
	return nil, errors.New("unexpected HubPush call in control loop test")
}

type fakeSessionTransportClient struct {
	dataStream    transportpb.TransportConnectorService_DataClient
	controlStream transportpb.TransportConnectorService_ControlClient
}

func (f *fakeSessionTransportClient) Data(ctx context.Context, _ ...grpc.CallOption) (transportpb.TransportConnectorService_DataClient, error) { //nolint:lll
	if f.dataStream == nil {
		return nil, errors.New("missing data stream")
	}
	if s, ok := f.dataStream.(*blockingDataSessionStream); ok {
		s.ctx = ctx
	}
	return f.dataStream, nil
}

func (f *fakeSessionTransportClient) Control(
	ctx context.Context,
	_ ...grpc.CallOption,
) (transportpb.TransportConnectorService_ControlClient, error) {
	if f.controlStream == nil {
		return nil, errors.New("missing control stream")
	}
	if s, ok := f.controlStream.(*blockingControlSessionStream); ok {
		s.ctx = ctx
	}
	return f.controlStream, nil
}

func (f *fakeSessionTransportClient) HubPush(context.Context, ...grpc.CallOption) (transportpb.TransportConnectorService_HubPushClient, error) { //nolint:lll
	return nil, errors.New("unexpected HubPush call in session test")
}

type blockingControlSessionStream struct {
	ctx       context.Context
	readyOnce sync.Once
}

func (s *blockingControlSessionStream) Send(*transportpb.ControlRequest) error { return nil }
func (s *blockingControlSessionStream) Recv() (*transportpb.ControlResponse, error) {
	var ready *transportpb.ControlResponse
	s.readyOnce.Do(func() {
		ready = &transportpb.ControlResponse{
			Action: transportpb.ControlAction_CONTROL_ACTION_CONNECTOR_READY,
			Metadata: map[string]string{
				coretransport.StartupCapabilitiesMetadataKey: coretransport.StartupCapabilitiesNone,
			},
		}
	})
	if ready != nil {
		return ready, nil
	}
	if s.ctx == nil {
		return nil, io.EOF
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}
func (s *blockingControlSessionStream) CloseSend() error             { return nil }
func (s *blockingControlSessionStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *blockingControlSessionStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *blockingControlSessionStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}
func (s *blockingControlSessionStream) SendMsg(any) error { return nil }
func (s *blockingControlSessionStream) RecvMsg(any) error { return nil }

type blockingDataSessionStream struct {
	ctx         context.Context
	sendStarted chan struct{}
	sendBlock   <-chan struct{}
	sendOnce    sync.Once
}

func (s *blockingDataSessionStream) Send(*transportpb.DataRequest) error {
	if s.sendStarted != nil {
		s.sendOnce.Do(func() { close(s.sendStarted) })
	}
	if s.sendBlock != nil {
		<-s.sendBlock
	}
	return nil
}
func (s *blockingDataSessionStream) Recv() (*transportpb.DataResponse, error) {
	if s.ctx == nil {
		return nil, io.EOF
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}
func (s *blockingDataSessionStream) CloseSend() error             { return nil }
func (s *blockingDataSessionStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *blockingDataSessionStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *blockingDataSessionStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}
func (s *blockingDataSessionStream) SendMsg(any) error { return nil }
func (s *blockingDataSessionStream) RecvMsg(any) error { return nil }

func TestConnectorDataSendLoopCanceledExitAttemptsCloseSendButReturnsContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stream := &fakeDataSendLoopStream{closeSendErr: errors.New("close send failed")}
	err := connectorDataSendLoop(ctx, stream, make(chan engram.StreamMessage), streamRuntimeOptions{})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled on canceled teardown, got %v", err)
	}
	if stream.closeSendCalls != 1 {
		t.Fatalf("expected CloseSend to be attempted once on canceled teardown, got %d", stream.closeSendCalls)
	}
	if strings.Contains(err.Error(), "close send failed") {
		t.Fatalf("expected canceled error to remain primary, got %v", err)
	}
}

func TestConnectorControlLoopCanceledExitAttemptsCloseSendButReturnsContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stream := &fakeControlLoopStream{closeSendErr: errors.New("close send failed")}
	client := &fakeControlLoopClient{stream: stream}
	err := connectorControlLoop(
		ctx,
		client,
		bindingReference{},
		defaultControlDirectiveHandler{},
		streamRuntimeOptions{
			controlRequests: make(chan *transportpb.ControlRequest),
		},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled on canceled teardown, got %v", err)
	}
	if stream.closeSendCalls != 1 {
		t.Fatalf("expected CloseSend to be attempted once on canceled teardown, got %d", stream.closeSendCalls)
	}
	if strings.Contains(err.Error(), "close send failed") {
		t.Fatalf("expected canceled error to remain primary, got %v", err)
	}
}

func TestConnectorControlLoopEOFReturnsRetriableError(t *testing.T) {
	stream := &fakeControlLoopStream{}
	client := &fakeControlLoopClient{stream: stream}
	err := connectorControlLoop(
		context.Background(),
		client,
		bindingReference{},
		defaultControlDirectiveHandler{},
		streamRuntimeOptions{
			controlRequests: make(chan *transportpb.ControlRequest),
		},
	)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF-wrapped control loop error, got %v", err)
	}
	if !isRetriableTransportSessionError(err) {
		t.Fatalf("expected control stream EOF to be retriable, got %v", err)
	}
}

func TestConnectorControlLoopInternalSendTimeoutIsTrackedAndDrainsAfterRelease(t *testing.T) {
	sendTracker := newTimedSendTracker()
	sendStarted := make(chan struct{})
	sendRelease := make(chan struct{})
	stream := &fakeControlLoopStream{
		sendStarted: sendStarted,
		sendBlock:   sendRelease,
		recvWait:    sendStarted, // keep recv side idle until send path is entered
	}
	client := &fakeControlLoopClient{stream: stream}

	ctx := t.Context()

	controlRequests := make(chan *transportpb.ControlRequest, 1)
	controlRequests <- &transportpb.ControlRequest{CustomAction: "blocked-send"}

	errCh := make(chan error, 1)
	go func() {
		errCh <- connectorControlLoop(
			ctx,
			client,
			bindingReference{},
			defaultControlDirectiveHandler{},
			streamRuntimeOptions{
				controlRequests:          controlRequests,
				messageTimeout:           15 * time.Millisecond,
				controlHeartbeatInterval: time.Hour,
				sendTracker:              sendTracker,
			},
		)
	}()

	select {
	case <-sendStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected control loop to start blocked internal send")
	}

	if err := waitForTimedSendCleanup(nil, sendTracker, 10*time.Millisecond); !errors.Is(err, errTimedSendCleanupTimeout) {
		t.Fatalf("expected timed send cleanup timeout while control send is blocked, got %v", err)
	}

	select {
	case err := <-errCh:
		if !strings.Contains(err.Error(), "control stream send failed") || !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("expected control send failure, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected connectorControlLoop to return timeout while send worker remains blocked")
	}

	close(sendRelease)
	if err := waitForTimedSendCleanup(nil, sendTracker, 200*time.Millisecond); err != nil {
		t.Fatalf("expected timed send tracker to drain after release, got %v", err)
	}
}

func TestProcessControlMessageSendTimeoutIsTrackedAndDrainsAfterRelease(t *testing.T) {
	sendTracker := newTimedSendTracker()
	sendStarted := make(chan struct{})
	sendRelease := make(chan struct{})
	stream := &fakeControlLoopStream{
		sendStarted: sendStarted,
		sendBlock:   sendRelease,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- processControlMessage(
			ctx,
			stream,
			defaultControlDirectiveHandler{},
			controlMessage{
				response: &transportpb.ControlResponse{
					Action: transportpb.ControlAction_CONTROL_ACTION_NOOP,
				},
			},
			15*time.Millisecond,
			cancel,
			sendTracker,
		)
	}()

	select {
	case <-sendStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected processControlMessage to start blocked response send")
	}

	if err := waitForTimedSendCleanup(nil, sendTracker, 10*time.Millisecond); !errors.Is(err, errTimedSendCleanupTimeout) {
		t.Fatalf("expected timed send cleanup timeout while control response send is blocked, got %v", err)
	}

	select {
	case err := <-errCh:
		if !strings.Contains(err.Error(), "control stream send failed") || !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("expected timed-out control response send failure, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected processControlMessage to return timeout while send worker remains blocked")
	}

	close(sendRelease)

	if err := waitForTimedSendCleanup(nil, sendTracker, 200*time.Millisecond); err != nil {
		t.Fatalf("expected timed send tracker to drain after release, got %v", err)
	}
}

func TestSendControlHeartbeatTimeoutIsTrackedAndDrainsAfterRelease(t *testing.T) {
	sendTracker := newTimedSendTracker()
	sendStarted := make(chan struct{})
	sendRelease := make(chan struct{})
	stream := &fakeControlLoopStream{
		sendStarted: sendStarted,
		sendBlock:   sendRelease,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- sendControlHeartbeat(ctx, stream, 15*time.Millisecond, cancel, sendTracker)
	}()

	select {
	case <-sendStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected sendControlHeartbeat to start blocked heartbeat send")
	}

	if err := waitForTimedSendCleanup(nil, sendTracker, 10*time.Millisecond); !errors.Is(err, errTimedSendCleanupTimeout) {
		t.Fatalf("expected timed send cleanup timeout while control heartbeat is blocked, got %v", err)
	}

	select {
	case err := <-errCh:
		if !strings.Contains(err.Error(), "control stream heartbeat failed") || !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("expected timed-out control heartbeat send failure, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected sendControlHeartbeat to return timeout while send worker remains blocked")
	}

	close(sendRelease)

	if err := waitForTimedSendCleanup(nil, sendTracker, 200*time.Millisecond); err != nil {
		t.Fatalf("expected timed send tracker to drain after release, got %v", err)
	}
}
