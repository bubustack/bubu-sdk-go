package sdk

import (
	context "context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bubu-sdk-go/k8s"
	sdkenv "github.com/bubustack/bubu-sdk-go/pkg/env"
	"github.com/bubustack/core/contracts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ErrSignalsUnavailable indicates that the current process cannot emit signals (e.g.,
// it is not running inside a StepRun workload). Callers may treat this as a soft
// failure and continue without emitting metadata.
var ErrSignalsUnavailable = errors.New("signal emission unavailable: not running inside a StepRun")

const (
	defaultMaxSignalPayloadBytes = 8 * 1024
	signalPatchTimeout           = 3 * time.Second
	signalMaxPayloadBytesEnv     = "BUBU_SIGNAL_MAX_PAYLOAD_BYTES"
)

var (
	signalEmitterMu     sync.Mutex
	signalEmitterInst   *signalEmitter
	signalEmitterErr    error // only set for permanent errors (missing env)
	signalClientFactory = func() (signalPatcher, error) { return k8s.SharedClient() }
	defaultSignalSeq    = NewSignalSequence(0)
)

type signalPatcher interface {
	PatchStepRunStatus(ctx context.Context, stepRunName string, status runsv1alpha1.StepRunStatus) error
}

type signalEmitter struct {
	client    signalPatcher
	stepRunID string
}

// EmitSignal patches the current StepRun status with a small JSON payload so that
// controllers and CEL expressions can react to live metadata. When value is nil,
// the signal is cleared. The latest value stored in status.signals is always a
// compact summary so raw payloads are not persisted in StepRun status. Signal
// events are sequenced by default.
func EmitSignal(ctx context.Context, key string, value any) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("signal key is required")
	}
	seq := uint64(0)
	if defaultSignalSeq != nil {
		seq = defaultSignalSeq.Next()
	}
	return emitSignalEvent(ctx, key, value, seq)
}

func summarizeSignalForStatus(value any, raw []byte) map[string]any {
	meta := summarizeSignalValue(value, raw)
	meta["redacted"] = true
	return meta
}

func summarizeSignalValue(value any, raw []byte) map[string]any {
	meta := map[string]any{
		"sizeBytes": len(raw),
	}
	if len(raw) > 0 {
		sum := sha256.Sum256(raw)
		meta["sha256"] = fmt.Sprintf("%x", sum)
	}
	kind, details := signalTypeSummary(unwrapSignalSummaryValue(value))
	meta["type"] = kind
	if len(details) > 0 {
		meta["details"] = details
	}
	return meta
}

func unwrapSignalSummaryValue(value any) any {
	switch v := value.(type) {
	case SequencedSignal:
		return v.Value
	case *SequencedSignal:
		if v == nil {
			return nil
		}
		return v.Value
	default:
		return value
	}
}

func signalTypeSummary(value any) (string, map[string]any) {
	if value == nil {
		return "null", nil
	}
	v := reflect.ValueOf(value)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return "null", nil
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Map:
		return "map", map[string]any{"len": v.Len()} //nolint:goconst
	case reflect.Slice, reflect.Array:
		return "array", map[string]any{"len": v.Len()}
	case reflect.String:
		return "string", map[string]any{"len": v.Len()}
	case reflect.Struct:
		return "struct", nil
	case reflect.Bool:
		return "bool", nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "int", nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "uint", nil
	case reflect.Float32, reflect.Float64:
		return "float", nil
	default:
		return v.Kind().String(), nil
	}
}

func getSignalEmitter() (*signalEmitter, error) {
	signalEmitterMu.Lock()
	defer signalEmitterMu.Unlock()
	if signalEmitterInst != nil {
		return signalEmitterInst, nil
	}
	if signalEmitterErr != nil {
		// Permanent error (e.g. missing env) — don't retry.
		return nil, signalEmitterErr
	}
	stepRunID := strings.TrimSpace(os.Getenv(contracts.StepRunNameEnv))
	if stepRunID == "" {
		signalEmitterErr = ErrSignalsUnavailable
		return nil, signalEmitterErr
	}
	client, err := signalClientFactory()
	if err != nil {
		// Transient error — don't cache, allow retry on next call.
		return nil, fmt.Errorf("failed to initialize signal client: %w", err)
	}
	signalEmitterInst = &signalEmitter{
		client:    client,
		stepRunID: stepRunID,
	}
	return signalEmitterInst, nil
}

func emitSignalEvent(ctx context.Context, key string, value any, seq uint64) error {
	emitter, err := getSignalEmitter()
	if err != nil {
		return err
	}
	maxPayloadBytes := resolveSignalMaxPayloadBytes()
	var payloadRaw []byte
	var statusRaw []byte
	if value != nil {
		payloadRaw, err = json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal signal payload: %w", err)
		}
		statusValue := statusSignalValue(value, payloadRaw, maxPayloadBytes)
		statusRaw, err = json.Marshal(statusValue)
		if err != nil {
			return fmt.Errorf("failed to marshal signal status summary: %w", err)
		}
		if len(statusRaw) > maxPayloadBytes {
			return fmt.Errorf("signal summary for %s exceeds %d bytes", key, maxPayloadBytes)
		}
	}
	return emitter.emit(ctx, key, statusRaw, value, payloadRaw, seq)
}

func statusSignalValue(value any, raw []byte, maxPayloadBytes int) any {
	switch v := value.(type) {
	case SignalEnvelope:
		if len(raw) <= maxPayloadBytes {
			return v
		}
	case *SignalEnvelope:
		if v != nil && len(raw) <= maxPayloadBytes {
			return *v
		}
	case SignalMeta:
		if len(raw) <= maxPayloadBytes {
			return v
		}
	case *SignalMeta:
		if v != nil && len(raw) <= maxPayloadBytes {
			return *v
		}
	case SequencedSignal:
		return statusSignalValue(v.Value, raw, maxPayloadBytes)
	case *SequencedSignal:
		if v != nil {
			return statusSignalValue(v.Value, raw, maxPayloadBytes)
		}
	}
	return summarizeSignalForStatus(value, raw)
}

func resolveSignalMaxPayloadBytes() int {
	return sdkenv.GetInt(signalMaxPayloadBytesEnv, defaultMaxSignalPayloadBytes)
}

func (e *signalEmitter) emit(ctx context.Context, key string, statusRaw []byte,
	signalValue any, payloadRaw []byte, seq uint64) error {
	if e == nil {
		return ErrSignalsUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, signalPatchTimeout)
	defer cancel()
	value := runtime.RawExtension{Raw: statusRaw}
	var eventPayload *runtime.RawExtension
	if len(payloadRaw) > 0 {
		eventSummaryRaw, err := json.Marshal(summarizeSignalValue(signalValue, payloadRaw))
		if err != nil {
			return fmt.Errorf("failed to marshal signal event summary: %w", err)
		}
		eventPayload = &runtime.RawExtension{Raw: eventSummaryRaw}
	}
	var eventTime *metav1.Time
	var events []runsv1alpha1.SignalEvent
	if seq > 0 {
		now := metav1.NewTime(time.Now().UTC())
		eventTime = &now
		events = []runsv1alpha1.SignalEvent{{
			Seq:       seq,
			Key:       key,
			EmittedAt: eventTime,
			Payload:   eventPayload,
		}}
	}
	patch := runsv1alpha1.StepRunStatus{
		Signals:      map[string]runtime.RawExtension{key: value},
		SignalEvents: events,
	}
	return e.client.PatchStepRunStatus(ctx, e.stepRunID, patch)
}

// testResetSignalEmitter resets the cached emitter between tests.
func testResetSignalEmitter() {
	signalEmitterMu.Lock()
	defer signalEmitterMu.Unlock()
	signalEmitterInst = nil
	signalEmitterErr = nil
	defaultSignalSeq = NewSignalSequence(0)
}
