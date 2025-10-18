package sdk

import (
	context "context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/contracts"
	"github.com/bubustack/bubu-sdk-go/k8s"
	"k8s.io/apimachinery/pkg/runtime"
)

// ErrSignalsUnavailable indicates that the current process cannot emit signals (e.g.,
// it is not running inside a StepRun workload). Callers may treat this as a soft
// failure and continue without emitting metadata.
var ErrSignalsUnavailable = errors.New("signal emission unavailable: not running inside a StepRun")

const (
	maxSignalPayloadBytes    = 8 * 1024
	signalPatchTimeout       = 3 * time.Second
	signalFieldManagerPrefix = "bubu-sdk-go"
)

var (
	signalEmitterOnce   sync.Once
	signalEmitterInst   *signalEmitter
	signalEmitterErr    error
	signalClientFactory = func() (signalPatcher, error) { return k8s.NewClient() }
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
// the signal is cleared. Payloads larger than 8 KiB are rejected to keep etcd usage
// predictable.
func EmitSignal(ctx context.Context, key string, value any) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("signal key is required")
	}
	emitter, err := getSignalEmitter()
	if err != nil {
		return err
	}
	var raw []byte
	if value != nil {
		raw, err = json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal signal payload: %w", err)
		}
		if len(raw) > maxSignalPayloadBytes {
			return fmt.Errorf("signal payload for %s exceeds %d bytes", key, maxSignalPayloadBytes)
		}
	}
	return emitter.emit(ctx, key, raw)
}

func getSignalEmitter() (*signalEmitter, error) {
	signalEmitterOnce.Do(func() {
		stepRunID := strings.TrimSpace(os.Getenv(contracts.StepRunNameEnv))
		if stepRunID == "" {
			signalEmitterErr = ErrSignalsUnavailable
			return
		}
		client, err := signalClientFactory()
		if err != nil {
			signalEmitterErr = fmt.Errorf("failed to initialize signal client: %w", err)
			return
		}
		signalEmitterInst = &signalEmitter{
			client:    client,
			stepRunID: stepRunID,
		}
	})
	return signalEmitterInst, signalEmitterErr
}

func (e *signalEmitter) emit(ctx context.Context, key string, raw []byte) error {
	if e == nil {
		return ErrSignalsUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, signalPatchTimeout)
	defer cancel()
	value := runtime.RawExtension{Raw: raw}
	patch := runsv1alpha1.StepRunStatus{
		Signals: map[string]runtime.RawExtension{key: value},
	}
	return e.client.PatchStepRunStatus(ctx, e.stepRunID, patch)
}

// testResetSignalEmitter resets the cached emitter between tests.
func testResetSignalEmitter() {
	signalEmitterOnce = sync.Once{}
	signalEmitterInst = nil
	signalEmitterErr = nil
}
