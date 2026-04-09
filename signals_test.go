package sdk

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/core/contracts"
)

type recordingSignalPatcher struct {
	calls []runsv1alpha1.StepRunStatus
}

func (r *recordingSignalPatcher) PatchStepRunStatus( //nolint:lll
	_ context.Context, _ string, status runsv1alpha1.StepRunStatus,
) error {
	r.calls = append(r.calls, status)
	return nil
}

func TestResolveSignalMaxPayloadBytesFromEnv(t *testing.T) {
	t.Setenv(signalMaxPayloadBytesEnv, "256")
	if got := resolveSignalMaxPayloadBytes(); got != 256 {
		t.Fatalf("resolveSignalMaxPayloadBytes() = %d, want 256", got)
	}
}

func TestResolveSignalMaxPayloadBytesFallsBackOnInvalidEnv(t *testing.T) {
	t.Setenv(signalMaxPayloadBytesEnv, "invalid")
	if got := resolveSignalMaxPayloadBytes(); got != defaultMaxSignalPayloadBytes {
		t.Fatalf("resolveSignalMaxPayloadBytes() = %d, want %d", got, defaultMaxSignalPayloadBytes)
	}
}

func TestEmitSignalAcceptsNilContext(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	testResetSignalEmitter()

	prevFactory := signalClientFactory
	patcher := &recordingSignalPatcher{}
	signalClientFactory = func() (signalPatcher, error) { return patcher, nil }
	t.Cleanup(func() {
		signalClientFactory = prevFactory
		testResetSignalEmitter()
	})

	if err := EmitSignal(nil, "state", map[string]any{"ok": true}); err != nil { //nolint:staticcheck
		t.Fatalf("EmitSignal(nil, ...) returned error: %v", err)
	}
	if len(patcher.calls) != 1 {
		t.Fatalf("expected one patch call, got %d", len(patcher.calls))
	}

	raw := patcher.calls[0].Signals["state"].Raw
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal signal status summary: %v", err)
	}
	if payload["redacted"] != true {
		t.Fatalf("expected redacted summary, got %#v", payload["redacted"])
	}
	if payload["type"] != "map" {
		t.Fatalf("unexpected summary type: %#v", payload["type"])
	}
}

func TestEmitSequencedSignalAcceptsNilContext(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	testResetSignalEmitter()

	prevFactory := signalClientFactory
	patcher := &recordingSignalPatcher{}
	signalClientFactory = func() (signalPatcher, error) { return patcher, nil }
	t.Cleanup(func() {
		signalClientFactory = prevFactory
		testResetSignalEmitter()
	})

	if err := EmitSequencedSignal(nil, "state", 9, map[string]any{"ok": true}); err != nil { //nolint:staticcheck
		t.Fatalf("EmitSequencedSignal(nil, ...) returned error: %v", err)
	}
	if len(patcher.calls) != 1 {
		t.Fatalf("expected one patch call, got %d", len(patcher.calls))
	}
	if len(patcher.calls[0].SignalEvents) != 1 {
		t.Fatalf("expected one signal event, got %d", len(patcher.calls[0].SignalEvents))
	}
	if patcher.calls[0].SignalEvents[0].Seq != 9 {
		t.Fatalf("expected sequence 9, got %d", patcher.calls[0].SignalEvents[0].Seq)
	}
}

func TestEmitSequencedSignalStoresSummaryPayloadInEvents(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	testResetSignalEmitter()

	prevFactory := signalClientFactory
	patcher := &recordingSignalPatcher{}
	signalClientFactory = func() (signalPatcher, error) { return patcher, nil }
	t.Cleanup(func() {
		signalClientFactory = prevFactory
		testResetSignalEmitter()
	})

	if err := EmitSequencedSignal(context.Background(), "state", 7, map[string]any{
		"token": "s3cr3t",
		"ok":    true,
	}); err != nil {
		t.Fatalf("EmitSequencedSignal(...) returned error: %v", err)
	}

	if len(patcher.calls) != 1 {
		t.Fatalf("expected one patch call, got %d", len(patcher.calls))
	}
	if len(patcher.calls[0].SignalEvents) != 1 {
		t.Fatalf("expected one signal event, got %d", len(patcher.calls[0].SignalEvents))
	}

	signalRaw := patcher.calls[0].Signals["state"].Raw
	if strings.Contains(string(signalRaw), "s3cr3t") {
		t.Fatalf("expected latest status signal to stay summarized, got %s", signalRaw)
	}

	eventPayload := patcher.calls[0].SignalEvents[0].Payload
	if eventPayload == nil {
		t.Fatal("expected signal event payload summary")
	}
	if strings.Contains(string(eventPayload.Raw), "s3cr3t") {
		t.Fatalf("expected signal event summary to redact raw payload, got %s", eventPayload.Raw)
	}

	var summary map[string]any
	if err := json.Unmarshal(eventPayload.Raw, &summary); err != nil {
		t.Fatalf("unmarshal signal event summary: %v", err)
	}
	if summary["type"] != "map" {
		t.Fatalf("unexpected signal event summary type: %#v", summary["type"])
	}
	if summary["sizeBytes"] == nil {
		t.Fatalf("expected signal event summary size, got %#v", summary["sizeBytes"])
	}
	if summary["sizeBytes"] == float64(len(signalRaw)) {
		t.Fatalf("expected event summary size to describe the original payload, got status-summary size %#v", summary["sizeBytes"]) //nolint:lll
	}
	if _, ok := summary["sha256"].(string); !ok {
		t.Fatalf("expected signal event summary hash, got %#v", summary["sha256"])
	}
}

func TestEmitSignalTruncationOmitsMapKeysFromSummary(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	testResetSignalEmitter()

	prevFactory := signalClientFactory
	patcher := &recordingSignalPatcher{}
	signalClientFactory = func() (signalPatcher, error) { return patcher, nil }
	t.Cleanup(func() {
		signalClientFactory = prevFactory
		testResetSignalEmitter()
	})

	payload := map[string]any{
		"verySensitiveFieldName": strings.Repeat("x", defaultMaxSignalPayloadBytes),
	}
	if err := EmitSignal(context.Background(), "state", payload); err != nil {
		t.Fatalf("EmitSignal(...) returned error: %v", err)
	}

	if len(patcher.calls) != 1 {
		t.Fatalf("expected one patch call, got %d", len(patcher.calls))
	}

	signalRaw := patcher.calls[0].Signals["state"].Raw
	if strings.Contains(string(signalRaw), "verySensitiveFieldName") {
		t.Fatalf("expected truncated signal summary to omit original map keys, got %s", signalRaw)
	}

	var summary map[string]any
	if err := json.Unmarshal(signalRaw, &summary); err != nil {
		t.Fatalf("unmarshal truncated signal summary: %v", err)
	}
	if summary["redacted"] != true {
		t.Fatalf("expected redacted summary marker, got %#v", summary["redacted"])
	}
	details, ok := summary["details"].(map[string]any)
	if !ok {
		t.Fatalf("expected truncated summary details, got %#v", summary["details"])
	}
	if _, exists := details["keys"]; exists {
		t.Fatalf("expected truncated summary details to omit key names, got %#v", details)
	}
}

func TestEmitSignalStoresSummaryInStatusSignals(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	testResetSignalEmitter()

	prevFactory := signalClientFactory
	patcher := &recordingSignalPatcher{}
	signalClientFactory = func() (signalPatcher, error) { return patcher, nil }
	t.Cleanup(func() {
		signalClientFactory = prevFactory
		testResetSignalEmitter()
	})

	if err := EmitSignal(context.Background(), "state", map[string]any{
		"token": "s3cr3t",
		"ok":    true,
	}); err != nil {
		t.Fatalf("EmitSignal(...) returned error: %v", err)
	}

	if len(patcher.calls) != 1 {
		t.Fatalf("expected one patch call, got %d", len(patcher.calls))
	}

	signalRaw := patcher.calls[0].Signals["state"].Raw
	if strings.Contains(string(signalRaw), "s3cr3t") {
		t.Fatalf("expected redacted signal payload in status.signals, got %s", signalRaw)
	}

	var summary map[string]any
	if err := json.Unmarshal(signalRaw, &summary); err != nil {
		t.Fatalf("unmarshal redacted signal summary: %v", err)
	}
	if summary["redacted"] != true {
		t.Fatalf("expected redacted marker, got %#v", summary["redacted"])
	}
	if summary["type"] != "map" {
		t.Fatalf("expected map summary type, got %#v", summary["type"])
	}
	if _, ok := summary["sha256"].(string); !ok {
		t.Fatalf("expected summary hash, got %#v", summary["sha256"])
	}
}

func TestEmitSignalReturnsErrorWhenSummaryExceedsConfiguredMaxPayloadBytes(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	t.Setenv(signalMaxPayloadBytesEnv, "32")
	testResetSignalEmitter()

	prevFactory := signalClientFactory
	patcher := &recordingSignalPatcher{}
	signalClientFactory = func() (signalPatcher, error) { return patcher, nil }
	t.Cleanup(func() {
		signalClientFactory = prevFactory
		testResetSignalEmitter()
	})

	err := EmitSignal(context.Background(), "state", map[string]any{"ok": true})
	if err == nil {
		t.Fatal("expected EmitSignal to fail when summary exceeds configured max payload bytes")
	}
	if !strings.Contains(err.Error(), "signal summary for state exceeds 32 bytes") {
		t.Fatalf("expected configured-limit summary error, got %v", err)
	}
	if len(patcher.calls) != 0 {
		t.Fatalf("expected no patch calls on payload cap violation, got %d", len(patcher.calls))
	}
}
