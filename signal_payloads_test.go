package sdk

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bubustack/core/contracts"
)

func TestEmitTextSignalPreservesMetadataOnlyByDefault(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	testResetSignalEmitter()

	prevFactory := signalClientFactory
	patcher := &recordingSignalPatcher{}
	signalClientFactory = func() (signalPatcher, error) { return patcher, nil }
	t.Cleanup(func() {
		signalClientFactory = prevFactory
		testResetSignalEmitter()
	})

	if err := EmitTextSignal(context.Background(), "summary", "hello world", TextSignalOptions{}); err != nil {
		t.Fatalf("EmitTextSignal(...) returned error: %v", err)
	}

	if len(patcher.calls) != 1 {
		t.Fatalf("expected one patch call, got %d", len(patcher.calls))
	}

	var envelope SignalEnvelope
	if err := json.Unmarshal(patcher.calls[0].Signals["summary"].Raw, &envelope); err != nil {
		t.Fatalf("unmarshal signal envelope: %v", err)
	}
	if envelope.Sample != nil {
		t.Fatalf("expected metadata-only signal by default, got sample %#v", envelope.Sample)
	}
}

func TestEmitTextSignalIncludesBoundedSampleWhenRequested(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	testResetSignalEmitter()

	prevFactory := signalClientFactory
	patcher := &recordingSignalPatcher{}
	signalClientFactory = func() (signalPatcher, error) { return patcher, nil }
	t.Cleanup(func() {
		signalClientFactory = prevFactory
		testResetSignalEmitter()
	})

	if err := EmitTextSignal(context.Background(), "summary", "hello world", TextSignalOptions{
		SampleBytes: 5,
		SampleExtras: map[string]any{
			"lang": "en",
		},
	}); err != nil {
		t.Fatalf("EmitTextSignal(...) returned error: %v", err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(patcher.calls[0].Signals["summary"].Raw, &envelope); err != nil {
		t.Fatalf("unmarshal signal envelope: %v", err)
	}
	sample, ok := envelope["sample"].(map[string]any)
	if !ok {
		t.Fatalf("expected sample object, got %#v", envelope["sample"])
	}
	if sample["text"] != "hello" {
		t.Fatalf("unexpected sampled text: %#v", sample["text"])
	}
	if sample["truncated"] != true {
		t.Fatalf("expected truncated marker, got %#v", sample["truncated"])
	}
	if sample["lang"] != "en" {
		t.Fatalf("expected sample extras to merge, got %#v", sample["lang"])
	}
}

func TestEmitTextSignalUsesDefaultSampleSizeWhenOnlyExtrasRequested(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	testResetSignalEmitter()

	prevFactory := signalClientFactory
	patcher := &recordingSignalPatcher{}
	signalClientFactory = func() (signalPatcher, error) { return patcher, nil }
	t.Cleanup(func() {
		signalClientFactory = prevFactory
		testResetSignalEmitter()
	})

	text := make([]byte, defaultSignalSampleBytes+10)
	for i := range text {
		text[i] = 'a'
	}

	if err := EmitTextSignal(context.Background(), "summary", string(text), TextSignalOptions{
		SampleExtras: map[string]any{"source": "generated"},
	}); err != nil {
		t.Fatalf("EmitTextSignal(...) returned error: %v", err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(patcher.calls[0].Signals["summary"].Raw, &envelope); err != nil {
		t.Fatalf("unmarshal signal envelope: %v", err)
	}
	sample := envelope["sample"].(map[string]any)
	if len(sample["text"].(string)) != defaultSignalSampleBytes {
		t.Fatalf("expected default sample length %d, got %d", defaultSignalSampleBytes, len(sample["text"].(string)))
	}
	if sample["source"] != "generated" {
		t.Fatalf("expected merged sample extras, got %#v", sample["source"])
	}
}
