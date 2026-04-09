package testkit

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/stretchr/testify/require"
)

type secretFailBatchEngram struct {
	initCalled    bool
	processCalled bool
}

func (e *secretFailBatchEngram) Init(ctx context.Context, config struct{}, secrets *engram.Secrets) error {
	e.initCalled = true
	return nil
}

func (e *secretFailBatchEngram) Process(
	ctx context.Context,
	execCtx *engram.ExecutionContext,
	inputs struct{},
) (*engram.Result, error) {
	e.processCalled = true
	return engram.NewResultFrom("ok"), nil
}

type secretFailStreamEngram struct {
	initCalled   bool
	streamCalled bool
}

func (e *secretFailStreamEngram) Init(ctx context.Context, config struct{}, secrets *engram.Secrets) error {
	e.initCalled = true
	return nil
}

func (e *secretFailStreamEngram) Stream(
	ctx context.Context,
	in <-chan engram.InboundMessage,
	out chan<- engram.StreamMessage,
) error {
	e.streamCalled = true
	return nil
}

type panicStreamEngram struct{}

func (e *panicStreamEngram) Init(ctx context.Context, config struct{}, secrets *engram.Secrets) error {
	return nil
}

func (e *panicStreamEngram) Stream(
	ctx context.Context,
	in <-chan engram.InboundMessage,
	out chan<- engram.StreamMessage,
) error {
	panic("boom")
}

func TestBatchHarnessRunFailsOnSecretExpansionError(t *testing.T) {
	missingDir := filepath.Join(t.TempDir(), "missing")
	eng := &secretFailBatchEngram{}

	_, err := (BatchHarness[struct{}, struct{}]{
		Engram:  eng,
		Secrets: map[string]string{"db": "file:" + missingDir},
	}).Run(context.Background())

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to expand secrets")
	require.Contains(t, err.Error(), `secret "db" (file)`)
	require.NotContains(t, err.Error(), missingDir)
	require.False(t, eng.initCalled, "Init should not run when secret expansion fails")
	require.False(t, eng.processCalled, "Process should not run when secret expansion fails")
}

func TestStreamHarnessRunFailsOnSecretExpansionError(t *testing.T) {
	missingDir := filepath.Join(t.TempDir(), "missing")
	eng := &secretFailStreamEngram{}

	_, err := (StreamHarness[struct{}]{
		Engram:  eng,
		Secrets: map[string]string{"db": "file:" + missingDir},
	}).Run(context.Background())

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to expand secrets")
	require.Contains(t, err.Error(), `secret "db" (file)`)
	require.NotContains(t, err.Error(), missingDir)
	require.False(t, eng.initCalled, "Init should not run when secret expansion fails")
	require.False(t, eng.streamCalled, "Stream should not run when secret expansion fails")
}

func TestStreamHarnessRunConvertsStreamPanicToError(t *testing.T) {
	_, err := (StreamHarness[struct{}]{
		Engram: &panicStreamEngram{},
	}).Run(context.Background())

	require.Error(t, err)
	require.Contains(t, err.Error(), "engram stream panicked")
	require.Contains(t, err.Error(), "boom")
}

type ackingStreamEngram struct{}

func (e *ackingStreamEngram) Init(ctx context.Context, config struct{}, secrets *engram.Secrets) error {
	return nil
}

func (e *ackingStreamEngram) Stream(
	ctx context.Context,
	in <-chan engram.InboundMessage,
	out chan<- engram.StreamMessage,
) error {
	for msg := range in {
		msg.Done()
	}
	return nil
}

func TestStreamHarnessRunNotifiesOnInputProcessed(t *testing.T) {
	var processed atomic.Int32

	_, err := (StreamHarness[struct{}]{
		Engram: &ackingStreamEngram{},
		Inputs: []engram.StreamMessage{
			{Payload: []byte(`{"a":1}`)},
			{Payload: []byte(`{"b":2}`)},
		},
		OnInputProcessed: func(engram.StreamMessage) {
			processed.Add(1)
		},
	}).Run(context.Background())

	require.NoError(t, err)
	require.Equal(t, int32(2), processed.Load())
}
