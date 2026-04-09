package sdk

import (
	"context"
	"errors"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/core/contracts"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
)

type stubSignalReplayClient struct {
	namespace string
	stepRun   runsv1alpha1.StepRun
	getErr    error
	lastCtx   context.Context
	lastKey   types.NamespacedName
}

func (s *stubSignalReplayClient) GetNamespace() string {
	return s.namespace
}

func (s *stubSignalReplayClient) GetStepRun(
	ctx context.Context,
	key types.NamespacedName,
	stepRun *runsv1alpha1.StepRun,
) error {
	s.lastCtx = ctx
	s.lastKey = key
	if s.getErr != nil {
		return s.getErr
	}
	*stepRun = s.stepRun
	return nil
}

func TestReplaySignalsReturnsUnavailableWithoutStepRunContext(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "")
	t.Setenv(contracts.StepRunNamespaceEnv, "")

	prevFactory := signalReplayClientFactory
	clientInitialized := false
	signalReplayClientFactory = func() (signalReplayClient, error) {
		clientInitialized = true
		return nil, errors.New("should not initialize client")
	}
	t.Cleanup(func() {
		signalReplayClientFactory = prevFactory
	})

	events, err := ReplaySignals(nil, "", "", 0) //nolint:staticcheck
	require.ErrorIs(t, err, ErrSignalsUnavailable)
	require.Nil(t, events)
	require.False(t, clientInitialized, "client should not initialize when step-run identity is absent")
}

func TestReplaySignalsAcceptsNilContextAndUsesClientNamespace(t *testing.T) {
	prevFactory := signalReplayClientFactory
	client := &stubSignalReplayClient{
		namespace: "signals-ns",
		stepRun: runsv1alpha1.StepRun{
			Status: runsv1alpha1.StepRunStatus{
				SignalEvents: []runsv1alpha1.SignalEvent{
					{Seq: 2, Key: "later"},
					{Seq: 1, Key: "earlier"},
				},
			},
		},
	}
	signalReplayClientFactory = func() (signalReplayClient, error) {
		return client, nil
	}
	t.Cleanup(func() {
		signalReplayClientFactory = prevFactory
	})

	events, err := ReplaySignals(nil, "step-1", "", 0) //nolint:staticcheck
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.NotNil(t, client.lastCtx)
	require.Equal(t, types.NamespacedName{Name: "step-1", Namespace: "signals-ns"}, client.lastKey)
	require.Equal(t, uint64(1), events[0].Seq)
	require.Equal(t, uint64(2), events[1].Seq)
}
