/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sdk

import (
	context "context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type mockEffectPatcher struct {
	mock.Mock
}

func (m *mockEffectPatcher) PatchStepRunStatus(ctx context.Context,
	stepRunName string, status runsv1alpha1.StepRunStatus) error {
	args := m.Called(ctx, stepRunName, status)
	return args.Error(0)
}

type mockEffectReader struct {
	stepRun   *runsv1alpha1.StepRun
	namespace string
	err       error
}

func (m *mockEffectReader) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	if m.err != nil {
		return m.err
	}
	if m.stepRun == nil {
		return errors.New("step run not found")
	}
	target := obj.(*runsv1alpha1.StepRun)
	*target = *m.stepRun
	return nil
}

func (m *mockEffectReader) List(_ context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	if m.err != nil {
		return m.err
	}
	return nil
}

func (m *mockEffectReader) GetNamespace() string {
	return m.namespace
}

type fakeEffectClusterClient struct {
	client.Client
	namespace string
}

func newFakeEffectClusterClient(t *testing.T, namespace string, objects ...client.Object) *fakeEffectClusterClient {
	t.Helper()
	scheme := k8sruntime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add StepRun scheme: %v", err)
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StepRun{}).
		WithObjects(objects...).
		Build()
	return &fakeEffectClusterClient{Client: kubeClient, namespace: namespace}
}

func (c *fakeEffectClusterClient) GetNamespace() string {
	return c.namespace
}

func (c *fakeEffectClusterClient) PatchStepRunStatus(ctx context.Context, stepRunName string, status runsv1alpha1.StepRunStatus) error { //nolint:lll
	var stepRun runsv1alpha1.StepRun
	if err := c.Get(ctx, client.ObjectKey{Name: stepRunName, Namespace: c.namespace}, &stepRun); err != nil {
		return err
	}
	base := stepRun.DeepCopy()
	stepRun.Status.Effects = append(stepRun.Status.Effects, status.Effects...)
	return c.Status().Patch(ctx, &stepRun, client.MergeFrom(base))
}

func TestRecordEffect_EmitsPatch(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	testResetEffectEmitter()

	prevFactory := effectClientFactory
	mockClient := &mockEffectPatcher{}
	effectClientFactory = func() (effectPatcher, error) { return mockClient, nil }
	t.Cleanup(func() {
		effectClientFactory = prevFactory
		testResetEffectEmitter()
	})

	mockClient.On("PatchStepRunStatus", mock.Anything, "step-1", mock.MatchedBy(func(status runsv1alpha1.StepRunStatus) bool { //nolint:lll
		if len(status.Effects) != 1 {
			return false
		}
		eff := status.Effects[0]
		if eff.Key != "effect-1" || eff.Status != "succeeded" {
			return false
		}
		if eff.EmittedAt == nil || eff.EmittedAt.IsZero() {
			return false
		}
		if eff.Details == nil || len(eff.Details.Raw) == 0 {
			return false
		}
		var details map[string]any
		if err := json.Unmarshal(eff.Details.Raw, &details); err != nil {
			return false
		}
		return details["providerId"] == "abc"
	})).Return(nil)

	err := RecordEffect(context.Background(), "effect-1", "succeeded", map[string]any{"providerId": "abc"})
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestRecordEffect_MissingStepRun(t *testing.T) {
	testResetEffectEmitter()
	prevFactory := effectClientFactory
	t.Cleanup(func() {
		effectClientFactory = prevFactory
		testResetEffectEmitter()
	})

	err := RecordEffect(context.Background(), "effect-1", "succeeded", nil)
	assert.ErrorIs(t, err, ErrEffectsUnavailable)
}

func TestRecordEffect_MissingKey(t *testing.T) {
	prevFactory := effectClientFactory
	t.Cleanup(func() {
		effectClientFactory = prevFactory
		testResetEffectEmitter()
	})

	err := RecordEffect(context.Background(), " ", "succeeded", nil)
	assert.Error(t, err)
}

func TestExecuteEffectOnce_SkipsWhenRecorded(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	t.Setenv(contracts.StepRunNamespaceEnv, "test-ns")

	prevReader := effectReaderFactory
	prevFactory := effectClientFactory
	t.Cleanup(func() {
		effectReaderFactory = prevReader
		effectClientFactory = prevFactory
		testResetEffectEmitter()
	})

	effectReaderFactory = func() (effectReader, error) {
		return &mockEffectReader{
			namespace: "test-ns",
			stepRun: &runsv1alpha1.StepRun{
				ObjectMeta: metav1.ObjectMeta{Name: "step-1", Namespace: "test-ns"},
				Status: runsv1alpha1.StepRunStatus{
					Effects: []runsv1alpha1.EffectRecord{{Seq: 1, Key: "effect-1", Status: "succeeded"}},
				},
			},
		}, nil
	}
	mockClient := &mockEffectPatcher{}
	effectClientFactory = func() (effectPatcher, error) { return mockClient, nil }

	result, already, err := ExecuteEffectOnce(context.Background(), "effect-1", func(context.Context) (any, error) {
		t.Fatalf("effect should not execute when already recorded")
		return nil, nil
	})
	assert.Nil(t, result)
	assert.True(t, already)
	assert.ErrorIs(t, err, ErrEffectAlreadyRecorded)
	mockClient.AssertNotCalled(t, "PatchStepRunStatus", mock.Anything, mock.Anything, mock.Anything)
}

func TestExecuteEffectOnce_RecordsOnSuccess(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	t.Setenv(contracts.StepRunNamespaceEnv, "test-ns")

	prevReader := effectReaderFactory
	prevFactory := effectClientFactory
	prevClaim := effectClaimFactory
	t.Cleanup(func() {
		effectReaderFactory = prevReader
		effectClientFactory = prevFactory
		effectClaimFactory = prevClaim
		testResetEffectEmitter()
	})

	effectReaderFactory = func() (effectReader, error) {
		return &mockEffectReader{
			namespace: "test-ns",
			stepRun: &runsv1alpha1.StepRun{
				ObjectMeta: metav1.ObjectMeta{Name: "step-1", Namespace: "test-ns"},
				Status:     runsv1alpha1.StepRunStatus{},
			},
		}, nil
	}
	claimClient := newFakeEffectClusterClient(t, "test-ns")
	effectClaimFactory = func() (effectClaimClient, error) { return claimClient, nil }
	mockClient := &mockEffectPatcher{}
	effectClientFactory = func() (effectPatcher, error) { return mockClient, nil }

	mockClient.On("PatchStepRunStatus", mock.Anything, "step-1", mock.MatchedBy(func(status runsv1alpha1.StepRunStatus) bool { //nolint:lll
		if len(status.Effects) != 1 {
			return false
		}
		eff := status.Effects[0]
		return eff.Key == "effect-2" && eff.Status == "succeeded"
	})).Return(nil)

	result, already, err := ExecuteEffectOnce(context.Background(), "effect-2", func(context.Context) (any, error) {
		return map[string]any{"providerId": "xyz"}, nil
	})
	assert.False(t, already)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockClient.AssertExpectations(t)
}

func TestExecuteEffectOnce_DoesNotReexecuteAfterPatchFailure(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	t.Setenv(contracts.StepRunNamespaceEnv, "test-ns")
	testResetEffectEmitter()

	prevReader := effectReaderFactory
	prevFactory := effectClientFactory
	prevClaim := effectClaimFactory
	t.Cleanup(func() {
		effectReaderFactory = prevReader
		effectClientFactory = prevFactory
		effectClaimFactory = prevClaim
		testResetEffectEmitter()
	})

	effectReaderFactory = func() (effectReader, error) {
		return &mockEffectReader{
			namespace: "test-ns",
			stepRun: &runsv1alpha1.StepRun{
				ObjectMeta: metav1.ObjectMeta{Name: "step-1", Namespace: "test-ns"},
				Status:     runsv1alpha1.StepRunStatus{},
			},
		}, nil
	}

	patchErr := errors.New("patch failed")
	claimClient := newFakeEffectClusterClient(t, "test-ns")
	effectClaimFactory = func() (effectClaimClient, error) { return claimClient, nil }
	mockClient := &mockEffectPatcher{}
	effectClientFactory = func() (effectPatcher, error) { return mockClient, nil }
	mockClient.
		On("PatchStepRunStatus", mock.Anything, "step-1", mock.Anything).
		Return(patchErr).
		Once()

	var calls atomic.Int32
	firstResult, firstAlready, firstErr := ExecuteEffectOnce(context.Background(), "effect-3", func(context.Context) (any, error) { //nolint:lll
		calls.Add(1)
		return map[string]any{"providerId": "xyz"}, nil
	})
	assert.False(t, firstAlready)
	assert.ErrorIs(t, firstErr, patchErr)
	assert.NotNil(t, firstResult)

	var recordedClaim runsv1alpha1.EffectClaim
	err := claimClient.Get(context.Background(), client.ObjectKey{
		Name:      effectReservationClaimName("test-ns", "step-1", "effect-3"),
		Namespace: "test-ns",
	}, &recordedClaim)
	assert.NoError(t, err)
	assert.Equal(t, runsv1alpha1.EffectClaimCompletionStatusCompleted, effectReservationState(&recordedClaim))

	testResetEffectEmitter()

	secondResult, secondAlready, secondErr := ExecuteEffectOnce(context.Background(), "effect-3", func(context.Context) (any, error) { //nolint:lll
		calls.Add(1)
		return map[string]any{"providerId": "should-not-run"}, nil
	})
	assert.Nil(t, secondResult)
	assert.True(t, secondAlready)
	assert.ErrorIs(t, secondErr, ErrEffectAlreadyRecorded)
	assert.EqualValues(t, 1, calls.Load())

	mockClient.AssertExpectations(t)
}

func TestEffectExecutionLeaseEvictsCompletedGateWhenRefsDropToZero(t *testing.T) {
	testResetEffectEmitter()
	lease, already := acquireEffectExecutionLease("ns:step:effect")
	require.False(t, already)
	require.NotNil(t, lease)

	releaseEffectExecutionLease(lease, true)

	effectExecMu.Lock()
	defer effectExecMu.Unlock()
	_, exists := effectExecGates["ns:step:effect"]
	require.False(t, exists)
}

func TestExecuteEffectOnce_ReleasesReservationAfterEffectFailure(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	t.Setenv(contracts.StepRunNamespaceEnv, "test-ns")
	testResetEffectEmitter()

	prevReader := effectReaderFactory
	prevFactory := effectClientFactory
	prevClaim := effectClaimFactory
	t.Cleanup(func() {
		effectReaderFactory = prevReader
		effectClientFactory = prevFactory
		effectClaimFactory = prevClaim
		testResetEffectEmitter()
	})

	effectReaderFactory = func() (effectReader, error) {
		return &mockEffectReader{
			namespace: "test-ns",
			stepRun: &runsv1alpha1.StepRun{
				ObjectMeta: metav1.ObjectMeta{Name: "step-1", Namespace: "test-ns"},
				Status:     runsv1alpha1.StepRunStatus{},
			},
		}, nil
	}

	claimClient := newFakeEffectClusterClient(t, "test-ns")
	effectClaimFactory = func() (effectClaimClient, error) { return claimClient, nil }
	mockClient := &mockEffectPatcher{}
	effectClientFactory = func() (effectPatcher, error) { return mockClient, nil }
	mockClient.
		On("PatchStepRunStatus", mock.Anything, "step-1", mock.Anything).
		Return(nil).
		Once()

	var calls atomic.Int32
	firstResult, firstAlready, firstErr := ExecuteEffectOnce(context.Background(), "effect-release", func(context.Context) (any, error) { //nolint:lll
		calls.Add(1)
		return nil, errors.New("boom")
	})
	assert.Nil(t, firstResult)
	assert.False(t, firstAlready)
	assert.EqualError(t, firstErr, "boom")

	var claim runsv1alpha1.EffectClaim
	err := claimClient.Get(context.Background(), client.ObjectKey{
		Name:      effectReservationClaimName("test-ns", "step-1", "effect-release"),
		Namespace: "test-ns",
	}, &claim)
	assert.NoError(t, err)
	assert.Equal(t, runsv1alpha1.EffectClaimCompletionStatusReleased, claim.Spec.CompletionStatus)
	assert.Empty(t, strings.TrimSpace(claim.Spec.HolderIdentity))

	testResetEffectEmitter()

	secondResult, secondAlready, secondErr := ExecuteEffectOnce(context.Background(), "effect-release", func(context.Context) (any, error) { //nolint:lll
		calls.Add(1)
		return map[string]any{"providerId": "xyz"}, nil
	})
	assert.NoError(t, secondErr)
	assert.False(t, secondAlready)
	assert.NotNil(t, secondResult)
	assert.EqualValues(t, 2, calls.Load())

	mockClient.AssertExpectations(t)
}

func TestReserveEffect_RecoversStaleReservation(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	t.Setenv(contracts.StepRunNamespaceEnv, "test-ns")
	testResetEffectEmitter()

	prevReader := effectReaderFactory
	prevClaim := effectClaimFactory
	t.Cleanup(func() {
		effectReaderFactory = prevReader
		effectClaimFactory = prevClaim
		testResetEffectEmitter()
	})

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{Name: "step-1", Namespace: "test-ns"},
		Status:     runsv1alpha1.StepRunStatus{},
	}
	staleAt := metav1.NewMicroTime(time.Now().Add(-2 * time.Second).UTC())
	durationSeconds := int32(1)
	staleClaim := &runsv1alpha1.EffectClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      effectReservationClaimName("test-ns", "step-1", "effect-stale"),
			Namespace: "test-ns",
		},
		Spec: runsv1alpha1.EffectClaimSpec{
			StepRunRef: refs.StepRunReference{
				ObjectReference: refs.ObjectReference{Name: "step-1"},
				UID:             &stepRun.UID,
			},
			EffectKey:            "effect-stale",
			HolderIdentity:       "old-holder",
			AcquireTime:          &staleAt,
			RenewTime:            &staleAt,
			LeaseDurationSeconds: durationSeconds,
		},
	}

	effectReaderFactory = func() (effectReader, error) {
		return &mockEffectReader{namespace: "test-ns", stepRun: stepRun}, nil
	}
	claimClient := newFakeEffectClusterClient(t, "test-ns", stepRun, staleClaim)
	effectClaimFactory = func() (effectClaimClient, error) { return claimClient, nil }

	reservation, already, err := reserveEffect(context.Background(), "effect-stale")
	assert.NoError(t, err)
	assert.False(t, already)
	if reservation == nil {
		t.Fatal("expected recovered reservation")
	}

	var claim runsv1alpha1.EffectClaim
	err = claimClient.Get(context.Background(), client.ObjectKey{
		Name:      staleClaim.Name,
		Namespace: "test-ns",
	}, &claim)
	assert.NoError(t, err)
	assert.Equal(t, runsv1alpha1.EffectClaimCompletionStatus(""), effectReservationState(&claim))
	assert.NotEqual(t, "old-holder", strings.TrimSpace(claim.Spec.HolderIdentity))
	assert.Equal(t, reservation.holderIdentity, strings.TrimSpace(claim.Spec.HolderIdentity))
	assert.False(t, effectReservationIsStale(&claim, time.Now().UTC()))
}

func TestReserveEffect_DoesNotRecoverActiveReservation(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	t.Setenv(contracts.StepRunNamespaceEnv, "test-ns")
	testResetEffectEmitter()

	prevReader := effectReaderFactory
	prevClaim := effectClaimFactory
	t.Cleanup(func() {
		effectReaderFactory = prevReader
		effectClaimFactory = prevClaim
		testResetEffectEmitter()
	})

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{Name: "step-1", Namespace: "test-ns"},
		Status:     runsv1alpha1.StepRunStatus{},
	}
	now := metav1.NewMicroTime(time.Now().UTC())
	durationSeconds := int32(60)
	activeClaim := &runsv1alpha1.EffectClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      effectReservationClaimName("test-ns", "step-1", "effect-active"),
			Namespace: "test-ns",
		},
		Spec: runsv1alpha1.EffectClaimSpec{
			StepRunRef: refs.StepRunReference{
				ObjectReference: refs.ObjectReference{Name: "step-1"},
				UID:             &stepRun.UID,
			},
			EffectKey:            "effect-active",
			HolderIdentity:       "active-holder",
			AcquireTime:          &now,
			RenewTime:            &now,
			LeaseDurationSeconds: durationSeconds,
		},
	}

	effectReaderFactory = func() (effectReader, error) {
		return &mockEffectReader{namespace: "test-ns", stepRun: stepRun}, nil
	}
	claimClient := newFakeEffectClusterClient(t, "test-ns", stepRun, activeClaim)
	effectClaimFactory = func() (effectClaimClient, error) { return claimClient, nil }

	reservation, already, err := reserveEffect(context.Background(), "effect-active")
	assert.NoError(t, err)
	assert.True(t, already)
	assert.Nil(t, reservation)
}

func TestReleaseEffectReservation_DoesNotClearReservationOwnedByAnotherHolder(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	t.Setenv(contracts.StepRunNamespaceEnv, "test-ns")
	testResetEffectEmitter()

	claim := &runsv1alpha1.EffectClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      effectReservationClaimName("test-ns", "step-1", "effect-guarded"),
			Namespace: "test-ns",
		},
		Spec: runsv1alpha1.EffectClaimSpec{
			StepRunRef: refs.StepRunReference{
				ObjectReference: refs.ObjectReference{Name: "step-1"},
			},
			EffectKey:      "effect-guarded",
			HolderIdentity: "current-holder",
		},
	}
	claimClient := newFakeEffectClusterClient(t, "test-ns", claim)

	err := releaseEffectReservation(context.Background(), &effectReservation{
		client:         claimClient,
		claim:          claim.DeepCopy(),
		holderIdentity: "stale-holder",
	})
	assert.NoError(t, err)

	var remaining runsv1alpha1.EffectClaim
	err = claimClient.Get(context.Background(), client.ObjectKey{Name: claim.Name, Namespace: claim.Namespace}, &remaining)
	assert.NoError(t, err)
	assert.Equal(t, "current-holder", strings.TrimSpace(remaining.Spec.HolderIdentity))
}

func TestReserveEffect_OnlyOneWorkerRecoversStaleReservation(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	t.Setenv(contracts.StepRunNamespaceEnv, "test-ns")
	testResetEffectEmitter()

	prevReader := effectReaderFactory
	prevClaim := effectClaimFactory
	t.Cleanup(func() {
		effectReaderFactory = prevReader
		effectClaimFactory = prevClaim
		testResetEffectEmitter()
	})

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{Name: "step-1", Namespace: "test-ns"},
		Status:     runsv1alpha1.StepRunStatus{},
	}
	staleAt := metav1.NewMicroTime(time.Now().Add(-2 * time.Second).UTC())
	durationSeconds := int32(1)
	staleClaim := &runsv1alpha1.EffectClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      effectReservationClaimName("test-ns", "step-1", "effect-race"),
			Namespace: "test-ns",
		},
		Spec: runsv1alpha1.EffectClaimSpec{
			StepRunRef: refs.StepRunReference{
				ObjectReference: refs.ObjectReference{Name: "step-1"},
				UID:             &stepRun.UID,
			},
			EffectKey:            "effect-race",
			HolderIdentity:       "old-holder",
			AcquireTime:          &staleAt,
			RenewTime:            &staleAt,
			LeaseDurationSeconds: durationSeconds,
		},
	}

	effectReaderFactory = func() (effectReader, error) {
		return &mockEffectReader{namespace: "test-ns", stepRun: stepRun}, nil
	}
	claimClient := newFakeEffectClusterClient(t, "test-ns", stepRun, staleClaim)
	effectClaimFactory = func() (effectClaimClient, error) { return claimClient, nil }

	type result struct {
		reservation *effectReservation
		already     bool
		err         error
	}
	results := make(chan result, 2)

	run := func() {
		reservation, already, err := reserveEffect(context.Background(), "effect-race")
		results <- result{reservation: reservation, already: already, err: err}
	}

	go run()
	go run()

	first := <-results
	second := <-results

	successes := 0
	alreadyCount := 0
	for _, item := range []result{first, second} {
		assert.NoError(t, item.err)
		if item.already {
			alreadyCount++
		} else {
			successes++
			if item.reservation == nil {
				t.Fatal("expected recovered reservation for successful worker")
			}
		}
	}
	assert.Equal(t, 1, successes)
	assert.Equal(t, 1, alreadyCount)
}

func TestExecuteEffectOnce_RenewsReservationDuringLongExecution(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	t.Setenv(contracts.StepRunNamespaceEnv, "test-ns")
	testResetEffectEmitter()

	prevReader := effectReaderFactory
	prevFactory := effectClientFactory
	prevClaim := effectClaimFactory
	prevLeaseDuration := effectLeaseDuration
	prevRenewInterval := effectLeaseRenewIntervalFunc
	t.Cleanup(func() {
		effectReaderFactory = prevReader
		effectClientFactory = prevFactory
		effectClaimFactory = prevClaim
		effectLeaseDuration = prevLeaseDuration
		effectLeaseRenewIntervalFunc = prevRenewInterval
		testResetEffectEmitter()
	})

	effectLeaseDuration = 150 * time.Millisecond
	effectLeaseRenewIntervalFunc = func(time.Duration) time.Duration { return 30 * time.Millisecond }

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{Name: "step-1", Namespace: "test-ns"},
		Status:     runsv1alpha1.StepRunStatus{},
	}
	effectReaderFactory = func() (effectReader, error) {
		return &mockEffectReader{namespace: "test-ns", stepRun: stepRun}, nil
	}

	claimClient := newFakeEffectClusterClient(t, "test-ns", stepRun)
	effectClaimFactory = func() (effectClaimClient, error) { return claimClient, nil }
	store := newStatefulEffectStore("step-1", "test-ns")
	effectClientFactory = func() (effectPatcher, error) { return &statefulEffectPatcher{store: store}, nil }

	started := make(chan struct{})
	release := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		_, _, err := ExecuteEffectOnce(context.Background(), "effect-renew", func(context.Context) (any, error) {
			close(started)
			<-release
			return map[string]any{"providerId": "renewed"}, nil
		})
		errCh <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected effect execution to start")
	}

	claimName := effectReservationClaimName("test-ns", "step-1", "effect-renew")
	require.Eventually(t, func() bool {
		var claim runsv1alpha1.EffectClaim
		if err := claimClient.Get(context.Background(), client.ObjectKey{Name: claimName, Namespace: "test-ns"}, &claim); err != nil { //nolint:lll
			return false
		}
		return claim.Spec.AcquireTime != nil && claim.Spec.RenewTime != nil &&
			claim.Spec.RenewTime.After(claim.Spec.AcquireTime.Time)
	}, time.Second, 20*time.Millisecond, "expected claim renew time to advance while effect is running")

	reservation, already, err := reserveEffect(context.Background(), "effect-renew")
	assert.NoError(t, err)
	assert.True(t, already)
	assert.Nil(t, reservation)

	close(release)

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("expected ExecuteEffectOnce to finish after release")
	}
}

func TestExecuteEffectOnce_DeduperKeyIncludesNamespace(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	t.Setenv(contracts.StepRunNamespaceEnv, "ns-a")
	testResetEffectEmitter()

	prevReader := effectReaderFactory
	prevFactory := effectClientFactory
	prevClaim := effectClaimFactory
	t.Cleanup(func() {
		effectReaderFactory = prevReader
		effectClientFactory = prevFactory
		effectClaimFactory = prevClaim
		testResetEffectEmitter()
	})

	effectReaderFactory = func() (effectReader, error) {
		ns := strings.TrimSpace(os.Getenv(contracts.StepRunNamespaceEnv))
		return &mockEffectReader{
			namespace: ns,
			stepRun: &runsv1alpha1.StepRun{
				ObjectMeta: metav1.ObjectMeta{Name: "step-1", Namespace: ns},
				Status:     runsv1alpha1.StepRunStatus{},
			},
		}, nil
	}

	effectClaimFactory = func() (effectClaimClient, error) {
		return newFakeEffectClusterClient(t, strings.TrimSpace(os.Getenv(contracts.StepRunNamespaceEnv))), nil
	}
	mockClient := &mockEffectPatcher{}
	effectClientFactory = func() (effectPatcher, error) { return mockClient, nil }
	mockClient.
		On("PatchStepRunStatus", mock.Anything, "step-1", mock.Anything).
		Return(nil).
		Twice()

	var calls atomic.Int32
	resultA, alreadyA, errA := ExecuteEffectOnce(context.Background(), "effect-shared", func(context.Context) (any, error) { //nolint:lll
		calls.Add(1)
		return map[string]any{"namespace": "ns-a"}, nil
	})
	assert.NoError(t, errA)
	assert.False(t, alreadyA)
	assert.NotNil(t, resultA)

	t.Setenv(contracts.StepRunNamespaceEnv, "ns-b")
	resultB, alreadyB, errB := ExecuteEffectOnce(context.Background(), "effect-shared", func(context.Context) (any, error) { //nolint:lll
		calls.Add(1)
		return map[string]any{"namespace": "ns-b"}, nil
	})
	assert.NoError(t, errB)
	assert.False(t, alreadyB)
	assert.NotNil(t, resultB)
	assert.EqualValues(t, 2, calls.Load())

	mockClient.AssertExpectations(t)
}

type statefulEffectStore struct {
	mu       sync.Mutex
	stepRun  runsv1alpha1.StepRun
	patches  int
	statuses []runsv1alpha1.StepRunStatus
}

func newStatefulEffectStore(stepRunName string, namespace string) *statefulEffectStore {
	return &statefulEffectStore{
		stepRun: runsv1alpha1.StepRun{
			ObjectMeta: metav1.ObjectMeta{Name: stepRunName, Namespace: namespace},
		},
	}
}

func (s *statefulEffectStore) appendStatus(status runsv1alpha1.StepRunStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.patches++
	s.statuses = append(s.statuses, status)
	s.stepRun.Status.Effects = append(s.stepRun.Status.Effects, status.Effects...)
}

func (s *statefulEffectStore) snapshot() (int, []runsv1alpha1.EffectRecord, []runsv1alpha1.StepRunStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	effects := append([]runsv1alpha1.EffectRecord(nil), s.stepRun.Status.Effects...)
	statuses := append([]runsv1alpha1.StepRunStatus(nil), s.statuses...)
	return s.patches, effects, statuses
}

type statefulEffectPatcher struct {
	store *statefulEffectStore
}

func (p *statefulEffectPatcher) PatchStepRunStatus(_ context.Context, _ string, status runsv1alpha1.StepRunStatus) error { //nolint:lll
	p.store.appendStatus(status)
	return nil
}

type statefulEffectReader struct {
	store     *statefulEffectStore
	namespace string
}

func (r *statefulEffectReader) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error { //nolint:lll
	target := obj.(*runsv1alpha1.StepRun)
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	*target = *r.store.stepRun.DeepCopy()
	return nil
}

func (r *statefulEffectReader) List(_ context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	return nil
}

func (r *statefulEffectReader) GetNamespace() string {
	return r.namespace
}

func TestExecuteEffectOnce_DeduplicatesConcurrentCalls(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	t.Setenv(contracts.StepRunNamespaceEnv, "test-ns")
	testResetEffectEmitter()

	prevReader := effectReaderFactory
	prevFactory := effectClientFactory
	prevClaim := effectClaimFactory
	t.Cleanup(func() {
		effectReaderFactory = prevReader
		effectClientFactory = prevFactory
		effectClaimFactory = prevClaim
		testResetEffectEmitter()
	})

	store := newStatefulEffectStore("step-1", "test-ns")
	effectReaderFactory = func() (effectReader, error) {
		return &statefulEffectReader{store: store, namespace: "test-ns"}, nil
	}
	claimClient := newFakeEffectClusterClient(t, "test-ns")
	effectClaimFactory = func() (effectClaimClient, error) { return claimClient, nil }
	effectClientFactory = func() (effectPatcher, error) {
		return &statefulEffectPatcher{store: store}, nil
	}

	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})

	type resultTuple struct {
		result  any
		already bool
		err     error
	}
	results := make(chan resultTuple, 2)

	run := func() {
		result, already, err := ExecuteEffectOnce(context.Background(), "effect-3", func(context.Context) (any, error) {
			if calls.Add(1) == 1 {
				close(started)
				<-release
			}
			return map[string]any{"providerId": "xyz"}, nil
		})
		results <- resultTuple{result: result, already: already, err: err}
	}

	go run()
	<-started
	go run()
	time.Sleep(20 * time.Millisecond)
	close(release)

	first := <-results
	second := <-results

	if calls.Load() != 1 {
		t.Fatalf("expected effect function to execute once, got %d", calls.Load())
	}
	patches, effects, _ := store.snapshot()
	if patches != 1 {
		t.Fatalf("expected one effect patch, got %d", patches)
	}
	if len(effects) != 1 {
		t.Fatalf("expected one recorded effect, got %d", len(effects))
	}

	successes := 0
	alreadyRecorded := 0
	for _, item := range []resultTuple{first, second} {
		if item.err == nil {
			successes++
			if item.already {
				t.Fatal("successful execution should not report already=true")
			}
			if item.result == nil {
				t.Fatal("successful execution should return a result")
			}
			continue
		}
		if item.already && errors.Is(item.err, ErrEffectAlreadyRecorded) {
			alreadyRecorded++
			continue
		}
		t.Fatalf("unexpected concurrent ExecuteEffectOnce result: already=%v err=%v", item.already, item.err)
	}
	if successes != 1 || alreadyRecorded != 1 {
		t.Fatalf("expected one success and one already-recorded result, got success=%d already=%d", successes, alreadyRecorded) //nolint:lll
	}
}

func TestRecordEffect_TruncatesOversizedDetails(t *testing.T) {
	t.Setenv(contracts.StepRunNameEnv, "step-1")
	t.Setenv(effectMaxDetailsBytesEnv, "256")
	testResetEffectEmitter()

	prevFactory := effectClientFactory
	store := newStatefulEffectStore("step-1", "test-ns")
	effectClientFactory = func() (effectPatcher, error) {
		return &statefulEffectPatcher{store: store}, nil
	}
	t.Cleanup(func() {
		effectClientFactory = prevFactory
		testResetEffectEmitter()
	})

	err := RecordEffect(context.Background(), "effect-oversized", "succeeded", map[string]any{
		"secret": string(make([]byte, 512)),
	})
	assert.NoError(t, err)

	_, _, statuses := store.snapshot()
	if len(statuses) != 1 {
		t.Fatalf("expected one recorded status patch, got %d", len(statuses))
	}
	if len(statuses[0].Effects) != 1 {
		t.Fatalf("expected one effect record, got %d", len(statuses[0].Effects))
	}
	if statuses[0].Effects[0].Details == nil {
		t.Fatal("expected truncated effect details payload")
	}

	var payload map[string]any
	if err := json.Unmarshal(statuses[0].Effects[0].Details.Raw, &payload); err != nil {
		t.Fatalf("unmarshal truncated effect details: %v", err)
	}
	if payload["truncated"] != true {
		t.Fatalf("expected truncated marker, got %#v", payload)
	}
	if payload["type"] != "map" { //nolint:goconst
		t.Fatalf("expected map type summary, got %#v", payload["type"])
	}
}
