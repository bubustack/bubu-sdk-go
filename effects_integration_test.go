//go:build integration

package sdk

import (
	context "context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/contracts"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

type envtestEffectClient struct {
	client.Client
	namespace string
}

func (c *envtestEffectClient) GetNamespace() string {
	return c.namespace
}

func (c *envtestEffectClient) PatchStepRunStatus(ctx context.Context, stepRunName string, status runsv1alpha1.StepRunStatus) error {
	var stepRun runsv1alpha1.StepRun
	if err := c.Get(ctx, client.ObjectKey{Name: stepRunName, Namespace: c.namespace}, &stepRun); err != nil {
		return err
	}
	base := stepRun.DeepCopy()
	stepRun.Status.Effects = append(stepRun.Status.Effects, status.Effects...)
	return c.Status().Patch(ctx, &stepRun, client.MergeFrom(base))
}

func resolveEffectsCRDPath(t *testing.T) string {
	t.Helper()

	if override := os.Getenv("BOBRAPET_CRD_PATH"); override != "" {
		if info, err := os.Stat(override); err == nil && info.IsDir() {
			return override
		}
		t.Fatalf("BOBRAPET_CRD_PATH=%q does not exist or is not a directory", override)
	}

	candidates := []string{
		filepath.Join("..", "bobrapet", "config", "crd", "bases"),
		filepath.Join("..", "..", "bobrapet", "config", "crd", "bases"),
	}
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate
		}
	}

	t.Skip("bobrapet CRDs not found; set BOBRAPET_CRD_PATH or run tests within the bobrapet+bubu-sdk-go workspace")
	return ""
}

func setupEffectsEnvtest(t *testing.T) *envtestEffectClient {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration envtest in short mode")
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping integration envtest")
	}

	testEnv := &envtest.Environment{
		CRDDirectoryPaths: []string{resolveEffectsCRDPath(t)},
	}
	cfg, err := testEnv.Start()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, testEnv.Stop())
	})

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))

	apiClient, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)

	const namespace = "default"
	err = apiClient.Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		require.NoError(t, err)
	}

	return &envtestEffectClient{Client: apiClient, namespace: namespace}
}

func newIntegrationStepRun(name, namespace string) *runsv1alpha1.StepRun {
	return &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: runsv1alpha1.StepRunSpec{
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun-" + name},
			},
			StepID: "step-1",
		},
	}
}

func TestExecuteEffectOnce_RecoversExpiredClaimLive(t *testing.T) {
	effectClient := setupEffectsEnvtest(t)
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

	const stepRunName = "effect-live-recover"
	t.Setenv(contracts.StepRunNameEnv, stepRunName)
	t.Setenv(contracts.StepRunNamespaceEnv, effectClient.namespace)

	stepRun := newIntegrationStepRun(stepRunName, effectClient.namespace)
	require.NoError(t, effectClient.Create(context.Background(), stepRun))

	staleAt := metav1.NewMicroTime(time.Now().Add(-2 * time.Second).UTC())
	durationSeconds := int32(1)
	claim := &runsv1alpha1.EffectClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      effectReservationClaimName(effectClient.namespace, stepRunName, "effect-live"),
			Namespace: effectClient.namespace,
		},
		Spec: runsv1alpha1.EffectClaimSpec{
			StepRunRef: refs.StepRunReference{
				ObjectReference: refs.ObjectReference{Name: stepRunName},
				UID:             &stepRun.UID,
			},
			EffectKey:            "effect-live",
			HolderIdentity:       "old-holder",
			AcquireTime:          &staleAt,
			RenewTime:            &staleAt,
			LeaseDurationSeconds: durationSeconds,
		},
	}
	require.NoError(t, effectClient.Create(context.Background(), claim))

	effectReaderFactory = func() (effectReader, error) { return effectClient, nil }
	effectClientFactory = func() (effectPatcher, error) { return effectClient, nil }
	effectClaimFactory = func() (effectClaimClient, error) { return effectClient, nil }

	result, already, err := ExecuteEffectOnce(context.Background(), "effect-live", func(context.Context) (any, error) {
		return map[string]any{"providerId": "live"}, nil
	})
	require.NoError(t, err)
	require.False(t, already)
	require.NotNil(t, result)

	var updatedClaim runsv1alpha1.EffectClaim
	require.NoError(t, effectClient.Get(context.Background(), client.ObjectKey{Name: claim.Name, Namespace: claim.Namespace}, &updatedClaim))
	require.Equal(t, runsv1alpha1.EffectClaimCompletionStatusCompleted, effectReservationState(&updatedClaim))

	var updatedStepRun runsv1alpha1.StepRun
	require.NoError(t, effectClient.Get(context.Background(), client.ObjectKey{Name: stepRunName, Namespace: effectClient.namespace}, &updatedStepRun))
	require.Len(t, updatedStepRun.Status.Effects, 1)
	require.Equal(t, "effect-live", updatedStepRun.Status.Effects[0].Key)
}

func TestReserveEffect_OnlyOneWorkerRecoversStaleReservationLive(t *testing.T) {
	effectClient := setupEffectsEnvtest(t)
	testResetEffectEmitter()

	prevReader := effectReaderFactory
	prevClaim := effectClaimFactory
	t.Cleanup(func() {
		effectReaderFactory = prevReader
		effectClaimFactory = prevClaim
		testResetEffectEmitter()
	})

	const stepRunName = "effect-live-race"
	t.Setenv(contracts.StepRunNameEnv, stepRunName)
	t.Setenv(contracts.StepRunNamespaceEnv, effectClient.namespace)

	stepRun := newIntegrationStepRun(stepRunName, effectClient.namespace)
	require.NoError(t, effectClient.Create(context.Background(), stepRun))

	staleAt := metav1.NewMicroTime(time.Now().Add(-2 * time.Second).UTC())
	durationSeconds := int32(1)
	claim := &runsv1alpha1.EffectClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      effectReservationClaimName(effectClient.namespace, stepRunName, "effect-race-live"),
			Namespace: effectClient.namespace,
		},
		Spec: runsv1alpha1.EffectClaimSpec{
			StepRunRef: refs.StepRunReference{
				ObjectReference: refs.ObjectReference{Name: stepRunName},
				UID:             &stepRun.UID,
			},
			EffectKey:            "effect-race-live",
			HolderIdentity:       "old-holder",
			AcquireTime:          &staleAt,
			RenewTime:            &staleAt,
			LeaseDurationSeconds: durationSeconds,
		},
	}
	require.NoError(t, effectClient.Create(context.Background(), claim))

	effectReaderFactory = func() (effectReader, error) { return effectClient, nil }
	effectClaimFactory = func() (effectClaimClient, error) { return effectClient, nil }

	type result struct {
		reservation *effectReservation
		already     bool
		err         error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	run := func() {
		defer wg.Done()
		reservation, already, err := reserveEffect(context.Background(), "effect-race-live")
		results <- result{reservation: reservation, already: already, err: err}
	}

	go run()
	go run()
	wg.Wait()
	close(results)

	successes := 0
	alreadyCount := 0
	for item := range results {
		require.NoError(t, item.err)
		if item.already {
			alreadyCount++
			continue
		}
		successes++
		require.NotNil(t, item.reservation)
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, alreadyCount)

	var updatedClaim runsv1alpha1.EffectClaim
	require.NoError(t, effectClient.Get(context.Background(), client.ObjectKey{Name: claim.Name, Namespace: claim.Namespace}, &updatedClaim))
	require.Equal(t, runsv1alpha1.EffectClaimCompletionStatus(""), effectReservationState(&updatedClaim))
	require.NotEqual(t, "old-holder", strings.TrimSpace(updatedClaim.Spec.HolderIdentity))
}

func TestReleaseEffectReservation_DoesNotClearRecoveredClaimLive(t *testing.T) {
	effectClient := setupEffectsEnvtest(t)

	claim := &runsv1alpha1.EffectClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      effectReservationClaimName(effectClient.namespace, "step-1", "effect-live-release-guard"),
			Namespace: effectClient.namespace,
		},
		Spec: runsv1alpha1.EffectClaimSpec{
			StepRunRef: refs.StepRunReference{
				ObjectReference: refs.ObjectReference{Name: "step-1"},
			},
			EffectKey:      "effect-live-release-guard",
			HolderIdentity: "current-holder",
		},
	}
	require.NoError(t, effectClient.Create(context.Background(), claim))

	err := releaseEffectReservation(context.Background(), &effectReservation{
		client:         effectClient,
		claim:          claim.DeepCopy(),
		holderIdentity: "stale-holder",
	})
	require.NoError(t, err)

	var remaining runsv1alpha1.EffectClaim
	err = effectClient.Get(context.Background(), client.ObjectKey{Name: claim.Name, Namespace: claim.Namespace}, &remaining)
	require.NoError(t, err)
	require.Equal(t, "current-holder", strings.TrimSpace(remaining.Spec.HolderIdentity))
}

func TestExecuteEffectOnce_RenewsReservationDuringLongExecutionLive(t *testing.T) {
	effectClient := setupEffectsEnvtest(t)
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

	const stepRunName = "effect-live-renew"
	t.Setenv(contracts.StepRunNameEnv, stepRunName)
	t.Setenv(contracts.StepRunNamespaceEnv, effectClient.namespace)

	stepRun := newIntegrationStepRun(stepRunName, effectClient.namespace)
	require.NoError(t, effectClient.Create(context.Background(), stepRun))

	effectReaderFactory = func() (effectReader, error) { return effectClient, nil }
	effectClientFactory = func() (effectPatcher, error) { return effectClient, nil }
	effectClaimFactory = func() (effectClaimClient, error) { return effectClient, nil }

	started := make(chan struct{})
	release := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		_, _, err := ExecuteEffectOnce(context.Background(), "effect-live-renew", func(context.Context) (any, error) {
			close(started)
			<-release
			return map[string]any{"providerId": "renew-live"}, nil
		})
		errCh <- err
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("expected effect execution to start")
	}

	claimName := effectReservationClaimName(effectClient.namespace, stepRunName, "effect-live-renew")
	require.Eventually(t, func() bool {
		var claim runsv1alpha1.EffectClaim
		if err := effectClient.Get(context.Background(), client.ObjectKey{Name: claimName, Namespace: effectClient.namespace}, &claim); err != nil {
			return false
		}
		return claim.Spec.AcquireTime != nil && claim.Spec.RenewTime != nil &&
			claim.Spec.RenewTime.Time.After(claim.Spec.AcquireTime.Time)
	}, 2*time.Second, 20*time.Millisecond, "expected live claim renew time to advance while effect is running")

	reservation, already, err := reserveEffect(context.Background(), "effect-live-renew")
	require.NoError(t, err)
	require.True(t, already)
	require.Nil(t, reservation)

	close(release)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("expected ExecuteEffectOnce to finish after release")
	}
}
