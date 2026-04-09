//go:build integration

package envtest

import (
	"context"
	"testing"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPatchStepRunStatus_DedupesEffectsByKeyWhenSeqZero(t *testing.T) {
	h := setupSDKEnvtest(t, runsv1alpha1.AddToScheme)
	apiClient := h.apiClient
	sdkClient := h.sdkClient
	namespace := h.namespace

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "effect-dedupe-live",
			Namespace: namespace,
		},
		Spec: runsv1alpha1.StepRunSpec{
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun-effect-dedupe"},
			},
			StepID: "step-1",
		},
	}
	require.NoError(t, apiClient.Create(context.Background(), stepRun))

	now := metav1.NewTime(time.Now().UTC())
	firstPatch := runsv1alpha1.StepRunStatus{
		Effects: []runsv1alpha1.EffectRecord{{
			Seq:       0,
			Key:       "effect-1",
			Status:    "succeeded",
			EmittedAt: &now,
			Details:   &runtime.RawExtension{Raw: []byte(`{"source":"first"}`)},
		}},
	}
	require.NoError(t, sdkClient.PatchStepRunStatus(context.Background(), stepRun.Name, firstPatch))

	secondPatch := runsv1alpha1.StepRunStatus{
		Effects: []runsv1alpha1.EffectRecord{{
			Seq:       0,
			Key:       "effect-1",
			Status:    "succeeded",
			EmittedAt: &now,
			Details:   &runtime.RawExtension{Raw: []byte(`{"source":"second"}`)},
		}},
	}
	require.NoError(t, sdkClient.PatchStepRunStatus(context.Background(), stepRun.Name, secondPatch))

	updated := &runsv1alpha1.StepRun{}
	require.NoError(t, apiClient.Get(
		context.Background(),
		client.ObjectKey{Name: stepRun.Name, Namespace: namespace},
		updated,
	))
	require.Len(t, updated.Status.Effects, 1)
	require.Equal(t, "effect-1", updated.Status.Effects[0].Key)
	require.Equal(t, "succeeded", updated.Status.Effects[0].Status)
	require.NotNil(t, updated.Status.Effects[0].Details)
	require.JSONEq(t, `{"source":"first"}`, string(updated.Status.Effects[0].Details.Raw))
}

func TestPatchStepRunStatus_PreservesNeedsWhenIncomingOmitsThemLive(t *testing.T) {
	h := setupSDKEnvtest(t, runsv1alpha1.AddToScheme)
	apiClient := h.apiClient
	sdkClient := h.sdkClient
	namespace := h.namespace

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step-needs-preserve-live",
			Namespace: namespace,
		},
		Spec: runsv1alpha1.StepRunSpec{
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun-needs-preserve"},
			},
			StepID: "step-1",
		},
	}
	require.NoError(t, apiClient.Create(context.Background(), stepRun))

	withStatus := stepRun.DeepCopy()
	withStatus.Status.Phase = enums.PhasePending
	withStatus.Status.Needs = []string{"prior-step"}
	require.NoError(t, apiClient.Status().Update(context.Background(), withStatus))

	require.NoError(t, sdkClient.PatchStepRunStatus(context.Background(), stepRun.Name, runsv1alpha1.StepRunStatus{
		Phase: enums.PhaseRunning,
	}))

	updated := &runsv1alpha1.StepRun{}
	require.NoError(t, apiClient.Get(
		context.Background(),
		client.ObjectKey{Name: stepRun.Name, Namespace: namespace},
		updated,
	))
	require.Equal(t, enums.PhaseRunning, updated.Status.Phase)
	require.Equal(t, []string{"prior-step"}, updated.Status.Needs)
}

func TestPatchStepRunStatus_ExplicitEmptyNeedsClearsLive(t *testing.T) {
	h := setupSDKEnvtest(t, runsv1alpha1.AddToScheme)
	apiClient := h.apiClient
	sdkClient := h.sdkClient
	namespace := h.namespace

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step-needs-clear-live",
			Namespace: namespace,
		},
		Spec: runsv1alpha1.StepRunSpec{
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun-needs-clear"},
			},
			StepID: "step-1",
		},
	}
	require.NoError(t, apiClient.Create(context.Background(), stepRun))

	withStatus := stepRun.DeepCopy()
	withStatus.Status.Phase = enums.PhasePending
	withStatus.Status.Needs = []string{"prior-step"}
	require.NoError(t, apiClient.Status().Update(context.Background(), withStatus))

	require.NoError(t, sdkClient.PatchStepRunStatus(context.Background(), stepRun.Name, runsv1alpha1.StepRunStatus{
		Phase: enums.PhaseRunning,
		Needs: []string{},
	}))

	updated := &runsv1alpha1.StepRun{}
	require.NoError(t, apiClient.Get(
		context.Background(),
		client.ObjectKey{Name: stepRun.Name, Namespace: namespace},
		updated,
	))
	require.Equal(t, enums.PhaseRunning, updated.Status.Phase)
	require.Empty(t, updated.Status.Needs)
}
