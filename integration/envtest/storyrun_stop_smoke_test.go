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
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestStopStoryRun_RequestsGracefulCancelLive(t *testing.T) {
	h := setupSDKEnvtest(t, runsv1alpha1.AddToScheme)
	apiClient := h.apiClient
	sdkClient := h.sdkClient
	namespace := h.namespace
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stop-storyrun-running-live",
			Namespace: namespace,
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{Name: "story-stop-live"},
			},
		},
	}
	require.NoError(t, apiClient.Create(context.Background(), storyRun))

	startedAt := metav1.NewTime(time.Now().UTC().Add(-2 * time.Second))
	statusObj := storyRun.DeepCopy()
	statusObj.Status.Phase = enums.PhaseRunning
	statusObj.Status.StartedAt = &startedAt
	require.NoError(t, apiClient.Status().Update(context.Background(), statusObj))

	require.NoError(t, sdkClient.StopStoryRun(context.Background(), storyRun.Name, namespace))

	updated := &runsv1alpha1.StoryRun{}
	require.NoError(t, apiClient.Get(
		context.Background(),
		client.ObjectKey{Name: storyRun.Name, Namespace: namespace},
		updated,
	))

	require.Equal(t, enums.PhaseRunning, updated.Status.Phase)
	require.Empty(t, updated.Status.Message)
	require.Nil(t, updated.Status.FinishedAt)
	require.NotNil(t, updated.Status.StartedAt)
	require.Empty(t, updated.Status.Duration)
	require.NotNil(t, updated.Spec.CancelRequested)
	require.True(t, *updated.Spec.CancelRequested)
}

func TestStopStoryRun_RequestsGracefulCancelForPausedRunLive(t *testing.T) {
	h := setupSDKEnvtest(t, runsv1alpha1.AddToScheme)
	apiClient := h.apiClient
	sdkClient := h.sdkClient
	namespace := h.namespace
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stop-storyrun-paused-live",
			Namespace: namespace,
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{Name: "story-stop-live-paused"},
			},
		},
	}
	require.NoError(t, apiClient.Create(context.Background(), storyRun))

	statusObj := storyRun.DeepCopy()
	statusObj.Status.Phase = enums.PhasePaused
	statusObj.Status.Message = "paused by controller"
	require.NoError(t, apiClient.Status().Update(context.Background(), statusObj))

	require.NoError(t, sdkClient.StopStoryRun(context.Background(), storyRun.Name, namespace))

	updated := &runsv1alpha1.StoryRun{}
	require.NoError(t, apiClient.Get(
		context.Background(),
		client.ObjectKey{Name: storyRun.Name, Namespace: namespace},
		updated,
	))
	require.Equal(t, enums.PhasePaused, updated.Status.Phase)
	require.Equal(t, "paused by controller", updated.Status.Message)
	require.Nil(t, updated.Status.FinishedAt)
	require.NotNil(t, updated.Spec.CancelRequested)
	require.True(t, *updated.Spec.CancelRequested)
}
