//go:build integration

package envtest

import (
	"context"
	"testing"
	"time"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	bubuk8s "github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func newImpulseForStatsTest(name, namespace string) *bubuv1alpha1.Impulse {
	return &bubuv1alpha1.Impulse{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: bubuv1alpha1.ImpulseSpec{
			TemplateRef: refs.ImpulseTemplateReference{
				Name: "impulse-template",
			},
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{Name: "story-for-stats"},
			},
		},
	}
}

func TestUpdateImpulseTriggerStats_AppliesDeltaLive(t *testing.T) {
	h := setupSDKEnvtest(t, bubuv1alpha1.AddToScheme)
	apiClient := h.apiClient
	sdkClient := h.sdkClient
	namespace := h.namespace
	impulse := newImpulseForStatsTest("impulse-stats-live", namespace)
	require.NoError(t, apiClient.Create(context.Background(), impulse))

	lastTrigger := time.Date(2026, time.March, 27, 10, 0, 0, 0, time.FixedZone("UTC+4", 4*60*60))
	lastSuccess := lastTrigger.Add(30 * time.Second)
	lastThrottled := lastTrigger.Add(45 * time.Second)

	require.NoError(t, sdkClient.UpdateImpulseTriggerStats(context.Background(), impulse.Name, namespace, bubuk8s.TriggerStatsDelta{
		TriggersReceived:  3,
		StoriesLaunched:   2,
		FailedTriggers:    1,
		ThrottledTriggers: 1,
		LastTrigger:       lastTrigger,
		LastSuccess:       &lastSuccess,
		LastThrottled:     &lastThrottled,
	}))

	updated := &bubuv1alpha1.Impulse{}
	require.NoError(t, apiClient.Get(
		context.Background(),
		client.ObjectKey{Name: impulse.Name, Namespace: namespace},
		updated,
	))

	require.Equal(t, int64(3), updated.Status.TriggersReceived)
	require.Equal(t, int64(2), updated.Status.StoriesLaunched)
	require.Equal(t, int64(1), updated.Status.FailedTriggers)
	require.Equal(t, int64(1), updated.Status.ThrottledTriggers)
	require.NotNil(t, updated.Status.LastTrigger)
	require.NotNil(t, updated.Status.LastSuccess)
	require.NotNil(t, updated.Status.LastThrottled)
	require.Equal(t, updated.Generation, updated.Status.ObservedGeneration)
	require.True(t, updated.Status.LastTrigger.Time.Equal(lastTrigger.UTC()))
	require.True(t, updated.Status.LastSuccess.Time.Equal(lastSuccess.UTC()))
	require.True(t, updated.Status.LastThrottled.Time.Equal(lastThrottled.UTC()))
}

func TestUpdateImpulseTriggerStats_PreservesUnsetTimestampsLive(t *testing.T) {
	h := setupSDKEnvtest(t, bubuv1alpha1.AddToScheme)
	apiClient := h.apiClient
	sdkClient := h.sdkClient
	namespace := h.namespace
	impulse := newImpulseForStatsTest("impulse-stats-preserve-live", namespace)
	require.NoError(t, apiClient.Create(context.Background(), impulse))

	baseTrigger := metav1.NewTime(time.Date(2026, time.March, 27, 9, 0, 0, 0, time.UTC))
	baseSuccess := metav1.NewTime(time.Date(2026, time.March, 27, 9, 1, 0, 0, time.UTC))
	baseThrottled := metav1.NewTime(time.Date(2026, time.March, 27, 9, 2, 0, 0, time.UTC))

	statusObj := impulse.DeepCopy()
	statusObj.Status = bubuv1alpha1.ImpulseStatus{
		TriggersReceived:  10,
		StoriesLaunched:   6,
		FailedTriggers:    2,
		ThrottledTriggers: 4,
		LastTrigger:       &baseTrigger,
		LastSuccess:       &baseSuccess,
		LastThrottled:     &baseThrottled,
	}
	require.NoError(t, apiClient.Status().Update(context.Background(), statusObj))

	nextTrigger := time.Date(2026, time.March, 27, 10, 0, 0, 0, time.UTC)
	require.NoError(t, sdkClient.UpdateImpulseTriggerStats(context.Background(), impulse.Name, namespace, bubuk8s.TriggerStatsDelta{
		TriggersReceived: 1,
		LastTrigger:      nextTrigger,
	}))

	updated := &bubuv1alpha1.Impulse{}
	require.NoError(t, apiClient.Get(
		context.Background(),
		client.ObjectKey{Name: impulse.Name, Namespace: namespace},
		updated,
	))

	require.Equal(t, int64(11), updated.Status.TriggersReceived)
	require.Equal(t, int64(6), updated.Status.StoriesLaunched)
	require.Equal(t, int64(2), updated.Status.FailedTriggers)
	require.Equal(t, int64(4), updated.Status.ThrottledTriggers)
	require.NotNil(t, updated.Status.LastTrigger)
	require.NotNil(t, updated.Status.LastSuccess)
	require.NotNil(t, updated.Status.LastThrottled)
	require.True(t, updated.Status.LastTrigger.Time.Equal(nextTrigger.UTC()))
	require.True(t, updated.Status.LastSuccess.Time.Equal(baseSuccess.Time))
	require.True(t, updated.Status.LastThrottled.Time.Equal(baseThrottled.Time))
}
