//go:build integration

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
package envtest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	"github.com/bubustack/bubu-sdk-go/k8s"
	sdkerrors "github.com/bubustack/bubu-sdk-go/pkg/errors"
	"github.com/bubustack/core/contracts"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type storyEnvtestHarness struct {
	apiClient client.Client
	sdkClient *k8s.Client
	namespace string
}

func setupStoryEnvtest(t *testing.T) *storyEnvtestHarness {
	t.Helper()

	base := setupSDKEnvtest(t, runsv1alpha1.AddToScheme, bubuv1alpha1.AddToScheme)

	return &storyEnvtestHarness{
		apiClient: base.apiClient,
		sdkClient: base.sdkClient,
		namespace: base.namespace,
	}
}

func createTestStory(t *testing.T, apiClient client.Client, namespace, name string) *bubuv1alpha1.Story {
	t.Helper()

	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{
				Name: "start",
				Type: enums.StepTypeCondition,
			}},
		},
	}
	require.NoError(t, apiClient.Create(context.Background(), story))
	return story
}

func startStoryTriggerResolver(t *testing.T, apiClient client.Client, namespace string) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	pollInterval := triggerResolverPollInterval()
	requestTimeout := triggerResolverRequestTimeout()
	stopTimeout := triggerResolverStopTimeout()

	go func() {
		defer close(done)
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				resolveCtx, resolveCancel := context.WithTimeout(ctx, requestTimeout)
				resolvePendingStoryTriggers(resolveCtx, apiClient, namespace)
				resolveCancel()
			}
		}
	}()

	t.Cleanup(func() {
		cancel()
		if !waitForSignal(done, stopTimeout) {
			t.Fatalf("story trigger resolver did not stop within %s", stopTimeout)
		}
	})
}

func resolvePendingStoryTriggers(ctx context.Context, apiClient client.Client, namespace string) {
	list := &runsv1alpha1.StoryTriggerList{}
	if err := apiClient.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return
	}
	for i := range list.Items {
		trigger := list.Items[i].DeepCopy()
		if trigger.Status.Decision != "" {
			continue
		}
		_ = resolveStoryTrigger(ctx, apiClient, trigger)
	}
}

func resolveStoryTrigger(ctx context.Context, apiClient client.Client, trigger *runsv1alpha1.StoryTrigger) error {
	if err := apiClient.Get(ctx, trigger.Spec.StoryRef.ToNamespacedName(trigger), &bubuv1alpha1.Story{}); err != nil {
		if apierrors.IsNotFound(err) {
			return updateStoryTriggerStatus(ctx, apiClient, trigger, runsv1alpha1.StoryTriggerDecisionRejected, "StoryNotFound", err.Error(), nil)
		}
		return err
	}

	inputHash, err := runsidentity.ComputeTriggerInputHashFromRawExtension(trigger.Spec.Inputs)
	if err != nil {
		return updateStoryTriggerStatus(ctx, apiClient, trigger, runsv1alpha1.StoryTriggerDecisionRejected, "InputHashMismatch", err.Error(), nil)
	}
	if expected := trigger.Spec.DeliveryIdentity.InputHash; expected != "" && expected != inputHash {
		return updateStoryTriggerStatus(ctx, apiClient, trigger, runsv1alpha1.StoryTriggerDecisionRejected, "InputHashMismatch", "spec.deliveryIdentity.inputHash does not match spec.inputs", nil)
	}

	storyRun := desiredStoryRunForTrigger(trigger, inputHash)
	decision := runsv1alpha1.StoryTriggerDecisionCreated
	reason := "StoryRunCreated"
	message := fmt.Sprintf("created StoryRun %s/%s", storyRun.Namespace, storyRun.Name)

	if err := apiClient.Create(ctx, storyRun); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}

		existing := &runsv1alpha1.StoryRun{}
		if getErr := apiClient.Get(ctx, client.ObjectKeyFromObject(storyRun), existing); getErr != nil {
			return getErr
		}
		if !storyRunMatchesTrigger(existing, trigger, inputHash) {
			return updateStoryTriggerStatus(ctx, apiClient, trigger, runsv1alpha1.StoryTriggerDecisionRejected, "StoryRunConflict", fmt.Sprintf("existing StoryRun %s/%s does not match StoryTrigger storyRef and inputs", existing.Namespace, existing.Name), nil)
		}
		if storyRunOriginatesFromTrigger(existing, trigger) {
			decision = runsv1alpha1.StoryTriggerDecisionCreated
			reason = "StoryRunCreated"
			message = fmt.Sprintf("recovered previously created StoryRun %s/%s", existing.Namespace, existing.Name)
		} else {
			decision = runsv1alpha1.StoryTriggerDecisionReused
			reason = "StoryRunReused"
			message = fmt.Sprintf("reused existing StoryRun %s/%s", existing.Namespace, existing.Name)
		}
		storyRun = existing
	}

	return updateStoryTriggerStatus(ctx, apiClient, trigger, decision, reason, message, storyRun)
}

func desiredStoryRunForTrigger(trigger *runsv1alpha1.StoryTrigger, inputHash string) *runsv1alpha1.StoryRun {
	identity := runsidentity.StoryTriggerIdentity(trigger.Spec.DeliveryIdentity.Key, trigger.Spec.DeliveryIdentity.SubmissionID)
	storyNamespace := trigger.Spec.StoryRef.ToNamespacedName(trigger).Namespace

	annotations := map[string]string{
		runsidentity.StoryRunTriggerRequestNameAnnotation: trigger.Name,
		runsidentity.StoryRunTriggerRequestUIDAnnotation:  string(trigger.GetUID()),
	}
	if key := trigger.Spec.DeliveryIdentity.Key; key != "" {
		annotations[runsidentity.StoryRunTriggerTokenAnnotation] = key
		annotations[runsidentity.StoryRunTriggerInputHashAnnotation] = inputHash
	}

	var impulseRef *refs.ImpulseReference
	if trigger.Spec.ImpulseRef != nil {
		impulseRef = trigger.Spec.ImpulseRef.DeepCopy()
	}

	var inputs *runtime.RawExtension
	if trigger.Spec.Inputs != nil {
		inputs = trigger.Spec.Inputs.DeepCopy()
	}

	return &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:        runsidentity.DeriveStoryRunName(storyNamespace, trigger.Spec.StoryRef.Name, identity),
			Namespace:   trigger.Namespace,
			Annotations: annotations,
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef:   *trigger.Spec.StoryRef.DeepCopy(),
			ImpulseRef: impulseRef,
			Inputs:     inputs,
		},
	}
}

func storyRunMatchesTrigger(existing *runsv1alpha1.StoryRun, trigger *runsv1alpha1.StoryTrigger, triggerHash string) bool {
	if existing == nil || trigger == nil {
		return false
	}
	if existing.Spec.StoryRef.ToNamespacedName(existing) != trigger.Spec.StoryRef.ToNamespacedName(trigger) {
		return false
	}
	if existing.Spec.StoryRef.Version != trigger.Spec.StoryRef.Version {
		return false
	}
	existingHash, err := runsidentity.ComputeTriggerInputHashFromRawExtension(existing.Spec.Inputs)
	if err != nil {
		return false
	}
	return existingHash == triggerHash
}

func storyRunOriginatesFromTrigger(existing *runsv1alpha1.StoryRun, trigger *runsv1alpha1.StoryTrigger) bool {
	if existing == nil || trigger == nil {
		return false
	}
	annotations := existing.GetAnnotations()
	if len(annotations) == 0 {
		return false
	}
	if annotations[runsidentity.StoryRunTriggerRequestNameAnnotation] != trigger.Name {
		return false
	}
	uid := annotations[runsidentity.StoryRunTriggerRequestUIDAnnotation]
	return uid != "" && uid == string(trigger.GetUID())
}

func updateStoryTriggerStatus(
	ctx context.Context,
	apiClient client.Client,
	trigger *runsv1alpha1.StoryTrigger,
	decision runsv1alpha1.StoryTriggerDecision,
	reason string,
	message string,
	storyRun *runsv1alpha1.StoryRun,
) error {
	current := &runsv1alpha1.StoryTrigger{}
	if err := apiClient.Get(ctx, client.ObjectKeyFromObject(trigger), current); err != nil {
		return err
	}
	now := metav1.Now()
	current.Status.ObservedGeneration = current.Generation
	if current.Status.AcceptedAt == nil {
		current.Status.AcceptedAt = &now
	}
	current.Status.CompletedAt = &now
	current.Status.Decision = decision
	current.Status.Reason = reason
	current.Status.Message = message
	current.Status.StoryRunRef = storyRunRef(storyRun)
	return apiClient.Status().Update(ctx, current)
}

func storyRunRef(storyRun *runsv1alpha1.StoryRun) *refs.StoryRunReference {
	if storyRun == nil {
		return nil
	}
	ref := &refs.StoryRunReference{
		ObjectReference: refs.ObjectReference{Name: storyRun.Name},
	}
	if storyRun.Namespace != "" {
		namespace := storyRun.Namespace
		ref.Namespace = &namespace
	}
	return ref
}

func TestStorySubmissionSmoke(t *testing.T) {
	h := setupStoryEnvtest(t)
	story := createTestStory(t, h.apiClient, h.namespace, "smoke-story")
	startStoryTriggerResolver(t, h.apiClient, h.namespace)

	run, err := h.sdkClient.TriggerStory(context.Background(), story.Name, h.namespace, map[string]any{"hello": "world"})
	require.NoError(t, err)
	require.NotNil(t, run)

	fetched := &runsv1alpha1.StoryRun{}
	require.NoError(t, h.apiClient.Get(context.Background(), client.ObjectKey{Name: run.Name, Namespace: h.namespace}, fetched))
	require.Equal(t, "smoke-story", fetched.Spec.StoryRef.Name)
	require.NotNil(t, fetched.Spec.Inputs)
	require.Contains(t, string(fetched.Spec.Inputs.Raw), "hello")

	triggers := &runsv1alpha1.StoryTriggerList{}
	require.NoError(t, h.apiClient.List(context.Background(), triggers, client.InNamespace(h.namespace)))
	require.Len(t, triggers.Items, 1)
	require.Equal(t, runsv1alpha1.StoryTriggerDecisionCreated, triggers.Items[0].Status.Decision)
	require.NotNil(t, triggers.Items[0].Status.StoryRunRef)
	require.Equal(t, run.Name, triggers.Items[0].Status.StoryRunRef.Name)
}

func TestStorySubmissionReusesStoryRunForSameTriggerToken(t *testing.T) {
	h := setupStoryEnvtest(t)
	story := createTestStory(t, h.apiClient, h.namespace, "smoke-story-dedupe")
	startStoryTriggerResolver(t, h.apiClient, h.namespace)

	inputs := map[string]any{"hello": "world"}
	token := "test-trigger-token"
	triggerCtx := k8s.WithTriggerToken(context.Background(), token)

	first, err := h.sdkClient.TriggerStory(triggerCtx, story.Name, h.namespace, inputs)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.NotEmpty(t, first.Name)

	second, err := h.sdkClient.TriggerStory(triggerCtx, story.Name, h.namespace, inputs)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, first.Name, second.Name)

	list := &runsv1alpha1.StoryRunList{}
	require.NoError(t, h.apiClient.List(context.Background(), list, client.InNamespace(h.namespace)))
	require.Len(t, list.Items, 1)
	require.Equal(t, first.Name, list.Items[0].Name)

	triggers := &runsv1alpha1.StoryTriggerList{}
	require.NoError(t, h.apiClient.List(context.Background(), triggers, client.InNamespace(h.namespace)))
	require.Len(t, triggers.Items, 1)
	require.Equal(t, runsv1alpha1.StoryTriggerDecisionCreated, triggers.Items[0].Status.Decision)
}

func TestStorySubmissionRejectsTokenReuseWithDifferentInputs(t *testing.T) {
	h := setupStoryEnvtest(t)
	story := createTestStory(t, h.apiClient, h.namespace, "smoke-story-token-mismatch")
	startStoryTriggerResolver(t, h.apiClient, h.namespace)

	token := "test-trigger-token-mismatch"
	triggerCtx := k8s.WithTriggerToken(context.Background(), token)

	first, err := h.sdkClient.TriggerStory(triggerCtx, story.Name, h.namespace, map[string]any{"hello": "world"})
	require.NoError(t, err)
	require.NotNil(t, first)

	second, err := h.sdkClient.TriggerStory(triggerCtx, story.Name, h.namespace, map[string]any{"hello": "different"})
	require.Error(t, err)
	require.Nil(t, second)
	require.Contains(t, err.Error(), "different immutable request identity")

	list := &runsv1alpha1.StoryRunList{}
	require.NoError(t, h.apiClient.List(context.Background(), list, client.InNamespace(h.namespace)))
	require.Len(t, list.Items, 1)
	require.Equal(t, first.Name, list.Items[0].Name)
}

func TestStorySubmissionReusesPreexistingTokenStoryRun(t *testing.T) {
	h := setupStoryEnvtest(t)
	story := createTestStory(t, h.apiClient, h.namespace, "smoke-story-preexisting")
	startStoryTriggerResolver(t, h.apiClient, h.namespace)

	token := "test-trigger-token-preexisting"
	inputs := map[string]any{"hello": "world"}
	inputBytes, err := json.Marshal(inputs)
	require.NoError(t, err)

	preexistingName := runsidentity.DeriveStoryRunName(h.namespace, story.Name, token)
	storyRefNamespace := h.namespace
	preexisting := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      preexistingName,
			Namespace: h.namespace,
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{
					Name:      story.Name,
					Namespace: &storyRefNamespace,
				},
			},
			Inputs: &runtime.RawExtension{Raw: inputBytes},
		},
	}
	require.NoError(t, h.apiClient.Create(context.Background(), preexisting))

	triggerCtx := k8s.WithTriggerToken(context.Background(), token)
	got, err := h.sdkClient.TriggerStory(triggerCtx, story.Name, h.namespace, inputs)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, preexistingName, got.Name)

	list := &runsv1alpha1.StoryRunList{}
	require.NoError(t, h.apiClient.List(context.Background(), list, client.InNamespace(h.namespace)))
	require.Len(t, list.Items, 1)
	require.Equal(t, preexistingName, list.Items[0].Name)

	triggers := &runsv1alpha1.StoryTriggerList{}
	require.NoError(t, h.apiClient.List(context.Background(), triggers, client.InNamespace(h.namespace)))
	require.Len(t, triggers.Items, 1)
	require.Equal(t, runsv1alpha1.StoryTriggerDecisionReused, triggers.Items[0].Status.Decision)
}

func TestStorySubmissionRejectsPreexistingTokenStoryRunWithDifferentInputs(t *testing.T) {
	h := setupStoryEnvtest(t)
	story := createTestStory(t, h.apiClient, h.namespace, "smoke-story-preexisting-mismatch")
	startStoryTriggerResolver(t, h.apiClient, h.namespace)

	token := "test-trigger-token-preexisting-mismatch"
	existingInputs := map[string]any{"hello": "world"}
	existingInputBytes, err := json.Marshal(existingInputs)
	require.NoError(t, err)

	preexistingName := runsidentity.DeriveStoryRunName(h.namespace, story.Name, token)
	storyRefNamespace := h.namespace
	preexisting := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      preexistingName,
			Namespace: h.namespace,
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{
					Name:      story.Name,
					Namespace: &storyRefNamespace,
				},
			},
			Inputs: &runtime.RawExtension{Raw: existingInputBytes},
		},
	}
	require.NoError(t, h.apiClient.Create(context.Background(), preexisting))

	triggerCtx := k8s.WithTriggerToken(context.Background(), token)
	got, err := h.sdkClient.TriggerStory(triggerCtx, story.Name, h.namespace, map[string]any{"hello": "different"})
	require.Error(t, err)
	require.Nil(t, got)
	require.Contains(t, err.Error(), "does not match StoryTrigger storyRef and inputs")

	list := &runsv1alpha1.StoryRunList{}
	require.NoError(t, h.apiClient.List(context.Background(), list, client.InNamespace(h.namespace)))
	require.Len(t, list.Items, 1)
	require.Equal(t, preexistingName, list.Items[0].Name)

	triggers := &runsv1alpha1.StoryTriggerList{}
	require.NoError(t, h.apiClient.List(context.Background(), triggers, client.InNamespace(h.namespace)))
	require.Len(t, triggers.Items, 1)
	require.Equal(t, runsv1alpha1.StoryTriggerDecisionRejected, triggers.Items[0].Status.Decision)
}

func TestStorySubmissionPendingTimeoutReturnsRetryable(t *testing.T) {
	t.Setenv(contracts.K8sOperationTimeoutEnv, "150ms")

	h := setupStoryEnvtest(t)
	story := createTestStory(t, h.apiClient, h.namespace, "smoke-story-pending")

	got, err := h.sdkClient.TriggerStory(context.Background(), story.Name, h.namespace, map[string]any{"hello": "world"})
	require.Error(t, err)
	require.Nil(t, got)
	require.ErrorIs(t, err, sdkerrors.ErrRetryable)
	require.Contains(t, err.Error(), "still pending")

	triggers := &runsv1alpha1.StoryTriggerList{}
	require.NoError(t, h.apiClient.List(context.Background(), triggers, client.InNamespace(h.namespace)))
	require.Len(t, triggers.Items, 1)
	require.Empty(t, triggers.Items[0].Status.Decision)
}
