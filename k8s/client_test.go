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

package k8s

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bobrapetv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	sdkerrors "github.com/bubustack/bubu-sdk-go/pkg/errors"
	"github.com/bubustack/core/contracts"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type transientGetClient struct {
	ctrlclient.Client
	getErrors []error
	getCalls  int
}

func (c *transientGetClient) Get(
	ctx context.Context,
	key ctrlclient.ObjectKey,
	obj ctrlclient.Object,
	opts ...ctrlclient.GetOption,
) error {
	c.getCalls++
	if idx := c.getCalls - 1; idx < len(c.getErrors) && c.getErrors[idx] != nil {
		return c.getErrors[idx]
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

type transientStoryTriggerGetClient struct {
	ctrlclient.Client
	triggerName  string
	failuresLeft int
	failuresSeen int
}

func (c *transientStoryTriggerGetClient) Get(
	ctx context.Context,
	key ctrlclient.ObjectKey,
	obj ctrlclient.Object,
	opts ...ctrlclient.GetOption,
) error {
	if _, ok := obj.(*runsv1alpha1.StoryTrigger); ok && key.Name == c.triggerName && c.failuresLeft > 0 {
		c.failuresLeft--
		c.failuresSeen++
		return apierrors.NewTimeoutError("simulated transient get timeout", 1)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

type transientCreateClient struct {
	ctrlclient.Client
	createErrors []error
	createCalls  int
}

func (c *transientCreateClient) Create(
	ctx context.Context,
	obj ctrlclient.Object,
	opts ...ctrlclient.CreateOption,
) error {
	c.createCalls++
	if idx := c.createCalls - 1; idx < len(c.createErrors) && c.createErrors[idx] != nil {
		return c.createErrors[idx]
	}
	return c.Client.Create(ctx, obj, opts...)
}

type storyTriggerResolvingClient struct {
	ctrlclient.Client
}

func newStoryTriggerResolvingFakeClient(objects ...ctrlclient.Object) ctrlclient.Client { //nolint:unparam
	base := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(
			&runsv1alpha1.StepRun{},
			&runsv1alpha1.StoryRun{},
			&runsv1alpha1.StoryTrigger{},
			&bobrapetv1alpha1.Impulse{},
		).
		WithObjects(objects...).
		Build()
	return &storyTriggerResolvingClient{Client: base}
}

func (c *storyTriggerResolvingClient) Create(
	ctx context.Context,
	obj ctrlclient.Object,
	opts ...ctrlclient.CreateOption,
) error {
	if err := c.Client.Create(ctx, obj, opts...); err != nil {
		return err
	}
	trigger, ok := obj.(*runsv1alpha1.StoryTrigger)
	if !ok {
		return nil
	}
	return c.resolveStoryTrigger(ctx, ctrlclient.ObjectKeyFromObject(trigger))
}

func (c *storyTriggerResolvingClient) resolveStoryTrigger(ctx context.Context, key ctrlclient.ObjectKey) error {
	trigger := &runsv1alpha1.StoryTrigger{}
	if err := c.Client.Get(ctx, key, trigger); err != nil { //nolint:staticcheck
		return err
	}
	if trigger.Status.Decision != "" {
		return nil
	}

	inputHash, err := runsidentity.ComputeTriggerInputHashFromRawExtension(trigger.Spec.Inputs)
	if err != nil {
		return err
	}

	storyRun := desiredStoryRunForTriggerTest(trigger, inputHash)
	decision := runsv1alpha1.StoryTriggerDecisionCreated
	reason := "StoryRunCreated"
	message := fmt.Sprintf("created StoryRun %s/%s", storyRun.Namespace, storyRun.Name)

	if err := c.Client.Create(ctx, storyRun); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}

		existing := &runsv1alpha1.StoryRun{}
		//nolint:staticcheck,lll
		if getErr := c.Client.Get(ctx, ctrlclient.ObjectKeyFromObject(storyRun), existing); getErr != nil {
			return getErr
		}
		if !storyRunMatchesTriggerForTest(existing, trigger, inputHash) {
			decision = runsv1alpha1.StoryTriggerDecisionRejected
			reason = "StoryRunConflict"
			message = fmt.Sprintf("existing StoryRun %s/%s does not match StoryTrigger storyRef and inputs", existing.Namespace, existing.Name) //nolint:lll
			storyRun = nil
		} else {
			decision = runsv1alpha1.StoryTriggerDecisionReused
			reason = "StoryRunReused"
			message = fmt.Sprintf("reused existing StoryRun %s/%s", existing.Namespace, existing.Name)
			storyRun = existing
		}
	}

	now := metav1.Now()
	trigger.Status.ObservedGeneration = trigger.Generation
	if trigger.Status.AcceptedAt == nil {
		trigger.Status.AcceptedAt = &now
	}
	trigger.Status.CompletedAt = &now
	trigger.Status.Decision = decision
	trigger.Status.Reason = reason
	trigger.Status.Message = message
	trigger.Status.StoryRunRef = storyRunRefForTest(storyRun)
	return c.Client.Status().Update(ctx, trigger)
}

func desiredStoryRunForTriggerTest(trigger *runsv1alpha1.StoryTrigger, inputHash string) *runsv1alpha1.StoryRun {
	identity := runsidentity.StoryTriggerIdentity(
		strings.TrimSpace(trigger.Spec.DeliveryIdentity.Key),
		strings.TrimSpace(trigger.Spec.DeliveryIdentity.SubmissionID),
	)
	storyNamespace := trigger.Spec.StoryRef.ToNamespacedName(trigger).Namespace

	annotations := map[string]string{
		runsidentity.StoryRunTriggerRequestNameAnnotation: trigger.Name,
	}
	if uid := string(trigger.GetUID()); uid != "" {
		annotations[runsidentity.StoryRunTriggerRequestUIDAnnotation] = uid
	}
	if key := strings.TrimSpace(trigger.Spec.DeliveryIdentity.Key); key != "" {
		annotations[runsidentity.StoryRunTriggerTokenAnnotation] = key
		annotations[runsidentity.StoryRunTriggerInputHashAnnotation] = inputHash
	}

	var impulseRef *refs.ImpulseReference
	if trigger.Spec.ImpulseRef != nil {
		impulseRef = trigger.Spec.ImpulseRef.DeepCopy()
	}

	var inputs *k8sruntime.RawExtension
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

func storyRunMatchesTriggerForTest(existing *runsv1alpha1.StoryRun, trigger *runsv1alpha1.StoryTrigger, triggerHash string) bool { //nolint:lll
	if existing == nil || trigger == nil {
		return false
	}
	if existing.Spec.StoryRef.ToNamespacedName(existing) != trigger.Spec.StoryRef.ToNamespacedName(trigger) {
		return false
	}
	if strings.TrimSpace(existing.Spec.StoryRef.Version) != strings.TrimSpace(trigger.Spec.StoryRef.Version) {
		return false
	}
	existingHash, err := runsidentity.ComputeTriggerInputHashFromRawExtension(existing.Spec.Inputs)
	if err != nil {
		return false
	}
	return existingHash == triggerHash
}

func storyRunRefForTest(storyRun *runsv1alpha1.StoryRun) *refs.StoryRunReference {
	if storyRun == nil {
		return nil
	}
	ref := &refs.StoryRunReference{
		ObjectReference: refs.ObjectReference{
			Name: storyRun.Name,
		},
	}
	if storyRun.Namespace != "" {
		namespace := storyRun.Namespace
		ref.Namespace = &namespace
	}
	return ref
}

func singleStoryTrigger(t *testing.T, c ctrlclient.Client, namespace string) *runsv1alpha1.StoryTrigger {
	t.Helper()
	list := &runsv1alpha1.StoryTriggerList{}
	if err := c.List(context.Background(), list, ctrlclient.InNamespace(namespace)); err != nil {
		t.Fatalf("failed to list StoryTriggers: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected exactly 1 StoryTrigger, got %d", len(list.Items))
	}
	return list.Items[0].DeepCopy()
}

type timeoutGetClient struct {
	ctrlclient.Client
}

func (c *timeoutGetClient) Get(
	ctx context.Context,
	key ctrlclient.ObjectKey,
	obj ctrlclient.Object,
	opts ...ctrlclient.GetOption,
) error {
	return apierrors.NewTimeoutError("simulated timeout", 1)
}

type transientStatusPatchClient struct {
	ctrlclient.Client
	statusWriter ctrlclient.SubResourceWriter
}

func (c *transientStatusPatchClient) Status() ctrlclient.SubResourceWriter {
	return c.statusWriter
}

type transientStatusWriter struct {
	ctrlclient.SubResourceWriter
	patchErrors []error
	patchCalls  int
}

func (w *transientStatusWriter) Patch(
	ctx context.Context,
	obj ctrlclient.Object,
	patch ctrlclient.Patch,
	opts ...ctrlclient.SubResourcePatchOption,
) error {
	w.patchCalls++
	if idx := w.patchCalls - 1; idx < len(w.patchErrors) && w.patchErrors[idx] != nil {
		return w.patchErrors[idx]
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

func TestGetPodNamespace(t *testing.T) {
	testCases := []struct {
		name     string
		envVars  map[string]string
		expected string
	}{
		{
			name: contracts.TargetStoryNamespaceEnv + " should have the highest precedence",
			envVars: map[string]string{
				contracts.TargetStoryNamespaceEnv: "story-ns",
				contracts.ImpulseNamespaceEnv:     "impulse-ns",
				contracts.StepRunNamespaceEnv:     "steprun-ns",
				contracts.PodNamespaceEnv:         "pod-ns",
			},
			expected: "story-ns",
		},
		{
			name: contracts.ImpulseNamespaceEnv + " should have second precedence",
			envVars: map[string]string{
				contracts.ImpulseNamespaceEnv: "impulse-ns",
				contracts.StepRunNamespaceEnv: "steprun-ns",
				contracts.PodNamespaceEnv:     "pod-ns",
			},
			expected: "impulse-ns",
		},
		{
			name: contracts.StepRunNamespaceEnv + " should have third precedence",
			envVars: map[string]string{
				contracts.StepRunNamespaceEnv: "steprun-ns",
				contracts.PodNamespaceEnv:     "pod-ns",
			},
			expected: "steprun-ns",
		},
		{
			name: contracts.PodNamespaceEnv + " should have fourth precedence",
			envVars: map[string]string{
				contracts.PodNamespaceEnv: "pod-ns",
			},
			expected: "pod-ns",
		},
		{
			name:     "Should return empty string if no env vars are set",
			envVars:  map[string]string{},
			expected: "",
		},
		{
			name: "Should ignore empty env vars and use the next in precedence",
			envVars: map[string]string{
				contracts.TargetStoryNamespaceEnv: "",
				contracts.ImpulseNamespaceEnv:     "impulse-ns",
			},
			expected: "impulse-ns",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, key := range []string{
				contracts.TargetStoryNamespaceEnv,
				contracts.ImpulseNamespaceEnv,
				contracts.StepRunNamespaceEnv,
				contracts.PodNamespaceEnv,
			} {
				t.Setenv(key, "")
			}

			for key, value := range tc.envVars {
				t.Setenv(key, value)
			}

			if got := getPodNamespace(); got != tc.expected {
				t.Errorf("getPodNamespace() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestResolveImpulseRefFromEnv(t *testing.T) {
	t.Run("missing env returns nil", func(t *testing.T) {
		t.Setenv(contracts.ImpulseNameEnv, "")
		t.Setenv(contracts.ImpulseNamespaceEnv, "")
		if ref := resolveImpulseRefFromEnv(); ref != nil {
			t.Fatalf("expected nil impulse ref, got %+v", ref)
		}
	})

	t.Run("name only populates reference", func(t *testing.T) {
		t.Setenv(contracts.ImpulseNameEnv, "impulse-a")
		t.Setenv(contracts.ImpulseNamespaceEnv, "")
		ref := resolveImpulseRefFromEnv()
		if ref == nil {
			t.Fatal("expected impulse ref, got nil")
		}
		if ref.Name != "impulse-a" {
			t.Fatalf("expected impulse name impulse-a, got %q", ref.Name)
		}
		if ref.Namespace != nil {
			t.Fatalf("expected nil namespace, got %q", *ref.Namespace)
		}
	})

	t.Run("name and namespace populated", func(t *testing.T) {
		t.Setenv(contracts.ImpulseNameEnv, "impulse-b")
		t.Setenv(contracts.ImpulseNamespaceEnv, "impulse-ns")
		ref := resolveImpulseRefFromEnv()
		if ref == nil {
			t.Fatal("expected impulse ref, got nil")
		}
		if ref.Name != "impulse-b" {
			t.Fatalf("expected impulse name impulse-b, got %q", ref.Name)
		}
		if ref.Namespace == nil || *ref.Namespace != "impulse-ns" {
			t.Fatalf("expected namespace impulse-ns, got %#v", ref.Namespace)
		}
	})
}

func TestWithTriggerToken_AllowsNilContextAndStoresToken(t *testing.T) {
	ctx := WithTriggerToken(nil, "token-123") //nolint:staticcheck
	if ctx == nil {
		t.Fatal("expected context when attaching token to nil context")
	}
	if got := TriggerTokenFromContext(ctx); got != "token-123" {
		t.Fatalf("TriggerTokenFromContext() = %q, want %q", got, "token-123")
	}
}

func TestWithTriggerToken_EmptyTokenPreservesNilContext(t *testing.T) {
	if got := WithTriggerToken(nil, ""); got != nil { //nolint:staticcheck
		t.Fatalf("expected nil context passthrough for empty token, got %#v", got)
	}
}

func TestInit(t *testing.T) {
	// Test that the init function sets up the scheme properly
	// We can't directly test init(), but we can verify the scheme variable is not nil
	// and contains our custom resource types.
	if scheme == nil {
		t.Fatal("Scheme should be initialized")
	}

	gvk := runsv1alpha1.GroupVersion.WithKind("StoryRun")
	if !scheme.Recognizes(gvk) {
		t.Errorf("Scheme does not recognize StoryRun GVK: %v", gvk)
	}
}

func TestClientStructure(t *testing.T) {
	// Verify the Client struct can be created
	client := &Client{
		namespace: "test",
	}

	if client.namespace != "test" {
		t.Errorf("Client.namespace = %v, want test", client.namespace)
	}
}

func TestDeriveStoryRunName(t *testing.T) {
	tests := []struct {
		name           string
		storyNamespace string
		storyName      string
		token          string
	}{
		{
			name:      "simple alphanumeric token",
			storyName: "workflow",
			token:     "abc123",
		},
		{
			name:      "token with invalid characters gets sanitized",
			storyName: "workflow",
			token:     "Order 42 / Completed",
		},
		{
			name:      "token differing only by case still gets unique suffix",
			storyName: "workflow",
			token:     "Order-42",
		},
		{
			name:      "long token falls back to hash suffix",
			storyName: "workflow",
			token:     strings.Repeat("a", 300),
		},
		{
			name:      "token with only invalid characters falls back to hash",
			storyName: "workflow",
			token:     "!!!",
		},
		{
			name:      "long story name still respects metadata length",
			storyName: strings.Repeat("b", 250),
			token:     "token",
		},
		{
			name:           "cross-namespace tokens always include hash",
			storyNamespace: "team-a",
			storyName:      "workflow",
			token:          "deploy-42",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveStoryRunName(tc.storyNamespace, tc.storyName, tc.token)
			want := runsidentity.DeriveStoryRunName(tc.storyNamespace, tc.storyName, tc.token)
			if got != want {
				t.Fatalf("deriveStoryRunName() = %q, want %q", got, want)
			}
			if len(got) > metadataNameMaxLength {
				t.Fatalf("deriveStoryRunName() length = %d, exceeds %d", len(got), metadataNameMaxLength)
			}
		})
	}
}

func TestDeriveStorageInputKeyUsesInputHash(t *testing.T) {
	storyNamespace := "team-a"
	storyName := "workflow"
	token := "token-123"

	inputsA := map[string]any{"key": "value"}
	fingerprintA := computeInputFingerprint(inputsA)
	keyOne := deriveStorageInputKey(storyNamespace, storyName, token, fingerprintA)
	keyTwo := deriveStorageInputKey(storyNamespace, storyName, token, fingerprintA)
	if keyOne != keyTwo {
		t.Fatalf("expected identical keys for identical inputs, got %q vs %q", keyOne, keyTwo)
	}

	inputsB := map[string]any{"key": "different"}
	fingerprintB := computeInputFingerprint(inputsB)
	if fingerprintB == fingerprintA {
		t.Fatalf("fingerprints should differ for different payloads")
	}
	keyThree := deriveStorageInputKey(storyNamespace, storyName, token, fingerprintB)
	if keyThree == keyOne {
		t.Fatalf("storage key should change when input fingerprint changes (got %q)", keyThree)
	}
}

func TestComputeInputFingerprintDeterministic(t *testing.T) {
	inputOne := map[string]any{
		"alpha": "a",
		"beta":  []any{map[string]any{"nested": 1}, "value"},
	}
	inputTwo := map[string]any{
		"beta":  []any{map[string]any{"nested": 1}, "value"},
		"alpha": "a",
	}

	hashOne := computeInputFingerprint(inputOne)
	hashTwo := computeInputFingerprint(inputTwo)
	if hashOne == "" || hashTwo == "" {
		t.Fatalf("expected non-empty fingerprints (hashOne=%q, hashTwo=%q)", hashOne, hashTwo)
	}
	if hashOne != hashTwo {
		t.Fatalf("expected deterministic fingerprint, got %q vs %q", hashOne, hashTwo)
	}
}

func TestStoryTriggerRequestMatchesNamespace(t *testing.T) {
	payload := []byte(`{"key":"value"}`)
	buildTrigger := func(ns string) *runsv1alpha1.StoryTrigger {
		storyRef := refs.StoryReference{
			ObjectReference: refs.ObjectReference{Name: "workflow"},
		}
		if ns != "" {
			storyRef.Namespace = &ns
		}
		return &runsv1alpha1.StoryTrigger{
			Spec: runsv1alpha1.StoryTriggerSpec{
				StoryRef: storyRef,
				Inputs:   &k8sruntime.RawExtension{Raw: payload},
				DeliveryIdentity: runsv1alpha1.TriggerDeliveryIdentity{
					SubmissionID: "submission",
				},
			},
		}
	}

	a := buildTrigger("team-a")
	b := buildTrigger("team-b")
	defaultNs := buildTrigger("")

	if !storyTriggerRequestMatches(a, buildTrigger("team-a")) {
		t.Fatalf("expected StoryTriggers in the same namespace to match")
	}
	if storyTriggerRequestMatches(a, b) {
		t.Fatalf("expected StoryTriggers in different namespaces not to match")
	}
	if storyTriggerRequestMatches(a, defaultNs) {
		t.Fatalf("expected StoryTriggers with nil namespace pointers not to match explicit namespaces")
	}
}

func TestTriggerStory(t *testing.T) {
	fakeClient := newStoryTriggerResolvingFakeClient()
	c := &Client{
		Client:    fakeClient,
		namespace: "test-ns",
	}

	ctx := context.Background()
	storyName := "my-story"
	inputs := map[string]any{
		"param1": "value1",
		"param2": 123,
	}

	storyRun, err := c.TriggerStory(ctx, storyName, "", inputs)
	if err != nil {
		t.Fatalf("TriggerStory() failed: %v", err)
	}

	if storyRun == nil {
		t.Fatal("TriggerStory() returned nil storyRun")
	}

	createdTrigger := singleStoryTrigger(t, fakeClient, "test-ns")
	if createdTrigger.Namespace != "test-ns" { //nolint:goconst
		t.Errorf("StoryTrigger namespace = %v, want 'test-ns'", createdTrigger.Namespace)
	}
	if createdTrigger.Spec.StoryRef.Name != storyName {
		t.Errorf("StoryTrigger storyRef name = %v, want '%s'", createdTrigger.Spec.StoryRef.Name, storyName)
	}

	var createdInputs map[string]any
	if err := json.Unmarshal(createdTrigger.Spec.Inputs.Raw, &createdInputs); err != nil {
		t.Fatalf("Failed to unmarshal inputs from created StoryTrigger: %v", err)
	}

	expectedInputs := map[string]any{
		"param1": "value1",
		"param2": float64(123),
	}
	if !reflect.DeepEqual(createdInputs, expectedInputs) {
		t.Errorf("StoryTrigger inputs = %v, want %v", createdInputs, expectedInputs)
	}

	createdStoryRun := &runsv1alpha1.StoryRun{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: storyRun.Name, Namespace: storyRun.Namespace}, createdStoryRun)
	if err != nil {
		t.Fatalf("Failed to get created StoryRun: %v", err)
	}
	if createdStoryRun.Spec.StoryRef.Name != storyName {
		t.Errorf("StoryRun storyRef name = %v, want '%s'", createdStoryRun.Spec.StoryRef.Name, storyName)
	}
}

func TestTriggerStoryIncludesImpulseRef(t *testing.T) {
	t.Setenv(contracts.ImpulseNameEnv, "live-impulse")
	t.Setenv(contracts.ImpulseNamespaceEnv, "impulse-ns")

	fakeClient := newStoryTriggerResolvingFakeClient()
	c := &Client{
		Client:    fakeClient,
		namespace: "stories",
	}

	ctx := context.Background()
	storyRun, err := c.TriggerStory(ctx, "impulse-story", "", map[string]any{})
	if err != nil {
		t.Fatalf("TriggerStory() failed: %v", err)
	}
	if storyRun == nil {
		t.Fatal("TriggerStory() returned nil storyRun")
	}

	created := singleStoryTrigger(t, fakeClient, "stories")
	if created.Spec.ImpulseRef == nil {
		t.Fatal("expected impulseRef populated on StoryTrigger")
	}
	if created.Spec.ImpulseRef.Name != "live-impulse" {
		t.Fatalf("expected impulse name live-impulse, got %q", created.Spec.ImpulseRef.Name)
	}
	if created.Spec.ImpulseRef.Namespace == nil || *created.Spec.ImpulseRef.Namespace != "impulse-ns" {
		t.Fatalf("expected impulse namespace impulse-ns, got %#v", created.Spec.ImpulseRef.Namespace)
	}
}

func TestTriggerStoryDeterministicStorageKey(t *testing.T) {
	t.Setenv(contracts.StorageProviderEnv, "file")
	storageDir := t.TempDir()
	t.Setenv(contracts.StoragePathEnv, storageDir)
	t.Setenv(contracts.MaxInlineSizeEnv, "1")

	fakeClient := newStoryTriggerResolvingFakeClient()
	c := &Client{
		Client:    fakeClient,
		namespace: "test-ns",
	}

	ctx := WithTriggerToken(context.Background(), "consistent-token")
	storyName := "deterministic-story"
	inputs := map[string]any{
		"payload": strings.Repeat("x", 2048),
	}

	first, err := c.TriggerStory(ctx, storyName, "", inputs)
	if err != nil {
		t.Fatalf("first TriggerStory() call failed: %v", err)
	}
	second, err := c.TriggerStory(ctx, storyName, "", inputs)
	if err != nil {
		t.Fatalf("second TriggerStory() call failed: %v", err)
	}

	ref1 := storageRefFromStoryRun(t, first)
	ref2 := storageRefFromStoryRun(t, second)

	if ref1 != ref2 {
		t.Fatalf("storage reference mismatch between idempotent triggers: %q != %q", ref1, ref2)
	}

	if _, err := os.Stat(filepath.Join(storageDir, ref1)); err != nil {
		t.Fatalf("expected offloaded inputs at %s: %v", filepath.Join(storageDir, ref1), err)
	}
}

func TestTriggerStoryTokenMismatchErrors(t *testing.T) {
	fakeClient := newStoryTriggerResolvingFakeClient()
	c := &Client{
		Client:    fakeClient,
		namespace: "test-ns",
	}

	ctx := WithTriggerToken(context.Background(), "consistent-token")
	storyName := "idempotent-story"
	if _, err := c.TriggerStory(ctx, storyName, "", map[string]any{"value": "first"}); err != nil {
		t.Fatalf("initial TriggerStory() call failed: %v", err)
	}

	if _, err := c.TriggerStory(ctx, storyName, "", map[string]any{"value": "second"}); err == nil {
		t.Fatal("expected error when reusing trigger token with different inputs")
	}
}

func TestTriggerStoryContextTokenOverridesEnv(t *testing.T) {
	t.Setenv(contracts.TriggerTokenEnv, "env-token")

	fakeClient := newStoryTriggerResolvingFakeClient()
	c := &Client{
		Client:    fakeClient,
		namespace: "token-ns",
	}

	ctx := WithTriggerToken(context.Background(), "ctx-token")
	storyName := "token-story"
	sr, err := c.TriggerStory(ctx, storyName, "", map[string]any{})
	if err != nil {
		t.Fatalf("TriggerStory() error = %v", err)
	}
	expected := deriveStoryRunName("token-ns", storyName, "ctx-token")
	if sr == nil || sr.Name != expected {
		t.Fatalf("expected StoryRun name %q, got %+v", expected, sr)
	}
	created := singleStoryTrigger(t, fakeClient, "token-ns")
	inputBytes, _ := json.Marshal(map[string]any{})
	expectedHash, err := runsidentity.ComputeTriggerInputHash(inputBytes)
	if err != nil {
		t.Fatalf("failed to compute trigger input hash: %v", err)
	}
	if created.Spec.DeliveryIdentity.Mode == nil || *created.Spec.DeliveryIdentity.Mode != bobrapetv1alpha1.TriggerDedupeToken { //nolint:lll
		t.Fatalf("expected token delivery identity, got %#v", created.Spec.DeliveryIdentity.Mode)
	}
	if got := created.Spec.DeliveryIdentity.Key; got != "ctx-token" {
		t.Fatalf("expected delivery identity key ctx-token, got %q", got)
	}
	if got := created.Spec.DeliveryIdentity.SubmissionID; got != "ctx-token" {
		t.Fatalf("expected submissionID ctx-token, got %q", got)
	}
	if got := created.Spec.DeliveryIdentity.InputHash; got != expectedHash {
		t.Fatalf("expected trigger input hash %q, got %q", expectedHash, got)
	}
}

func TestTriggerStoryNilInputsUsesEmptyObject(t *testing.T) {
	fakeClient := newStoryTriggerResolvingFakeClient()
	c := &Client{
		Client:    fakeClient,
		namespace: "token-ns",
	}

	ctx := WithTriggerToken(context.Background(), "ctx-token")
	storyName := "nil-inputs-story"
	sr, err := c.TriggerStory(ctx, storyName, "", nil)
	if err != nil {
		t.Fatalf("TriggerStory() error = %v", err)
	}
	expected := deriveStoryRunName("token-ns", storyName, "ctx-token")
	if sr == nil || sr.Name != expected {
		t.Fatalf("expected StoryRun name %q, got %+v", expected, sr)
	}

	created := singleStoryTrigger(t, fakeClient, "token-ns")
	if created.Spec.Inputs == nil {
		t.Fatalf("expected StoryTrigger inputs to be set")
	}
	if got := bytes.TrimSpace(created.Spec.Inputs.Raw); string(got) != "{}" {
		t.Fatalf("expected StoryTrigger inputs to be {}, got %q", string(got))
	}
	expectedHash, err := runsidentity.ComputeTriggerInputHash([]byte(`{}`))
	if err != nil {
		t.Fatalf("failed to compute trigger input hash: %v", err)
	}
	if got := created.Spec.DeliveryIdentity.InputHash; got != expectedHash {
		t.Fatalf("expected trigger input hash %q, got %q", expectedHash, got)
	}
}

func TestTriggerStoryDedupeKeyPolicy(t *testing.T) {
	t.Setenv(triggerDedupeModeEnv, "key")
	t.Setenv(triggerDedupeKeyTemplateEnv, "{{ inputs.eventId }}")

	fakeClient := newStoryTriggerResolvingFakeClient()
	c := &Client{
		Client:    fakeClient,
		namespace: "token-ns",
	}

	ctx := context.Background()
	storyName := "policy-story"
	inputs := map[string]any{"eventId": "evt-123"}
	sr, err := c.TriggerStory(ctx, storyName, "", inputs)
	if err != nil {
		t.Fatalf("TriggerStory() error = %v", err)
	}

	hash := sha256.Sum256([]byte("evt-123"))
	token := hex.EncodeToString(hash[:])
	expected := deriveStoryRunName("token-ns", storyName, token)
	if sr == nil || sr.Name != expected {
		t.Fatalf("expected StoryRun name %q, got %+v", expected, sr)
	}

	created := singleStoryTrigger(t, fakeClient, "token-ns")
	inputBytes, _ := json.Marshal(inputs)
	expectedHash, err := runsidentity.ComputeTriggerInputHash(inputBytes)
	if err != nil {
		t.Fatalf("failed to compute trigger input hash: %v", err)
	}
	if created.Spec.DeliveryIdentity.Mode == nil || *created.Spec.DeliveryIdentity.Mode != bobrapetv1alpha1.TriggerDedupeKey { //nolint:lll
		t.Fatalf("expected key delivery identity, got %#v", created.Spec.DeliveryIdentity.Mode)
	}
	if got := created.Spec.DeliveryIdentity.Key; got != token {
		t.Fatalf("expected delivery identity key %q, got %q", token, got)
	}
	if got := created.Spec.DeliveryIdentity.SubmissionID; got != token {
		t.Fatalf("expected submissionID %q, got %q", token, got)
	}
	if got := created.Spec.DeliveryIdentity.InputHash; got != expectedHash {
		t.Fatalf("expected trigger input hash %q, got %q", expectedHash, got)
	}
}

func TestTriggerStoryTokenPolicyRequiresToken(t *testing.T) {
	t.Setenv(triggerDedupeModeEnv, "token")

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&runsv1alpha1.StepRun{}).Build()
	c := &Client{
		Client:    fakeClient,
		namespace: "token-ns",
	}

	if _, err := c.TriggerStory(context.Background(), "token-policy-story", "", map[string]any{}); err == nil {
		t.Fatal("expected error when trigger token is required but missing")
	}
}

func TestTriggerStory_RetriesAlreadyExistsGetFailureAndSucceeds(t *testing.T) {
	t.Setenv(triggerRetryMaxAttemptsEnv, "2")
	t.Setenv(triggerRetryBackoffEnv, "constant")
	t.Setenv(triggerRetryBaseDelayEnv, "1ms")
	t.Setenv(triggerRetryMaxDelayEnv, "2ms")

	const namespace = "retry-ns"
	const storyName = "retry-story"
	inputs := map[string]any{"value": "stable"}
	inputBytes, err := json.Marshal(inputs)
	if err != nil {
		t.Fatalf("marshal inputs: %v", err)
	}
	ctx := WithTriggerToken(context.Background(), "retry-token")
	storyRunName := deriveStoryRunName(namespace, storyName, "retry-token")
	triggerName := runsidentity.DeriveStoryTriggerName(namespace, storyName, "retry-token", "retry-token")
	storyRefNamespace := namespace

	existingRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      storyRunName,
			Namespace: namespace,
			Annotations: map[string]string{
				runsidentity.StoryRunTriggerRequestNameAnnotation: triggerName,
				runsidentity.StoryRunTriggerTokenAnnotation:       "retry-token",
			},
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{Name: storyName},
			},
			Inputs: &k8sruntime.RawExtension{Raw: inputBytes},
		},
	}
	inputHash, err := runsidentity.ComputeTriggerInputHash(inputBytes)
	if err != nil {
		t.Fatalf("compute trigger input hash: %v", err)
	}
	existingRun.Annotations[runsidentity.StoryRunTriggerInputHashAnnotation] = inputHash

	existingTrigger := &runsv1alpha1.StoryTrigger{
		ObjectMeta: metav1.ObjectMeta{
			Name:      triggerName,
			Namespace: namespace,
		},
		Spec: runsv1alpha1.StoryTriggerSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{Name: storyName},
			},
			Inputs: &k8sruntime.RawExtension{Raw: inputBytes},
			DeliveryIdentity: runsv1alpha1.TriggerDeliveryIdentity{
				Mode:         ptr.To(bobrapetv1alpha1.TriggerDedupeToken),
				Key:          "retry-token",
				InputHash:    inputHash,
				SubmissionID: "retry-token",
			},
		},
		Status: runsv1alpha1.StoryTriggerStatus{
			Decision: runsv1alpha1.StoryTriggerDecisionCreated,
			StoryRunRef: &refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: storyRunName, Namespace: &storyRefNamespace},
			},
		},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StepRun{}, &runsv1alpha1.StoryRun{}, &runsv1alpha1.StoryTrigger{}).
		WithObjects(existingRun, existingTrigger).
		Build()
	retryingClient := &transientStoryTriggerGetClient{
		Client:       baseClient,
		triggerName:  triggerName,
		failuresLeft: 1,
	}
	c := &Client{
		Client:    retryingClient,
		namespace: namespace,
	}

	got, err := c.TriggerStory(ctx, storyName, "", inputs)
	if err != nil {
		t.Fatalf("TriggerStory() failed: %v", err)
	}
	if got == nil || got.Name != storyRunName {
		t.Fatalf("expected StoryRun %q, got %+v", storyRunName, got)
	}
	if retryingClient.failuresSeen != 1 {
		t.Fatalf("expected exactly 1 transient Get failure, saw %d", retryingClient.failuresSeen)
	}
}

func TestTriggerStory_RetriesTransientCreateWithoutToken(t *testing.T) {
	t.Setenv(triggerRetryMaxAttemptsEnv, "2")
	t.Setenv(triggerRetryBackoffEnv, "constant")
	t.Setenv(triggerRetryBaseDelayEnv, "1ms")
	t.Setenv(triggerRetryMaxDelayEnv, "2ms")

	baseClient := newStoryTriggerResolvingFakeClient()
	retryingClient := &transientCreateClient{
		Client:       baseClient,
		createErrors: []error{apierrors.NewTimeoutError("simulated transient create timeout", 1)},
	}
	c := &Client{
		Client:    retryingClient,
		namespace: "retry-ns",
	}

	got, err := c.TriggerStory(context.Background(), "untokened-story", "", map[string]any{"value": "once"})
	if err != nil {
		t.Fatalf("expected retry to recover for untokened StoryTrigger create, got %v", err)
	}
	if got == nil {
		t.Fatal("expected resolved StoryRun after retry")
	}
	if retryingClient.createCalls != 2 {
		t.Fatalf("expected untokened StoryTrigger create to retry once, got %d calls", retryingClient.createCalls)
	}
}

func TestTriggerStory_RetriesTransientCreateWhenTokenMakesRequestIdempotent(t *testing.T) {
	t.Setenv(triggerRetryMaxAttemptsEnv, "2")
	t.Setenv(triggerRetryBackoffEnv, "constant")
	t.Setenv(triggerRetryBaseDelayEnv, "1ms")
	t.Setenv(triggerRetryMaxDelayEnv, "2ms")

	const namespace = "retry-ns"
	const storyName = "tokened-retry-story"
	ctx := WithTriggerToken(context.Background(), "retry-token")

	baseClient := newStoryTriggerResolvingFakeClient()
	retryingClient := &transientCreateClient{
		Client:       baseClient,
		createErrors: []error{apierrors.NewTimeoutError("simulated transient create timeout", 1)},
	}
	c := &Client{
		Client:    retryingClient,
		namespace: namespace,
	}

	got, err := c.TriggerStory(ctx, storyName, "", map[string]any{"value": "stable"})
	if err != nil {
		t.Fatalf("expected retry to recover for idempotent create, got %v", err)
	}
	expectedName := deriveStoryRunName(namespace, storyName, "retry-token")
	if got == nil || got.Name != expectedName {
		t.Fatalf("expected StoryRun %q after retry, got %+v", expectedName, got)
	}
	if retryingClient.createCalls != 2 {
		t.Fatalf("expected tokened trigger create to retry once, got %d calls", retryingClient.createCalls)
	}

	var created runsv1alpha1.StoryRun //nolint:staticcheck
	//nolint:lll,staticcheck
	if err := retryingClient.Client.Get(ctx, types.NamespacedName{Name: expectedName, Namespace: namespace}, &created); err != nil {
		t.Fatalf("failed to fetch retried StoryRun: %v", err)
	}
}

func TestTriggerStory_RetryExhaustedOnAlreadyExistsGetFailure(t *testing.T) {
	t.Setenv(triggerRetryMaxAttemptsEnv, "2")
	t.Setenv(triggerRetryBackoffEnv, "constant")
	t.Setenv(triggerRetryBaseDelayEnv, "1ms")
	t.Setenv(triggerRetryMaxDelayEnv, "2ms")

	const namespace = "retry-ns"
	const storyName = "retry-story-exhausted"
	inputs := map[string]any{"value": "stable"}
	inputBytes, err := json.Marshal(inputs)
	if err != nil {
		t.Fatalf("marshal inputs: %v", err)
	}
	ctx := WithTriggerToken(context.Background(), "retry-token-exhausted")
	storyRunName := deriveStoryRunName(namespace, storyName, "retry-token-exhausted")
	triggerName := runsidentity.DeriveStoryTriggerName(namespace, storyName, "retry-token-exhausted", "retry-token-exhausted") //nolint:lll
	storyRefNamespace := namespace

	existingRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      storyRunName,
			Namespace: namespace,
			Annotations: map[string]string{
				runsidentity.StoryRunTriggerRequestNameAnnotation: triggerName,
				runsidentity.StoryRunTriggerTokenAnnotation:       "retry-token-exhausted",
			},
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{Name: storyName},
			},
			Inputs: &k8sruntime.RawExtension{Raw: inputBytes},
		},
	}
	inputHash, err := runsidentity.ComputeTriggerInputHash(inputBytes)
	if err != nil {
		t.Fatalf("compute trigger input hash: %v", err)
	}
	existingRun.Annotations[runsidentity.StoryRunTriggerInputHashAnnotation] = inputHash

	existingTrigger := &runsv1alpha1.StoryTrigger{
		ObjectMeta: metav1.ObjectMeta{
			Name:      triggerName,
			Namespace: namespace,
		},
		Spec: runsv1alpha1.StoryTriggerSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{Name: storyName},
			},
			Inputs: &k8sruntime.RawExtension{Raw: inputBytes},
			DeliveryIdentity: runsv1alpha1.TriggerDeliveryIdentity{
				Mode:         ptr.To(bobrapetv1alpha1.TriggerDedupeToken),
				Key:          "retry-token-exhausted",
				InputHash:    inputHash,
				SubmissionID: "retry-token-exhausted",
			},
		},
		Status: runsv1alpha1.StoryTriggerStatus{
			Decision: runsv1alpha1.StoryTriggerDecisionCreated,
			StoryRunRef: &refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: storyRunName, Namespace: &storyRefNamespace},
			},
		},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StepRun{}, &runsv1alpha1.StoryRun{}, &runsv1alpha1.StoryTrigger{}).
		WithObjects(existingRun, existingTrigger).
		Build()
	retryingClient := &transientStoryTriggerGetClient{
		Client:       baseClient,
		triggerName:  triggerName,
		failuresLeft: 2, // exhaust both attempts
	}
	c := &Client{
		Client:    retryingClient,
		namespace: namespace,
	}

	got, err := c.TriggerStory(ctx, storyName, "", inputs)
	if err == nil {
		t.Fatal("expected retry exhaustion error, got nil")
	}
	if got != nil {
		t.Fatalf("expected nil StoryRun on error, got %+v", got)
	}
	if !errors.Is(err, sdkerrors.ErrRetryable) {
		t.Fatalf("expected retryable error, got %v", err)
	}
	if retryingClient.failuresSeen != 2 {
		t.Fatalf("expected 2 transient Get failures, saw %d", retryingClient.failuresSeen)
	}
}

func TestStopStoryRun_RequestsGracefulCancelForRunningStoryRun(t *testing.T) {
	startedAt := metav1.NewTime(time.Now().Add(-95 * time.Second).UTC().Truncate(time.Second))
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storyrun-running",
			Namespace: "test-ns",
		},
		Status: runsv1alpha1.StoryRunStatus{
			Phase:     enums.PhaseRunning,
			StartedAt: &startedAt,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StoryRun{}).
		WithObjects(storyRun).
		Build()
	c := &Client{
		Client:    fakeClient,
		namespace: "test-ns",
	}

	if err := c.StopStoryRun(context.Background(), "storyrun-running", ""); err != nil {
		t.Fatalf("StopStoryRun() failed: %v", err)
	}

	var updated runsv1alpha1.StoryRun
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "storyrun-running",
		Namespace: "test-ns",
	}, &updated); err != nil {
		t.Fatalf("failed to get updated StoryRun: %v", err)
	}
	if updated.Status.Phase != enums.PhaseRunning {
		t.Fatalf("StoryRun phase = %s, want %s", updated.Status.Phase, enums.PhaseRunning)
	}
	if updated.Status.Message != "" {
		t.Fatalf("StoryRun message = %q, want empty", updated.Status.Message)
	}
	if updated.Status.FinishedAt != nil {
		t.Fatalf("expected FinishedAt to remain nil, got %#v", updated.Status.FinishedAt)
	}
	if updated.Status.StartedAt == nil || !updated.Status.StartedAt.Time.Equal(startedAt.Time) {
		t.Fatalf("StartedAt = %#v, want %s", updated.Status.StartedAt, startedAt.Time)
	}
	if updated.Status.Duration != "" {
		t.Fatalf("expected Duration to remain empty, got %q", updated.Status.Duration)
	}
	if updated.Spec.CancelRequested == nil || !*updated.Spec.CancelRequested {
		t.Fatalf("expected CancelRequested=true, got %#v", updated.Spec.CancelRequested)
	}
}

func TestStopStoryRun_TerminalPhaseNoop(t *testing.T) {
	finishedAt := metav1.NewTime(time.Now().Add(-5 * time.Second).UTC().Truncate(time.Second))
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storyrun-terminal",
			Namespace: "test-ns",
		},
		Status: runsv1alpha1.StoryRunStatus{
			Phase:      enums.PhaseSucceeded,
			Message:    "already finished",
			FinishedAt: &finishedAt,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StoryRun{}).
		WithObjects(storyRun).
		Build()
	c := &Client{
		Client:    fakeClient,
		namespace: "test-ns",
	}

	if err := c.StopStoryRun(context.Background(), "storyrun-terminal", ""); err != nil {
		t.Fatalf("StopStoryRun() failed: %v", err)
	}

	var updated runsv1alpha1.StoryRun
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "storyrun-terminal",
		Namespace: "test-ns",
	}, &updated); err != nil {
		t.Fatalf("failed to get updated StoryRun: %v", err)
	}
	if updated.Status.Phase != enums.PhaseSucceeded {
		t.Fatalf("StoryRun phase = %s, want %s", updated.Status.Phase, enums.PhaseSucceeded)
	}
	if updated.Status.Message != "already finished" {
		t.Fatalf("StoryRun message = %q, want %q", updated.Status.Message, "already finished")
	}
	if updated.Status.FinishedAt == nil || !updated.Status.FinishedAt.Time.Equal(finishedAt.Time) {
		t.Fatalf("FinishedAt = %#v, want %s", updated.Status.FinishedAt, finishedAt.Time)
	}
}

func TestStopStoryRun_RequestsGracefulCancelForPausedStoryRun(t *testing.T) {
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storyrun-paused",
			Namespace: "test-ns",
		},
		Status: runsv1alpha1.StoryRunStatus{
			Phase:   enums.PhasePaused,
			Message: "paused by controller",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StoryRun{}).
		WithObjects(storyRun).
		Build()
	c := &Client{
		Client:    fakeClient,
		namespace: "test-ns",
	}

	if err := c.StopStoryRun(context.Background(), "storyrun-paused", ""); err != nil {
		t.Fatalf("StopStoryRun() failed: %v", err)
	}

	var updated runsv1alpha1.StoryRun
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "storyrun-paused",
		Namespace: "test-ns",
	}, &updated); err != nil {
		t.Fatalf("failed to get StoryRun after unsupported stop attempt: %v", err)
	}
	if updated.Status.Phase != enums.PhasePaused {
		t.Fatalf("StoryRun phase = %s, want %s", updated.Status.Phase, enums.PhasePaused)
	}
	if updated.Status.Message != "paused by controller" {
		t.Fatalf("StoryRun message = %q, want %q", updated.Status.Message, "paused by controller")
	}
	if updated.Status.FinishedAt != nil {
		t.Fatalf("expected FinishedAt to remain nil, got %#v", updated.Status.FinishedAt)
	}
	if updated.Spec.CancelRequested == nil || !*updated.Spec.CancelRequested {
		t.Fatalf("expected CancelRequested=true, got %#v", updated.Spec.CancelRequested)
	}
}

func TestStopStoryRun_NotFound(t *testing.T) {
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StoryRun{}).
		Build()
	c := &Client{
		Client:    fakeClient,
		namespace: "test-ns",
	}

	err := c.StopStoryRun(context.Background(), "missing-storyrun", "")
	if err == nil {
		t.Fatal("expected not-found error")
	}
	if !errors.Is(err, sdkerrors.ErrNotFound) {
		t.Fatalf("expected errors.Is(err, ErrNotFound), got %v", err)
	}
}

func TestUpdateImpulseTriggerStats_AppliesDelta(t *testing.T) {
	lastTrigger := time.Date(2026, time.March, 27, 10, 0, 0, 0, time.FixedZone("UTC+2", 2*60*60))
	lastSuccess := lastTrigger.Add(2 * time.Minute)
	lastThrottled := lastTrigger.Add(3 * time.Minute)
	impulse := &bobrapetv1alpha1.Impulse{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-impulse",
			Namespace:  "test-ns",
			Generation: 7,
		},
		Status: bobrapetv1alpha1.ImpulseStatus{
			TriggersReceived:  10,
			StoriesLaunched:   3,
			FailedTriggers:    1,
			ThrottledTriggers: 2,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&bobrapetv1alpha1.Impulse{}).
		WithObjects(impulse).
		Build()
	c := &Client{
		Client:    fakeClient,
		namespace: "test-ns",
	}

	err := c.UpdateImpulseTriggerStats(context.Background(), "test-impulse", "", TriggerStatsDelta{
		TriggersReceived:  2,
		StoriesLaunched:   1,
		FailedTriggers:    1,
		ThrottledTriggers: 3,
		LastTrigger:       lastTrigger,
		LastSuccess:       &lastSuccess,
		LastThrottled:     &lastThrottled,
	})
	if err != nil {
		t.Fatalf("UpdateImpulseTriggerStats() failed: %v", err)
	}

	var updated bobrapetv1alpha1.Impulse
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "test-impulse",
		Namespace: "test-ns",
	}, &updated); err != nil {
		t.Fatalf("failed to get updated Impulse: %v", err)
	}
	if updated.Status.ObservedGeneration != 7 {
		t.Fatalf("ObservedGeneration = %d, want 7", updated.Status.ObservedGeneration)
	}
	if updated.Status.TriggersReceived != 12 {
		t.Fatalf("TriggersReceived = %d, want 12", updated.Status.TriggersReceived)
	}
	if updated.Status.StoriesLaunched != 4 {
		t.Fatalf("StoriesLaunched = %d, want 4", updated.Status.StoriesLaunched)
	}
	if updated.Status.FailedTriggers != 2 {
		t.Fatalf("FailedTriggers = %d, want 2", updated.Status.FailedTriggers)
	}
	if updated.Status.ThrottledTriggers != 5 {
		t.Fatalf("ThrottledTriggers = %d, want 5", updated.Status.ThrottledTriggers)
	}
	if updated.Status.LastTrigger == nil || !updated.Status.LastTrigger.Time.Equal(lastTrigger.UTC()) {
		t.Fatalf("LastTrigger = %#v, want %s", updated.Status.LastTrigger, lastTrigger.UTC())
	}
	if updated.Status.LastSuccess == nil || !updated.Status.LastSuccess.Time.Equal(lastSuccess.UTC()) {
		t.Fatalf("LastSuccess = %#v, want %s", updated.Status.LastSuccess, lastSuccess.UTC())
	}
	if updated.Status.LastThrottled == nil || !updated.Status.LastThrottled.Time.Equal(lastThrottled.UTC()) {
		t.Fatalf("LastThrottled = %#v, want %s", updated.Status.LastThrottled, lastThrottled.UTC())
	}
}

func TestUpdateImpulseTriggerStats_NilContextRejected(t *testing.T) {
	c := &Client{namespace: "test-ns"}
	err := c.UpdateImpulseTriggerStats(nil, "test-impulse", "", TriggerStatsDelta{ //nolint:staticcheck
		TriggersReceived: 1,
	})
	if err == nil {
		t.Fatal("expected nil context to be rejected")
	}
	if !strings.Contains(err.Error(), "context must not be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUpdateImpulseTriggerStats_EmptyImpulseNameRejected(t *testing.T) {
	c := &Client{namespace: "test-ns"}
	err := c.UpdateImpulseTriggerStats(context.Background(), " ", "", TriggerStatsDelta{
		TriggersReceived: 1,
	})
	if err == nil {
		t.Fatal("expected empty impulse name to be rejected")
	}
	if !strings.Contains(err.Error(), "impulse name is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUpdateImpulseTriggerStats_NegativeDeltaRejected(t *testing.T) {
	c := &Client{namespace: "test-ns"}
	err := c.UpdateImpulseTriggerStats(context.Background(), "test-impulse", "", TriggerStatsDelta{
		TriggersReceived: -1,
	})
	if err == nil {
		t.Fatal("expected negative delta to be rejected")
	}
	if !strings.Contains(err.Error(), "TriggersReceived must not be negative") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func storageRefFromStoryRun(t *testing.T, sr *runsv1alpha1.StoryRun) string {
	t.Helper()
	if sr == nil || sr.Spec.Inputs == nil {
		t.Fatalf("story run inputs missing: %+v", sr)
	}
	var spec map[string]any
	if err := json.Unmarshal(sr.Spec.Inputs.Raw, &spec); err != nil {
		t.Fatalf("failed to unmarshal story inputs: %v", err)
	}
	if raw, exists := spec["$bubuStorageRef"]; exists {
		ref, ok := raw.(string)
		if !ok {
			t.Fatalf("storage reference expected string, got %T", raw)
		}
		return ref
	}
	for _, v := range spec {
		if nested, ok := v.(map[string]any); ok {
			if raw, exists := nested["$bubuStorageRef"]; exists {
				ref, ok := raw.(string)
				if !ok {
					t.Fatalf("storage reference expected string, got %T", raw)
				}
				return ref
			}
		}
	}
	t.Fatalf("expected storage reference in inputs, got %v", spec)
	return ""
}

func TestPatchStepRunStatus(t *testing.T) {
	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-step-run",
			Namespace: "test-ns",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StepRun{}).
		WithObjects(stepRun).Build()

	c := &Client{
		Client:    fakeClient,
		namespace: "test-ns",
	}

	ctx := context.Background()
	patchData := runsv1alpha1.StepRunStatus{
		Phase: "Succeeded",
	}

	err := c.PatchStepRunStatus(ctx, "test-step-run", patchData)
	if err != nil {
		t.Fatalf("PatchStepRunStatus() failed: %v", err)
	}

	// Verify the patch
	updatedStepRun := &runsv1alpha1.StepRun{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: "test-step-run", Namespace: "test-ns"}, updatedStepRun)
	if err != nil {
		t.Fatalf("Failed to get updated StepRun: %v", err)
	}

	if updatedStepRun.Status.Phase != "Succeeded" {
		t.Errorf("StepRun status phase = %v, want 'Succeeded'", updatedStepRun.Status.Phase)
	}
}

func TestGetMaxPatchRetries_ClampsNegativeValues(t *testing.T) {
	t.Setenv(contracts.K8sPatchMaxRetriesEnv, "-3")
	if got := getMaxPatchRetries(); got != 0 {
		t.Fatalf("expected negative retry config to clamp to 0, got %d", got)
	}
}

func TestPatchStepRunStatus_NilContextRejected(t *testing.T) {
	c := &Client{}                                                                  //nolint:staticcheck
	err := c.PatchStepRunStatus(nil, "test-step-run", runsv1alpha1.StepRunStatus{}) //nolint:staticcheck
	if err == nil {
		t.Fatal("expected nil context to be rejected")
	}
	if !strings.Contains(err.Error(), "context must not be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPatchStepRunStatus_NegativeRetryConfigStillAttemptsInitialPatch(t *testing.T) {
	t.Setenv(contracts.K8sPatchMaxRetriesEnv, "-1")

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-step-run-negative-retries",
			Namespace: "test-ns",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StepRun{}).
		WithObjects(stepRun).Build()

	c := &Client{
		Client:    fakeClient,
		namespace: "test-ns",
	}

	err := c.PatchStepRunStatus(context.Background(), "test-step-run-negative-retries", runsv1alpha1.StepRunStatus{
		Phase: enums.PhaseSucceeded,
	})
	if err != nil {
		t.Fatalf("PatchStepRunStatus() failed: %v", err)
	}

	updatedStepRun := &runsv1alpha1.StepRun{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "test-step-run-negative-retries",
		Namespace: "test-ns",
	}, updatedStepRun)
	if err != nil {
		t.Fatalf("Failed to get updated StepRun: %v", err)
	}
	if updatedStepRun.Status.Phase != enums.PhaseSucceeded {
		t.Fatalf("StepRun status phase = %v, want %v", updatedStepRun.Status.Phase, enums.PhaseSucceeded)
	}
}

func TestPatchStepRunStatus_RetriesTransientGetFailure(t *testing.T) {
	t.Setenv(contracts.K8sPatchMaxRetriesEnv, "1")

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-step-run-transient-get",
			Namespace: "test-ns",
		},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StepRun{}).
		WithObjects(stepRun).
		Build()

	retryingClient := &transientGetClient{
		Client:    baseClient,
		getErrors: []error{apierrors.NewTimeoutError("temporary timeout", 1)},
	}
	c := &Client{
		Client:    retryingClient,
		namespace: "test-ns",
	}

	err := c.PatchStepRunStatus(context.Background(), "test-step-run-transient-get", runsv1alpha1.StepRunStatus{
		Phase: enums.PhaseSucceeded,
	})
	if err != nil {
		t.Fatalf("PatchStepRunStatus() failed after transient Get error: %v", err)
	}
	if retryingClient.getCalls != 2 {
		t.Fatalf("expected 2 Get calls, got %d", retryingClient.getCalls)
	}

	updatedStepRun := &runsv1alpha1.StepRun{}
	err = baseClient.Get(context.Background(), types.NamespacedName{
		Name:      "test-step-run-transient-get",
		Namespace: "test-ns",
	}, updatedStepRun)
	if err != nil {
		t.Fatalf("Failed to get updated StepRun: %v", err)
	}
	if updatedStepRun.Status.Phase != enums.PhaseSucceeded {
		t.Fatalf("StepRun status phase = %v, want %v", updatedStepRun.Status.Phase, enums.PhaseSucceeded)
	}
}

func TestPatchStepRunStatus_TransientGetFailureReturnsRetryableAfterExhaustion(t *testing.T) {
	t.Setenv(contracts.K8sPatchMaxRetriesEnv, "1")

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-step-run-transient-get-fail",
			Namespace: "test-ns",
		},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StepRun{}).
		WithObjects(stepRun).
		Build()

	retryingClient := &transientGetClient{
		Client: baseClient,
		getErrors: []error{
			apierrors.NewTimeoutError("temporary timeout", 1),
			apierrors.NewTimeoutError("temporary timeout again", 1),
		},
	}
	c := &Client{
		Client:    retryingClient,
		namespace: "test-ns",
	}

	err := c.PatchStepRunStatus(context.Background(), "test-step-run-transient-get-fail", runsv1alpha1.StepRunStatus{
		Phase: enums.PhaseSucceeded,
	})
	if err == nil {
		t.Fatal("expected transient Get error after retry exhaustion")
	}
	if !errors.Is(err, sdkerrors.ErrRetryable) {
		t.Fatalf("expected retryable error after retry exhaustion, got %v", err)
	}
	if retryingClient.getCalls != 2 {
		t.Fatalf("expected 2 Get calls, got %d", retryingClient.getCalls)
	}
}

func TestPatchStepRunStatus_CancelledContextAbortsRetryBackoff(t *testing.T) {
	t.Setenv(contracts.K8sPatchMaxRetriesEnv, "1")

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StepRun{}).
		Build()
	c := &Client{
		Client:    &timeoutGetClient{Client: baseClient},
		namespace: "test-ns",
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := c.PatchStepRunStatus(ctx, "test-step-run-cancel-backoff", runsv1alpha1.StepRunStatus{
		Phase: enums.PhaseRunning,
	})
	if err == nil {
		t.Fatal("expected context cancellation error during retry backoff")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled sentinel, got %v", err)
	}
	if !strings.Contains(err.Error(), "status patch retry aborted due to context cancellation") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPatchStepRunStatus_RetriesConflictOnStatusPatch(t *testing.T) {
	t.Setenv(contracts.K8sPatchMaxRetriesEnv, "1")

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-step-run-conflict-retry",
			Namespace: "test-ns",
		},
		Status: runsv1alpha1.StepRunStatus{
			Phase: enums.PhasePending,
		},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StepRun{}).
		WithObjects(stepRun).
		Build()

	conflictErr := apierrors.NewConflict(
		schema.GroupResource{Group: "runs.bubustack.io", Resource: "stepruns"},
		"test-step-run-conflict-retry",
		errors.New("simulated conflict"),
	)
	statusWriter := &transientStatusWriter{
		SubResourceWriter: baseClient.Status(),
		patchErrors:       []error{conflictErr},
	}
	retryingClient := &transientStatusPatchClient{
		Client:       baseClient,
		statusWriter: statusWriter,
	}
	c := &Client{
		Client:    retryingClient,
		namespace: "test-ns",
	}

	err := c.PatchStepRunStatus(context.Background(), "test-step-run-conflict-retry", runsv1alpha1.StepRunStatus{
		Phase: enums.PhaseRunning,
	})
	if err != nil {
		t.Fatalf("PatchStepRunStatus() failed after transient conflict: %v", err)
	}
	if statusWriter.patchCalls != 2 {
		t.Fatalf("expected 2 status patch calls, got %d", statusWriter.patchCalls)
	}

	updated := &runsv1alpha1.StepRun{}
	err = baseClient.Get(context.Background(), types.NamespacedName{
		Name:      "test-step-run-conflict-retry",
		Namespace: "test-ns",
	}, updated)
	if err != nil {
		t.Fatalf("Failed to get updated StepRun: %v", err)
	}
	if updated.Status.Phase != enums.PhaseRunning {
		t.Fatalf("StepRun status phase = %v, want %v", updated.Status.Phase, enums.PhaseRunning)
	}
}

func TestPatchStepRunStatus_ConflictReturnsErrConflictAfterExhaustion(t *testing.T) {
	t.Setenv(contracts.K8sPatchMaxRetriesEnv, "0")

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-step-run-conflict-exhaustion",
			Namespace: "test-ns",
		},
		Status: runsv1alpha1.StepRunStatus{
			Phase: enums.PhasePending,
		},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StepRun{}).
		WithObjects(stepRun).
		Build()

	conflictErr := apierrors.NewConflict(
		schema.GroupResource{Group: "runs.bubustack.io", Resource: "stepruns"},
		"test-step-run-conflict-exhaustion",
		errors.New("simulated conflict"),
	)
	statusWriter := &transientStatusWriter{
		SubResourceWriter: baseClient.Status(),
		patchErrors:       []error{conflictErr},
	}
	retryingClient := &transientStatusPatchClient{
		Client:       baseClient,
		statusWriter: statusWriter,
	}
	c := &Client{
		Client:    retryingClient,
		namespace: "test-ns",
	}

	err := c.PatchStepRunStatus(context.Background(), "test-step-run-conflict-exhaustion", runsv1alpha1.StepRunStatus{
		Phase: enums.PhaseRunning,
	})
	if err == nil {
		t.Fatal("expected conflict error after retry exhaustion")
	}
	if !errors.Is(err, sdkerrors.ErrConflict) {
		t.Fatalf("expected ErrConflict sentinel, got %v", err)
	}
	if statusWriter.patchCalls != 1 {
		t.Fatalf("expected 1 status patch call, got %d", statusWriter.patchCalls)
	}
}

func TestPatchStepRunStatus_ZombiePodRejected(t *testing.T) {
	t.Setenv(contracts.PodNameEnv, "old-pod")

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-step-run-zombie",
			Namespace: "test-ns",
		},
		Status: runsv1alpha1.StepRunStatus{
			PodName: "current-pod",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StepRun{}).
		WithObjects(stepRun).Build()

	c := &Client{
		Client:    fakeClient,
		namespace: "test-ns",
	}

	ctx := context.Background()
	err := c.PatchStepRunStatus(ctx, "test-step-run-zombie", runsv1alpha1.StepRunStatus{Phase: "Failed"})
	if err == nil {
		t.Fatal("expected zombie pod fencing to reject the patch, got nil error")
	}
	if !strings.Contains(err.Error(), "zombie pod fencing") {
		t.Errorf("expected 'zombie pod fencing' in error, got: %v", err)
	}
}

func TestIsValidPhaseTransition_AllowsRunningToTimeout(t *testing.T) {
	if !isValidPhaseTransition(enums.PhaseRunning, enums.PhaseTimeout) {
		t.Fatalf("expected Running → Timeout to be valid")
	}
}

func TestMergeStepRunStatus_AllowsTimeoutTransition(t *testing.T) {
	existing := &runsv1alpha1.StepRunStatus{Phase: enums.PhaseRunning}
	incoming := &runsv1alpha1.StepRunStatus{Phase: enums.PhaseTimeout}

	merged, err := mergeStepRunStatus(existing, incoming)
	if err != nil {
		t.Fatalf("mergeStepRunStatus() error = %v", err)
	}
	if merged.Phase != enums.PhaseTimeout {
		t.Fatalf("merged phase = %s, want %s", merged.Phase, enums.PhaseTimeout)
	}
}

func TestIsValidPhaseTransition_NewTransitions(t *testing.T) {
	tests := []struct {
		from, to enums.Phase
		want     bool
	}{
		{enums.PhasePending, enums.PhaseTimeout, true},
		{enums.PhaseRunning, enums.PhaseAborted, true},
		{enums.PhaseScheduling, enums.PhaseCanceled, true},
		{enums.PhaseBlocked, enums.PhaseCanceled, true},
		// Failed → Succeeded: SDK completed work after controller set Failed
		{enums.PhaseFailed, enums.PhaseSucceeded, true},
		// Failed → Running: controller retry
		{enums.PhaseFailed, enums.PhaseRunning, true},
		// Terminal states should not allow transitions
		{enums.PhaseFinished, enums.PhaseRunning, false},
		{enums.PhaseSkipped, enums.PhaseRunning, false},
	}
	for _, tt := range tests {
		got := isValidPhaseTransition(tt.from, tt.to)
		if got != tt.want {
			t.Errorf("isValidPhaseTransition(%v→%v) = %v, want %v", tt.from, tt.to, got, tt.want)
		}
	}
}

func TestPatchStepRunStatus_InvalidTransitionNoRetry(t *testing.T) {
	// StepRun is in Succeeded (terminal) — any further phase transition is invalid
	stepRun := &runsv1alpha1.StepRun{}
	stepRun.Name = "test-step-run-invalid-transition"
	stepRun.Namespace = "test-ns"
	stepRun.Status.Phase = enums.PhaseSucceeded

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StepRun{}).
		WithObjects(stepRun).
		Build()
	c := &Client{Client: fakeClient, namespace: "test-ns"}

	err := c.PatchStepRunStatus(context.Background(), "test-step-run-invalid-transition",
		runsv1alpha1.StepRunStatus{Phase: enums.PhaseRunning})
	if err == nil {
		t.Fatal("expected ErrInvalidTransition, got nil")
	}
	if !errors.Is(err, sdkerrors.ErrInvalidTransition) {
		t.Errorf("expected errors.Is(err, ErrInvalidTransition), got: %v", err)
	}
}

func TestMergeStepRunStatus_AllowsFailedToSucceeded(t *testing.T) {
	existing := &runsv1alpha1.StepRunStatus{Phase: enums.PhaseFailed}
	incoming := &runsv1alpha1.StepRunStatus{Phase: enums.PhaseSucceeded}

	merged, err := mergeStepRunStatus(existing, incoming)
	if err != nil {
		t.Fatalf("mergeStepRunStatus(Failed→Succeeded) error = %v", err)
	}
	if merged.Phase != enums.PhaseSucceeded {
		t.Fatalf("merged phase = %s, want %s", merged.Phase, enums.PhaseSucceeded)
	}
}

func TestPatchStepRunStatus_FailedToSucceeded(t *testing.T) {
	stepRun := &runsv1alpha1.StepRun{}
	stepRun.Name = "test-step-run-late-success"
	stepRun.Namespace = "test-ns"
	stepRun.Status.Phase = enums.PhaseFailed

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StepRun{}).
		WithObjects(stepRun).
		Build()
	c := &Client{Client: fakeClient, namespace: "test-ns"}

	err := c.PatchStepRunStatus(context.Background(), "test-step-run-late-success",
		runsv1alpha1.StepRunStatus{Phase: enums.PhaseSucceeded})
	if err != nil {
		t.Fatalf("PatchStepRunStatus(Failed→Succeeded) should succeed, got: %v", err)
	}

	var updated runsv1alpha1.StepRun
	if err := fakeClient.Get(context.Background(),
		types.NamespacedName{Name: "test-step-run-late-success", Namespace: "test-ns"},
		&updated); err != nil {
		t.Fatalf("failed to get updated StepRun: %v", err)
	}
	if updated.Status.Phase != enums.PhaseSucceeded {
		t.Fatalf("expected phase Succeeded after patch, got %s", updated.Status.Phase)
	}
}

func TestMergeStepRunStatus_PreservesFieldsWhenIncomingOmitsThem(t *testing.T) {
	existing := &runsv1alpha1.StepRunStatus{
		Output: &k8sruntime.RawExtension{Raw: []byte(`"old"`)},
		Logs:   &k8sruntime.RawExtension{Raw: []byte(`"logs"`)},
		Error:  &runsv1alpha1.StructuredError{Message: "err"},
		Needs:  []string{"prior-step"},
	}
	incoming := &runsv1alpha1.StepRunStatus{}

	merged, err := mergeStepRunStatus(existing, incoming)
	if err != nil {
		t.Fatalf("mergeStepRunStatus() error = %v", err)
	}
	if merged.Output == nil || string(merged.Output.Raw) != `"old"` {
		t.Fatalf("expected Output to be preserved, got %v", merged.Output)
	}
	if merged.Logs == nil || string(merged.Logs.Raw) != `"logs"` {
		t.Fatalf("expected Logs to be preserved, got %v", merged.Logs)
	}
	if merged.Error == nil || merged.Error.Message != "err" {
		t.Fatalf("expected Error to be preserved, got %v", merged.Error)
	}
	if !reflect.DeepEqual(merged.Needs, []string{"prior-step"}) {
		t.Fatalf("expected Needs to be preserved, got %v", merged.Needs)
	}
}

func TestMergeStepRunStatus_ExplicitEmptyNeedsClearsExistingNeeds(t *testing.T) {
	existing := &runsv1alpha1.StepRunStatus{
		Needs: []string{"prior-step"},
	}
	incoming := &runsv1alpha1.StepRunStatus{
		Needs: []string{},
	}

	merged, err := mergeStepRunStatus(existing, incoming)
	if err != nil {
		t.Fatalf("mergeStepRunStatus() error = %v", err)
	}
	if merged.Needs == nil {
		t.Fatal("expected explicit empty Needs slice to be preserved, got nil")
	}
	if len(merged.Needs) != 0 {
		t.Fatalf("expected Needs to be cleared, got %v", merged.Needs)
	}
}

func TestPatchStepRunStatus_PreservesNeedsWhenIncomingOmitsThem(t *testing.T) {
	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-step-run-preserve-needs",
			Namespace: "test-ns",
		},
		Status: runsv1alpha1.StepRunStatus{
			Phase: enums.PhasePending,
			Needs: []string{"prior-step"},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&runsv1alpha1.StepRun{}).
		WithObjects(stepRun).
		Build()
	c := &Client{
		Client:    fakeClient,
		namespace: "test-ns",
	}

	err := c.PatchStepRunStatus(context.Background(), "test-step-run-preserve-needs", runsv1alpha1.StepRunStatus{
		Phase: enums.PhaseRunning,
	})
	if err != nil {
		t.Fatalf("PatchStepRunStatus() failed: %v", err)
	}

	updated := &runsv1alpha1.StepRun{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      "test-step-run-preserve-needs",
		Namespace: "test-ns",
	}, updated)
	if err != nil {
		t.Fatalf("Failed to get updated StepRun: %v", err)
	}
	if updated.Status.Phase != enums.PhaseRunning {
		t.Fatalf("StepRun status phase = %v, want %v", updated.Status.Phase, enums.PhaseRunning)
	}
	if !reflect.DeepEqual(updated.Status.Needs, []string{"prior-step"}) {
		t.Fatalf("expected Needs to be preserved, got %v", updated.Status.Needs)
	}
}

func TestMergeStepRunStatus_DedupesEffectsByKeyWhenSeqZero(t *testing.T) {
	existing := &runsv1alpha1.StepRunStatus{
		Effects: []runsv1alpha1.EffectRecord{
			{Seq: 1, Key: "effect-1", Status: "succeeded"},
		},
	}
	incoming := &runsv1alpha1.StepRunStatus{
		Effects: []runsv1alpha1.EffectRecord{
			{Seq: 0, Key: "effect-1", Status: "succeeded"},
			{Seq: 0, Key: "effect-2", Status: "succeeded"},
		},
	}

	merged, err := mergeStepRunStatus(existing, incoming)
	if err != nil {
		t.Fatalf("mergeStepRunStatus() error = %v", err)
	}
	if len(merged.Effects) != 2 {
		t.Fatalf("expected 2 effects after merge, got %d", len(merged.Effects))
	}
	if merged.Effects[0].Key != "effect-1" {
		t.Fatalf("expected existing effect to remain first, got %q", merged.Effects[0].Key)
	}
	if merged.Effects[0].Seq != 1 {
		t.Fatalf("expected existing effect seq to remain 1, got %d", merged.Effects[0].Seq)
	}
	if merged.Effects[1].Key != "effect-2" {
		t.Fatalf("expected new effect to be appended, got %q", merged.Effects[1].Key)
	}
}

func TestClassifyK8sError(t *testing.T) {
	conflict := apierrors.NewConflict(
		schema.GroupResource{Group: "runs.bubustack.io", Resource: "storyruns"},
		"example",
		errors.New("conflict"),
	)
	if err := classifyK8sError(conflict); !errors.Is(err, sdkerrors.ErrConflict) {
		t.Fatalf("expected ErrConflict sentinel, got %v", err)
	}

	timeout := apierrors.NewTimeoutError("timed out", 1)
	if err := classifyK8sError(timeout); !errors.Is(err, sdkerrors.ErrRetryable) {
		t.Fatalf("expected ErrRetryable for timeout, got %v", err)
	}

	if err := classifyK8sError(nil); err != nil {
		t.Fatalf("expected nil passthrough, got %v", err)
	}
}

// Note: NewClient, TriggerStory, and PatchStepRunStatus require a real/fake Kubernetes cluster
// and are better tested via integration tests in the operator repository.
// These functions involve:
// - Kubernetes API server communication
// - Custom Resource Definitions (CRDs)
// - RBAC permissions
// - In-cluster or kubeconfig authentication
//
// Testing them with fakes doesn't provide meaningful coverage as the fake client
// doesn't accurately represent real Kubernetes behavior.
