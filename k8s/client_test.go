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
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/contracts"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	sdkerrors "github.com/bubustack/bubu-sdk-go/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

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
			name:     "Should fallback to default if no env vars are set",
			envVars:  map[string]string{},
			expected: "default",
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
			// Clean up env vars before each test
			os.Clearenv()

			for key, value := range tc.envVars {
				err := os.Setenv(key, value)
				if err != nil {
					t.Fatal(err)
				}
			}

			// Defer cleanup to after the test
			defer func() {
				for key := range tc.envVars {
					err := os.Unsetenv(key)
					if err != nil {
						t.Fatal(err)
					}
				}
			}()

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
	longToken := strings.Repeat("a", 300)
	longStory := strings.Repeat("b", 250)

	tests := []struct {
		name           string
		storyNamespace string
		storyName      string
		token          string
		expected       string
		suffixCheck    func(string) bool
	}{
		{
			name:      "simple alphanumeric token",
			storyName: "workflow",
			token:     "abc123",
			expected:  "workflow-abc123",
		},
		{
			name:      "token with invalid characters gets sanitized",
			storyName: "workflow",
			token:     "Order 42 / Completed",
			expected:  "workflow-order-42-completed-" + shortTokenHash("/workflow/Order 42 / Completed"),
		},
		{
			name:      "token differing only by case still gets unique suffix",
			storyName: "workflow",
			token:     "Order-42",
			expected:  "workflow-order-42-" + shortTokenHash("/workflow/Order-42"),
		},
		{
			name:      "long token falls back to hash suffix",
			storyName: "workflow",
			token:     longToken,
			expected:  "workflow-" + shortTokenHash("/workflow/"+longToken),
		},
		{
			name:      "token with only invalid characters falls back to hash",
			storyName: "workflow",
			token:     "!!!",
			expected:  "workflow-" + shortTokenHash("/workflow/!!!"),
		},
		{
			name:      "long story name trims prefix but keeps hash suffix",
			storyName: longStory,
			token:     "token",
			suffixCheck: func(got string) bool {
				wantSuffix := shortTokenHash("/" + longStory + "/token")
				return len(got) <= metadataNameMaxLength && strings.HasSuffix(got, wantSuffix)
			},
		},
		{
			name:           "cross-namespace tokens always include hash",
			storyNamespace: "team-a",
			storyName:      "workflow",
			token:          "deploy-42",
			expected:       "workflow-deploy-42-" + shortTokenHash("team-a/workflow/deploy-42"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveStoryRunName(tc.storyNamespace, tc.storyName, tc.token)
			if tc.suffixCheck != nil {
				if !tc.suffixCheck(got) {
					t.Fatalf("deriveStoryRunName() = %q failed suffix check", got)
				}
				return
			}
			if got != tc.expected {
				t.Fatalf("deriveStoryRunName() = %q, want %q", got, tc.expected)
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

func TestStoryRunRequestMatchesNamespace(t *testing.T) {
	payload := []byte(`{"key":"value"}`)
	buildRun := func(ns string) *runsv1alpha1.StoryRun {
		storyRef := refs.StoryReference{
			ObjectReference: refs.ObjectReference{Name: "workflow"},
		}
		if ns != "" {
			storyRef.Namespace = &ns
		}
		return &runsv1alpha1.StoryRun{
			Spec: runsv1alpha1.StoryRunSpec{
				StoryRef: storyRef,
				Inputs:   &k8sruntime.RawExtension{Raw: payload},
			},
		}
	}

	a := buildRun("team-a")
	b := buildRun("team-b")
	defaultNs := buildRun("")

	if !storyRunRequestMatches(a, buildRun("team-a")) {
		t.Fatalf("expected StoryRuns in the same namespace to match")
	}
	if storyRunRequestMatches(a, b) {
		t.Fatalf("expected StoryRuns in different namespaces not to match")
	}
	if storyRunRequestMatches(a, defaultNs) {
		t.Fatalf("expected StoryRuns with nil namespace pointers not to match explicit namespaces")
	}
}

func TestTriggerStory(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&runsv1alpha1.StepRun{}).Build()
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

	// Verify the created StoryRun
	createdStoryRun := &runsv1alpha1.StoryRun{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: storyRun.Name, Namespace: storyRun.Namespace}, createdStoryRun)
	if err != nil {
		t.Fatalf("Failed to get created StoryRun: %v", err)
	}

	if createdStoryRun.Namespace != "test-ns" {
		t.Errorf("StoryRun namespace = %v, want 'test-ns'", createdStoryRun.Namespace)
	}
	if createdStoryRun.Spec.StoryRef.Name != storyName {
		t.Errorf("StoryRun storyRef name = %v, want '%s'", createdStoryRun.Spec.StoryRef.Name, storyName)
	}

	var createdInputs map[string]any
	if err := json.Unmarshal(createdStoryRun.Spec.Inputs.Raw, &createdInputs); err != nil {
		t.Fatalf("Failed to unmarshal inputs from created StoryRun: %v", err)
	}

	// JSON unmarshaling converts numbers to float64
	expectedInputs := map[string]any{
		"param1": "value1",
		"param2": float64(123),
	}
	if !reflect.DeepEqual(createdInputs, expectedInputs) {
		t.Errorf("StoryRun inputs = %v, want %v", createdInputs, expectedInputs)
	}
}

func TestTriggerStoryIncludesImpulseRef(t *testing.T) {
	t.Setenv(contracts.ImpulseNameEnv, "live-impulse")
	t.Setenv(contracts.ImpulseNamespaceEnv, "impulse-ns")

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&runsv1alpha1.StepRun{}).Build()
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

	var created runsv1alpha1.StoryRun
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: storyRun.Name, Namespace: storyRun.Namespace}, &created); err != nil {
		t.Fatalf("failed to fetch storyrun: %v", err)
	}
	if created.Spec.ImpulseRef == nil {
		t.Fatal("expected impulseRef populated on StoryRun")
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

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&runsv1alpha1.StepRun{}).Build()
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
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&runsv1alpha1.StepRun{}).Build()
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

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&runsv1alpha1.StepRun{}).Build()
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
	expected := deriveStoryRunName("", storyName, "ctx-token")
	if sr == nil || sr.Name != expected {
		t.Fatalf("expected StoryRun name %q, got %+v", expected, sr)
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

func TestMergeStepRunStatus_ClearsPayloadFields(t *testing.T) {
	existing := &runsv1alpha1.StepRunStatus{
		Output: &k8sruntime.RawExtension{Raw: []byte(`"old"`)},
		Error:  &k8sruntime.RawExtension{Raw: []byte(`"err"`)},
		Manifest: map[string]runsv1alpha1.StepManifestData{
			"result": {Error: "stale"},
		},
		ManifestWarnings: []string{"warn"},
		Needs:            []string{"prior-step"},
	}
	incoming := &runsv1alpha1.StepRunStatus{}

	merged, err := mergeStepRunStatus(existing, incoming)
	if err != nil {
		t.Fatalf("mergeStepRunStatus() error = %v", err)
	}
	if merged.Output != nil || merged.Error != nil {
		t.Fatalf("expected Output/Error to be cleared, got output=%v error=%v", merged.Output, merged.Error)
	}
	if merged.Manifest != nil || merged.ManifestWarnings != nil {
		t.Fatalf("expected manifest fields to be cleared, got %+v / %+v", merged.Manifest, merged.ManifestWarnings)
	}
	if merged.Needs != nil {
		t.Fatalf("expected Needs to be cleared, got %v", merged.Needs)
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
