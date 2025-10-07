package k8s

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
			name: "BUBU_TARGET_STORY_NAMESPACE should have the highest precedence",
			envVars: map[string]string{
				"BUBU_TARGET_STORY_NAMESPACE": "story-ns",
				"BUBU_IMPULSE_NAMESPACE":      "impulse-ns",
				"BUBU_STEPRUN_NAMESPACE":      "steprun-ns",
				"BUBU_POD_NAMESPACE":          "pod-ns",
				"POD_NAMESPACE":               "k8s-pod-ns",
			},
			expected: "story-ns",
		},
		{
			name: "BUBU_IMPULSE_NAMESPACE should have second precedence",
			envVars: map[string]string{
				"BUBU_IMPULSE_NAMESPACE": "impulse-ns",
				"BUBU_STEPRUN_NAMESPACE": "steprun-ns",
				"BUBU_POD_NAMESPACE":     "pod-ns",
				"POD_NAMESPACE":          "k8s-pod-ns",
			},
			expected: "impulse-ns",
		},
		{
			name: "BUBU_STEPRUN_NAMESPACE should have third precedence",
			envVars: map[string]string{
				"BUBU_STEPRUN_NAMESPACE": "steprun-ns",
				"BUBU_POD_NAMESPACE":     "pod-ns",
				"POD_NAMESPACE":          "k8s-pod-ns",
			},
			expected: "steprun-ns",
		},
		{
			name: "BUBU_POD_NAMESPACE should have fourth precedence",
			envVars: map[string]string{
				"BUBU_POD_NAMESPACE": "pod-ns",
				"POD_NAMESPACE":      "k8s-pod-ns",
			},
			expected: "pod-ns",
		},
		{
			name: "POD_NAMESPACE should have fifth precedence",
			envVars: map[string]string{
				"POD_NAMESPACE": "k8s-pod-ns",
			},
			expected: "k8s-pod-ns",
		},
		{
			name:     "Should fallback to default if no env vars are set",
			envVars:  map[string]string{},
			expected: "default",
		},
		{
			name: "Should ignore empty env vars and use the next in precedence",
			envVars: map[string]string{
				"BUBU_TARGET_STORY_NAMESPACE": "",
				"BUBU_IMPULSE_NAMESPACE":      "impulse-ns",
			},
			expected: "impulse-ns",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up env vars before each test
			os.Clearenv()

			for key, value := range tc.envVars {
				os.Setenv(key, value)
			}

			// Defer cleanup to after the test
			defer func() {
				for key := range tc.envVars {
					os.Unsetenv(key)
				}
			}()

			if got := getPodNamespace(); got != tc.expected {
				t.Errorf("getPodNamespace() = %v, want %v", got, tc.expected)
			}
		})
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

	storyRun, err := c.TriggerStory(ctx, storyName, inputs)
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

func TestPatchStepRunStatus(t *testing.T) {
	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-step-run",
			Namespace: "test-ns",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&runsv1alpha1.StepRun{}).WithObjects(stepRun).Build()
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
