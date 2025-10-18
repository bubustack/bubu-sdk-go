package k8s

import (
	"context"

	"github.com/stretchr/testify/mock"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
)

// MockClient is a mock implementation of the ClientInterface for testing.
type MockClient struct {
	mock.Mock
}

// GetNamespace returns the mocked namespace value for testing.
func (m *MockClient) GetNamespace() string {
	args := m.Called()
	return args.String(0)
}

// TriggerStory mocks the TriggerStory method for testing.
func (m *MockClient) TriggerStory(
	ctx context.Context, storyName string, inputs map[string]any,
) (*runsv1alpha1.StoryRun, error) {
	args := m.Called(ctx, storyName, inputs)
	if sr := args.Get(0); sr != nil {
		return sr.(*runsv1alpha1.StoryRun), args.Error(1)
	}
	return nil, args.Error(1)
}

// PatchStepRunStatus mocks the PatchStepRunStatus method for testing.
func (m *MockClient) PatchStepRunStatus(
	ctx context.Context, stepRunName string, patchData runsv1alpha1.StepRunStatus,
) error {
	args := m.Called(ctx, stepRunName, patchData)
	return args.Error(0)
}
