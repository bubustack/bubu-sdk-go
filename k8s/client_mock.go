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
	"fmt"

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
	ctx context.Context, storyName string, storyNamespace string, inputs map[string]any,
) (*runsv1alpha1.StoryRun, error) {
	args := m.Called(ctx, storyName, storyNamespace, inputs)
	if sr := args.Get(0); sr != nil {
		typed, ok := sr.(*runsv1alpha1.StoryRun)
		if !ok {
			return nil, fmt.Errorf("mock TriggerStory expected *runsv1alpha1.StoryRun, got %T", sr)
		}
		return typed, args.Error(1)
	}
	return nil, args.Error(1)
}

// StopStoryRun mocks the StopStoryRun method for testing.
func (m *MockClient) StopStoryRun(ctx context.Context, storyRunName, namespace string) error {
	args := m.Called(ctx, storyRunName, namespace)
	return args.Error(0)
}

// PatchStepRunStatus mocks the PatchStepRunStatus method for testing.
func (m *MockClient) PatchStepRunStatus(
	ctx context.Context, stepRunName string, patchData runsv1alpha1.StepRunStatus,
) error {
	args := m.Called(ctx, stepRunName, patchData)
	return args.Error(0)
}
