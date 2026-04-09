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

package sdk

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/bubustack/bubu-sdk-go/runtime"
	"github.com/bubustack/core/contracts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Mocks
type MockBatchEngram[C any, I any] struct {
	mock.Mock
}

func (m *MockBatchEngram[C, I]) Init(ctx context.Context, config C, secrets *engram.Secrets) error {
	args := m.Called(ctx, config, secrets)
	return args.Error(0)
}

func (m *MockBatchEngram[C, I]) Process(
	ctx context.Context, execCtx *engram.ExecutionContext, inputs I,
) (*engram.Result, error) {
	args := m.Called(ctx, execCtx, inputs)
	if res := args.Get(0); res != nil {
		return res.(*engram.Result), args.Error(1)
	}
	return nil, args.Error(1)
}

type timeoutEngram struct{}

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

func (timeoutEngram) Init(ctx context.Context, config struct{}, secrets *engram.Secrets) error {
	return nil
}

func (timeoutEngram) Process(
	ctx context.Context,
	execCtx *engram.ExecutionContext,
	inputs struct{},
) (*engram.Result, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type timeoutSuccessEngram struct{}

func (timeoutSuccessEngram) Init(ctx context.Context, config struct{}, secrets *engram.Secrets) error {
	return nil
}

func (timeoutSuccessEngram) Process(
	ctx context.Context,
	execCtx *engram.ExecutionContext,
	inputs struct{},
) (*engram.Result, error) {
	<-ctx.Done()
	return &engram.Result{Data: "late-success"}, nil
}

type panicProcessEngram struct{}

func (panicProcessEngram) Init(ctx context.Context, config struct{}, secrets *engram.Secrets) error {
	return nil
}

func (panicProcessEngram) Process(
	ctx context.Context,
	execCtx *engram.ExecutionContext,
	inputs struct{},
) (*engram.Result, error) {
	panic(errors.New("process boom"))
}

type secretExpansionInitTrackingEngram struct {
	initCalled bool
}

func (e *secretExpansionInitTrackingEngram) Init(
	ctx context.Context,
	config struct{},
	secrets *engram.Secrets,
) error {
	e.initCalled = true
	return nil
}

func (e *secretExpansionInitTrackingEngram) Process(
	ctx context.Context,
	execCtx *engram.ExecutionContext,
	inputs struct{},
) (*engram.Result, error) {
	return engram.NewResultFrom("ok"), nil
}

func TestCallWithPanicRecoveryNoValue_ConvertsPanicsToErrors(t *testing.T) {
	cause := errors.New("boom")
	err := callWithPanicRecoveryNoValue("test component", func() error {
		panic(cause)
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test component panicked")
	assert.True(t, errors.Is(err, cause))
}

func TestRun_Success_NoBridge(t *testing.T) {
	// Setup environment
	err := os.Setenv(contracts.StoryNameEnv, "test-story")
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	err = os.Setenv(contracts.StepRunNameEnv, "test-step-run")
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	err = os.Setenv(contracts.StepRunNamespaceEnv, "default")
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	defer func() {
		err = os.Unsetenv(contracts.StoryNameEnv)
		if err != nil {
			t.Fatalf("Unsetenv() error = %v", err)
		}
	}()
	defer func() {
		err = os.Unsetenv(contracts.StepRunNameEnv)
		if err != nil {
			t.Fatalf("Unsetenv() error = %v", err)
		}
	}()
	defer func() {
		err = os.Unsetenv(contracts.StepRunNamespaceEnv)
		if err != nil {
			t.Fatalf("Unsetenv() error = %v", err)
		}
	}()

	// Mocks
	mockEngram := &MockBatchEngram[map[string]any, any]{}
	mockSM := &storage.MockManager{}
	mockK8s := &k8s.MockClient{}

	// Expectations
	mockEngram.On("Init", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockEngram.On("Process", mock.Anything, mock.Anything, mock.Anything).Return(&engram.Result{Data: "success"}, nil)
	mockSM.On("Hydrate", mock.Anything, mock.Anything).Return(map[string]any{}, nil)
	mockSM.On("Dehydrate", mock.Anything, "success", storage.NamespacedKey("default", "test-step-run")).Return("dehydrated", nil) //nolint:lll
	mockK8s.On("PatchStepRunStatus", mock.Anything, "test-step-run", mock.Anything).Return(nil)

	// Disable bridge
	err = os.Setenv(contracts.HybridBridgeEnv, "false")
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	defer func() {
		err = os.Unsetenv(contracts.HybridBridgeEnv)
		if err != nil {
			t.Fatalf("Unsetenv() error = %v", err)
		}
	}()

	// Run the function with injected mocks
	err = runWithClients(context.Background(), mockEngram, mockK8s, mockSM)
	assert.NoError(t, err)

	// Assert that all expectations were met
	mockEngram.AssertExpectations(t)
	mockSM.AssertExpectations(t)
	mockK8s.AssertExpectations(t)
}

func TestRunWithClients_InitFailurePatchesStatus(t *testing.T) {
	t.Setenv(contracts.StoryNameEnv, "init-failure-story")
	t.Setenv(contracts.StepRunNameEnv, "step-init-fail")

	initErr := errors.New("init boom")

	mockEngram := &MockBatchEngram[map[string]any, any]{}
	mockEngram.On("Init", mock.Anything, mock.Anything, mock.Anything).Return(initErr)

	mockSM := &storage.MockManager{}
	mockK8s := &k8s.MockClient{}
	mockK8s.On("PatchStepRunStatus", mock.Anything, "step-init-fail",
		mock.MatchedBy(func(status runsv1alpha1.StepRunStatus) bool {
			return status.Phase == enums.PhaseFailed &&
				status.ExitCode == 1 &&
				status.ExitClass == enums.ExitClassTerminal &&
				strings.Contains(status.LastFailureMsg, initErr.Error())
		}),
	).Return(nil)

	err := runWithClients(context.Background(), mockEngram, mockK8s, mockSM)
	assert.ErrorIs(t, err, initErr)

	mockEngram.AssertExpectations(t)
	mockK8s.AssertExpectations(t)
	mockSM.AssertNotCalled(t, "Hydrate", mock.Anything, mock.Anything)
	mockSM.AssertNotCalled(t, "Dehydrate", mock.Anything, mock.Anything, mock.Anything)
}

func TestRunWithClientsTimeoutForcesExitCode(t *testing.T) {
	t.Setenv(contracts.StepTimeoutEnv, "1ms")

	execCtxData := &runtime.ExecutionContextData{
		Inputs:  map[string]any{},
		Config:  map[string]any{},
		Secrets: map[string]string{},
		StoryInfo: engram.StoryInfo{
			StoryName:        "timeout-story",
			StoryRunID:       "story-run",
			StepName:         "step",
			StepRunID:        "step-run",
			StepRunNamespace: "default",
		},
		StartedAt: metav1.Now(),
	}

	mockSM := &storage.MockManager{}
	mockSM.On("Hydrate", mock.Anything, mock.Anything).Return(map[string]any{}, nil)

	mockK8s := &k8s.MockClient{}
	mockK8s.On("PatchStepRunStatus", mock.Anything, "step-run",
		mock.MatchedBy(func(status runsv1alpha1.StepRunStatus) bool {
			return status.ExitCode == 124 &&
				status.ExitClass == enums.ExitClassRetry &&
				status.Phase == enums.PhaseTimeout
		}),
	).Return(nil)

	originalExit := exitProcess
	defer func() { exitProcess = originalExit }()

	var (
		exitCalled bool
		exitCode   int
	)
	exitProcess = func(code int) {
		exitCalled = true
		exitCode = code
	}

	err := runWithClientsWithContext(context.Background(), timeoutEngram{}, mockK8s, mockSM, execCtxData)
	if err == nil {
		t.Fatalf("expected timeout error")
	}
	assert.True(t, errors.Is(err, ErrBatchTimeout))
	assert.True(t, exitCalled, "expected exitProcess to be invoked")
	assert.Equal(t, 124, exitCode)

	mockSM.AssertExpectations(t)
	mockK8s.AssertExpectations(t)
}

func TestRunWithClientsTimeoutSuccessStillForcesExitCode(t *testing.T) {
	t.Setenv(contracts.StepTimeoutEnv, "1ms")

	execCtxData := &runtime.ExecutionContextData{
		Inputs:  map[string]any{},
		Config:  map[string]any{},
		Secrets: map[string]string{},
		StoryInfo: engram.StoryInfo{
			StoryName:        "timeout-story",
			StoryRunID:       "story-run",
			StepName:         "step",
			StepRunID:        "step-run",
			StepRunNamespace: "default",
		},
		StartedAt: metav1.Now(),
	}

	mockK8s := &k8s.MockClient{}
	mockK8s.On("PatchStepRunStatus", mock.Anything, "step-run",
		mock.MatchedBy(func(status runsv1alpha1.StepRunStatus) bool {
			return status.ExitCode == 124 &&
				status.ExitClass == enums.ExitClassRetry &&
				status.Phase == enums.PhaseTimeout
		}),
	).Return(nil)

	originalExit := exitProcess
	defer func() { exitProcess = originalExit }()

	var (
		exitCalled bool
		exitCode   int
	)
	exitProcess = func(code int) {
		exitCalled = true
		exitCode = code
	}

	err := runWithClientsWithContext(context.Background(), timeoutSuccessEngram{}, mockK8s, noopStorageManager{}, execCtxData) //nolint:lll
	if err == nil {
		t.Fatalf("expected timeout error")
	}
	assert.True(t, errors.Is(err, ErrBatchTimeout))
	assert.True(t, exitCalled, "expected exitProcess to be invoked")
	assert.Equal(t, 124, exitCode)

	mockK8s.AssertExpectations(t)
}

func TestRunWithClients_ProcessPanicPatchesStatus(t *testing.T) {
	execCtxData := &runtime.ExecutionContextData{
		Inputs:  map[string]any{},
		Config:  map[string]any{},
		Secrets: map[string]string{},
		StoryInfo: engram.StoryInfo{
			StoryName:        "panic-story",
			StoryRunID:       "story-run",
			StepName:         "step",
			StepRunID:        "step-run",
			StepRunNamespace: "default",
		},
		StartedAt: metav1.Now(),
	}

	mockK8s := &k8s.MockClient{}
	mockK8s.On("PatchStepRunStatus", mock.Anything, "step-run",
		mock.MatchedBy(func(status runsv1alpha1.StepRunStatus) bool {
			return status.Phase == enums.PhaseFailed &&
				status.ExitCode == 1 &&
				strings.Contains(status.LastFailureMsg, "panicked")
		}),
	).Return(nil)

	err := runWithClientsWithContext(context.Background(), panicProcessEngram{}, mockK8s, noopStorageManager{}, execCtxData) //nolint:lll
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "panicked")

	mockK8s.AssertExpectations(t)
}

func TestInitializeEngramFailsOnSecretExpansionError(t *testing.T) {
	missingDir := filepath.Join(t.TempDir(), "missing")
	eng := &secretExpansionInitTrackingEngram{}

	err := initializeEngram[struct{}, struct{}](
		context.Background(),
		eng,
		noopStorageManager{},
		&runtime.ExecutionContextData{
			Config:  map[string]any{},
			Secrets: map[string]string{"db": "file:" + missingDir},
		},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to expand secrets")
	assert.Contains(t, err.Error(), `secret "db" (file)`)
	assert.NotContains(t, err.Error(), missingDir)
	assert.False(t, eng.initCalled, "Init should not run when secret expansion fails")
}

func TestHandleResultAndPatchStatus(t *testing.T) {
	ctx := context.Background()
	execCtxData := &runtime.ExecutionContextData{
		StoryInfo: engram.StoryInfo{StepRunID: "step-1", StepRunNamespace: "default"},
	}
	processErr := errors.New("process error")
	patchErr := errors.New("patch error")

	tests := []struct {
		name              string
		result            *engram.Result
		processErr        error
		dehydrateErr      error
		patchErr          error
		expectedPhase     enums.Phase
		expectDehydrate   bool
		expectPatch       bool
		wantErr           bool
		expectedFinalHerr string
	}{
		{
			name:            "success",
			result:          &engram.Result{Data: "ok"},
			processErr:      nil,
			expectedPhase:   enums.PhaseSucceeded,
			expectDehydrate: true,
			expectPatch:     true,
			wantErr:         false,
		},
		{
			name:            "process error",
			result:          nil,
			processErr:      processErr,
			expectedPhase:   enums.PhaseFailed,
			expectDehydrate: false, // dehydrateWithFallback returns early for nil data
			expectPatch:     true,
			wantErr:         true,
		},
		{
			name:            "patch error",
			result:          &engram.Result{Data: "ok"},
			processErr:      nil,
			patchErr:        patchErr,
			expectedPhase:   enums.PhaseSucceeded,
			expectDehydrate: true,
			expectPatch:     true,
			wantErr:         true,
		},
		{
			name:            "process and patch error",
			result:          nil,
			processErr:      processErr,
			patchErr:        patchErr,
			expectedPhase:   enums.PhaseFailed,
			expectDehydrate: false, // dehydrateWithFallback returns early for nil data
			expectPatch:     true,
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSM := new(storage.MockManager)
			mockK8s := new(k8s.MockClient)

			if tt.expectDehydrate {
				mockSM.On(
					"Dehydrate",
					mock.Anything,
					mock.Anything,
					storage.NamespacedKey(execCtxData.StoryInfo.StepRunNamespace, execCtxData.StoryInfo.StepRunID),
				).Return(mock.Anything, tt.dehydrateErr)
			}
			if tt.expectPatch {
				mockK8s.On("PatchStepRunStatus", ctx, execCtxData.StoryInfo.StepRunID, mock.Anything).Return(tt.patchErr)
			}

			_, _, err, _ := handleResultAndPatchStatus(ctx, mockSM, mockK8s, execCtxData, tt.result, tt.processErr, false, nil)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockSM.AssertExpectations(t)
			mockK8s.AssertExpectations(t)
		})
	}
}
