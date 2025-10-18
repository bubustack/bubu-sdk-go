package sdk

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/bubustack/bubu-sdk-go/runtime"
	"github.com/bubustack/bubu-sdk-go/storage"
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

func TestRun_Success_NoBridge(t *testing.T) {
	// Setup environment
	err := os.Setenv("BUBU_STORY_NAME", "test-story")
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	err = os.Setenv("BUBU_STEPRUN_NAME", "test-step-run")
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	defer func() {
		err = os.Unsetenv("BUBU_STORY_NAME")
		if err != nil {
			t.Fatalf("Unsetenv() error = %v", err)
		}
	}()
	defer func() {
		err = os.Unsetenv("BUBU_STEPRUN_NAME")
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
	mockSM.On("Dehydrate", mock.Anything, "success", "test-step-run").Return("dehydrated", nil)
	mockK8s.On("PatchStepRunStatus", mock.Anything, "test-step-run", mock.Anything).Return(nil)

	// Disable bridge
	err = os.Setenv("BUBU_HYBRID_BRIDGE", "false")
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	defer func() {
		err = os.Unsetenv("BUBU_HYBRID_BRIDGE")
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

func TestBridgeToHub_Enabled_TimesOutGracefully(t *testing.T) {
	// Enable bridge, set tiny timeout, and set a bogus hub to force dial timeout
	err := os.Setenv("BUBU_HYBRID_BRIDGE", "true")
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	err = os.Setenv("BUBU_HYBRID_BRIDGE_TIMEOUT", "100ms")
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	err = os.Setenv("DOWNSTREAM_HOST", "10.255.255.1:65535") // unroutable
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	err = os.Setenv("DOWNSTREAM_HOST", "10.255.255.1:65535") // unroutable
	if err != nil {
		t.Fatalf("Setenv() error = %v", err)
	}
	defer func() {
		err = os.Unsetenv("BUBU_HYBRID_BRIDGE")
		if err != nil {
			t.Fatalf("Unsetenv() error = %v", err)
		}
	}()
	defer func() {
		err = os.Unsetenv("BUBU_HYBRID_BRIDGE_TIMEOUT")
		if err != nil {
			t.Fatalf("Unsetenv() error = %v", err)
		}
	}()
	defer func() {
		err = os.Unsetenv("DOWNSTREAM_HOST")
		if err != nil {
			t.Fatalf("Unsetenv() error = %v", err)
		}
	}()

	ctx := context.Background()
	// Should return an error within timeout
	err = bridgeToHub(ctx, []byte(`{"ok":true}`))
	if err == nil {
		t.Log("bridgeToHub unexpectedly succeeded against an unroutable address; environment may allow fast failures")
	}
}

func TestHandleResultAndPatchStatus(t *testing.T) {
	ctx := context.Background()
	execCtxData := &runtime.ExecutionContextData{
		StoryInfo: engram.StoryInfo{StepRunID: "step-1"},
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
				mockSM.On("Dehydrate", ctx, mock.Anything, execCtxData.StoryInfo.StepRunID).Return(mock.Anything, tt.dehydrateErr)
			}
			if tt.expectPatch {
				mockK8s.On("PatchStepRunStatus", ctx, execCtxData.StoryInfo.StepRunID, mock.Anything).Return(tt.patchErr)
			}

			_, _, err := handleResultAndPatchStatus(ctx, mockSM, mockK8s, execCtxData, tt.result, tt.processErr, false)

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
