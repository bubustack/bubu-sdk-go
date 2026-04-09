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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bubu-sdk-go/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func requireStructuredStatusError(t *testing.T, status *runsv1alpha1.StepRunStatus) runsv1alpha1.StructuredError {
	t.Helper()
	require.NotNil(t, status)
	require.NotNil(t, status.Error)
	return *status.Error
}

func TestAppendStructuredError_Succeeded(t *testing.T) {
	status := &runsv1alpha1.StepRunStatus{}
	appendStructuredError(status, enums.PhaseSucceeded, false, nil)
	assert.Nil(t, status.Error, "no error should be set on success")
}

func TestAppendStructuredError_NilError(t *testing.T) {
	status := &runsv1alpha1.StepRunStatus{}
	appendStructuredError(status, enums.PhaseFailed, false, nil)
	assert.Nil(t, status.Error, "no error should be set when finalErr is nil")
}

func TestAppendStructuredError_ExecutionError(t *testing.T) {
	status := &runsv1alpha1.StepRunStatus{
		ExitCode:  1,
		ExitClass: enums.ExitClassTerminal,
	}
	appendStructuredError(status, enums.PhaseFailed, false, fmt.Errorf("engram process returned non-zero"))

	errObj := requireStructuredStatusError(t, status)

	assert.Equal(t, runsv1alpha1.StructuredErrorVersionV1, errObj.Version)
	assert.Equal(t, runsv1alpha1.StructuredErrorTypeExecution, errObj.Type)
	assert.Equal(t, "engram process returned non-zero", errObj.Message)
	require.NotNil(t, errObj.ExitCode)
	assert.Equal(t, int32(1), *errObj.ExitCode)
	assert.Equal(t, runsv1alpha1.StructuredErrorExitClass(enums.ExitClassTerminal), errObj.ExitClass)
	require.NotNil(t, errObj.Retryable)
	assert.False(t, *errObj.Retryable)
}

func TestAppendStructuredError_TimeoutError(t *testing.T) {
	status := &runsv1alpha1.StepRunStatus{
		ExitCode:  124,
		ExitClass: enums.ExitClassRetry,
	}
	appendStructuredError(status, enums.PhaseTimeout, true, fmt.Errorf("step exceeded 30s timeout"))

	errObj := requireStructuredStatusError(t, status)

	assert.Equal(t, runsv1alpha1.StructuredErrorVersionV1, errObj.Version)
	assert.Equal(t, runsv1alpha1.StructuredErrorTypeTimeout, errObj.Type)
	assert.Contains(t, errObj.Message, "30s")
	require.NotNil(t, errObj.ExitCode)
	assert.Equal(t, int32(124), *errObj.ExitCode)
	assert.Equal(t, runsv1alpha1.StructuredErrorExitClass(enums.ExitClassRetry), errObj.ExitClass)
	require.NotNil(t, errObj.Retryable)
	assert.True(t, *errObj.Retryable)
}

func TestAppendStructuredError_StorageError(t *testing.T) {
	status := &runsv1alpha1.StepRunStatus{ExitCode: 1, ExitClass: enums.ExitClassTerminal}
	appendStructuredError(status, enums.PhaseFailed, false, fmt.Errorf("failed to dehydrate output: storage backend unavailable")) //nolint:lll

	errObj := requireStructuredStatusError(t, status)
	assert.Equal(t, runsv1alpha1.StructuredErrorTypeStorage, errObj.Type)
}

func TestAppendStructuredError_ValidationError(t *testing.T) {
	status := &runsv1alpha1.StepRunStatus{ExitCode: 1, ExitClass: enums.ExitClassTerminal}
	appendStructuredError(status, enums.PhaseFailed, false, fmt.Errorf("input schema validation failed"))

	errObj := requireStructuredStatusError(t, status)
	assert.Equal(t, runsv1alpha1.StructuredErrorTypeValidation, errObj.Type)
}

func TestAppendStructuredError_SerializationError(t *testing.T) {
	status := &runsv1alpha1.StepRunStatus{ExitCode: 1, ExitClass: enums.ExitClassTerminal}
	appendStructuredError(status, enums.PhaseFailed, false, fmt.Errorf("failed to unmarshal config: invalid JSON"))

	errObj := requireStructuredStatusError(t, status)
	assert.Equal(t, runsv1alpha1.StructuredErrorTypeSerialization, errObj.Type)
}

func TestAppendStructuredError_InitializationError(t *testing.T) {
	status := &runsv1alpha1.StepRunStatus{ExitCode: 1, ExitClass: enums.ExitClassTerminal}
	appendStructuredError(status, enums.PhaseFailed, false, fmt.Errorf("engram initialization failed: missing API key"))

	errObj := requireStructuredStatusError(t, status)
	assert.Equal(t, runsv1alpha1.StructuredErrorTypeInitialization, errObj.Type)
}

func TestAppendStructuredError_RedactsAndTruncatesProviderMessageAfterMerge(t *testing.T) {
	status := &runsv1alpha1.StepRunStatus{
		ExitCode:  1,
		ExitClass: enums.ExitClassTerminal,
	}
	longSensitive := `password="s3cr3t" ` + strings.Repeat("x", maxErrorMessageBytes+64)

	appendStructuredError(
		status,
		enums.PhaseFailed,
		false,
		NewStructuredError(runsv1alpha1.StructuredErrorTypeExecution, longSensitive),
	)

	errObj := requireStructuredStatusError(t, status)
	assert.LessOrEqual(t, len(errObj.Message), maxErrorMessageBytes)
	assert.NotContains(t, errObj.Message, "s3cr3t")
	assert.Contains(t, errObj.Message, "[REDACTED]")
}

func TestWithStructuredErrorDetailsFallsBackWhenMarshalFails(t *testing.T) {
	err := NewStructuredError(
		runsv1alpha1.StructuredErrorTypeExecution,
		"public failure",
		WithStructuredErrorDetails(map[string]any{
			"bad": func() {},
		}),
	)

	provider, ok := err.(StructuredErrorProvider)
	require.True(t, ok)
	serr := provider.StructuredError()
	require.NotNil(t, serr.Details)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(serr.Details.Raw, &payload))
	assert.Equal(t, true, payload["unserializable"])
	assert.Equal(t, "map", payload["type"])
	assert.Equal(t, "json: unsupported type: func()", payload["marshalError"])

	details, ok := payload["details"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(1), details["len"])
}

func TestStructuredErrorErrorPrefersStructuredMessageOverCause(t *testing.T) {
	cause := errors.New("internal database password mismatch")
	err := NewStructuredError(
		runsv1alpha1.StructuredErrorTypeExecution,
		"public failure",
		WithStructuredErrorCause(cause),
	)

	assert.Equal(t, "public failure", err.Error())
	assert.ErrorIs(t, err, cause)
}

func TestStructuredErrorErrorFallsBackToCauseWhenMessageEmpty(t *testing.T) {
	cause := errors.New("internal fallback")
	err := NewStructuredError(
		runsv1alpha1.StructuredErrorTypeExecution,
		"",
		WithStructuredErrorCause(cause),
	)

	assert.Equal(t, "internal fallback", err.Error())
	assert.ErrorIs(t, err, cause)
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected runsv1alpha1.StructuredErrorType
	}{
		{"nil", nil, runsv1alpha1.StructuredErrorTypeUnknown},
		{"storage", fmt.Errorf("storage backend timeout"), runsv1alpha1.StructuredErrorTypeStorage},
		{"dehydrate", fmt.Errorf("failed to dehydrate output"), runsv1alpha1.StructuredErrorTypeStorage},
		{"unmarshal", fmt.Errorf("failed to unmarshal config"), runsv1alpha1.StructuredErrorTypeSerialization},
		{"marshal", fmt.Errorf("output marshal error"), runsv1alpha1.StructuredErrorTypeSerialization},
		{"schema", fmt.Errorf("input schema validation failed"), runsv1alpha1.StructuredErrorTypeValidation},
		{"validation", fmt.Errorf("validation error"), runsv1alpha1.StructuredErrorTypeValidation},
		{"init", fmt.Errorf("engram initialization failed"), runsv1alpha1.StructuredErrorTypeInitialization},
		{"generic", fmt.Errorf("something went wrong"), runsv1alpha1.StructuredErrorTypeExecution},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, classifyError(tt.err))
		})
	}
}

func TestNewStepRunStatus_SetsErrorOnFailure(t *testing.T) {
	execCtx := &runtime.ExecutionContextData{
		StartedAt: metav1.Time{Time: time.Now().Add(-5 * time.Second)},
	}
	status := newStepRunStatus(execCtx, enums.PhaseFailed, false, fmt.Errorf("test failure"))

	assert.Equal(t, enums.PhaseFailed, status.Phase)
	errObj := requireStructuredStatusError(t, &status)
	assert.Equal(t, runsv1alpha1.StructuredErrorTypeExecution, errObj.Type)
	assert.Equal(t, "test failure", errObj.Message)
}

func TestNewStepRunStatus_NoErrorOnSuccess(t *testing.T) {
	execCtx := &runtime.ExecutionContextData{
		StartedAt: metav1.Time{Time: time.Now().Add(-1 * time.Second)},
	}
	status := newStepRunStatus(execCtx, enums.PhaseSucceeded, false, nil)

	assert.Equal(t, enums.PhaseSucceeded, status.Phase)
	assert.Nil(t, status.Error, "status.error should be nil on success")
}

func TestNewStepRunStatus_TimeoutSetsErrorType(t *testing.T) {
	execCtx := &runtime.ExecutionContextData{
		StartedAt: metav1.Time{Time: time.Now().Add(-30 * time.Second)},
	}
	status := newStepRunStatus(execCtx, enums.PhaseTimeout, true, fmt.Errorf("timeout exceeded"))

	errObj := requireStructuredStatusError(t, &status)
	assert.Equal(t, runsv1alpha1.StructuredErrorTypeTimeout, errObj.Type)
}

func TestNewStepRunStatus_SanitizesPersistedErrorFields(t *testing.T) {
	execCtx := &runtime.ExecutionContextData{
		StartedAt: metav1.Time{Time: time.Now().Add(-3 * time.Second)},
	}
	err := fmt.Errorf(`upstream rejected request: Authorization: Bearer top-secret password="super-secret"`)

	status := newStepRunStatus(execCtx, enums.PhaseFailed, false, err)
	lastFailureStatus := runsv1alpha1.StepRunStatus{}
	applyStatusOverride(&lastFailureStatus, nil, err, false, enums.PhaseFailed, err)

	assert.NotContains(t, lastFailureStatus.LastFailureMsg, "top-secret")
	assert.NotContains(t, lastFailureStatus.LastFailureMsg, "super-secret")
	assert.Contains(t, lastFailureStatus.LastFailureMsg, "[REDACTED]")
	require.NotEmpty(t, status.Conditions)
	assert.NotContains(t, status.Conditions[0].Message, "top-secret")
	assert.NotContains(t, status.Conditions[0].Message, "super-secret")
	assert.Contains(t, status.Conditions[0].Message, "[REDACTED]")

	errObj := requireStructuredStatusError(t, &status)
	assert.NotContains(t, errObj.Message, "top-secret")
	assert.NotContains(t, errObj.Message, "super-secret")
	assert.Contains(t, errObj.Message, "[REDACTED]")
}

func TestNewStepRunStatus_RetryableStructuredErrorSetsExitClass(t *testing.T) {
	execCtx := &runtime.ExecutionContextData{
		StartedAt: metav1.Time{Time: time.Now().Add(-2 * time.Second)},
	}
	err := NewStructuredError(
		runsv1alpha1.StructuredErrorTypeExecution,
		"transient failure",
		WithStructuredErrorRetryable(true),
	)

	status := newStepRunStatus(execCtx, enums.PhaseFailed, false, err)

	assert.Equal(t, enums.ExitClassRetry, status.ExitClass)
	errObj := requireStructuredStatusError(t, &status)
	require.NotNil(t, errObj.Retryable)
	assert.True(t, *errObj.Retryable)
}
