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

package testkit

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bubu-sdk-go/engram"
)

// ValidateStructuredError checks that a StructuredError payload matches the v1 contract.
func ValidateStructuredError(errObj runsv1alpha1.StructuredError) error {
	if strings.TrimSpace(errObj.Version) == "" {
		return fmt.Errorf("structured error version is required")
	}
	if errObj.Version != runsv1alpha1.StructuredErrorVersionV1 {
		return fmt.Errorf("unsupported structured error version %q", errObj.Version)
	}
	if errObj.Type == "" {
		return fmt.Errorf("structured error type is required")
	}
	switch errObj.Type {
	case runsv1alpha1.StructuredErrorTypeTimeout,
		runsv1alpha1.StructuredErrorTypeStorage,
		runsv1alpha1.StructuredErrorTypeSerialization,
		runsv1alpha1.StructuredErrorTypeValidation,
		runsv1alpha1.StructuredErrorTypeInitialization,
		runsv1alpha1.StructuredErrorTypeExecution,
		runsv1alpha1.StructuredErrorTypeUnknown:
	default:
		return fmt.Errorf("unsupported structured error type %q", errObj.Type)
	}
	if strings.TrimSpace(errObj.Message) == "" {
		return fmt.Errorf("structured error message is required")
	}
	if exitClass := strings.TrimSpace(string(errObj.ExitClass)); exitClass != "" {
		switch runsv1alpha1.StructuredErrorExitClass(exitClass) {
		case runsv1alpha1.StructuredErrorExitClassSuccess,
			runsv1alpha1.StructuredErrorExitClassRetry,
			runsv1alpha1.StructuredErrorExitClassTerminal,
			runsv1alpha1.StructuredErrorExitClassRateLimited,
			runsv1alpha1.StructuredErrorExitClassUnknown:
		default:
			return fmt.Errorf("unsupported structured error exitClass %q", exitClass)
		}
	}
	return nil
}

// RequireStructuredError fails the test when the StructuredError payload is invalid.
func RequireStructuredError(t testing.TB, errObj runsv1alpha1.StructuredError) {
	t.Helper()
	if err := ValidateStructuredError(errObj); err != nil {
		t.Fatalf("invalid structured error: %v", err)
	}
}

// ValidateStreamMessage verifies the minimum validity requirements for streaming messages.
func ValidateStreamMessage(msg engram.StreamMessage) error {
	if strings.TrimSpace(msg.Kind) == engram.StreamMessageKindError {
		if err := ValidateStreamErrorMessage(msg); err != nil {
			return err
		}
	}
	if err := msg.Validate(); err != nil {
		return err
	}
	if msg.Audio == nil && msg.Video == nil && msg.Binary == nil && len(msg.Payload) == 0 && len(msg.Inputs) == 0 && len(msg.Transports) == 0 { //nolint:lll
		return fmt.Errorf("stream message missing payload, inputs, transports, or media")
	}
	return nil
}

// ValidateStreamErrorMessage verifies the StructuredError envelope for error StreamMessages.
func ValidateStreamErrorMessage(msg engram.StreamMessage) error {
	if strings.TrimSpace(msg.Kind) != engram.StreamMessageKindError {
		return fmt.Errorf("stream message kind %q is not error", msg.Kind)
	}
	if len(msg.Payload) == 0 {
		return fmt.Errorf("stream error message missing payload")
	}
	var errObj runsv1alpha1.StructuredError
	if err := json.Unmarshal(msg.Payload, &errObj); err != nil {
		return fmt.Errorf("stream error payload invalid: %w", err)
	}
	if err := ValidateStructuredError(errObj); err != nil {
		return fmt.Errorf("stream error payload invalid: %w", err)
	}
	return nil
}

// RequireStreamMessage fails the test when the message is invalid.
func RequireStreamMessage(t testing.TB, msg engram.StreamMessage) {
	t.Helper()
	if err := ValidateStreamMessage(msg); err != nil {
		t.Fatalf("invalid stream message: %v", err)
	}
}

// RequireStreamErrorMessage fails the test when the error envelope is invalid.
func RequireStreamErrorMessage(t testing.TB, msg engram.StreamMessage) {
	t.Helper()
	if err := ValidateStreamErrorMessage(msg); err != nil {
		t.Fatalf("invalid stream error message: %v", err)
	}
}
