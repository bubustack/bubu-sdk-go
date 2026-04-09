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
	"fmt"
	"strings"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bubu-sdk-go/engram"
)

// NewStreamErrorMessage wraps a StructuredError into a StreamMessage payload with Kind "error".
func NewStreamErrorMessage(
	errObj runsv1alpha1.StructuredError,
	opts ...StreamMessageOption) (engram.StreamMessage,
	error,
) {
	raw, err := json.Marshal(errObj)
	if err != nil {
		return engram.StreamMessage{}, fmt.Errorf("marshal structured error: %w", err)
	}
	options := append([]StreamMessageOption{WithJSONPayload(raw)}, opts...)
	return NewStreamMessage(engram.StreamMessageKindError, options...), nil
}

// ParseStreamErrorMessage extracts StructuredError payloads from StreamMessage Kind "error".
func ParseStreamErrorMessage(msg engram.StreamMessage) (runsv1alpha1.StructuredError, bool, error) {
	if strings.TrimSpace(msg.Kind) != engram.StreamMessageKindError {
		return runsv1alpha1.StructuredError{}, false, nil
	}
	if len(msg.Payload) == 0 {
		return runsv1alpha1.StructuredError{}, true, fmt.Errorf("stream error message missing payload")
	}
	var out runsv1alpha1.StructuredError
	if err := json.Unmarshal(msg.Payload, &out); err != nil {
		return runsv1alpha1.StructuredError{}, true, fmt.Errorf("stream error payload invalid: %w", err)
	}
	return out, true, nil
}
