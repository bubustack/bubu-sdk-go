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

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
)

// StructuredErrorProvider allows errors to supply a versioned StructuredError payload.
type StructuredErrorProvider interface {
	StructuredError() runsv1alpha1.StructuredError
}

type structuredError struct {
	structured runsv1alpha1.StructuredError
	cause      error
}

// StructuredError returns the versioned error payload to attach to StepRun.status.error.
func (e *structuredError) StructuredError() runsv1alpha1.StructuredError {
	return e.structured
}

// Error implements the error interface.
func (e *structuredError) Error() string {
	if e == nil {
		return "engram failed"
	}
	if e.structured.Message != "" {
		return e.structured.Message
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	return "engram failed"
}

// Unwrap exposes the underlying cause.
func (e *structuredError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// StructuredErrorOption mutates a structured error before it is returned.
type StructuredErrorOption func(*structuredError)

// WithStructuredErrorRetryable annotates the error as retryable/terminal.
func WithStructuredErrorRetryable(retryable bool) StructuredErrorOption {
	return func(e *structuredError) {
		e.structured.Retryable = &retryable
	}
}

// WithStructuredErrorExitClass sets the desired exit class ("retry", "terminal", etc.).
func WithStructuredErrorExitClass(exitClass enums.ExitClass) StructuredErrorOption {
	return func(e *structuredError) {
		e.structured.ExitClass = runsv1alpha1.StructuredErrorExitClass(exitClass)
	}
}

// WithStructuredErrorCode sets a component-specific error code.
func WithStructuredErrorCode(code string) StructuredErrorOption {
	return func(e *structuredError) {
		e.structured.Code = code
	}
}

// WithStructuredErrorDetails attaches structured metadata for diagnostics.
func WithStructuredErrorDetails(details map[string]any) StructuredErrorOption {
	return func(e *structuredError) {
		if details == nil {
			e.structured.Details = nil
			return
		}
		raw, err := json.Marshal(details)
		if err != nil {
			e.structured.Details = fallbackStructuredErrorDetails(details, err)
			return
		}
		e.structured.Details = &k8sruntime.RawExtension{Raw: raw}
	}
}

func fallbackStructuredErrorDetails(details map[string]any, marshalErr error) *k8sruntime.RawExtension {
	fallback := map[string]any{
		"unserializable": true,
		"marshalError":   marshalErr.Error(),
	}
	kind, summary := signalTypeSummary(details)
	fallback["type"] = kind
	if len(summary) > 0 {
		fallback["details"] = summary
	}
	raw, err := json.Marshal(fallback)
	if err != nil {
		return nil
	}
	return &k8sruntime.RawExtension{Raw: raw}
}

// WithStructuredErrorCause preserves the underlying error for wrapping/unwrapping.
func WithStructuredErrorCause(cause error) StructuredErrorOption {
	return func(e *structuredError) {
		e.cause = cause
	}
}

// NewStructuredError returns an error that carries a StructuredError payload.
func NewStructuredError(typ runsv1alpha1.StructuredErrorType, message string, opts ...StructuredErrorOption) error {
	serr := &structuredError{
		structured: runsv1alpha1.StructuredError{
			Version: runsv1alpha1.StructuredErrorVersionV1,
			Type:    typ,
			Message: message,
		},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(serr)
		}
	}
	return serr
}
