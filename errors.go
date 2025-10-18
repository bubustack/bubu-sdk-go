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
	"errors"
	"fmt"
	"time"
)

// ErrBatchTimeout is a sentinel used with errors.Is to detect batch timeouts.
var (
	ErrBatchTimeout           = errors.New("bubu batch execution timed out")
	ErrStoryRunNotFound       = errors.New("storyrun not found")
	ErrImpulseSessionExists   = errors.New("impulse session already active")
	ErrImpulseSessionNotFound = errors.New("impulse session not found")
)

// BatchTimeoutError conveys that a batch engram exceeded its configured timeout.
type BatchTimeoutError struct {
	Timeout time.Duration
	Cause   error
}

// Error implements the error interface.
func (e *BatchTimeoutError) Error() string {
	if e == nil {
		return ErrBatchTimeout.Error()
	}
	if e.Cause != nil {
		return fmt.Sprintf("batch execution timed out after %s: %v", e.Timeout, e.Cause)
	}
	return fmt.Sprintf("batch execution timed out after %s", e.Timeout)
}

// Unwrap exposes the underlying cause for errors.Unwrap / errors.Is checks.
func (e *BatchTimeoutError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Is allows errors.Is(err, ErrBatchTimeout) to match *BatchTimeoutError values.
func (e *BatchTimeoutError) Is(target error) bool {
	return target == ErrBatchTimeout
}

// BatchExitCode returns the recommended container exit code for an error.
// Timeout errors map to 124 (GNU timeout), all other non-nil errors default to 1.
func BatchExitCode(err error) int {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, ErrBatchTimeout):
		return 124
	default:
		return 1
	}
}
