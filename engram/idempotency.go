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

package engram

import (
	"errors"
	"fmt"
	"strings"
)

// ErrIdempotencyUnavailable indicates that the execution context lacks stable identifiers.
var ErrIdempotencyUnavailable = errors.New("idempotency unavailable: missing storyrun or steprun identity")

// IdempotencyKey derives a stable key from the StoryRun and StepRun identity.
func IdempotencyKey(info StoryInfo) (string, error) {
	storyRun := strings.TrimSpace(info.StoryRunID)
	stepRun := strings.TrimSpace(info.StepRunID)
	if storyRun == "" || stepRun == "" {
		return "", ErrIdempotencyUnavailable
	}
	namespace := strings.TrimSpace(info.StepRunNamespace)
	if namespace == "" {
		return fmt.Sprintf("storyrun/%s/steprun/%s", storyRun, stepRun), nil
	}
	return fmt.Sprintf("ns/%s/storyrun/%s/steprun/%s", namespace, storyRun, stepRun), nil
}

// IdempotencyKey returns a stable key for the current execution context.
func (e *ExecutionContext) IdempotencyKey() (string, error) {
	if e == nil {
		return "", ErrIdempotencyUnavailable
	}
	return IdempotencyKey(e.storyInfo)
}
