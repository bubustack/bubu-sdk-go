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

package conformance

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	sdk "github.com/bubustack/bubu-sdk-go"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/testkit"
)

// StreamSuite defines conformance checks for a StreamingEngram.
type StreamSuite[C any] struct {
	Engram                 engram.StreamingEngram[C]
	Context                context.Context
	Config                 C
	Inputs                 []engram.StreamMessage
	Secrets                map[string]string
	StoryInfo              engram.StoryInfo
	CELContext             map[string]any
	RequireValidMessages   bool
	RequireNonEmptyOutput  bool
	MinOutputCount         int
	RequireAllInputsDone   bool
	ExpectError            bool
	RequireStructuredError bool
	ValidateError          func(error) error
	ValidateOutputMessage  func(engram.StreamMessage) error
}

// Run executes Init + Stream and enforces the configured contract checks.
func (s StreamSuite[C]) Run(t testing.TB) {
	t.Helper()
	ctx := s.Context
	if ctx == nil {
		ctx = context.Background()
	}
	var inputDoneCount atomic.Int32
	h := testkit.StreamHarness[C]{
		Engram:     s.Engram,
		Config:     s.Config,
		Inputs:     s.Inputs,
		Secrets:    s.Secrets,
		StoryInfo:  s.StoryInfo,
		CELContext: s.CELContext,
		OnInputProcessed: func(engram.StreamMessage) {
			inputDoneCount.Add(1)
		},
	}
	outputs, err := h.Run(ctx)
	if err := s.validateOutcome(outputs, err, int(inputDoneCount.Load())); err != nil {
		t.Fatal(err)
	}
}

func (s StreamSuite[C]) validateOutputContract(outputs []engram.StreamMessage) error {
	minOutputCount := s.MinOutputCount
	if s.RequireNonEmptyOutput && minOutputCount < 1 {
		minOutputCount = 1
	}
	if minOutputCount > 0 && len(outputs) < minOutputCount {
		return fmt.Errorf("expected at least %d stream output message(s), got %d", minOutputCount, len(outputs))
	}
	return nil
}

func (s StreamSuite[C]) validateInputAcknowledgements(doneCount int) error {
	if !s.RequireAllInputsDone {
		return nil
	}
	if doneCount != len(s.Inputs) {
		return fmt.Errorf("expected %d input Done() call(s), got %d", len(s.Inputs), doneCount)
	}
	return nil
}

func (s StreamSuite[C]) validateOutputMessages(outputs []engram.StreamMessage) error {
	for _, msg := range outputs {
		if s.RequireValidMessages {
			if err := testkit.ValidateStreamMessage(msg); err != nil {
				return fmt.Errorf("invalid stream message: %w", err)
			}
		}
		if s.ValidateOutputMessage != nil {
			if err := s.ValidateOutputMessage(msg); err != nil {
				return fmt.Errorf("stream output validation failed: %w", err)
			}
		}
	}
	return nil
}

//nolint:gocyclo,lll
func (s StreamSuite[C]) validateOutcome(outputs []engram.StreamMessage, err error, doneCount int) error {
	expectError := s.ExpectError || s.RequireStructuredError
	if err != nil {
		if !expectError {
			return fmt.Errorf("stream run failed unexpectedly: %w", err)
		}
		if s.RequireStructuredError {
			var provider sdk.StructuredErrorProvider
			if !errors.As(err, &provider) {
				return fmt.Errorf("expected structured error, got %v", err)
			}
			if err := testkit.ValidateStructuredError(provider.StructuredError()); err != nil {
				return fmt.Errorf("invalid structured error: %w", err)
			}
		}
		if s.ValidateError != nil {
			if err := s.ValidateError(err); err != nil {
				return fmt.Errorf("error validation failed: %w", err)
			}
		}
		if err := s.validateOutputContract(outputs); err != nil {
			return fmt.Errorf("output validation failed (expected error path): %w", err)
		}
		if err := s.validateOutputMessages(outputs); err != nil {
			return fmt.Errorf("output validation failed (expected error path): %w", err)
		}
		return nil
	}
	if expectError {
		if s.RequireStructuredError {
			return fmt.Errorf("expected structured error, but stream completed successfully")
		}
		return fmt.Errorf("expected error, but stream completed successfully")
	}
	if err := s.validateOutputContract(outputs); err != nil {
		return fmt.Errorf("stream output contract failed: %w", err)
	}
	if err := s.validateInputAcknowledgements(doneCount); err != nil {
		return fmt.Errorf("stream input completion contract failed: %w", err)
	}
	if err := s.validateOutputMessages(outputs); err != nil {
		return err
	}
	return nil
}
