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
	"testing"

	sdk "github.com/bubustack/bubu-sdk-go"
	"github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/testkit"
)

// BatchSuite defines conformance checks for a BatchEngram.
type BatchSuite[C any, I any] struct {
	Engram                 engram.BatchEngram[C, I]
	Context                context.Context
	Config                 C
	Inputs                 I
	Secrets                map[string]string
	StoryInfo              engram.StoryInfo
	CELContext             map[string]any
	ExpectError            bool
	RequireStructuredError bool
	ValidateError          func(error) error
	ValidateResult         func(*engram.Result) error
}

// Run executes Init + Process and enforces the configured contract checks.
func (s BatchSuite[C, I]) Run(t testing.TB) {
	t.Helper()
	ctx := s.Context
	if ctx == nil {
		ctx = context.Background()
	}
	h := testkit.BatchHarness[C, I]{
		Engram:     s.Engram,
		Config:     s.Config,
		Inputs:     s.Inputs,
		Secrets:    s.Secrets,
		StoryInfo:  s.StoryInfo,
		CELContext: s.CELContext,
	}
	result, err := h.Run(ctx)
	if err := s.validateOutcome(result, err); err != nil {
		t.Fatal(err)
	}
}

func (s BatchSuite[C, I]) validateOutcome(result *engram.Result, err error) error {
	expectError := s.ExpectError || s.RequireStructuredError
	if err != nil {
		if !expectError {
			return fmt.Errorf("engram run failed unexpectedly: %w", err)
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
		return nil
	}
	if expectError {
		if s.RequireStructuredError {
			return fmt.Errorf("expected structured error, but engram completed successfully")
		}
		return fmt.Errorf("expected error, but engram completed successfully")
	}
	if s.ValidateResult != nil {
		if err := s.ValidateResult(result); err != nil {
			return fmt.Errorf("result validation failed: %w", err)
		}
	}
	return nil
}
