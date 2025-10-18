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

package storybuilder

import (
	"testing"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
)

func TestBuilder_AddEngramStep(t *testing.T) {
	b := New("demo", "default")
	ref := refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "worker"}}
	if err := b.AddEngramStep("first", ref, nil, map[string]any{"foo": "bar"}); err != nil {
		t.Fatalf("AddEngramStep() error = %v", err)
	}
	story, err := b.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if len(story.Spec.Steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(story.Spec.Steps))
	}
	if story.Spec.Steps[0].Name != "first" {
		t.Fatalf("unexpected step name: %s", story.Spec.Steps[0].Name)
	}
	if story.Spec.Steps[0].Ref == nil || story.Spec.Steps[0].Ref.Name != "worker" {
		t.Fatalf("expected engram reference 'worker' got %+v", story.Spec.Steps[0].Ref)
	}
}

func TestBuilder_DependencyValidation(t *testing.T) {
	b := New("demo", "default")
	step := bubuv1alpha1.Step{Name: "a"}
	if err := b.AddStep(step); err != nil {
		t.Fatalf("AddStep() error = %v", err)
	}
	dependent := bubuv1alpha1.Step{Name: "b", Needs: []string{"a"}}
	if err := b.AddStep(dependent); err != nil {
		t.Fatalf("AddStep() dependency error = %v", err)
	}
	invalid := bubuv1alpha1.Step{Name: "c", Needs: []string{"missing"}}
	if err := b.AddStep(invalid); err == nil {
		t.Fatalf("expected error for missing dependency")
	}
}

func TestBuilder_BuildRequiresSteps(t *testing.T) {
	b := New("demo", "default")
	if _, err := b.Build(); err == nil {
		t.Fatalf("expected build to fail without steps")
	}
}
