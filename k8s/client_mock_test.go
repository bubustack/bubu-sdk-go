package k8s

import (
	"context"
	"strings"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
)

func TestMockClientTriggerStoryReturnsTypedStoryRun(t *testing.T) {
	m := &MockClient{}
	expected := &runsv1alpha1.StoryRun{}
	inputs := map[string]any{"key": "value"}
	m.On("TriggerStory", context.Background(), "story", "ns", inputs).Return(expected, nil)

	got, err := m.TriggerStory(context.Background(), "story", "ns", inputs)
	if err != nil {
		t.Fatalf("TriggerStory() error = %v", err)
	}
	if got != expected {
		t.Fatalf("TriggerStory() returned unexpected StoryRun: got %+v want %+v", got, expected)
	}
}

func TestMockClientTriggerStoryRejectsWrongReturnType(t *testing.T) {
	m := &MockClient{}
	m.On("TriggerStory", context.Background(), "story", "ns", map[string]any(nil)).Return("not-a-storyrun", nil)

	got, err := m.TriggerStory(context.Background(), "story", "ns", nil)
	if got != nil {
		t.Fatalf("expected nil StoryRun on wrong type, got %+v", got)
	}
	if err == nil {
		t.Fatal("expected descriptive error for wrong mock return type")
	}
	if !strings.Contains(err.Error(), "expected *runsv1alpha1.StoryRun") {
		t.Fatalf("unexpected error: %v", err)
	}
}
