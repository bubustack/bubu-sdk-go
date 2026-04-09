package engram

import "testing"

func TestIdempotencyKey(t *testing.T) {
	info := StoryInfo{
		StoryRunID:       "story-1",
		StepRunID:        "step-1",
		StepRunNamespace: "default",
	}
	got, err := IdempotencyKey(info)
	if err != nil {
		t.Fatalf("IdempotencyKey() error = %v", err)
	}
	if got != "ns/default/storyrun/story-1/steprun/step-1" {
		t.Fatalf("IdempotencyKey() = %q", got)
	}

	info.StepRunNamespace = ""
	got, err = IdempotencyKey(info)
	if err != nil {
		t.Fatalf("IdempotencyKey() without namespace error = %v", err)
	}
	if got != "storyrun/story-1/steprun/step-1" {
		t.Fatalf("IdempotencyKey() without namespace = %q", got)
	}
}

func TestIdempotencyKey_MissingIdentity(t *testing.T) {
	if _, err := IdempotencyKey(StoryInfo{}); err == nil {
		t.Fatal("expected error for missing identity")
	}
}
