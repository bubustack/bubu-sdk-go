package sdk_test

import (
	"context"
	"errors"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sdk "github.com/bubustack/bubu-sdk-go"
)

type stubStoryRuntime struct {
	startFn func(
		ctx context.Context,
		storyName string,
		storyNamespace string,
		inputs map[string]any,
	) (*runsv1alpha1.StoryRun, error)
	stopFn func(ctx context.Context, storyRunName, storyNamespace string) error
}

func (s stubStoryRuntime) start(
	ctx context.Context,
	storyName string,
	storyNamespace string,
	inputs map[string]any,
) (*runsv1alpha1.StoryRun, error) {
	if s.startFn != nil {
		return s.startFn(ctx, storyName, storyNamespace, inputs)
	}
	return &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-run",
			Namespace: "default",
		},
	}, nil
}

func (s stubStoryRuntime) stop(ctx context.Context, storyRunName, storyNamespace string) error {
	if s.stopFn != nil {
		return s.stopFn(ctx, storyRunName, storyNamespace)
	}
	return nil
}

func TestStoryDispatcher_TriggerAndStop(t *testing.T) {
	stub := stubStoryRuntime{}
	dispatcher := sdk.NewStoryDispatcher(
		sdk.WithStoryRuntime(stub.start, stub.stop),
	)

	ctx := context.Background()
	res, err := dispatcher.Trigger(ctx, sdk.StoryTriggerRequest{
		Key:       "room-1",
		StoryName: "demo-story",
	})
	if err != nil {
		t.Fatalf("Trigger() error = %v", err)
	}
	if res.StoryRun == nil {
		t.Fatal("Trigger() returned nil StoryRun")
	}
	if !dispatcher.HasSession("room-1") {
		t.Fatal("expected session to be tracked after Trigger")
	}

	session, err := dispatcher.Stop(ctx, "room-1")
	if err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if session == nil || session.Key != "room-1" {
		t.Fatalf("Stop() returned unexpected session: %#v", session)
	}
	if dispatcher.HasSession("room-1") {
		t.Fatal("expected session to be cleared after Stop")
	}
}

func TestStoryDispatcher_DuplicateKey(t *testing.T) {
	dispatcher := sdk.NewStoryDispatcher(
		sdk.WithStoryRuntime(
			func(
				ctx context.Context,
				storyName string,
				storyNamespace string,
				inputs map[string]any,
			) (*runsv1alpha1.StoryRun, error) {
				return &runsv1alpha1.StoryRun{
					ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "default"},
				}, nil
			},
			func(ctx context.Context, storyRunName, storyNamespace string) error { return nil },
		),
	)

	ctx := context.Background()
	if _, err := dispatcher.Trigger(ctx, sdk.StoryTriggerRequest{Key: "dup", StoryName: "demo"}); err != nil {
		t.Fatalf("initial Trigger() failed: %v", err)
	}
	if _, err := dispatcher.Trigger(
		ctx,
		sdk.StoryTriggerRequest{Key: "dup", StoryName: "demo"},
	); !errors.Is(err, sdk.ErrImpulseSessionExists) {
		t.Fatalf("expected ErrImpulseSessionExists, got %v", err)
	}
}

func TestStoryDispatcher_StopNotFound(t *testing.T) {
	stopped := false
	dispatcher := sdk.NewStoryDispatcher(
		sdk.WithStoryRuntime(
			func(
				ctx context.Context,
				storyName string,
				storyNamespace string,
				inputs map[string]any,
			) (*runsv1alpha1.StoryRun, error) {
				return &runsv1alpha1.StoryRun{
					ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "default"},
				}, nil
			},
			func(ctx context.Context, storyRunName, storyNamespace string) error {
				stopped = true
				return sdk.ErrStoryRunNotFound
			},
		),
	)

	ctx := context.Background()
	if _, err := dispatcher.Trigger(ctx, sdk.StoryTriggerRequest{Key: "room-1", StoryName: "demo"}); err != nil {
		t.Fatalf("Trigger() failed: %v", err)
	}

	session, err := dispatcher.Stop(ctx, "room-1")
	if !errors.Is(err, sdk.ErrStoryRunNotFound) {
		t.Fatalf("expected ErrStoryRunNotFound, got %v", err)
	}
	if session == nil || session.StoryRun != "run-1" {
		t.Fatalf("unexpected session returned from Stop: %#v", session)
	}
	if !stopped {
		t.Fatal("stop runtime was not invoked")
	}
	if dispatcher.HasSession("room-1") {
		t.Fatal("session should not be reinserted when storyrun is already gone")
	}
}

func TestStoryDispatcher_SessionCloning(t *testing.T) {
	dispatcher := sdk.NewStoryDispatcher(
		sdk.WithStoryRuntime(
			func(ctx context.Context, storyName, storyNamespace string, inputs map[string]any) (*runsv1alpha1.StoryRun, error) {
				return &runsv1alpha1.StoryRun{
					ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "default"},
				}, nil
			},
			func(ctx context.Context, storyRunName, storyNamespace string) error { return nil },
		),
	)
	_, err := dispatcher.Trigger(
		context.Background(),
		sdk.StoryTriggerRequest{
			Key:       "a",
			StoryName: "demo",
			Metadata:  map[string]string{"policy": "welcome"},
		},
	)
	if err != nil {
		t.Fatalf("Trigger() failed: %v", err)
	}

	got, ok := dispatcher.Session("a")
	if !ok || got == nil {
		t.Fatalf("Session() failed, ok=%v value=%v", ok, got)
	}
	got.Metadata["policy"] = "mutated"

	next, ok := dispatcher.Session("a")
	if !ok || next.Metadata["policy"] != "welcome" {
		t.Fatalf("Session metadata should be cloned, got %#v", next.Metadata)
	}
}
