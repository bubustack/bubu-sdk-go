package k8s

import (
	"context"
	"errors"
	"net/url"
	"syscall"
	"testing"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	sdkerrors "github.com/bubustack/bubu-sdk-go/pkg/errors"
)

func TestClassifyK8sErrorRetryableTransportErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "connection reset",
			err: &url.Error{
				Op:  "Post",
				URL: "https://api.example.test/storyruns",
				Err: syscall.ECONNRESET,
			},
		},
		{
			name: "connection refused",
			err: &url.Error{
				Op:  "Post",
				URL: "https://api.example.test/storyruns",
				Err: syscall.ECONNREFUSED,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := classifyK8sError(tt.err); !errors.Is(err, sdkerrors.ErrRetryable) {
				t.Fatalf("expected ErrRetryable, got %v", err)
			}
		})
	}
}

func TestRetryTriggerStoryRetriesTransientTransportError(t *testing.T) {
	policy := &triggerRetryPolicy{
		maxAttempts: 3,
		baseDelay:   time.Millisecond,
		maxDelay:    time.Millisecond,
		backoff:     "constant",
	}

	attempts := 0
	expected := &runsv1alpha1.StoryRun{}
	got, err := retryTriggerStory(context.Background(), policy, func(context.Context) (*runsv1alpha1.StoryRun, error) {
		attempts++
		if attempts < 3 {
			return nil, &url.Error{
				Op:  "Post",
				URL: "https://api.example.test/storyruns",
				Err: syscall.ECONNRESET,
			}
		}
		return expected, nil
	})
	if err != nil {
		t.Fatalf("retryTriggerStory() error = %v", err)
	}
	if got != expected {
		t.Fatalf("retryTriggerStory() returned unexpected StoryRun pointer: got %+v want %+v", got, expected)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetryTriggerStoryDoesNotRetryPermanentError(t *testing.T) {
	policy := &triggerRetryPolicy{
		maxAttempts: 3,
		baseDelay:   time.Millisecond,
		maxDelay:    time.Millisecond,
		backoff:     "constant",
	}

	attempts := 0
	permanentErr := errors.New("invalid trigger payload")
	_, err := retryTriggerStory(context.Background(), policy, func(context.Context) (*runsv1alpha1.StoryRun, error) {
		attempts++
		return nil, permanentErr
	})
	if !errors.Is(err, permanentErr) {
		t.Fatalf("expected permanent error to be returned, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected permanent error to stop retries after 1 attempt, got %d", attempts)
	}
}
