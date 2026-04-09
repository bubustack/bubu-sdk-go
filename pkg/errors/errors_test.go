package errors

import (
	stderrors "errors"
	"fmt"
	"testing"
)

func TestSentinelIdentity(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "retryable", err: ErrRetryable},
		{name: "conflict", err: ErrConflict},
		{name: "not found", err: ErrNotFound},
		{name: "nil context", err: ErrNilContext},
		{name: "invalid transition", err: ErrInvalidTransition},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !stderrors.Is(tt.err, tt.err) {
				t.Fatalf("expected sentinel %q to match itself via errors.Is", tt.name)
			}
			wrapped := fmt.Errorf("wrap: %w", tt.err)
			if !stderrors.Is(wrapped, tt.err) {
				t.Fatalf("expected wrapped sentinel %q to remain detectable via errors.Is", tt.name)
			}
		})
	}
}
