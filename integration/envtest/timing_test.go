package envtest

import (
	"testing"
	"time"
)

func TestParsePositiveDurationEnv(t *testing.T) {
	const key = "BUBU_ENVTEST_DURATION_TEST"
	const fallback = 250 * time.Millisecond

	t.Run("missing uses fallback", func(t *testing.T) {
		t.Setenv(key, "")
		got := parsePositiveDurationEnv(key, fallback)
		if got != fallback {
			t.Fatalf("expected fallback %s, got %s", fallback, got)
		}
	})

	t.Run("invalid uses fallback", func(t *testing.T) {
		t.Setenv(key, "not-a-duration")
		got := parsePositiveDurationEnv(key, fallback)
		if got != fallback {
			t.Fatalf("expected fallback %s, got %s", fallback, got)
		}
	})

	t.Run("non-positive uses fallback", func(t *testing.T) {
		t.Setenv(key, "0s")
		got := parsePositiveDurationEnv(key, fallback)
		if got != fallback {
			t.Fatalf("expected fallback %s, got %s", fallback, got)
		}

		t.Setenv(key, "-1s")
		got = parsePositiveDurationEnv(key, fallback)
		if got != fallback {
			t.Fatalf("expected fallback %s, got %s", fallback, got)
		}
	})

	t.Run("valid positive duration overrides fallback", func(t *testing.T) {
		t.Setenv(key, "125ms")
		got := parsePositiveDurationEnv(key, fallback)
		if got != 125*time.Millisecond {
			t.Fatalf("expected 125ms, got %s", got)
		}
	})
}

func TestWaitForSignal(t *testing.T) {
	t.Run("returns true when signal arrives", func(t *testing.T) {
		done := make(chan struct{})
		go func() {
			time.Sleep(5 * time.Millisecond)
			close(done)
		}()

		if ok := waitForSignal(done, 200*time.Millisecond); !ok {
			t.Fatalf("expected waitForSignal to return true")
		}
	})

	t.Run("returns false on timeout", func(t *testing.T) {
		done := make(chan struct{})
		if ok := waitForSignal(done, 10*time.Millisecond); ok {
			t.Fatalf("expected waitForSignal to return false")
		}
	})
}
