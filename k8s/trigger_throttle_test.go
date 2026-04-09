package k8s

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLoadTriggerThrottlePolicyFromEnvEmpty(t *testing.T) {
	t.Setenv(triggerThrottleRateEnv, "")
	t.Setenv(triggerThrottleBurstEnv, "")
	t.Setenv(triggerThrottleMaxInFlight, "")
	policy, err := loadTriggerThrottlePolicyFromEnv()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if policy != nil {
		t.Fatalf("expected nil policy, got %#v", policy)
	}
}

func TestLoadTriggerThrottlePolicyFromEnvInvalid(t *testing.T) {
	t.Setenv(triggerThrottleRateEnv, "-1")
	if _, err := loadTriggerThrottlePolicyFromEnv(); err == nil {
		t.Fatal("expected error for negative ratePerSecond")
	}
}

func TestNewTriggerThrottleDefaultsBurst(t *testing.T) {
	policy := &triggerThrottlePolicy{ratePerSecond: 5}
	throttle := newTriggerThrottle(policy)
	if throttle == nil || throttle.limiter == nil {
		t.Fatal("expected limiter to be created")
	}
	if got := throttle.limiter.Burst(); got != 5 {
		t.Fatalf("expected burst to default to rate (5), got %d", got)
	}
}

func TestTriggerThrottleAcquireWaitsForSemaphoreRelease(t *testing.T) {
	throttle := newTriggerThrottle(&triggerThrottlePolicy{maxInFlight: 1})
	if throttle == nil {
		t.Fatal("expected throttle to be created")
	}

	waited, release, err := throttle.Acquire(context.Background())
	if err != nil {
		t.Fatalf("first Acquire() failed: %v", err)
	}
	if waited {
		t.Fatal("first Acquire() should not report waiting")
	}
	if release == nil {
		t.Fatal("first Acquire() should return release func")
	}

	type acquireResult struct {
		waited  bool
		release func()
		err     error
	}
	resultCh := make(chan acquireResult, 1)
	go func() {
		waited, release, err := throttle.Acquire(context.Background())
		resultCh <- acquireResult{waited: waited, release: release, err: err}
	}()

	select {
	case result := <-resultCh:
		if result.release != nil {
			result.release()
		}
		t.Fatal("second Acquire() should block until the first release")
	case <-time.After(20 * time.Millisecond):
	}

	release()

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("second Acquire() failed: %v", result.err)
		}
		if !result.waited {
			t.Fatal("second Acquire() should report waiting under semaphore contention")
		}
		if result.release == nil {
			t.Fatal("second Acquire() should return release func")
		}
		result.release()
	case <-time.After(200 * time.Millisecond):
		t.Fatal("second Acquire() did not resume after release")
	}

	if got := len(throttle.sem); got != 0 {
		t.Fatalf("expected semaphore to be fully released, got %d in-flight", got)
	}
}

func TestTriggerThrottleAcquireCancelsWhileWaitingOnSemaphore(t *testing.T) {
	throttle := newTriggerThrottle(&triggerThrottlePolicy{maxInFlight: 1})
	if throttle == nil {
		t.Fatal("expected throttle to be created")
	}

	_, release, err := throttle.Acquire(context.Background())
	if err != nil {
		t.Fatalf("first Acquire() failed: %v", err)
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	waited, release2, err := throttle.Acquire(ctx)
	if !waited {
		t.Fatal("Acquire() should report waiting when blocked on semaphore")
	}
	if release2 != nil {
		t.Fatal("Acquire() should not return release func on cancellation")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline error, got %v", err)
	}
}

func TestTriggerThrottleAcquireCancelsDuringLimiterWaitAndReleasesSemaphore(t *testing.T) {
	throttle := newTriggerThrottle(&triggerThrottlePolicy{
		ratePerSecond: 1,
		burst:         1,
		maxInFlight:   1,
	})
	if throttle == nil {
		t.Fatal("expected throttle to be created")
	}

	_, release, err := throttle.Acquire(context.Background())
	if err != nil {
		t.Fatalf("initial Acquire() failed: %v", err)
	}
	release()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	waited, release2, err := throttle.Acquire(ctx)
	if !waited {
		t.Fatal("Acquire() should report waiting when limiter delay is non-zero")
	}
	if release2 != nil {
		t.Fatal("Acquire() should not return release func on limiter cancellation")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline error, got %v", err)
	}
	if got := len(throttle.sem); got != 0 {
		t.Fatalf("expected limiter cancellation to release semaphore, got %d in-flight", got)
	}
}
