package k8s

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type triggerThrottlePolicy struct {
	ratePerSecond int
	burst         int
	maxInFlight   int
}

type triggerThrottle struct {
	limiter *rate.Limiter
	sem     chan struct{}
}

var (
	throttleMu   sync.Mutex
	throttleKey  string
	throttleImpl *triggerThrottle
)

func loadTriggerThrottlePolicyFromEnv() (*triggerThrottlePolicy, error) {
	rateRaw := strings.TrimSpace(os.Getenv(triggerThrottleRateEnv))
	burstRaw := strings.TrimSpace(os.Getenv(triggerThrottleBurstEnv))
	maxRaw := strings.TrimSpace(os.Getenv(triggerThrottleMaxInFlight))
	if rateRaw == "" && burstRaw == "" && maxRaw == "" {
		return nil, nil
	}
	policy := &triggerThrottlePolicy{}
	if rateRaw != "" {
		val, err := strconv.Atoi(rateRaw)
		if err != nil || val < 0 {
			return nil, fmt.Errorf("%s must be a non-negative integer", triggerThrottleRateEnv)
		}
		policy.ratePerSecond = val
	}
	if burstRaw != "" {
		val, err := strconv.Atoi(burstRaw)
		if err != nil || val < 0 {
			return nil, fmt.Errorf("%s must be a non-negative integer", triggerThrottleBurstEnv)
		}
		policy.burst = val
	}
	if maxRaw != "" {
		val, err := strconv.Atoi(maxRaw)
		if err != nil || val < 0 {
			return nil, fmt.Errorf("%s must be a non-negative integer", triggerThrottleMaxInFlight)
		}
		policy.maxInFlight = val
	}
	return policy, nil
}

func getTriggerThrottle() (*triggerThrottle, error) {
	policy, err := loadTriggerThrottlePolicyFromEnv()
	if err != nil || policy == nil {
		return nil, err
	}
	key := fmt.Sprintf("rate=%d;burst=%d;max=%d", policy.ratePerSecond, policy.burst, policy.maxInFlight)
	throttleMu.Lock()
	defer throttleMu.Unlock()
	if throttleImpl != nil && throttleKey == key {
		return throttleImpl, nil
	}
	throttleKey = key
	throttleImpl = newTriggerThrottle(policy)
	return throttleImpl, nil
}

func newTriggerThrottle(policy *triggerThrottlePolicy) *triggerThrottle {
	if policy == nil {
		return nil
	}
	var limiter *rate.Limiter
	if policy.ratePerSecond > 0 {
		burst := policy.burst
		if burst <= 0 {
			burst = policy.ratePerSecond
		}
		limiter = rate.NewLimiter(rate.Limit(policy.ratePerSecond), burst)
	}
	var sem chan struct{}
	if policy.maxInFlight > 0 {
		sem = make(chan struct{}, policy.maxInFlight)
	}
	if limiter == nil && sem == nil {
		return nil
	}
	return &triggerThrottle{limiter: limiter, sem: sem}
}

func (t *triggerThrottle) Acquire(ctx context.Context) (bool, func(), error) {
	if t == nil {
		return false, func() {}, nil
	}
	waited := false
	release := func() {}
	if t.sem != nil {
		select {
		case t.sem <- struct{}{}:
			release = func() { <-t.sem }
		default:
			waited = true
			select {
			case t.sem <- struct{}{}:
				release = func() { <-t.sem }
			case <-ctx.Done():
				return waited, nil, ctx.Err()
			}
		}
	}
	if t.limiter != nil {
		res := t.limiter.Reserve()
		if !res.OK() {
			release()
			return waited, nil, fmt.Errorf("trigger throttle reservation failed")
		}
		delay := res.Delay()
		if delay > 0 {
			waited = true
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				res.Cancel()
				release()
				return waited, nil, ctx.Err()
			}
		}
	}
	return waited, release, nil
}
