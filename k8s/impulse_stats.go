package k8s

import (
	"context"
	"fmt"
	"strings"
	"time"

	v1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TriggerStatsDelta describes the per-trigger counters to apply to an Impulse status.
type TriggerStatsDelta struct {
	// TriggersReceived increments the total number of trigger attempts observed by the impulse.
	TriggersReceived int64
	// StoriesLaunched increments the total number of StoryRuns successfully created.
	StoriesLaunched int64
	// FailedTriggers increments the total number of trigger attempts that failed before launch.
	FailedTriggers int64
	// ThrottledTriggers increments the total number of triggers rejected by throttling policy.
	ThrottledTriggers int64
	// LastTrigger records when the most recent trigger attempt was received.
	LastTrigger time.Time
	// LastSuccess records when the most recent successful StoryRun launch completed.
	LastSuccess *time.Time
	// LastThrottled records when the most recent trigger was throttled.
	LastThrottled *time.Time
}

func (d TriggerStatsDelta) isZero() bool {
	lastSuccessEmpty := d.LastSuccess == nil || d.LastSuccess.IsZero()
	lastThrottledEmpty := d.LastThrottled == nil || d.LastThrottled.IsZero()
	return d.TriggersReceived == 0 &&
		d.StoriesLaunched == 0 &&
		d.FailedTriggers == 0 &&
		d.ThrottledTriggers == 0 &&
		d.LastTrigger.IsZero() &&
		lastSuccessEmpty &&
		lastThrottledEmpty
}

func (d TriggerStatsDelta) validate() error {
	switch {
	case d.TriggersReceived < 0:
		return fmt.Errorf("trigger stats delta TriggersReceived must not be negative")
	case d.StoriesLaunched < 0:
		return fmt.Errorf("trigger stats delta StoriesLaunched must not be negative")
	case d.FailedTriggers < 0:
		return fmt.Errorf("trigger stats delta FailedTriggers must not be negative")
	case d.ThrottledTriggers < 0:
		return fmt.Errorf("trigger stats delta ThrottledTriggers must not be negative")
	default:
		return nil
	}
}

// UpdateImpulseTriggerStats applies the provided delta to the impulse status counters.
// Callers need RBAC for `impulses` `get` and `impulses/status` `patch`.
// Uses retry-on-conflict to avoid lost updates when the controller patches the same
// Impulse status concurrently.
func (c *Client) UpdateImpulseTriggerStats(
	ctx context.Context,
	impulseName string,
	namespace string,
	delta TriggerStatsDelta,
) error {
	if ctx == nil {
		return fmt.Errorf("context must not be nil")
	}
	impulseName = strings.TrimSpace(impulseName)
	if impulseName == "" {
		return fmt.Errorf("impulse name is required")
	}
	if err := delta.validate(); err != nil {
		return err
	}
	if delta.isZero() {
		return nil
	}

	targetNamespace := strings.TrimSpace(namespace)
	if targetNamespace == "" {
		targetNamespace = c.namespace
	}
	if targetNamespace == "" {
		return fmt.Errorf("failed to resolve namespace for impulse %q", impulseName)
	}

	key := types.NamespacedName{Name: impulseName, Namespace: targetNamespace}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		apiCtx, cancel := context.WithTimeout(ctx, getOperationTimeout())
		defer cancel()

		var impulse v1alpha1.Impulse
		if err := c.Get(apiCtx, key, &impulse); err != nil {
			return wrapK8sError(err, "failed to get impulse '%s' in namespace '%s'", impulseName, targetNamespace)
		}

		before := impulse.DeepCopy()
		status := &impulse.Status
		status.ObservedGeneration = impulse.Generation
		status.TriggersReceived += delta.TriggersReceived
		status.StoriesLaunched += delta.StoriesLaunched
		status.FailedTriggers += delta.FailedTriggers
		status.ThrottledTriggers += delta.ThrottledTriggers

		status.LastTrigger = advanceTimestamp(status.LastTrigger, &delta.LastTrigger)
		status.LastSuccess = advanceTimestamp(status.LastSuccess, delta.LastSuccess)
		status.LastThrottled = advanceTimestamp(status.LastThrottled, delta.LastThrottled)

		return c.Status().Patch(apiCtx, &impulse, client.MergeFrom(before))
	})
}

// advanceTimestamp returns the newer of current and candidate. If candidate is
// nil or zero it returns current unchanged.
func advanceTimestamp(current *metav1.Time, candidate *time.Time) *metav1.Time {
	if candidate == nil || candidate.IsZero() {
		return current
	}
	mt := metav1.NewTime(candidate.UTC())
	if current == nil || current.Before(&metav1.Time{Time: candidate.UTC()}) {
		return &mt
	}
	return current
}
