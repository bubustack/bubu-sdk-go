package k8s

import (
	"context"
	"fmt"
	"strings"
	"time"

	v1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TriggerStatsDelta describes the per-trigger counters to apply to an Impulse status.
type TriggerStatsDelta struct {
	TriggersReceived int64
	StoriesLaunched  int64
	FailedTriggers   int64
	LastTrigger      time.Time
	LastSuccess      *time.Time
}

func (d TriggerStatsDelta) isZero() bool {
	lastSuccessEmpty := d.LastSuccess == nil || d.LastSuccess.IsZero()
	return d.TriggersReceived == 0 &&
		d.StoriesLaunched == 0 &&
		d.FailedTriggers == 0 &&
		d.LastTrigger.IsZero() &&
		lastSuccessEmpty
}

// UpdateImpulseTriggerStats applies the provided delta to the impulse status counters.
func (c *Client) UpdateImpulseTriggerStats(
	ctx context.Context,
	impulseName string,
	namespace string,
	delta TriggerStatsDelta,
) error {
	if impulseName == "" || delta.isZero() {
		return nil
	}

	targetNamespace := strings.TrimSpace(namespace)
	if targetNamespace == "" {
		targetNamespace = c.namespace
	}
	if targetNamespace == "" {
		return fmt.Errorf("failed to resolve namespace for impulse %q", impulseName)
	}

	apiCtx, cancel := context.WithTimeout(ctx, getOperationTimeout())
	defer cancel()

	key := types.NamespacedName{Name: impulseName, Namespace: targetNamespace}
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

	if !delta.LastTrigger.IsZero() {
		ts := metav1.NewTime(delta.LastTrigger.UTC())
		status.LastTrigger = &ts
	}
	if delta.LastSuccess != nil && !delta.LastSuccess.IsZero() {
		ts := metav1.NewTime(delta.LastSuccess.UTC())
		status.LastSuccess = &ts
	}

	if err := c.Status().Patch(apiCtx, &impulse, client.MergeFrom(before)); err != nil {
		return wrapK8sError(err, "failed to patch impulse '%s' trigger stats", impulseName)
	}

	return nil
}
