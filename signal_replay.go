package sdk

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/bubustack/core/contracts"
	"k8s.io/apimachinery/pkg/types"
)

type signalReplayClient interface {
	GetNamespace() string
	GetStepRun(ctx context.Context, key types.NamespacedName, stepRun *runsv1alpha1.StepRun) error
}

type sharedSignalReplayClient struct {
	client *k8s.Client
}

func (c sharedSignalReplayClient) GetNamespace() string {
	if c.client == nil {
		return ""
	}
	return c.client.GetNamespace()
}

func (c sharedSignalReplayClient) GetStepRun(
	ctx context.Context,
	key types.NamespacedName,
	stepRun *runsv1alpha1.StepRun,
) error {
	return c.client.Get(ctx, key, stepRun)
}

var signalReplayClientFactory = func() (signalReplayClient, error) {
	client, err := k8s.SharedClient()
	if err != nil {
		return nil, err
	}
	return sharedSignalReplayClient{client: client}, nil
}

// ReplaySignals returns the signal events for a StepRun, optionally filtered by sequence.
// When stepRunName or namespace are empty, environment defaults are used.
func ReplaySignals(
	ctx context.Context,
	stepRunName,
	namespace string,
	sinceSeq uint64) ([]runsv1alpha1.SignalEvent,
	error,
) {
	if ctx == nil {
		ctx = context.Background()
	}
	stepRunName = strings.TrimSpace(stepRunName)
	if stepRunName == "" {
		stepRunName = strings.TrimSpace(os.Getenv(contracts.StepRunNameEnv))
	}
	if stepRunName == "" {
		return nil, ErrSignalsUnavailable
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = strings.TrimSpace(os.Getenv(contracts.StepRunNamespaceEnv))
	}
	client, err := signalReplayClientFactory()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize signal replay client: %w", err)
	}
	if namespace == "" {
		namespace = client.GetNamespace()
		if namespace == "" {
			return nil, ErrSignalsUnavailable
		}
	}

	var stepRun runsv1alpha1.StepRun
	if err := client.GetStepRun(ctx, types.NamespacedName{Name: stepRunName, Namespace: namespace}, &stepRun); err != nil {
		return nil, fmt.Errorf("failed to fetch StepRun for signal replay: %w", err)
	}

	if len(stepRun.Status.SignalEvents) == 0 {
		return nil, nil
	}

	filtered := make([]runsv1alpha1.SignalEvent, 0, len(stepRun.Status.SignalEvents))
	for _, evt := range stepRun.Status.SignalEvents {
		if sinceSeq > 0 && evt.Seq <= sinceSeq {
			continue
		}
		filtered = append(filtered, evt)
	}
	if len(filtered) == 0 {
		return nil, nil
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].Seq < filtered[j].Seq })
	return filtered, nil
}
