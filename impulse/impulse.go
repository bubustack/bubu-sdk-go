package impulse

import (
	"context"
	"fmt"
	"os"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(runsv1alpha1.AddToScheme(scheme))
}

// CreateStoryRun creates a new StoryRun resource in the Kubernetes cluster.
// It is intended to be used by Impulse workloads running within a pod.
// It handles the in-cluster client configuration and API interaction.
func CreateStoryRun(ctx context.Context, storyRun *runsv1alpha1.StoryRun) error {
	// If the namespace is not provided, default it to the pod's own namespace.
	// This is the "silent" behavior the user expects from the SDK.
	if storyRun.Namespace == "" {
		storyRun.Namespace = getPodNamespace()
	}

	// Get in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	// Create a new Kubernetes client
	kubeClient, err := client.New(config, client.Options{
		Scheme: scheme,
	})
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create the StoryRun resource
	if err := kubeClient.Create(ctx, storyRun); err != nil {
		return fmt.Errorf("failed to create storyrun: %w", err)
	}

	return nil
}

// CreateAndWatchStoryRun creates a StoryRun and then watches it until it reaches a terminal phase.
// It returns the completed StoryRun object, allowing the caller to inspect the final status and outputs.
// This is designed for synchronous Impulse patterns where the caller needs to wait for a result.
func CreateAndWatchStoryRun(ctx context.Context, storyRun *runsv1alpha1.StoryRun) (*runsv1alpha1.StoryRun, error) {
	// If the namespace is not provided, default it to the pod's own namespace.
	if storyRun.Namespace == "" {
		storyRun.Namespace = getPodNamespace()
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	kubeClient, err := client.New(config, client.Options{
		Scheme: scheme,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// First, create the StoryRun
	if err := kubeClient.Create(ctx, storyRun); err != nil {
		return nil, fmt.Errorf("failed to create storyrun for watching: %w", err)
	}

	// Now, set up a watch on the StoryRun we just created
	watcher, err := kubeClient.Watch(ctx, storyRun, client.InNamespace(storyRun.Namespace))
	if err != nil {
		return nil, fmt.Errorf("failed to create watch for storyrun %s: %w", storyRun.Name, err)
	}
	defer watcher.Stop()

	for {
		select {
		case event := <-watcher.ResultChan():
			if event.Type == watch.Error {
				return nil, fmt.Errorf("watch error for storyrun %s", storyRun.Name)
			}

			sr, ok := event.Object.(*runsv1alpha1.StoryRun)
			if !ok {
				continue // Not a StoryRun object
			}

			// Check if the StoryRun has reached a terminal phase
			if sr.Status.Phase.IsTerminal() {
				return sr, nil // Success! Return the completed object.
			}

		case <-ctx.Done():
			return nil, ctx.Err() // Context was cancelled (e.g., client timeout)
		}
	}
}

// getPodNamespace gets the namespace of the pod where this code is running.
// Kubernetes automatically injects this information via the Downward API,
// which is configured by the Impulse controller.
func getPodNamespace() string {
	if ns, ok := os.LookupEnv("POD_NAMESPACE"); ok {
		return ns
	}
	// Fallback for local testing where the env var might not be present.
	return "default"
}
