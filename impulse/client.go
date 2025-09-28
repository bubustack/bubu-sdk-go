package impulse

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
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

// Client is a wrapper around the Kubernetes client that is scoped to the
// namespace of the running Impulse and provides helper methods for creating
// StoryRuns.
type Client struct {
	kubeClient client.Client
	namespace  string
	storyName  string
}

// NewClient creates a new Impulse client. It's intended to be called by the
// SDK runtime.
func NewClient(storyName string) (*Client, error) {
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

	return &Client{
		kubeClient: kubeClient,
		namespace:  getPodNamespace(),
		storyName:  storyName,
	}, nil
}

// CreateStoryRun creates a new StoryRun resource in the cluster. The StoryRun's
// namespace will be automatically set to the Impulse's own namespace.
func (c *Client) CreateStoryRun(ctx context.Context, storyRun *runsv1alpha1.StoryRun) error {
	storyRun.Namespace = c.namespace
	if err := c.kubeClient.Create(ctx, storyRun); err != nil {
		return fmt.Errorf("failed to create storyrun: %w", err)
	}
	return nil
}

// TriggerStory creates a new StoryRun for the configured story with the provided inputs.
// It handles the construction of the StoryRun object.
func (c *Client) TriggerStory(ctx context.Context, inputs map[string]interface{}) (*runsv1alpha1.StoryRun, error) {
	// Marshal the inputs map into a RawExtension.
	inputBytes, err := json.Marshal(inputs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal inputs: %w", err)
	}

	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: v1.ObjectMeta{
			Namespace:    c.namespace,
			GenerateName: fmt.Sprintf("%s-", c.storyName),
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{
					Name: c.storyName,
				},
			},
			Inputs: &runtime.RawExtension{Raw: inputBytes},
		},
	}

	if err := c.kubeClient.Create(ctx, storyRun); err != nil {
		return nil, fmt.Errorf("failed to create storyrun: %w", err)
	}
	return storyRun, nil
}

// getPodNamespace gets the namespace of the pod where this code is running.
func getPodNamespace() string {
	if ns, ok := os.LookupEnv("POD_NAMESPACE"); ok {
		return ns
	}
	// Fallback for local testing.
	return "default"
}
