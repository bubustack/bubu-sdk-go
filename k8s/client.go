package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bubustack/bobrapet/pkg/refs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(runsv1alpha1.AddToScheme(scheme))
}

// Client is a wrapper around the controller-runtime Kubernetes client.
type Client struct {
	client.Client
	namespace string
}

// NewClient creates a new Kubernetes client with an in-cluster configuration.
func NewClient() (*Client, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		// Fallback to KUBECONFIG for local development.
		kubeconfigPath := os.Getenv("KUBECONFIG")
		if kubeconfigPath == "" {
			// Default kubeconfig path
			home, _ := os.UserHomeDir()
			kubeconfigPath = fmt.Sprintf("%s/.kube/config", home)
		}
		if _, statErr := os.Stat(kubeconfigPath); statErr == nil {
			if cfg, buildErr := clientcmd.BuildConfigFromFlags("", kubeconfigPath); buildErr == nil {
				config = cfg
			} else {
				return nil, fmt.Errorf("failed to build kubeconfig from %s: %w", kubeconfigPath, buildErr)
			}
		} else {
			return nil, fmt.Errorf("failed to get in-cluster config and no kubeconfig at %s: %w", kubeconfigPath, err)
		}
	}

	kubeClient, err := client.New(config, client.Options{
		Scheme: scheme,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	return &Client{
		Client:    kubeClient,
		namespace: getPodNamespace(),
	}, nil
}

func (c *Client) GetNamespace() string {
	return c.namespace
}

// TriggerStory creates a new StoryRun for the configured story with the provided inputs.
func (c *Client) TriggerStory(ctx context.Context, storyName string, inputs map[string]interface{}) (*runsv1alpha1.StoryRun, error) {
	inputBytes, err := json.Marshal(inputs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal inputs: %w", err)
	}

	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    c.namespace,
			GenerateName: fmt.Sprintf("%s-", storyName),
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{
					Name: storyName,
				},
			},
			Inputs: &runtime.RawExtension{Raw: inputBytes},
		},
	}

	if err := c.Create(ctx, storyRun); err != nil {
		return nil, fmt.Errorf("failed to create storyrun: %w", err)
	}
	return storyRun, nil
}

func (c *Client) PatchStepRunStatus(ctx context.Context, stepRunName string, patchData runsv1alpha1.StepRunStatus) error {
	stepRun := &runsv1alpha1.StepRun{}
	if err := c.Get(ctx, types.NamespacedName{Name: stepRunName, Namespace: c.namespace}, stepRun); err != nil {
		return fmt.Errorf("failed to get StepRun '%s' for status patch: %w", stepRunName, err)
	}

	patch := client.MergeFrom(stepRun.DeepCopy())
	stepRun.Status = patchData
	if err := c.Status().Patch(ctx, stepRun, patch); err != nil {
		return fmt.Errorf("failed to patch StepRun '%s' status: %w", stepRunName, err)
	}
	return nil
}

// getPodNamespace gets the namespace of the pod where this code is running.
func getPodNamespace() string {
	if ns, ok := os.LookupEnv("BUBU_TARGET_STORY_NAMESPACE"); ok && ns != "" {
		return ns
	}
	if ns, ok := os.LookupEnv("BUBU_IMPULSE_NAMESPACE"); ok && ns != "" {
		return ns
	}
	if ns, ok := os.LookupEnv("BUBU_STEPRUN_NAMESPACE"); ok && ns != "" {
		return ns
	}
	if ns, ok := os.LookupEnv("BUBU_POD_NAMESPACE"); ok && ns != "" {
		return ns
	}
	if ns, ok := os.LookupEnv("POD_NAMESPACE"); ok {
		return ns
	}
	// Fallback for local testing.
	return "default"
}
