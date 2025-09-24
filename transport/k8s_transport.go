// Package transport provides the different mechanisms (Kubernetes API, gRPC, etc.)
// that the SDK runtime uses to communicate with the outside world.
package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bubu-sdk-go/engram"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/scheme"
)

// RawExecutionContext holds the unprocessed data fetched by the transport layer.
// The runtime will use this to build the rich, developer-facing ExecutionContext.
type RawExecutionContext struct {
	Inputs           map[string]interface{}
	StoryRunID       string
	StepRunID        string
	StepName         string
	StoryName        string
	DownstreamTarget []runsv1alpha1.DownstreamTarget
}

// KubernetesTransport implements the primary communication channel for BatchEngrams.
type KubernetesTransport struct {
	client    *rest.RESTClient
	clientset *kubernetes.Clientset
	namespace string
	stepRunID string
}

// NewKubernetesTransport creates a new transport for communicating with the Kubernetes API
func NewKubernetesTransport(stepRunName, stepRunNamespace string) (*KubernetesTransport, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	// Add our custom scheme to the default scheme builder.
	// This is necessary for the REST client to know how to encode and decode our CRDs.
	runsv1alpha1.AddToScheme(scheme.Scheme)

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	// Create a custom REST client for our CRD
	config.ContentConfig.GroupVersion = &runsv1alpha1.GroupVersion
	config.APIPath = "/apis"
	config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	config.UserAgent = rest.DefaultKubernetesUserAgent()

	restClient, err := rest.RESTClientFor(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST client for CRDs: %w", err)
	}

	return &KubernetesTransport{
		client:    restClient,
		clientset: clientset,
		namespace: stepRunNamespace,
		stepRunID: stepRunName,
	}, nil
}

// GetExecutionContext fetches the full StepRun resource.
func (k *KubernetesTransport) GetExecutionContext(ctx context.Context) (*RawExecutionContext, error) {
	var stepRun runsv1alpha1.StepRun
	err := k.client.Get().
		Namespace(k.namespace).
		Resource("stepruns").
		Name(k.stepRunID).
		Do(ctx).
		Into(&stepRun)

	if err != nil {
		return nil, fmt.Errorf("failed to get StepRun '%s' from API server: %w", k.stepRunID, err)
	}

	var inputs map[string]interface{}
	if stepRun.Spec.Input != nil && len(stepRun.Spec.Input.Raw) > 0 {
		if err := json.Unmarshal(stepRun.Spec.Input.Raw, &inputs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal inputs: %w", err)
		}
	}

	storyName := stepRun.Spec.StoryRunRef.Name
	if name, ok := stepRun.Labels["bubu.sh/story-name"]; ok {
		storyName = name
	}

	return &RawExecutionContext{
		Inputs:           inputs,
		StoryRunID:       stepRun.Spec.StoryRunRef.Name,
		StepRunID:        stepRun.Name,
		StepName:         stepRun.Spec.StepID,
		StoryName:        storyName,
		DownstreamTarget: stepRun.Spec.DownstreamTargets,
	}, nil
}

// UpdateResult sends a PATCH request to update the StepRun status.
func (k *KubernetesTransport) UpdateResult(ctx context.Context, result *engram.Result) error {
	patch := k.buildStatusPatch(result)
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal status patch: %w", err)
	}

	err = k.client.Patch(types.MergePatchType).
		Namespace(k.namespace).
		Resource("stepruns").
		Name(k.stepRunID).
		SubResource("status").
		Body(patchBytes).
		Do(ctx).
		Error()

	// Handle race condition where the status was updated by another process
	if errors.IsConflict(err) {
		// Just re-fetch and try again once.
		time.Sleep(100 * time.Millisecond) // Small backoff
		err = k.client.Patch(types.MergePatchType).
			Namespace(k.namespace).
			Resource("stepruns").
			Name(k.stepRunID).
			SubResource("status").
			Body(patchBytes).
			Do(ctx).
			Error()
	}

	if err != nil {
		return fmt.Errorf("failed to patch StepRun status after retries: %w", err)
	}
	return nil
}

func (k *KubernetesTransport) buildStatusPatch(result *engram.Result) map[string]interface{} {
	status := map[string]interface{}{
		"finishedAt": metav1.Now().Format(time.RFC3339),
	}

	if result.Error != nil {
		status["phase"] = "Failed"
		status["lastFailureMsg"] = result.Error.Error()
	} else {
		status["phase"] = "Succeeded"
	}

	if result.Data != nil {
		raw, err := json.Marshal(result.Data)
		if err != nil {
			status["phase"] = "Failed"
			status["lastFailureMsg"] = "Failed to marshal output data: " + err.Error()
		} else {
			status["output"] = json.RawMessage(raw)
		}
	}

	return map[string]interface{}{"status": status}
}
