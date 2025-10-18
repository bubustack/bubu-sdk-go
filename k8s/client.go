package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bubu-sdk-go/pkg/kube/apply"
	"github.com/bubustack/bubu-sdk-go/pkg/metrics"
	"github.com/bubustack/bubu-sdk-go/storage"
	"github.com/google/uuid"
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

// Client is a wrapper around the controller-runtime Kubernetes client that provides
// SDK-specific functionality for interacting with bobrapet resources.
type Client struct {
	client.Client
	namespace string
}

// NewClient creates a new Kubernetes client with an in-cluster configuration.
func NewClient() (*Client, error) {
	config, err := GetConfig()
	if err != nil {
		return nil, err
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

// GetConfig creates a new Kubernetes REST config, applying SDK-specific defaults
// and environment variable overrides. This function is exposed to allow consumers
// to create their own clients with the same settings.
func GetConfig() (*rest.Config, error) {
	config, err := buildBaseRestConfig()
	if err != nil {
		return nil, err
	}
	applyDefaultRestConfigSettings(config)
	applyEnvOverridesToRestConfig(config)
	config.UserAgent = resolveUserAgent()
	return config, nil
}

// buildBaseRestConfig returns in-cluster config or falls back to KUBECONFIG.
func buildBaseRestConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	kubeconfigPath := os.Getenv("KUBECONFIG")
	if kubeconfigPath == "" {
		home, _ := os.UserHomeDir()
		kubeconfigPath = fmt.Sprintf("%s/.kube/config", home)
	}
	if _, statErr := os.Stat(kubeconfigPath); statErr != nil {
		return nil, fmt.Errorf(
			"failed to get in-cluster config and no kubeconfig at %s: %w",
			kubeconfigPath, statErr,
		)
	}
	cfg, buildErr := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if buildErr != nil {
		return nil, fmt.Errorf("failed to build kubeconfig from %s: %w", kubeconfigPath, buildErr)
	}
	return cfg, nil
}

// applyDefaultRestConfigSettings sets controller-safe defaults.
func applyDefaultRestConfigSettings(config *rest.Config) {
	if config.QPS == 0 {
		config.QPS = 20
	}
	if config.Burst == 0 {
		config.Burst = 40
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
}

// applyEnvOverridesToRestConfig applies env var overrides for QPS/Burst/Timeout.
func applyEnvOverridesToRestConfig(config *rest.Config) {
	if v := os.Getenv("BUBU_K8S_QPS"); v != "" {
		if f, err := strconv.ParseFloat(v, 32); err == nil && f > 0 {
			config.QPS = float32(f)
		}
	}
	if v := os.Getenv("BUBU_K8S_BURST"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			config.Burst = i
		}
	}
	if v := os.Getenv("BUBU_K8S_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			config.Timeout = d
		}
	}
}

// resolveUserAgent computes the User-Agent to use.
func resolveUserAgent() string {
	if v := os.Getenv("BUBU_K8S_USER_AGENT"); v != "" {
		return v
	}
	return "bubu-sdk-go"
}

// GetNamespace returns the Kubernetes namespace that the client is configured to use.
func (c *Client) GetNamespace() string {
	return c.namespace
}

// getOperationTimeout returns the timeout for K8s operations from env or default
func getOperationTimeout() time.Duration {
	if v := os.Getenv("BUBU_K8S_OPERATION_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 30 * time.Second // Default: 30s
}

// getMaxPatchRetries returns the max retries for conflict-retry from env or default
func getMaxPatchRetries() int {
	if v := os.Getenv("BUBU_K8S_PATCH_MAX_RETRIES"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			return i
		}
	}
	return 5 // Default: 5 retries
}

// isValidPhaseTransition checks if a phase transition is legal according to the StepRun state machine.
// This prevents status corruption from stale patches or race conditions.
func isValidPhaseTransition(from, to enums.Phase) bool {
	// Empty string means unset (initial state)
	if from == "" {
		// Any initial phase is valid
		return true
	}

	// Same phase is always valid (idempotent update)
	if from == to {
		return true
	}

	// Define the state machine
	// Valid transitions based on bobrapet controller design:
	//   Pending → Running, Failed, Canceled
	//   Running → Succeeded, Failed, Canceled, Paused
	//   Paused → Running, Canceled, Failed
	//   Succeeded → (terminal, no transitions)
	//   Failed → (terminal, but can retry → Running)
	//   Canceled → (terminal, no transitions)
	//   Timeout → (terminal, no transitions)
	//   Aborted → (terminal, no transitions)
	//   Compensated → (terminal, no transitions)
	//   Blocked → Pending, Running, Failed
	//   Scheduling → Running, Failed

	validTransitions := map[enums.Phase]map[enums.Phase]bool{
		enums.PhasePending: {
			enums.PhaseRunning:  true,
			enums.PhaseFailed:   true,
			enums.PhaseCanceled: true,
		},
		enums.PhaseRunning: {
			enums.PhaseSucceeded: true,
			enums.PhaseFailed:    true,
			enums.PhaseCanceled:  true,
			enums.PhasePaused:    true,
		},
		enums.PhasePaused: {
			enums.PhaseRunning:  true,
			enums.PhaseCanceled: true,
			enums.PhaseFailed:   true,
		},
		enums.PhaseFailed: {
			// Allow retry: Failed → Running
			enums.PhaseRunning: true,
		},
		enums.PhaseSucceeded: {
			// Terminal state: no transitions allowed
		},
		enums.PhaseCanceled: {
			// Terminal state: no transitions allowed
		},
		enums.PhaseTimeout: {
			// Terminal state: no transitions allowed
		},
		enums.PhaseAborted: {
			// Terminal state: no transitions allowed
		},
		enums.PhaseCompensated: {
			// Terminal state: no transitions allowed
		},
		enums.PhaseBlocked: {
			// Non-terminal: can transition when dependencies become available
			enums.PhasePending: true,
			enums.PhaseRunning: true,
			enums.PhaseFailed:  true,
		},
		enums.PhaseScheduling: {
			// Non-terminal: transitions when pod is scheduled or fails to schedule
			enums.PhaseRunning: true,
			enums.PhaseFailed:  true,
		},
	}

	allowed, exists := validTransitions[from]
	if !exists {
		// Unknown phase; conservatively reject
		return false
	}

	return allowed[to]
}

// validateStatusTransition checks if the incoming status update is legal given the current status.
// Returns an error if the transition is invalid.
func validateStatusTransition(existing *runsv1alpha1.StepRunStatus, incoming *runsv1alpha1.StepRunStatus) error {
	// If incoming doesn't specify phase, no validation needed
	if incoming.Phase == "" {
		return nil
	}

	if !isValidPhaseTransition(existing.Phase, incoming.Phase) {
		return fmt.Errorf("invalid phase transition: %s → %s (not allowed by state machine)", existing.Phase, incoming.Phase)
	}
	return nil
}

// mergeStepRunStatus merges non-zero fields from incoming into existing, preserving
// controller-managed fields unless explicitly provided. This avoids clobbering
// fields like Conditions, ObservedGeneration, and StartedAt when SDK patches status.
func mergeStepRunStatus(
	existing *runsv1alpha1.StepRunStatus,
	incoming *runsv1alpha1.StepRunStatus,
) (runsv1alpha1.StepRunStatus, error) {
	merged := *existing
	if incoming == nil {
		return merged, nil
	}
	if err := validateStatusTransition(existing, incoming); err != nil {
		return *existing, err
	}
	applyPhase(&merged, incoming)
	mergeTimingFields(&merged, incoming)
	mergeProcessDetails(&merged, incoming)
	mergeRetryFields(&merged, incoming)
	mergePayloads(&merged, incoming)
	mergeNeeds(&merged, incoming)
	return merged, nil
}

func applyPhase(merged *runsv1alpha1.StepRunStatus, incoming *runsv1alpha1.StepRunStatus) {
	if incoming.Phase != "" {
		merged.Phase = incoming.Phase
	}
}

func mergeTimingFields(merged *runsv1alpha1.StepRunStatus, incoming *runsv1alpha1.StepRunStatus) {
	if incoming.StartedAt != nil {
		merged.StartedAt = incoming.StartedAt
	}
	if incoming.FinishedAt != nil {
		merged.FinishedAt = incoming.FinishedAt
	}
	if incoming.Duration != "" {
		merged.Duration = incoming.Duration
	}
}

func mergeProcessDetails(merged *runsv1alpha1.StepRunStatus, incoming *runsv1alpha1.StepRunStatus) {
	if incoming.ExitCode != merged.ExitCode {
		merged.ExitCode = incoming.ExitCode
	}
	if incoming.ExitClass != "" {
		merged.ExitClass = incoming.ExitClass
	}
	if incoming.PodName != "" {
		merged.PodName = incoming.PodName
	}
}

func mergeRetryFields(merged *runsv1alpha1.StepRunStatus, incoming *runsv1alpha1.StepRunStatus) {
	if incoming.Retries != 0 {
		merged.Retries = incoming.Retries
	}
	if incoming.NextRetryAt != nil {
		merged.NextRetryAt = incoming.NextRetryAt
	}
	if incoming.LastFailureMsg != "" {
		merged.LastFailureMsg = incoming.LastFailureMsg
	}
}

func mergePayloads(merged *runsv1alpha1.StepRunStatus, incoming *runsv1alpha1.StepRunStatus) {
	if incoming.Output != nil {
		merged.Output = incoming.Output
	}
	if incoming.Error != nil {
		merged.Error = incoming.Error
	}
}

func mergeNeeds(merged *runsv1alpha1.StepRunStatus, incoming *runsv1alpha1.StepRunStatus) {
	if len(incoming.Needs) > 0 {
		merged.Needs = incoming.Needs
	}
}

// TriggerStory creates a new StoryRun for the configured story with the provided inputs.
func (c *Client) TriggerStory(
	ctx context.Context, storyName string, inputs map[string]any,
) (*runsv1alpha1.StoryRun, error) {
	// Set timeout for this operation
	ctx, cancel := context.WithTimeout(ctx, getOperationTimeout())
	defer cancel()

	start := time.Now()

	// Offload inputs if they are too large, replacing them with a storage reference.
	finalInputs, err := c.offloadInputsIfNecessary(ctx, storyName, inputs)
	if err != nil {
		metrics.RecordK8sOperation(ctx, "TriggerStory", time.Since(start).Seconds(), false)
		return nil, fmt.Errorf("failed to offload inputs: %w", err)
	}

	inputBytes, err := json.Marshal(finalInputs)
	if err != nil {
		metrics.RecordK8sOperation(ctx, "TriggerStory", time.Since(start).Seconds(), false)
		return nil, fmt.Errorf("failed to marshal inputs: %w", err)
	}

	// Idempotent creation via Server-Side Apply with deterministic name when BUBU_TRIGGER_TOKEN is set.
	// If no token is provided, fall back to GenerateName for unique creation semantics.
	token := os.Getenv("BUBU_TRIGGER_TOKEN")
	if token == "" {
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
			metrics.RecordK8sOperation(ctx, "TriggerStory", time.Since(start).Seconds(), false)
			return nil, fmt.Errorf("failed to create storyrun: %w", err)
		}
		metrics.RecordK8sOperation(ctx, "TriggerStory", time.Since(start).Seconds(), true)
		return storyRun, nil
	}

	// Derive deterministic name from story + token
	// Token is expected to be unique for the deduplicated trigger instance
	name := fmt.Sprintf("%s-%s", storyName, token)
	applyObj := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: c.namespace,
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
	applyObj.SetGroupVersionKind(runsv1alpha1.GroupVersion.WithKind("StoryRun"))
	force := true
	if err := apply.Apply(ctx, c.Client, applyObj, force); err != nil {
		metrics.RecordK8sOperation(ctx, "TriggerStory", time.Since(start).Seconds(), false)
		return nil, fmt.Errorf("failed to apply storyrun '%s' in namespace '%s': %w", name, c.namespace, err)
	}
	metrics.RecordK8sOperation(ctx, "TriggerStory", time.Since(start).Seconds(), true)
	// Return the applied object (contains only the fields we set). Fetch to return server state.
	created := &runsv1alpha1.StoryRun{}
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: c.namespace}, created); err != nil {
		// Best-effort: still return our object if GET fails post-apply
		return applyObj, nil
	}
	return created, nil
}

// offloadInputsIfNecessary checks if the inputs exceed the max inline size and, if so,
// offloads them to the configured storage backend, returning a storage reference.
// If inputs are small enough, it returns them unchanged.
func (c *Client) offloadInputsIfNecessary(ctx context.Context, storyName string, inputs map[string]any) (any, error) {
	// A StorageManager is needed to check size limits and perform offloading.
	sm, err := storage.NewManager(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage manager for input offloading: %w", err)
	}

	// Dehydrate will check the size and offload if necessary.
	// We use the storyName as a stable prefix for the storage path. The StoryRun's
	// name is generated and not known at this point, so we can't use it here.
	// The storyrun-controller will later pass this reference to the first step,
	// which will hydrate it using its own stepRunID in the path.
	// We generate a UUID to ensure the path is unique for this trigger.
	uniqueID := uuid.New().String()
	dehydratedInputs, err := sm.DehydrateInputs(ctx, inputs, uniqueID)
	if err != nil {
		return nil, fmt.Errorf("failed to dehydrate inputs for story '%s': %w", storyName, err)
	}

	return dehydratedInputs, nil
}

// PatchStepRunStatus updates the StepRun status with retry-on-conflict logic.
// This prevents lost updates when the controller patches status simultaneously.
func (c *Client) PatchStepRunStatus(
	ctx context.Context, stepRunName string, patchData runsv1alpha1.StepRunStatus,
) error {
	// Set timeout for this operation
	ctx, cancel := context.WithTimeout(ctx, getOperationTimeout())
	defer cancel()

	start := time.Now()
	maxRetries := getMaxPatchRetries()
	backoff := 100 * time.Millisecond

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		stepRun := &runsv1alpha1.StepRun{}
		if err := c.Get(ctx, types.NamespacedName{Name: stepRunName, Namespace: c.namespace}, stepRun); err != nil {
			metrics.RecordK8sOperation(ctx, "PatchStepRunStatus", time.Since(start).Seconds(), false)
			return fmt.Errorf("failed to get StepRun '%s' in namespace '%s' for status patch: %w",
				stepRunName, c.namespace, err,
			)
		}

		// Merge field-wise to avoid clobbering controller-managed fields
		merged, err := mergeStepRunStatus(&stepRun.Status, &patchData)
		if err != nil {
			// Validation error indicates stale read or invalid patch
			// Log and retry on next attempt
			metrics.RecordK8sOperation(ctx, "PatchStepRunStatus", time.Since(start).Seconds(), false)
			if attempt < maxRetries {
				lastErr = err
				// Use context-aware sleep to respect cancellation during backoff
				select {
				case <-ctx.Done():
					return fmt.Errorf("status patch retry aborted due to context cancellation: %w", ctx.Err())
				case <-time.After(backoff):
					// Continue to next attempt
				}
				backoff *= 2
				continue
			}
			return fmt.Errorf("failed to merge StepRun status: %w", err)
		}
		// Server-Side Apply on status with stable FieldManager
		applyObj := &runsv1alpha1.StepRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      stepRun.Name,
				Namespace: stepRun.Namespace,
			},
			Status: merged,
		}
		applyObj.SetGroupVersionKind(runsv1alpha1.GroupVersion.WithKind("StepRun"))
		force := true
		if err := c.Status().Patch(ctx, applyObj, client.Apply, &client.SubResourcePatchOptions{
			PatchOptions: client.PatchOptions{
				FieldManager: apply.FieldManager,
				Force:        &force,
			},
		}); err != nil {
			metrics.RecordK8sOperation(ctx, "PatchStepRunStatus", time.Since(start).Seconds(), false)
			if attempt < maxRetries {
				lastErr = err
				select {
				case <-ctx.Done():
					return fmt.Errorf("status patch retry aborted due to context cancellation: %w", ctx.Err())
				case <-time.After(backoff):
				}
				backoff *= 2
				continue
			}
			return fmt.Errorf("failed to apply StepRun '%s' status in namespace '%s': %w",
				stepRunName, c.namespace, err)
		}
		metrics.RecordK8sOperation(ctx, "PatchStepRunStatus", time.Since(start).Seconds(), true)
		return nil
	}
	return fmt.Errorf("failed to patch StepRun '%s' status after %d retries: %w", stepRunName, maxRetries, lastErr)
}

// ResolvePodNamespace exposes the environment-based namespace resolution used by the SDK.
// This avoids requiring a Kubernetes client when only the namespace is needed.
func ResolvePodNamespace() string {
	return getPodNamespace()
}

// getPodNamespace gets the namespace of the pod where this code is running.
// It checks a series of BUBU_* environment variables in order of precedence,
// which are set by the bobrapet controller for different execution contexts
// (Stories, Impulses, StepRuns). It also checks the standard POD_NAMESPACE
// env var set by Kubernetes Downward API, and falls back to "default" for
// local development.
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
