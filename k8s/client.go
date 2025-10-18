/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package k8s

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/contracts"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/bubu-sdk-go/pkg/env"
	sdkerrors "github.com/bubustack/bubu-sdk-go/pkg/errors"
	"github.com/bubustack/bubu-sdk-go/pkg/metrics"
	"github.com/google/uuid"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bubustack/bobrapet/pkg/refs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

const (
	metadataNameMaxLength       = 253
	serverSideApplyFieldManager = "bubu-sdk-go"
)

type triggerTokenContextKey struct{}

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(runsv1alpha1.AddToScheme(scheme))
	utilruntime.Must(transportv1alpha1.AddToScheme(scheme))
}

// WithTriggerToken attaches an idempotency token used by TriggerStory to derive a deterministic StoryRun name.
func WithTriggerToken(ctx context.Context, token string) context.Context {
	if ctx == nil {
		panic("k8s.WithTriggerToken requires a non-nil context")
	}
	if token == "" {
		return ctx
	}
	return context.WithValue(ctx, triggerTokenContextKey{}, token)
}

func triggerTokenFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(triggerTokenContextKey{}).(string); ok {
		return v
	}
	return ""
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
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
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

func resolveUserAgent() string {
	if v := os.Getenv(contracts.K8sUserAgentEnv); v != "" {
		return v
	}
	return "bubu-sdk-go"
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
	if v := os.Getenv(contracts.K8sTimeoutEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			config.Timeout = d
		}
	}
	// User-Agent for all requests from this client.
	config.UserAgent = "bubu-sdk-go"
	if v := os.Getenv(contracts.K8sUserAgentEnv); v != "" {
		config.UserAgent = v
	}
}

// NewClientForConfig creates a new Kubernetes client for the given config.
func NewClientForConfig(config *rest.Config) (*Client, error) {
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

// GetNamespace returns the Kubernetes namespace that the client is configured to use.
func (c *Client) GetNamespace() string {
	return c.namespace
}

// getOperationTimeout returns the timeout for K8s operations from env or default
func getOperationTimeout() time.Duration {
	return env.GetDurationWithFallback(contracts.K8sOperationTimeoutEnv, "", 30*time.Second)
}

// getMaxPatchRetries returns the max retries for conflict-retry from env or default
func getMaxPatchRetries() int {
	return env.GetIntWithFallback(contracts.K8sPatchMaxRetriesEnv, "", 5)
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
			enums.PhaseTimeout:   true,
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
	if incoming == nil {
		return
	}
	merged.Output = incoming.Output
	merged.Error = incoming.Error
	merged.Manifest = incoming.Manifest
	merged.ManifestWarnings = incoming.ManifestWarnings
}

func mergeNeeds(merged *runsv1alpha1.StepRunStatus, incoming *runsv1alpha1.StepRunStatus) {
	if incoming == nil {
		return
	}
	merged.Needs = incoming.Needs
	if len(incoming.Signals) > 0 {
		if merged.Signals == nil {
			merged.Signals = make(map[string]runtime.RawExtension, len(incoming.Signals))
		}
		for key, raw := range incoming.Signals {
			if len(raw.Raw) == 0 {
				delete(merged.Signals, key)
				continue
			}
			merged.Signals[key] = raw
		}
	}
}

// deriveStorageInputKey produces a deterministic storage key for Story inputs.
// When a trigger token is provided, the key is augmented with a content fingerprint
// so retries with different payloads cannot overwrite previously offloaded data.
func deriveStorageInputKey(storyNamespace, storyName, token, inputHash string) string {
	if token == "" {
		return uuid.New().String()
	}

	base := deriveStoryRunName(storyNamespace, storyName, token)
	if inputHash == "" {
		return base
	}

	suffix := inputHash
	if len(suffix) > 12 {
		suffix = suffix[:12]
	}
	return appendSuffixWithLimit(base, suffix)
}

func deriveStoryRunName(storyNamespace, storyName, token string) string {
	sanitized := sanitizeTokenSegment(token)
	hashSeed := storyNamespace + "/" + storyName + "/" + token
	hashed := shortTokenHash(hashSeed)
	needsHash := sanitized != token || storyNamespace != ""

	if sanitized == "" {
		sanitized = hashed
		needsHash = false
	}

	if needsHash {
		sanitized = fmt.Sprintf("%s-%s", sanitized, hashed)
	}

	base := fmt.Sprintf("%s-%s", storyName, sanitized)
	if len(base) <= metadataNameMaxLength {
		return base
	}

	maxPrefix := metadataNameMaxLength - 1 - len(hashed)
	if maxPrefix < 1 {
		maxPrefix = 1
	}
	prefix := storyName
	if len(prefix) > maxPrefix {
		prefix = prefix[:maxPrefix]
		prefix = strings.Trim(prefix, "-")
		if prefix == "" {
			prefix = "s"
		}
	}
	return fmt.Sprintf("%s-%s", prefix, hashed)
}

func sanitizeTokenSegment(token string) string {
	token = strings.ToLower(token)
	var b strings.Builder
	b.Grow(len(token))
	prevHyphen := false

	for _, r := range token {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevHyphen = false
		case r == '-':
			if b.Len() == 0 || prevHyphen {
				continue
			}
			b.WriteRune('-')
			prevHyphen = true
		default:
			if b.Len() == 0 || prevHyphen {
				continue
			}
			b.WriteRune('-')
			prevHyphen = true
		}
	}

	return strings.Trim(b.String(), "-")
}

func shortTokenHash(token string) string {
	sum := sha1.Sum([]byte(token))
	return hex.EncodeToString(sum[:8])
}

func computeInputFingerprint(inputs map[string]any) string {
	if len(inputs) == 0 {
		return ""
	}
	payload, err := json.Marshal(inputs)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func appendSuffixWithLimit(base, suffix string) string {
	if suffix == "" {
		return base
	}
	joined := fmt.Sprintf("%s-%s", base, suffix)
	if len(joined) <= metadataNameMaxLength {
		return joined
	}
	maxPrefix := metadataNameMaxLength - 1 - len(suffix)
	if maxPrefix < 1 {
		maxPrefix = 1
	}
	prefix := base
	if len(prefix) > maxPrefix {
		prefix = prefix[:maxPrefix]
		prefix = strings.Trim(prefix, "-")
		if prefix == "" {
			prefix = suffix[:1]
		}
	}
	return fmt.Sprintf("%s-%s", prefix, suffix)
}

func storyRunRequestMatches(existing, desired *runsv1alpha1.StoryRun) bool {
	if existing == nil || desired == nil {
		return false
	}
	if existing.Spec.StoryRef.Name != desired.Spec.StoryRef.Name {
		return false
	}
	if !sameNamespace(existing.Spec.StoryRef.Namespace, desired.Spec.StoryRef.Namespace) {
		return false
	}
	existingInputs := normalizeInputs(existing.Spec.Inputs)
	desiredInputs := normalizeInputs(desired.Spec.Inputs)
	return bytes.Equal(existingInputs, desiredInputs)
}

func sameNamespace(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func normalizeInputs(raw *runtime.RawExtension) []byte {
	if raw == nil || len(raw.Raw) == 0 {
		return nil
	}
	return bytes.TrimSpace(raw.Raw)
}

// TriggerStory creates a new StoryRun for the configured story with the provided inputs.
func (c *Client) TriggerStory(
	ctx context.Context, storyName string, storyNamespace string, inputs map[string]any,
) (*runsv1alpha1.StoryRun, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context must not be nil")
	}
	storageCtx := ctx
	apiCtx, cancel := context.WithTimeout(ctx, getOperationTimeout())
	defer cancel()

	start := time.Now()
	token := triggerTokenFromContext(ctx)
	if token == "" {
		token = strings.TrimSpace(os.Getenv(contracts.TriggerTokenEnv))
	}

	finalInputs, err := c.offloadInputsIfNecessary(storageCtx, storyNamespace, storyName, token, inputs)
	if err != nil {
		metrics.RecordK8sOperation(apiCtx, "TriggerStory", time.Since(start).Seconds(), false)
		return nil, fmt.Errorf("failed to offload inputs: %w", err)
	}

	inputBytes, err := json.Marshal(finalInputs)
	if err != nil {
		metrics.RecordK8sOperation(apiCtx, "TriggerStory", time.Since(start).Seconds(), false)
		return nil, fmt.Errorf("failed to marshal inputs: %w", err)
	}

	storyRef := refs.StoryReference{
		ObjectReference: refs.ObjectReference{Name: storyName},
	}
	impulseRef := resolveImpulseRefFromEnv()
	if storyNamespace != "" {
		ns := storyNamespace
		storyRef.Namespace = &ns
	}

	if token == "" {
		storyRun := &runsv1alpha1.StoryRun{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:    c.namespace,
				GenerateName: fmt.Sprintf("%s-", storyName),
			},
			Spec: runsv1alpha1.StoryRunSpec{
				StoryRef:   storyRef,
				Inputs:     &runtime.RawExtension{Raw: inputBytes},
				ImpulseRef: impulseRef,
			},
		}
		if err := c.Create(apiCtx, storyRun); err != nil {
			metrics.RecordK8sOperation(apiCtx, "TriggerStory", time.Since(start).Seconds(), false)
			return nil, wrapK8sError(err, "failed to create storyrun")
		}
		metrics.RecordK8sOperation(apiCtx, "TriggerStory", time.Since(start).Seconds(), true)
		return storyRun, nil
	}

	name := deriveStoryRunName(storyNamespace, storyName, token)
	request := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: c.namespace,
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef:   storyRef,
			Inputs:     &runtime.RawExtension{Raw: inputBytes},
			ImpulseRef: impulseRef,
		},
	}
	request.SetGroupVersionKind(runsv1alpha1.GroupVersion.WithKind("StoryRun"))
	if err := c.Create(apiCtx, request); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			metrics.RecordK8sOperation(apiCtx, "TriggerStory", time.Since(start).Seconds(), false)
			return nil, wrapK8sError(err, "failed to create storyrun '%s' in namespace '%s'", name, c.namespace)
		}

		existing := &runsv1alpha1.StoryRun{}
		if getErr := c.Get(apiCtx, types.NamespacedName{Name: name, Namespace: c.namespace}, existing); getErr != nil {
			metrics.RecordK8sOperation(apiCtx, "TriggerStory", time.Since(start).Seconds(), false)
			return nil, wrapK8sError(getErr, "storyrun '%s' already exists but could not be retrieved", name)
		}
		if !storyRunRequestMatches(existing, request) {
			metrics.RecordK8sOperation(apiCtx, "TriggerStory", time.Since(start).Seconds(), false)
			return nil, fmt.Errorf(
				"storyrun '%s' already exists with different inputs; reuse of trigger token is not allowed",
				name,
			)
		}
		metrics.RecordK8sOperation(apiCtx, "TriggerStory", time.Since(start).Seconds(), true)
		return existing, nil
	}

	metrics.RecordK8sOperation(apiCtx, "TriggerStory", time.Since(start).Seconds(), true)
	return request, nil
}

// StopStoryRun marks the specified StoryRun as finished. If namespace is empty, the client's namespace is used.
// Returns sdkerrors.ErrNotFound when the StoryRun does not exist.
func (c *Client) StopStoryRun(ctx context.Context, storyRunName, namespace string) error {
	if ctx == nil {
		return fmt.Errorf("context must not be nil")
	}
	targetNamespace := strings.TrimSpace(namespace)
	if targetNamespace == "" {
		targetNamespace = c.namespace
	}
	if targetNamespace == "" {
		return fmt.Errorf("failed to resolve namespace for storyrun %q", storyRunName)
	}

	apiCtx, cancel := context.WithTimeout(ctx, getOperationTimeout())
	defer cancel()
	start := time.Now()

	key := types.NamespacedName{Name: storyRunName, Namespace: targetNamespace}
	storyRun := &runsv1alpha1.StoryRun{}
	if err := c.Get(apiCtx, key, storyRun); err != nil {
		if apierrors.IsNotFound(err) {
			metrics.RecordK8sOperation(apiCtx, "StopStoryRun", time.Since(start).Seconds(), false)
			return fmt.Errorf("storyrun %s/%s not found: %w", targetNamespace, storyRunName, sdkerrors.ErrNotFound)
		}
		metrics.RecordK8sOperation(apiCtx, "StopStoryRun", time.Since(start).Seconds(), false)
		return wrapK8sError(err, "failed to get storyrun '%s' in namespace '%s'", storyRunName, targetNamespace)
	}

	if storyRun.Status.Phase.IsTerminal() {
		metrics.RecordK8sOperation(apiCtx, "StopStoryRun", time.Since(start).Seconds(), true)
		return nil
	}

	updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &runsv1alpha1.StoryRun{}
		if err := c.Get(apiCtx, key, current); err != nil {
			return err
		}
		if current.Status.Phase.IsTerminal() {
			storyRun = current
			return nil
		}

		before := current.DeepCopy()
		message := "StoryRun canceled via SDK"
		current.Status.Phase = enums.PhaseCanceled
		current.Status.Message = message
		now := metav1.Now()
		if current.Status.StartedAt != nil {
			duration := now.Sub(current.Status.StartedAt.Time)
			current.Status.Duration = duration.String()
		}
		current.Status.FinishedAt = &now
		if err := c.Status().Patch(apiCtx, current, client.MergeFrom(before)); err != nil {
			return err
		}
		storyRun = current
		return nil
	})
	if updateErr != nil {
		if apierrors.IsNotFound(updateErr) {
			metrics.RecordK8sOperation(apiCtx, "StopStoryRun", time.Since(start).Seconds(), false)
			return fmt.Errorf("storyrun %s/%s not found: %w", targetNamespace, storyRunName, sdkerrors.ErrNotFound)
		}
		metrics.RecordK8sOperation(apiCtx, "StopStoryRun", time.Since(start).Seconds(), false)
		return wrapK8sError(updateErr, "failed to update storyrun '%s' in namespace '%s'", storyRunName, targetNamespace)
	}

	metrics.RecordK8sOperation(apiCtx, "StopStoryRun", time.Since(start).Seconds(), true)
	return nil
}

// offloadInputsIfNecessary checks if the inputs exceed the max inline size and, if so,
// offloads them to the configured storage backend, returning a storage reference.
// If inputs are small enough, it returns them unchanged.
func (c *Client) offloadInputsIfNecessary(
	ctx context.Context,
	storyNamespace string,
	storyName string,
	token string,
	inputs map[string]any,
) (any, error) {
	// A StorageManager is needed to check size limits and perform offloading.
	sm, err := storage.SharedManager(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage manager for input offloading: %w", err)
	}

	// Dehydrate will check the size and offload if necessary.
	// We use the storyName as a stable prefix for the storage path. The StoryRun's
	// name is generated and not known at this point, so we can't use it here.
	// The storyrun-controller will later pass this reference to the first step,
	// which will hydrate it using its own stepRunID in the path.
	var inputFingerprint string
	if token != "" {
		inputFingerprint = computeInputFingerprint(inputs)
	}
	storageKey := deriveStorageInputKey(storyNamespace, storyName, token, inputFingerprint)
	namespacedKey := storage.NamespacedKey(c.namespace, storageKey)
	dehydratedInputs, err := sm.DehydrateInputs(ctx, inputs, namespacedKey)
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
			return wrapK8sError(err,
				"failed to get StepRun '%s' in namespace '%s' for status patch",
				stepRunName, c.namespace,
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
				FieldManager: serverSideApplyFieldManager,
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
			return wrapK8sError(err,
				"failed to apply StepRun '%s' status in namespace '%s'",
				stepRunName, c.namespace,
			)
		}
		metrics.RecordK8sOperation(ctx, "PatchStepRunStatus", time.Since(start).Seconds(), true)
		return nil
	}
	return wrapK8sError(lastErr, "failed to patch StepRun '%s' status after %d retries", stepRunName, maxRetries)
}

func classifyK8sError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
		apierrors.IsTimeout(err), apierrors.IsServerTimeout(err), apierrors.IsTooManyRequests(err):
		return fmt.Errorf("%w: %v", sdkerrors.ErrRetryable, err)
	case apierrors.IsConflict(err):
		return fmt.Errorf("%w: %v", sdkerrors.ErrConflict, err)
	case apierrors.IsNotFound(err):
		return fmt.Errorf("%w: %v", sdkerrors.ErrNotFound, err)
	default:
		return err
	}
}

func wrapK8sError(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	msg := fmt.Sprintf(format, args...)
	classified := classifyK8sError(err)
	if classified != err {
		return fmt.Errorf("%s: %w", msg, classified)
	}
	return fmt.Errorf("%s: %w", msg, err)
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
	if ns, ok := os.LookupEnv(contracts.TargetStoryNamespaceEnv); ok && ns != "" {
		return ns
	}
	if ns, ok := os.LookupEnv(contracts.ImpulseNamespaceEnv); ok && ns != "" {
		return ns
	}
	if ns, ok := os.LookupEnv(contracts.StepRunNamespaceEnv); ok && ns != "" {
		return ns
	}
	if ns, ok := os.LookupEnv(contracts.PodNamespaceEnv); ok && ns != "" {
		return ns
	}
	// Fallback for local testing.
	return "default"
}

func resolveImpulseRefFromEnv() *refs.ImpulseReference {
	name := strings.TrimSpace(os.Getenv(contracts.ImpulseNameEnv))
	if name == "" {
		return nil
	}
	ref := &refs.ImpulseReference{
		ObjectReference: refs.ObjectReference{Name: name},
	}
	if ns := strings.TrimSpace(os.Getenv(contracts.ImpulseNamespaceEnv)); ns != "" {
		ref.Namespace = ptr.To(ns)
	}
	return ref
}
