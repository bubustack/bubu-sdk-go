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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/bubu-sdk-go/pkg/env"
	sdkerrors "github.com/bubustack/bubu-sdk-go/pkg/errors"
	"github.com/bubustack/bubu-sdk-go/pkg/metrics"
	"github.com/bubustack/core/contracts"
	coreidentity "github.com/bubustack/core/runtime/identity"
	"github.com/google/uuid"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bubustack/bobrapet/pkg/refs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

const (
	metadataNameMaxLength = 253
	maxSignalEvents       = 256
	maxEffectRecords      = 256
)

type triggerTokenContextKey struct{}

var (
	scheme = runtime.NewScheme()

	// sharedClientMu protects the lazy singleton returned by SharedClient.
	sharedClientMu  sync.Mutex
	sharedClientVal *Client
)

// SharedClient returns a process-wide singleton Kubernetes client. Unlike NewClient,
// it does not allocate a new HTTP transport on every call. Initialization is retried
// on each call until it succeeds, so transient API-server unavailability does not
// permanently break the SDK.
func SharedClient() (*Client, error) {
	sharedClientMu.Lock()
	defer sharedClientMu.Unlock()
	if sharedClientVal != nil {
		return sharedClientVal, nil
	}
	c, err := NewClient()
	if err != nil {
		return nil, err
	}
	sharedClientVal = c
	return sharedClientVal, nil
}

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(coordinationv1.AddToScheme(scheme))
	utilruntime.Must(runsv1alpha1.AddToScheme(scheme))
	utilruntime.Must(transportv1alpha1.AddToScheme(scheme))
	utilruntime.Must(bubuv1alpha1.AddToScheme(scheme))
	utilruntime.Must(catalogv1alpha1.AddToScheme(scheme))
}

// WithTriggerToken attaches an idempotency token used by TriggerStory to derive
// a stable trigger-delivery identity for StoryTrigger requests.
// Nil contexts are accepted; when a non-empty token is provided, the helper
// falls back to context.Background() so token attachment stays panic-free.
func WithTriggerToken(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
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

// TriggerTokenFromContext returns the trigger token stored in the context, if any.
func TriggerTokenFromContext(ctx context.Context) string {
	return triggerTokenFromContext(ctx)
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

// applyEnvOverridesToRestConfig applies env var overrides for timeout and user-agent.
// QPS and Burst defaults are set by applyDefaultRestConfigSettings.
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
	return env.GetDuration(contracts.K8sOperationTimeoutEnv, 30*time.Second)
}

// getMaxPatchRetries returns the configured number of retries after the initial
// patch attempt, sourced from env or the default.
func getMaxPatchRetries() int {
	raw := strings.TrimSpace(os.Getenv(contracts.K8sPatchMaxRetriesEnv))
	if raw == "" {
		return 5
	}
	retries, err := strconv.Atoi(raw)
	if err != nil {
		slog.Warn("ignoring invalid env var integer, using default",
			"key", contracts.K8sPatchMaxRetriesEnv, "value", raw, "default", 5)
		return 5
	}
	if retries < 0 {
		return 0
	}
	return retries
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
			enums.PhaseTimeout:  true, // timeout before pod starts
		},
		enums.PhaseRunning: {
			enums.PhaseSucceeded: true,
			enums.PhaseFailed:    true,
			enums.PhaseCanceled:  true,
			enums.PhasePaused:    true,
			enums.PhaseTimeout:   true,
			enums.PhaseAborted:   true, // force-kill / saga abort
		},
		enums.PhasePaused: {
			enums.PhaseRunning:  true,
			enums.PhaseCanceled: true,
			enums.PhaseFailed:   true,
		},
		enums.PhaseFailed: {
			enums.PhaseRunning:   true, // Controller retry: Failed → Running
			enums.PhaseSucceeded: true, // Late success: SDK completed work after controller set Failed
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
		enums.PhaseFinished: {
			// Terminal state: no transitions allowed
		},
		enums.PhaseSkipped: {
			// Terminal state: no transitions allowed
		},
		enums.PhaseCompensated: {
			// Terminal state: no transitions allowed
		},
		enums.PhaseBlocked: {
			// Non-terminal: can transition when dependencies become available
			enums.PhasePending:  true,
			enums.PhaseRunning:  true,
			enums.PhaseFailed:   true,
			enums.PhaseCanceled: true, // canceled while blocked
		},
		enums.PhaseScheduling: {
			// Non-terminal: transitions when pod is scheduled or fails to schedule
			enums.PhaseRunning:  true,
			enums.PhaseFailed:   true,
			enums.PhaseCanceled: true, // canceled while scheduling
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
		return fmt.Errorf("%w: %s → %s (not allowed by state machine)",
			sdkerrors.ErrInvalidTransition, existing.Phase, incoming.Phase)
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
	mergeSignalEvents(&merged, incoming)
	mergeEffects(&merged, incoming)
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
	// Only apply ExitCode when the incoming patch explicitly sets a non-zero value.
	// This prevents logs-only or signal-only partial patches from resetting a
	// previously recorded non-zero exit code back to 0.
	if incoming.ExitCode != 0 && incoming.ExitCode != merged.ExitCode {
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
	// Only overwrite when the incoming patch actually provides the field.
	// Signal/Log/Effect patches send status with only their fields set; copying
	// nil Output/Logs/Error would wipe existing data (e.g. result output).
	if incoming.Output != nil {
		merged.Output = incoming.Output
	}
	if incoming.Logs != nil {
		merged.Logs = incoming.Logs
	}
	if incoming.Error != nil {
		merged.Error = incoming.Error
	}
}

func mergeNeeds(merged *runsv1alpha1.StepRunStatus, incoming *runsv1alpha1.StepRunStatus) {
	if incoming == nil {
		return
	}
	// Preserve controller-managed dependency state unless the caller explicitly
	// sets Needs (including an explicit empty slice to clear it).
	if incoming.Needs != nil {
		merged.Needs = incoming.Needs
	}
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

func mergeSignalEvents(merged *runsv1alpha1.StepRunStatus, incoming *runsv1alpha1.StepRunStatus) {
	if incoming == nil || len(incoming.SignalEvents) == 0 {
		return
	}
	existing := make(map[uint64]struct{}, len(merged.SignalEvents))
	var maxSeq uint64
	for _, evt := range merged.SignalEvents {
		existing[evt.Seq] = struct{}{}
		if evt.Seq > maxSeq {
			maxSeq = evt.Seq
		}
	}
	for _, evt := range incoming.SignalEvents {
		if evt.Seq == 0 {
			maxSeq++
			evt.Seq = maxSeq
		} else if evt.Seq > maxSeq {
			maxSeq = evt.Seq
		}
		if _, ok := existing[evt.Seq]; ok {
			continue
		}
		merged.SignalEvents = append(merged.SignalEvents, evt)
		existing[evt.Seq] = struct{}{}
	}
	if len(merged.SignalEvents) > maxSignalEvents {
		sort.Slice(merged.SignalEvents, func(i, j int) bool { return merged.SignalEvents[i].Seq < merged.SignalEvents[j].Seq }) //nolint:lll
		merged.SignalEvents = merged.SignalEvents[len(merged.SignalEvents)-maxSignalEvents:]
	}
}

func mergeEffects(merged *runsv1alpha1.StepRunStatus, incoming *runsv1alpha1.StepRunStatus) {
	if incoming == nil || len(incoming.Effects) == 0 {
		return
	}
	existing := make(map[uint64]struct{}, len(merged.Effects))
	existingKeys := make(map[string]struct{}, len(merged.Effects))
	var maxSeq uint64
	for _, evt := range merged.Effects {
		existing[evt.Seq] = struct{}{}
		if key := strings.TrimSpace(evt.Key); key != "" {
			existingKeys[key] = struct{}{}
		}
		if evt.Seq > maxSeq {
			maxSeq = evt.Seq
		}
	}
	for _, evt := range incoming.Effects {
		key := strings.TrimSpace(evt.Key)
		if evt.Seq == 0 && key != "" {
			if _, ok := existingKeys[key]; ok {
				continue
			}
		}
		if evt.Seq == 0 {
			maxSeq++
			evt.Seq = maxSeq
		} else if evt.Seq > maxSeq {
			maxSeq = evt.Seq
		}
		if _, ok := existing[evt.Seq]; ok {
			continue
		}
		merged.Effects = append(merged.Effects, evt)
		existing[evt.Seq] = struct{}{}
		if key != "" {
			existingKeys[key] = struct{}{}
		}
	}
	if len(merged.Effects) > maxEffectRecords {
		sort.Slice(merged.Effects, func(i, j int) bool { return merged.Effects[i].Seq < merged.Effects[j].Seq })
		merged.Effects = merged.Effects[len(merged.Effects)-maxEffectRecords:]
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
	return runsidentity.DeriveStoryRunName(storyNamespace, storyName, token)
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
	maxPrefix := max(metadataNameMaxLength-1-len(suffix), 1)
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

const storyTriggerPollInterval = 50 * time.Millisecond

// TriggerStory submits a StoryTrigger request for the configured Story and waits
// for the controller to resolve it into a StoryRun.
// Callers need RBAC for `storytriggers` `create`/`get` and `storyruns` `get`.
func (c *Client) TriggerStory(
	ctx context.Context, storyName string, storyNamespace string, inputs map[string]any,
) (*runsv1alpha1.StoryRun, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context must not be nil")
	}
	policy, err := loadTriggerDeliveryPolicyFromEnv()
	if err != nil {
		return nil, err
	}
	throttle, err := getTriggerThrottle()
	if err != nil {
		return nil, err
	}
	if throttle != nil {
		waited, release, acquireErr := throttle.Acquire(ctx)
		if acquireErr != nil {
			return nil, acquireErr
		}
		if release != nil {
			defer release()
		}
		if waited {
			c.recordTriggerThrottle(ctx)
		}
	}
	token := triggerTokenFromContext(ctx)
	if token == "" {
		token = strings.TrimSpace(os.Getenv(contracts.TriggerTokenEnv))
	}
	resolvedToken, err := resolveTriggerTokenForPolicy(ctx, c.namespace, storyName, storyNamespace, inputs, policy, token)
	if err != nil {
		return nil, err
	}
	if resolvedToken != "" {
		ctx = WithTriggerToken(ctx, resolvedToken)
	}
	request, err := c.buildStoryTriggerRequest(ctx, storyName, storyNamespace, inputs, resolvedToken, policy)
	if err != nil {
		return nil, err
	}
	var retryPolicy *triggerRetryPolicy
	if policy != nil {
		retryPolicy = policy.retry
	}
	return retryTriggerStory(ctx, retryPolicy, func(callCtx context.Context) (*runsv1alpha1.StoryRun, error) {
		return c.triggerStoryOnce(callCtx, request)
	})
}

func (c *Client) buildStoryTriggerRequest(
	ctx context.Context,
	storyName string,
	storyNamespace string,
	inputs map[string]any,
	resolvedToken string,
	policy *triggerDeliveryPolicy,
) (*runsv1alpha1.StoryTrigger, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context must not be nil")
	}
	if inputs == nil {
		inputs = map[string]any{}
	}

	apiCtx, cancel := context.WithTimeout(ctx, getOperationTimeout())
	defer cancel()

	targetNamespace := strings.TrimSpace(storyNamespace)
	if targetNamespace == "" {
		targetNamespace = c.namespace
	}

	storyVersion := c.resolveStoryVersion(apiCtx, targetNamespace, storyName)
	storageCtx := ctx
	if schemaID := storyInputSchemaID(targetNamespace, storyName); schemaID != "" || storyVersion != "" {
		storageCtx = storage.WithStorageSchema(storageCtx, schemaID, storyVersion)
	}

	dedupeMode, dedupeKey := resolveTriggerRequestIdentity(policy, resolvedToken)
	submissionID := deriveStoryTriggerSubmissionID(dedupeKey)
	storageIdentity := dedupeKey
	if storageIdentity == "" {
		storageIdentity = submissionID
	}

	finalInputs, err := c.offloadInputsIfNecessary(storageCtx, targetNamespace, storyName, storageIdentity, inputs)
	if err != nil {
		return nil, fmt.Errorf("failed to offload inputs: %w", err)
	}

	inputBytes, err := json.Marshal(finalInputs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal inputs: %w", err)
	}
	inputHash, err := runsidentity.ComputeTriggerInputHash(inputBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to compute trigger input hash: %w", err)
	}

	storyRef := refs.StoryReference{
		ObjectReference: refs.ObjectReference{Name: storyName},
	}
	if storyVersion != "" {
		storyRef.Version = storyVersion
	}
	if storyNamespace != "" {
		ns := storyNamespace
		storyRef.Namespace = &ns
	}

	labels, annotations := parentMetadataFromEnv()
	request := &runsv1alpha1.StoryTrigger{
		ObjectMeta: metav1.ObjectMeta{
			Name:        runsidentity.DeriveStoryTriggerName(targetNamespace, storyName, dedupeKey, submissionID),
			Namespace:   c.namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: runsv1alpha1.StoryTriggerSpec{
			StoryRef:   storyRef,
			ImpulseRef: resolveImpulseRefFromEnv(),
			Inputs:     &runtime.RawExtension{Raw: inputBytes},
			DeliveryIdentity: runsv1alpha1.TriggerDeliveryIdentity{
				Mode:         &dedupeMode,
				SubmissionID: submissionID,
			},
		},
	}
	if dedupeKey != "" {
		request.Spec.DeliveryIdentity.Key = dedupeKey
		request.Spec.DeliveryIdentity.InputHash = inputHash
	}
	request.SetGroupVersionKind(runsv1alpha1.GroupVersion.WithKind("StoryTrigger"))
	return request, nil
}

func resolveTriggerRequestIdentity(policy *triggerDeliveryPolicy, resolvedToken string) (bubuv1alpha1.TriggerDedupeMode, string) { //nolint:lll
	mode := bubuv1alpha1.TriggerDedupeNone
	key := strings.TrimSpace(resolvedToken)

	if key != "" {
		mode = bubuv1alpha1.TriggerDedupeToken
	}
	if policy == nil || policy.dedupe == nil {
		return mode, key
	}

	switch policy.dedupe.mode {
	case "key": //nolint:goconst
		return bubuv1alpha1.TriggerDedupeKey, key
	case "token": //nolint:goconst
		return bubuv1alpha1.TriggerDedupeToken, key
	case "none", "": //nolint:goconst
		if key != "" {
			return bubuv1alpha1.TriggerDedupeToken, key
		}
		return bubuv1alpha1.TriggerDedupeNone, ""
	default:
		return mode, key
	}
}

func deriveStoryTriggerSubmissionID(dedupeKey string) string {
	if strings.TrimSpace(dedupeKey) != "" {
		return dedupeKey
	}
	return uuid.New().String()
}

func (c *Client) triggerStoryOnce(ctx context.Context, request *runsv1alpha1.StoryTrigger) (*runsv1alpha1.StoryRun, error) { //nolint:lll
	if ctx == nil {
		return nil, fmt.Errorf("context must not be nil")
	}
	if request == nil {
		return nil, fmt.Errorf("story trigger request must not be nil")
	}

	start := time.Now()
	createCtx, cancel := context.WithTimeout(ctx, getOperationTimeout())
	defer cancel()

	current := request.DeepCopy()
	if err := c.Create(createCtx, current); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			metrics.RecordK8sOperation(createCtx, "TriggerStory", time.Since(start).Seconds(), false)
			return nil, wrapK8sError(err, "failed to create storytrigger '%s' in namespace '%s'", request.Name, request.Namespace) //nolint:lll
		}

		existing := &runsv1alpha1.StoryTrigger{}
		if getErr := c.Get(createCtx, types.NamespacedName{Name: request.Name, Namespace: request.Namespace}, existing); getErr != nil { //nolint:lll
			metrics.RecordK8sOperation(createCtx, "TriggerStory", time.Since(start).Seconds(), false)
			return nil, wrapK8sError(getErr, "storytrigger '%s' already exists but could not be retrieved", request.Name)
		}
		if !storyTriggerRequestMatches(existing, request) {
			metrics.RecordK8sOperation(createCtx, "TriggerStory", time.Since(start).Seconds(), false)
			return nil, fmt.Errorf(
				"storytrigger '%s' already exists with different immutable request identity; reuse is not allowed",
				request.Name,
			)
		}
		current = existing
	}

	storyRun, err := c.waitForStoryTriggerResolution(ctx, current)
	if err != nil {
		metrics.RecordK8sOperation(ctx, "TriggerStory", time.Since(start).Seconds(), false)
		return nil, err
	}
	metrics.RecordK8sOperation(ctx, "TriggerStory", time.Since(start).Seconds(), true)
	return storyRun, nil
}

func storyTriggerRequestMatches(existing, desired *runsv1alpha1.StoryTrigger) bool {
	if existing == nil || desired == nil {
		return false
	}
	if existing.Spec.StoryRef.Name != desired.Spec.StoryRef.Name {
		return false
	}
	if strings.TrimSpace(existing.Spec.StoryRef.Version) != strings.TrimSpace(desired.Spec.StoryRef.Version) {
		return false
	}
	if !sameNamespace(existing.Spec.StoryRef.Namespace, desired.Spec.StoryRef.Namespace) {
		return false
	}
	if !sameImpulseRef(existing.Spec.ImpulseRef, desired.Spec.ImpulseRef) {
		return false
	}
	if !sameTriggerDeliveryIdentity(existing.Spec.DeliveryIdentity, desired.Spec.DeliveryIdentity) {
		return false
	}
	return bytes.Equal(normalizeInputs(existing.Spec.Inputs), normalizeInputs(desired.Spec.Inputs))
}

func sameImpulseRef(a, b *refs.ImpulseReference) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.Name != b.Name || strings.TrimSpace(a.Version) != strings.TrimSpace(b.Version) {
		return false
	}
	return sameNamespace(a.Namespace, b.Namespace)
}

func sameTriggerDeliveryIdentity(a, b runsv1alpha1.TriggerDeliveryIdentity) bool {
	switch {
	case a.Mode == nil && b.Mode != nil, a.Mode != nil && b.Mode == nil:
		return false
	case a.Mode != nil && b.Mode != nil && *a.Mode != *b.Mode:
		return false
	}
	return strings.TrimSpace(a.Key) == strings.TrimSpace(b.Key) &&
		strings.TrimSpace(a.InputHash) == strings.TrimSpace(b.InputHash) &&
		strings.TrimSpace(a.SubmissionID) == strings.TrimSpace(b.SubmissionID)
}

func (c *Client) waitForStoryTriggerResolution(ctx context.Context, trigger *runsv1alpha1.StoryTrigger) (*runsv1alpha1.StoryRun, error) { //nolint:lll
	if trigger == nil {
		return nil, fmt.Errorf("story trigger request must not be nil")
	}

	waitCtx := ctx
	cancel := func() {}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		waitCtx, cancel = context.WithTimeout(ctx, getOperationTimeout())
	}
	defer cancel()

	key := types.NamespacedName{Name: trigger.Name, Namespace: trigger.Namespace}
	ticker := time.NewTicker(storyTriggerPollInterval)
	defer ticker.Stop()

	current := trigger.DeepCopy()
	for {
		switch current.Status.Decision {
		case runsv1alpha1.StoryTriggerDecisionCreated, runsv1alpha1.StoryTriggerDecisionReused:
			return c.getResolvedStoryRun(waitCtx, current)
		case runsv1alpha1.StoryTriggerDecisionRejected:
			message := strings.TrimSpace(current.Status.Message)
			if message == "" {
				message = fmt.Sprintf("storytrigger %s/%s was rejected", current.Namespace, current.Name)
			}
			return nil, fmt.Errorf("%s", message)
		}

		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("%w: storytrigger '%s/%s' is still pending", sdkerrors.ErrRetryable, trigger.Namespace, trigger.Name) //nolint:lll
		case <-ticker.C:
		}

		getCtx, cancelGet := context.WithTimeout(waitCtx, getOperationTimeout())
		fresh := &runsv1alpha1.StoryTrigger{}
		err := c.Get(getCtx, key, fresh)
		cancelGet()
		if err != nil {
			if waitCtx.Err() != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, fmt.Errorf("%w: storytrigger '%s/%s' is still pending", sdkerrors.ErrRetryable, trigger.Namespace, trigger.Name) //nolint:lll
			}
			return nil, wrapK8sError(err, "failed to get storytrigger '%s' in namespace '%s'", trigger.Name, trigger.Namespace)
		}
		current = fresh
	}
}

func (c *Client) getResolvedStoryRun(ctx context.Context, trigger *runsv1alpha1.StoryTrigger) (*runsv1alpha1.StoryRun, error) { //nolint:lll
	ref := trigger.Status.StoryRunRef
	if ref == nil {
		return nil, fmt.Errorf("%w: storytrigger '%s/%s' resolved without a storyrun reference", sdkerrors.ErrRetryable, trigger.Namespace, trigger.Name) //nolint:lll
	}

	runNamespace := trigger.Namespace
	if ref.Namespace != nil && strings.TrimSpace(*ref.Namespace) != "" {
		runNamespace = strings.TrimSpace(*ref.Namespace)
	}

	getCtx, cancel := context.WithTimeout(ctx, getOperationTimeout())
	defer cancel()

	run := &runsv1alpha1.StoryRun{}
	if err := c.Get(getCtx, types.NamespacedName{Name: ref.Name, Namespace: runNamespace}, run); err != nil {
		return nil, wrapK8sError(err, "failed to get resolved storyrun '%s' in namespace '%s'", ref.Name, runNamespace)
	}
	return run, nil
}

func storyInputSchemaID(namespace, name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return fmt.Sprintf("bubu://story/%s/inputs", name)
	}
	return fmt.Sprintf("bubu://story/%s/%s/inputs", namespace, name)
}

func (c *Client) resolveStoryVersion(ctx context.Context, namespace, name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	if namespace == "" {
		namespace = c.namespace
	}
	var story bubuv1alpha1.Story
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &story); err != nil {
		return ""
	}
	return strings.TrimSpace(story.Spec.Version)
}

// StopStoryRun requests graceful cancellation for the specified StoryRun. If
// namespace is empty, the client's namespace is used. Callers need RBAC for
// `storyruns` `get` and `storyruns` `patch`.
// Returns sdkerrors.ErrNotFound when the StoryRun does not exist.
func (c *Client) StopStoryRun(ctx context.Context, storyRunName, namespace string) error { //nolint:gocyclo
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

	start := time.Now()
	key := types.NamespacedName{Name: storyRunName, Namespace: targetNamespace}

	updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		apiCtx, cancel := context.WithTimeout(ctx, getOperationTimeout())
		defer cancel()

		current := &runsv1alpha1.StoryRun{}
		if err := c.Get(apiCtx, key, current); err != nil {
			return err
		}

		// If already in a terminal phase, respect it and don't overwrite.
		// This prevents race conditions where the controller sets Succeeded/Failed
		// between our read and write.
		if current.Status.Phase.IsTerminal() {
			return nil
		}

		// Request graceful cancel through spec instead of force-finishing status.
		if current.Spec.CancelRequested != nil && *current.Spec.CancelRequested {
			return nil
		}

		before := current.DeepCopy()
		cancelRequested := true
		current.Spec.CancelRequested = &cancelRequested
		if err := c.Patch(apiCtx, current, client.MergeFrom(before)); err != nil {
			return err
		}
		return nil
	})
	if updateErr != nil {
		if apierrors.IsNotFound(updateErr) {
			metrics.RecordK8sOperation(ctx, "StopStoryRun", time.Since(start).Seconds(), false)
			return fmt.Errorf("storyrun %s/%s not found: %w", targetNamespace, storyRunName, sdkerrors.ErrNotFound)
		}
		metrics.RecordK8sOperation(ctx, "StopStoryRun", time.Since(start).Seconds(), false)
		return wrapK8sError(updateErr, "failed to update storyrun '%s' in namespace '%s'", storyRunName, targetNamespace)
	}

	metrics.RecordK8sOperation(ctx, "StopStoryRun", time.Since(start).Seconds(), true)
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
	if sm == nil {
		// No storage backend configured; return inputs as-is (no offloading possible).
		return inputs, nil
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
// Uses exponential backoff with jitter to prevent "thundering herd" problems.
// Callers need RBAC for `stepruns` `get` and `stepruns/status` `patch`.
func (c *Client) PatchStepRunStatus( //nolint:gocyclo
	ctx context.Context, stepRunName string, patchData runsv1alpha1.StepRunStatus,
) error {
	if ctx == nil {
		return fmt.Errorf("context must not be nil")
	}
	// Set timeout for this operation
	ctx, cancel := context.WithTimeout(ctx, getOperationTimeout())
	defer cancel()

	start := time.Now()
	maxRetryCount := getMaxPatchRetries()
	backoff := 100 * time.Millisecond

	var lastErr error
	for attempt := 0; attempt <= maxRetryCount; attempt++ {
		stepRun := &runsv1alpha1.StepRun{}
		if err := c.Get(ctx, types.NamespacedName{Name: stepRunName, Namespace: c.namespace}, stepRun); err != nil {
			metrics.RecordK8sOperation(ctx, "PatchStepRunStatus", time.Since(start).Seconds(), false)
			wrappedErr := wrapK8sError(err,
				"failed to get StepRun '%s' in namespace '%s' for status patch",
				stepRunName, c.namespace,
			)
			if attempt < maxRetryCount && errors.Is(classifyK8sError(err), sdkerrors.ErrRetryable) {
				lastErr = wrappedErr
				jitteredBackoff := jitterDuration(backoff)
				select {
				case <-ctx.Done():
					return fmt.Errorf("status patch retry aborted due to context cancellation: %w", ctx.Err())
				case <-time.After(jitteredBackoff):
				}
				backoff *= 2
				continue
			}
			return wrappedErr
		}

		// Zombie pod fencing: reject patches from stale pods when another pod is already recorded.
		if podName := strings.TrimSpace(os.Getenv(contracts.PodNameEnv)); podName != "" {
			if existing := strings.TrimSpace(stepRun.Status.PodName); existing != "" && existing != podName {
				return fmt.Errorf("zombie pod fencing: current pod %q does not match existing %q; skipping patch",
					podName, existing)
			}
		}

		// Merge field-wise to avoid clobbering controller-managed fields
		merged, err := mergeStepRunStatus(&stepRun.Status, &patchData)
		if err != nil {
			metrics.RecordK8sOperation(ctx, "PatchStepRunStatus", time.Since(start).Seconds(), false)
			// Invalid transitions are permanent errors — retrying the same patch against
			// the same phase will never succeed. Return immediately without retrying.
			if errors.Is(err, sdkerrors.ErrInvalidTransition) {
				return err
			}
			// Other merge errors indicate stale read or transient conflict; retry.
			if attempt < maxRetryCount {
				lastErr = err
				// Use context-aware sleep with jitter to respect cancellation during backoff
				// and prevent synchronized retries from multiple SDK clients.
				jitteredBackoff := jitterDuration(backoff)
				select {
				case <-ctx.Done():
					return fmt.Errorf("status patch retry aborted due to context cancellation: %w", ctx.Err())
				case <-time.After(jitteredBackoff):
					// Continue to next attempt
				}
				backoff *= 2
				continue
			}
			return fmt.Errorf("failed to merge StepRun status: %w", err)
		}
		before := stepRun.DeepCopy()
		stepRun.Status = merged
		if err := c.Status().Patch(ctx, stepRun, client.MergeFrom(before)); err != nil {
			metrics.RecordK8sOperation(ctx, "PatchStepRunStatus", time.Since(start).Seconds(), false)
			if attempt < maxRetryCount {
				lastErr = err
				// Apply jitter to prevent synchronized retries across SDK instances.
				jitteredBackoff := jitterDuration(backoff)
				select {
				case <-ctx.Done():
					return fmt.Errorf("status patch retry aborted due to context cancellation: %w", ctx.Err())
				case <-time.After(jitteredBackoff):
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
	return wrapK8sError(lastErr, "failed to patch StepRun '%s' status after %d retries", stepRunName, maxRetryCount)
}

// jitterDuration adds random jitter (±25%) to a duration to prevent synchronized
// retries and "thundering herd" effects when multiple SDK clients retry simultaneously.
func jitterDuration(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	// Add ±25% jitter: multiply by (0.75 + random(0, 0.5))
	jitterFactor := 0.75 + rand.Float64()*0.5
	return time.Duration(float64(d) * jitterFactor)
}

func classifyK8sError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
		apierrors.IsTimeout(err), apierrors.IsServerTimeout(err), apierrors.IsTooManyRequests(err),
		isRetryableK8sTransportError(err):
		return fmt.Errorf("%w: %v", sdkerrors.ErrRetryable, err)
	case apierrors.IsConflict(err):
		return fmt.Errorf("%w: %v", sdkerrors.ErrConflict, err)
	case apierrors.IsNotFound(err):
		return fmt.Errorf("%w: %v", sdkerrors.ErrNotFound, err)
	default:
		return err
	}
}

func isRetryableK8sTransportError(err error) bool {
	if err == nil {
		return false
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			return true
		}
		inner := urlErr.Err
		if inner != nil && inner != err && isRetryableK8sTransportError(inner) {
			return true
		}
	}

	var timeoutErr interface{ Timeout() bool }
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		return true
	}

	var temporaryErr interface{ Temporary() bool }
	if errors.As(err, &temporaryErr) && temporaryErr.Temporary() {
		return true
	}

	for _, target := range []error{
		syscall.ECONNRESET,
		syscall.ECONNREFUSED,
		syscall.ETIMEDOUT,
		syscall.EHOSTUNREACH,
		syscall.ENETUNREACH,
		syscall.EPIPE,
	} {
		if errors.Is(err, target) {
			return true
		}
	}

	return false
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
// env var set by Kubernetes Downward API. When none are set, it returns an
// empty string so callers can fail closed on missing runtime namespace wiring.
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
	return ""
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
		ref.Namespace = new(ns)
	}
	return ref
}

func (c *Client) recordTriggerThrottle(ctx context.Context) {
	if c == nil {
		return
	}
	ref := resolveImpulseRefFromEnv()
	if ref == nil || ref.Name == "" {
		return
	}
	ns := ""
	if ref.Namespace != nil {
		ns = *ref.Namespace
	}
	now := time.Now().UTC()
	_ = c.UpdateImpulseTriggerStats(ctx, ref.Name, ns, TriggerStatsDelta{
		ThrottledTriggers: 1,
		LastThrottled:     &now,
	})
}

func parentMetadataFromEnv() (map[string]string, map[string]string) {
	labels := map[string]string{}
	annotations := map[string]string{}

	parentStoryRun := strings.TrimSpace(os.Getenv(contracts.StoryRunIDEnv))
	parentStepRun := strings.TrimSpace(os.Getenv(contracts.StepRunNameEnv))
	parentStep := strings.TrimSpace(os.Getenv(contracts.StepNameEnv))

	if parentStoryRun != "" {
		labels[contracts.ParentStoryRunLabel] = coreidentity.SafeLabelValue(parentStoryRun)
		annotations[contracts.ParentStoryRunLabel] = parentStoryRun
	}
	if parentStep != "" {
		labels[contracts.ParentStepLabel] = coreidentity.SafeLabelValue(parentStep)
		annotations[contracts.ParentStepLabel] = parentStep
	}
	if parentStepRun != "" {
		labels[contracts.StepRunLabelKey] = coreidentity.SafeLabelValue(parentStepRun)
		annotations["bubustack.io/parent-steprun"] = parentStepRun
	}

	if len(labels) == 0 {
		labels = nil
	}
	if len(annotations) == 0 {
		annotations = nil
	}
	return labels, annotations
}
