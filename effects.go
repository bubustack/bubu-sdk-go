package sdk

import (
	context "context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	"github.com/bubustack/bubu-sdk-go/k8s"
	sdkenv "github.com/bubustack/bubu-sdk-go/pkg/env"
	"github.com/bubustack/core/contracts"
	"github.com/google/uuid"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ErrEffectsUnavailable indicates that the current process cannot record effects (e.g.,
// it is not running inside a StepRun workload). Callers may treat this as a soft
// failure and continue without recording the effect.
var ErrEffectsUnavailable = errors.New("effect recording unavailable: not running inside a StepRun")

// ErrEffectAlreadyRecorded indicates that the requested effect key is already
// present in the current StepRun effect ledger.
var ErrEffectAlreadyRecorded = errors.New("effect already recorded")

const (
	effectPatchTimeout          = 3 * time.Second
	defaultMaxEffectDetailBytes = 8 * 1024
	effectMaxDetailsBytesEnv    = "BUBU_EFFECT_MAX_DETAILS_BYTES"
	defaultEffectLeaseDuration  = 10 * time.Minute
)

var (
	effectEmitterMu              sync.Mutex
	effectEmitterInst            *effectEmitter
	effectEmitterErr             error // only set for permanent errors (missing env)
	effectClientFactory          = func() (effectPatcher, error) { return k8s.SharedClient() }
	effectReaderFactory          = func() (effectReader, error) { return k8s.SharedClient() }
	effectClaimFactory           = func() (effectClaimClient, error) { return k8s.SharedClient() }
	effectLeaseDuration          = defaultEffectLeaseDuration
	effectLeaseRenewIntervalFunc = func(duration time.Duration) time.Duration {
		if duration <= 0 {
			duration = defaultEffectLeaseDuration
		}
		interval := max(min(duration/3, 30*time.Second), 100*time.Millisecond)
		return interval
	}
	effectExecMu    sync.Mutex
	effectExecGates = make(map[string]*effectExecutionGate)
)

type effectPatcher interface {
	PatchStepRunStatus(ctx context.Context, stepRunName string, status runsv1alpha1.StepRunStatus) error
}

type effectReader interface {
	client.Reader
	GetNamespace() string
}

type effectClaimClient interface {
	client.Reader
	client.Writer
	GetNamespace() string
}

type effectEmitter struct {
	client    effectPatcher
	stepRunID string
}

type effectExecutionGate struct {
	mu        sync.Mutex
	refs      int
	completed bool
}

type effectExecutionLease struct {
	key  string
	gate *effectExecutionGate
}

type effectReservation struct {
	client         effectClaimClient
	claim          *runsv1alpha1.EffectClaim
	holderIdentity string
}

type effectReservationRenewer struct {
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}

	mu  sync.Mutex
	err error
}

// RecordEffect appends an effect record to the current StepRun status ledger.
// The effect sequence is assigned server-side when Seq is 0.
func RecordEffect(ctx context.Context, key, status string, details any) error {
	return recordEffect(ctx, key, status, details, 0)
}

// HasEffect returns true if the current StepRun already recorded an effect for the key.
func HasEffect(ctx context.Context, key string) (bool, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return false, fmt.Errorf("effect key is required")
	}
	stepRun, _, err := getCurrentStepRun(ctx)
	if err != nil {
		return false, err
	}
	for _, eff := range stepRun.Status.Effects {
		if strings.TrimSpace(eff.Key) == key {
			return true, nil
		}
	}
	return false, nil
}

// ExecuteEffectOnce runs fn only if the effect key has not been recorded yet.
// It records a successful effect with the returned details. When the effect
// already exists, it returns `already=true` with `ErrEffectAlreadyRecorded`.
func ExecuteEffectOnce(ctx context.Context, key string, fn func(context.Context) (any, error)) (any, bool, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, false, fmt.Errorf("effect key is required")
	}
	if fn == nil {
		return nil, false, fmt.Errorf("effect function is required")
	}
	lease, already := acquireEffectExecutionLease(effectExecutionKey(key))
	if already {
		return nil, true, ErrEffectAlreadyRecorded
	}
	completed := false
	defer func() {
		releaseEffectExecutionLease(lease, completed)
	}()
	reservation, already, err := reserveEffect(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if already {
		completed = true
		return nil, true, ErrEffectAlreadyRecorded
	}
	execCtx := ctx
	execCancel := func() {} //nolint:ineffassign,staticcheck
	if execCtx == nil {
		execCtx = context.Background()
	}
	execCtx, execCancel = context.WithCancel(execCtx)
	defer execCancel()

	stopRenewal := startEffectReservationRenewal(reservation, execCancel)
	result, err := fn(execCtx)
	renewErr := stopRenewal()
	if err != nil {
		_ = releaseEffectReservation(ctx, reservation)
		if renewErr != nil {
			err = errors.Join(err, renewErr)
		}
		return result, false, err
	}
	if renewErr != nil {
		_ = releaseEffectReservation(ctx, reservation)
		return result, false, renewErr
	}
	// Mark the in-process gate as completed as soon as the effect function
	// succeeds so retries do not execute the side effect again if persistence
	// fails afterward.
	completed = true
	if err := completeEffectReservation(ctx, reservation, result); err != nil {
		return result, false, err
	}
	if err := RecordEffect(ctx, key, "succeeded", result); err != nil {
		return result, false, err
	}
	return result, false, nil
}

func recordEffect(ctx context.Context, key, status string, details any, seq uint64) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("effect key is required")
	}
	emitter, err := getEffectEmitter()
	if err != nil {
		return err
	}

	detailRaw, err := marshalEffectDetails(details)
	if err != nil {
		return err
	}

	now := metav1.NewTime(time.Now().UTC())
	patch := runsv1alpha1.StepRunStatus{
		Effects: []runsv1alpha1.EffectRecord{{
			Seq:       seq,
			Key:       key,
			Status:    strings.TrimSpace(status),
			EmittedAt: &now,
			Details:   detailRaw,
		}},
	}
	return emitter.emit(ctx, patch)
}

func marshalEffectDetails(details any) (*runtime.RawExtension, error) {
	if details == nil {
		return nil, nil
	}
	raw, err := json.Marshal(details)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal effect details: %w", err)
	}
	maxBytes := resolveEffectMaxDetailsBytes()
	if maxBytes > 0 && len(raw) > maxBytes {
		details = truncateEffectDetails(details, raw)
		raw, err = json.Marshal(details)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal truncated effect details: %w", err)
		}
		if len(raw) > maxBytes {
			return nil, fmt.Errorf("truncated effect details exceed %d bytes", maxBytes)
		}
	}
	value := runtime.RawExtension{Raw: raw}
	return &value, nil
}

func truncateEffectDetails(value any, raw []byte) map[string]any {
	meta := map[string]any{
		"truncated": true,
		"sizeBytes": len(raw),
	}
	if len(raw) > 0 {
		sum := sha256.Sum256(raw)
		meta["sha256"] = fmt.Sprintf("%x", sum)
	}
	kind, details := signalTypeSummary(value)
	meta["type"] = kind
	if len(details) > 0 {
		meta["details"] = details
	}
	return meta
}

func resolveEffectMaxDetailsBytes() int {
	return sdkenv.GetInt(effectMaxDetailsBytesEnv, defaultMaxEffectDetailBytes)
}

func getCurrentStepRun(ctx context.Context) (*runsv1alpha1.StepRun, string, error) {
	stepRunName := strings.TrimSpace(os.Getenv(contracts.StepRunNameEnv))
	if stepRunName == "" {
		return nil, "", ErrEffectsUnavailable
	}
	reader, err := effectReaderFactory()
	if err != nil {
		return nil, "", fmt.Errorf("failed to initialize effect reader: %w", err)
	}
	namespace := strings.TrimSpace(os.Getenv(contracts.StepRunNamespaceEnv))
	if namespace == "" {
		namespace = strings.TrimSpace(reader.GetNamespace())
	}
	if namespace == "" {
		return nil, "", ErrEffectsUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, effectPatchTimeout)
	defer cancel()

	var stepRun runsv1alpha1.StepRun
	if err := reader.Get(ctx, types.NamespacedName{Name: stepRunName, Namespace: namespace}, &stepRun); err != nil {
		return nil, "", fmt.Errorf("failed to fetch StepRun for effect lookup: %w", err)
	}
	return &stepRun, namespace, nil
}

func effectExecutionKey(key string) string {
	return strings.TrimSpace(os.Getenv(contracts.StepRunNamespaceEnv)) +
		":" + strings.TrimSpace(os.Getenv(contracts.StepRunNameEnv)) +
		":" + strings.TrimSpace(key)
}

func acquireEffectExecutionLease(key string) (*effectExecutionLease, bool) {
	effectExecMu.Lock()
	gate := effectExecGates[key]
	if gate == nil {
		gate = &effectExecutionGate{}
		effectExecGates[key] = gate
	}
	gate.refs++
	effectExecMu.Unlock()

	gate.mu.Lock()
	if gate.completed {
		effectExecMu.Lock()
		gate.refs--
		if gate.refs == 0 {
			delete(effectExecGates, key)
		}
		effectExecMu.Unlock()
		gate.mu.Unlock()
		return nil, true
	}
	return &effectExecutionLease{key: key, gate: gate}, false
}

func releaseEffectExecutionLease(lease *effectExecutionLease, completed bool) {
	if lease == nil || lease.gate == nil {
		return
	}
	effectExecMu.Lock()
	if completed {
		lease.gate.completed = true
	}
	lease.gate.refs--
	if lease.gate.refs == 0 {
		delete(effectExecGates, lease.key)
	}
	effectExecMu.Unlock()
	lease.gate.mu.Unlock()
}

func reserveEffect(ctx context.Context, key string) (*effectReservation, bool, error) {
	stepRun, namespace, err := getCurrentStepRun(ctx)
	if err != nil {
		return nil, false, err
	}
	for _, eff := range stepRun.Status.Effects {
		if strings.TrimSpace(eff.Key) == key {
			return nil, true, nil
		}
	}
	claimClient, err := effectClaimFactory()
	if err != nil {
		return nil, false, fmt.Errorf("failed to initialize effect reservation client: %w", err)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, effectPatchTimeout)
	defer cancel()

	claim := newEffectReservationClaim(stepRun, namespace, key)
	if err := claimClient.Create(ctx, claim); err == nil {
		return &effectReservation{
			client:         claimClient,
			claim:          claim,
			holderIdentity: strings.TrimSpace(claim.Spec.HolderIdentity),
		}, false, nil
	} else if !apierrors.IsAlreadyExists(err) {
		return nil, false, fmt.Errorf("failed to create effect reservation: %w", err)
	}

	existing := &runsv1alpha1.EffectClaim{}
	if err := claimClient.Get(ctx, types.NamespacedName{Name: claim.Name, Namespace: claim.Namespace}, existing); err != nil { //nolint:lll
		return nil, false, fmt.Errorf("failed to fetch effect reservation: %w", err)
	}
	if !effectReservationMatchesStepRun(existing, stepRun, key) {
		return nil, false, fmt.Errorf("existing effect reservation %s/%s does not match StepRun/effect identity", existing.Namespace, existing.Name) //nolint:lll
	}
	if effectReservationState(existing) == runsv1alpha1.EffectClaimCompletionStatusCompleted {
		return nil, true, nil
	}
	if effectReservationAvailableForReuse(existing) {
		return acquireExistingEffectReservation(ctx, claimClient, existing, stepRun, namespace, key, false)
	}
	if effectReservationIsStale(existing, time.Now().UTC()) {
		return acquireExistingEffectReservation(ctx, claimClient, existing, stepRun, namespace, key, true)
	}
	return nil, true, nil
}

func newEffectReservationClaim(stepRun *runsv1alpha1.StepRun, namespace, key string) *runsv1alpha1.EffectClaim {
	now := metav1.NewMicroTime(time.Now().UTC())
	durationSeconds := int32(effectLeaseDuration / time.Second)
	holderIdentity := newEffectReservationHolderIdentity(namespace, stepRun.Name, key)
	idempotencyKey := strings.TrimSpace(stepRun.Spec.IdempotencyKey)
	claim := &runsv1alpha1.EffectClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      effectReservationClaimName(namespace, stepRun.Name, key),
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: runsv1alpha1.GroupVersion.String(),
				Kind:       "StepRun",
				Name:       stepRun.Name,
				UID:        stepRun.UID,
			}},
		},
		Spec: runsv1alpha1.EffectClaimSpec{
			StepRunRef: refs.StepRunReference{
				ObjectReference: refs.ObjectReference{Name: stepRun.Name},
				UID:             &stepRun.UID,
			},
			EffectKey:            key,
			IdempotencyKey:       idempotencyKey,
			HolderIdentity:       holderIdentity,
			AcquireTime:          &now,
			RenewTime:            &now,
			LeaseDurationSeconds: durationSeconds,
		},
	}
	return claim
}

func effectReservationClaimName(namespace, stepRunName, key string) string {
	return runsidentity.DeriveEffectClaimName(namespace, stepRunName, key)
}

func effectReservationState(claim *runsv1alpha1.EffectClaim) runsv1alpha1.EffectClaimCompletionStatus {
	if claim == nil {
		return ""
	}
	return claim.Spec.CompletionStatus
}

func newEffectReservationHolderIdentity(namespace, stepRunName, key string) string {
	return fmt.Sprintf("%s:%s:%s:%s", namespace, stepRunName, key, uuid.NewString())
}

func effectReservationAvailableForReuse(claim *runsv1alpha1.EffectClaim) bool {
	if claim == nil {
		return false
	}
	if effectReservationState(claim) == runsv1alpha1.EffectClaimCompletionStatusCompleted {
		return false
	}
	return strings.TrimSpace(claim.Spec.HolderIdentity) == ""
}

func effectReservationIsStale(claim *runsv1alpha1.EffectClaim, now time.Time) bool {
	if claim == nil {
		return false
	}
	expiresAt := effectReservationExpiresAt(claim)
	if expiresAt.IsZero() {
		return false
	}
	return !now.Before(expiresAt)
}

func effectReservationExpiresAt(claim *runsv1alpha1.EffectClaim) time.Time {
	if claim == nil {
		return time.Time{}
	}
	duration := effectLeaseDuration
	if claim.Spec.LeaseDurationSeconds > 0 {
		duration = time.Duration(claim.Spec.LeaseDurationSeconds) * time.Second
	}
	switch {
	case claim.Spec.RenewTime != nil:
		return claim.Spec.RenewTime.Add(duration)
	case claim.Spec.AcquireTime != nil:
		return claim.Spec.AcquireTime.Add(duration)
	default:
		return claim.CreationTimestamp.Add(duration)
	}
}

func effectReservationOwnedBy(reservation *effectReservation, claim *runsv1alpha1.EffectClaim) bool {
	if reservation == nil || claim == nil {
		return false
	}
	return strings.TrimSpace(reservation.holderIdentity) != "" &&
		strings.TrimSpace(claim.Spec.HolderIdentity) == strings.TrimSpace(reservation.holderIdentity)
}

func startEffectReservationRenewal(reservation *effectReservation, cancel context.CancelFunc) func() error {
	if reservation == nil || reservation.client == nil || reservation.claim == nil {
		return func() error { return nil }
	}
	duration := effectLeaseDuration
	if reservation.claim.Spec.LeaseDurationSeconds > 0 {
		duration = time.Duration(reservation.claim.Spec.LeaseDurationSeconds) * time.Second
	}
	interval := effectLeaseRenewIntervalFunc(duration)
	if interval <= 0 {
		return func() error { return nil }
	}

	renewer := &effectReservationRenewer{
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	go func() {
		defer close(renewer.doneCh)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-renewer.stopCh:
				return
			case <-ticker.C:
				if err := renewEffectReservation(context.Background(), reservation); err != nil {
					renewer.mu.Lock()
					if renewer.err == nil {
						renewer.err = err
					}
					renewer.mu.Unlock()
					if cancel != nil {
						cancel()
					}
					return
				}
			}
		}
	}()

	return func() error {
		renewer.stopOnce.Do(func() {
			close(renewer.stopCh)
		})
		<-renewer.doneCh
		renewer.mu.Lock()
		defer renewer.mu.Unlock()
		return renewer.err
	}
}

func renewEffectReservation(ctx context.Context, reservation *effectReservation) error {
	if reservation == nil || reservation.client == nil || reservation.claim == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, effectPatchTimeout)
	defer cancel()

	current := &runsv1alpha1.EffectClaim{}
	if err := reservation.client.Get(ctx, types.NamespacedName{
		Name:      reservation.claim.Name,
		Namespace: reservation.claim.Namespace,
	}, current); err != nil {
		return fmt.Errorf("failed to fetch effect reservation for renewal: %w", err)
	}
	if !effectReservationOwnedBy(reservation, current) {
		if effectReservationState(current) == runsv1alpha1.EffectClaimCompletionStatusCompleted {
			reservation.claim = current
			return fmt.Errorf("effect reservation completed by another holder before renewal")
		}
		return fmt.Errorf("effect reservation ownership lost before renewal")
	}

	updated := current.DeepCopy()
	now := metav1.NewMicroTime(time.Now().UTC())
	if updated.Spec.AcquireTime == nil {
		updated.Spec.AcquireTime = &now
	}
	updated.Spec.RenewTime = &now
	if err := reservation.client.Update(ctx, updated); err != nil {
		return fmt.Errorf("failed to renew effect reservation: %w", err)
	}
	reservation.claim = updated
	return nil
}

func acquireExistingEffectReservation(
	ctx context.Context,
	claimClient effectClaimClient,
	existing *runsv1alpha1.EffectClaim,
	stepRun *runsv1alpha1.StepRun,
	namespace, key string,
	incrementTransitions bool,
) (*effectReservation, bool, error) {
	holderIdentity := newEffectReservationHolderIdentity(namespace, stepRun.Name, key)
	current := existing.DeepCopy()
	for range 3 {
		if effectReservationState(current) == runsv1alpha1.EffectClaimCompletionStatusCompleted {
			return nil, true, nil
		}
		if !incrementTransitions && !effectReservationAvailableForReuse(current) {
			return nil, true, nil
		}
		if incrementTransitions && !effectReservationIsStale(current, time.Now().UTC()) {
			return nil, true, nil
		}

		updated := current.DeepCopy()
		now := metav1.NewMicroTime(time.Now().UTC())
		durationSeconds := int32(effectLeaseDuration / time.Second)
		if updated.Spec.LeaseDurationSeconds <= 0 {
			updated.Spec.LeaseDurationSeconds = durationSeconds
		}
		updated.Spec.HolderIdentity = holderIdentity
		updated.Spec.AcquireTime = &now
		updated.Spec.RenewTime = &now
		if incrementTransitions {
			updated.Spec.LeaseTransitions = current.Spec.LeaseTransitions + 1
		}
		if err := claimClient.Update(ctx, updated); err == nil {
			return &effectReservation{
				client:         claimClient,
				claim:          updated,
				holderIdentity: holderIdentity,
			}, false, nil
		} else if !apierrors.IsConflict(err) {
			return nil, false, fmt.Errorf("failed to recover stale effect reservation: %w", err)
		}

		refreshed := &runsv1alpha1.EffectClaim{}
		if err := claimClient.Get(ctx, types.NamespacedName{Name: current.Name, Namespace: current.Namespace}, refreshed); err != nil { //nolint:lll
			return nil, false, fmt.Errorf("failed to refetch stale effect reservation: %w", err)
		}
		current = refreshed
	}
	return nil, true, nil
}

func completeEffectReservation(ctx context.Context, reservation *effectReservation, details any) error {
	if reservation == nil || reservation.client == nil || reservation.claim == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, effectPatchTimeout)
	defer cancel()

	current := &runsv1alpha1.EffectClaim{}
	if err := reservation.client.Get(ctx, types.NamespacedName{
		Name:      reservation.claim.Name,
		Namespace: reservation.claim.Namespace,
	}, current); err != nil {
		return fmt.Errorf("failed to fetch effect reservation before completion: %w", err)
	}
	if !effectReservationOwnedBy(reservation, current) {
		if effectReservationState(current) == runsv1alpha1.EffectClaimCompletionStatusCompleted {
			reservation.claim = current
			return nil
		}
		return fmt.Errorf("effect reservation ownership lost before completion")
	}

	updated := current.DeepCopy()
	detailRaw, err := marshalEffectDetails(details)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	renewedAt := metav1.NewMicroTime(now)
	completedAt := metav1.NewTime(now)
	updated.Spec.CompletionStatus = runsv1alpha1.EffectClaimCompletionStatusCompleted
	updated.Spec.CompletedAt = &completedAt
	updated.Spec.RenewTime = &renewedAt
	updated.Spec.Details = detailRaw
	if err := reservation.client.Update(ctx, updated); err != nil {
		return fmt.Errorf("failed to complete effect reservation: %w", err)
	}
	reservation.claim = updated
	return nil
}

func releaseEffectReservation(ctx context.Context, reservation *effectReservation) error {
	if reservation == nil || reservation.client == nil || reservation.claim == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, effectPatchTimeout)
	defer cancel()

	current := &runsv1alpha1.EffectClaim{}
	if err := reservation.client.Get(ctx, types.NamespacedName{
		Name:      reservation.claim.Name,
		Namespace: reservation.claim.Namespace,
	}, current); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to fetch effect reservation before release: %w", err)
	}
	if !effectReservationOwnedBy(reservation, current) {
		return nil
	}
	updated := current.DeepCopy()
	updated.Spec.HolderIdentity = ""
	updated.Spec.AcquireTime = nil
	updated.Spec.RenewTime = nil
	updated.Spec.CompletionStatus = runsv1alpha1.EffectClaimCompletionStatusReleased
	updated.Spec.CompletedAt = nil
	updated.Spec.Details = nil
	if err := reservation.client.Update(ctx, updated); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to release effect reservation: %w", err)
	}
	return nil
}

func effectReservationMatchesStepRun(claim *runsv1alpha1.EffectClaim, stepRun *runsv1alpha1.StepRun, key string) bool {
	if claim == nil || stepRun == nil {
		return false
	}
	if strings.TrimSpace(claim.Spec.EffectKey) != strings.TrimSpace(key) {
		return false
	}
	if strings.TrimSpace(claim.Spec.StepRunRef.Name) != strings.TrimSpace(stepRun.Name) {
		return false
	}
	if claim.Spec.StepRunRef.UID != nil && *claim.Spec.StepRunRef.UID != stepRun.UID {
		return false
	}
	stepRunKey := strings.TrimSpace(stepRun.Spec.IdempotencyKey)
	claimKey := strings.TrimSpace(claim.Spec.IdempotencyKey)
	return claimKey == "" || claimKey == stepRunKey
}

func getEffectEmitter() (*effectEmitter, error) {
	effectEmitterMu.Lock()
	defer effectEmitterMu.Unlock()
	if effectEmitterInst != nil {
		return effectEmitterInst, nil
	}
	if effectEmitterErr != nil {
		// Permanent error (e.g. missing env) — don't retry.
		return nil, effectEmitterErr
	}
	stepRunID := strings.TrimSpace(os.Getenv(contracts.StepRunNameEnv))
	if stepRunID == "" {
		effectEmitterErr = ErrEffectsUnavailable
		return nil, effectEmitterErr
	}
	client, err := effectClientFactory() //nolint:revive
	if err != nil {
		// Transient error — don't cache, allow retry on next call.
		return nil, fmt.Errorf("failed to initialize effect client: %w", err)
	}
	effectEmitterInst = &effectEmitter{
		client:    client,
		stepRunID: stepRunID,
	}
	return effectEmitterInst, nil
}

func (e *effectEmitter) emit(ctx context.Context, patch runsv1alpha1.StepRunStatus) error {
	if e == nil {
		return ErrEffectsUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, effectPatchTimeout)
	defer cancel()
	return e.client.PatchStepRunStatus(ctx, e.stepRunID, patch)
}

// testResetEffectEmitter resets the cached emitter between tests.
func testResetEffectEmitter() {
	effectEmitterMu.Lock()
	defer effectEmitterMu.Unlock()
	effectEmitterInst = nil
	effectEmitterErr = nil
	effectExecMu.Lock()
	defer effectExecMu.Unlock()
	effectExecGates = make(map[string]*effectExecutionGate)
}
