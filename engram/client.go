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

// package engram defines the core interfaces that developers implement to create
// components for the bobrapet ecosystem. These interfaces provide a structured,
// type-safe framework for building everything from simple, single-task jobs to
// complex, long-running event listeners.
package engram

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"mime"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/bubustack/tractatus/envelope"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"go.opentelemetry.io/otel/trace"
)

// Secrets provides sandboxed access to the secrets mapped to an Engram's StepRun.
// The SDK runtime populates this object from environment variables injected by the
// bobrapet controller, ensuring that Engrams only have access to the secrets
// explicitly declared in their corresponding Step definition in the Story.
type Secrets struct {
	rawSecrets map[string]string
}

var errInvalidSecretDescriptor = errors.New("invalid secret descriptor")

// ErrInvalidStreamMessage reports that a StreamMessage carries an unsupported or
// ambiguous payload shape.
var ErrInvalidStreamMessage = errors.New("invalid stream message")

// ErrSecretExpansionFailed reports that env/file secret descriptor expansion
// failed and the caller should decide whether to proceed with partial secrets.
var ErrSecretExpansionFailed = errors.New("secret expansion failed")

const (
	defaultSecretExpansionMaxFiles     = 256
	defaultSecretExpansionMaxFileBytes = 1 * 1024 * 1024
	defaultSecretExpansionMaxTotalSize = 8 * 1024 * 1024
)

// NewSecrets creates a new Secrets object, expanding descriptor-style env/file references.
// Callers must pass a non-nil context; cancellation stops in-flight I/O and returns the
// secrets collected so far to honor shutdown deadlines. Nil contexts are treated as API
// misuse and fail closed with an empty secret set.
func NewSecrets(ctx context.Context, rawSecrets map[string]string) *Secrets {
	secrets, err := NewSecretsWithError(ctx, rawSecrets)
	if err != nil && ctx == nil {
		slog.Default().Warn("engram.NewSecrets requires a non-nil context; returning empty secrets")
	}
	if secrets == nil {
		return &Secrets{rawSecrets: make(map[string]string)}
	}
	return secrets
}

// NewSecretsWithError expands descriptor-style env/file references and returns any expansion
// failure to callers that want to fail closed during SDK/runtime initialization.
func NewSecretsWithError(ctx context.Context, rawSecrets map[string]string) (*Secrets, error) {
	if ctx == nil {
		return nil, errors.New("engram.NewSecretsWithError requires a non-nil context")
	}
	if rawSecrets == nil {
		rawSecrets = make(map[string]string)
	}
	if len(rawSecrets) == 0 {
		return &Secrets{rawSecrets: rawSecrets}, nil
	}

	logger := slog.Default()
	expanded := make(map[string]string, len(rawSecrets))
	logicalNames := make([]string, 0, len(rawSecrets))
	for logicalName := range rawSecrets {
		logicalNames = append(logicalNames, logicalName)
	}
	sort.Strings(logicalNames)
	issues := make([]error, 0, len(rawSecrets))

	for _, logicalName := range logicalNames {
		descriptor := rawSecrets[logicalName]
		if err := ctx.Err(); err != nil {
			logger.Warn("secret expansion interrupted", "error", err)
			return &Secrets{rawSecrets: expanded}, err
		}
		switch {
		case strings.HasPrefix(descriptor, "file:"):
			descriptorSecrets := make(map[string]string)
			if err := expandSecretsFromFiles(ctx, descriptor, descriptorSecrets, logger); err != nil {
				if errors.Is(err, context.Canceled) {
					logger.Warn("secret file expansion canceled", secretExpansionLogArgs(logicalName, descriptor, err)...)
					return &Secrets{rawSecrets: expanded}, err
				}
				logger.Warn("secret file expansion incomplete", secretExpansionLogArgs(logicalName, descriptor, err)...)
				issues = append(issues, secretExpansionError(logicalName, descriptor, err))
				continue
			}
			mergeExpandedSecrets(expanded, descriptorSecrets, logger, "file")
		case strings.HasPrefix(descriptor, "env:"):
			descriptorSecrets := make(map[string]string)
			if err := expandSecretsFromEnv(ctx, descriptor, descriptorSecrets, logger); err != nil {
				if errors.Is(err, context.Canceled) {
					logger.Warn("secret env expansion canceled", secretExpansionLogArgs(logicalName, descriptor, err)...)
					return &Secrets{rawSecrets: expanded}, err
				}
				logger.Warn("secret env expansion incomplete", secretExpansionLogArgs(logicalName, descriptor, err)...)
				issues = append(issues, secretExpansionError(logicalName, descriptor, err))
				continue
			}
			mergeExpandedSecrets(expanded, descriptorSecrets, logger, "env")
		default:
			storeExpandedSecret(expanded, logicalName, descriptor, true, logger, secretSourceLabel("literal", logicalName))
		}
	}

	if len(issues) > 0 {
		return &Secrets{rawSecrets: expanded}, errors.Join(issues...)
	}
	return &Secrets{rawSecrets: expanded}, nil
}

func mergeExpandedSecrets(dest map[string]string, src map[string]string, logger *slog.Logger, sourceKind string) {
	if len(src) == 0 {
		return
	}
	keys := make([]string, 0, len(src))
	for key := range src {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		storeExpandedSecret(dest, key, src[key], false, logger, secretSourceLabel(sourceKind, key))
	}
}

func secretExpansionError(logicalName string, descriptor string, err error) error {
	return fmt.Errorf(
		"%w: secret %q (%s): %w",
		ErrSecretExpansionFailed,
		logicalName,
		secretDescriptorKind(descriptor),
		sanitizeSecretLogError(err),
	)
}

func secretExpansionLogArgs(logicalName string, descriptor string, err error) []any {
	args := []any{
		"secret", logicalName,
		"descriptorKind", secretDescriptorKind(descriptor),
	}
	if err != nil {
		args = append(args, "error", sanitizeSecretLogError(err))
	}
	return args
}

func secretDescriptorKind(descriptor string) string {
	switch {
	case strings.HasPrefix(descriptor, "file:"):
		return "file"
	case strings.HasPrefix(descriptor, "env:"):
		return "env"
	default:
		return "literal"
	}
}

func secretSourceLabel(kind string, key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return kind
	}
	return kind + ":" + key
}

func secretPathKey(rootPath string, path string) string {
	if rootPath == "" || path == "" {
		return ""
	}
	relPath, err := filepath.Rel(rootPath, path)
	if err == nil && relPath != "." {
		return filepath.ToSlash(relPath)
	}
	return filepath.Base(path)
}

func sanitizeSecretLogError(err error) error {
	if err == nil {
		return nil
	}
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return fmt.Errorf("%s: %w", pathErr.Op, pathErr.Err)
	}
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) {
		return fmt.Errorf("%s: %w", linkErr.Op, linkErr.Err)
	}
	return err
}

func storeExpandedSecret(
	dest map[string]string,
	key string,
	value string,
	overwrite bool,
	logger *slog.Logger,
	source string,
) {
	key = strings.TrimSpace(key)
	if key == "" {
		if logger != nil {
			logger.Warn("skipping secret with empty key", "source", source)
		}
		return
	}
	if _, exists := dest[key]; exists {
		if !overwrite {
			if logger != nil {
				logger.Warn("secret key collision; keeping existing value", "key", key, "source", source)
			}
			return
		}
		if logger != nil {
			logger.Warn("secret key collision; overriding existing value", "key", key, "source", source)
		}
	}
	dest[key] = value
}

func expandSecretsFromFiles(
	ctx context.Context,
	descriptor string,
	dest map[string]string,
	logger *slog.Logger,
) error {
	rootPath := strings.TrimSpace(strings.TrimPrefix(descriptor, "file:"))
	if rootPath == "" {
		return fmt.Errorf("%w: file secret path must not be empty", errInvalidSecretDescriptor)
	}
	rootInfo, err := os.Lstat(rootPath)
	if err != nil {
		return err
	}
	if rootInfo.Mode()&fs.ModeSymlink != 0 {
		return fmt.Errorf("%w: file secret path must not be a symlink", errInvalidSecretDescriptor)
	}
	if !rootInfo.IsDir() && !rootInfo.Mode().IsRegular() {
		return fmt.Errorf("%w: file secret path must reference a regular file or directory", errInvalidSecretDescriptor)
	}
	state := &secretFileExpansionState{}
	if !rootInfo.IsDir() {
		return expandSecretFile(ctx, rootPath, filepath.Base(rootPath), rootInfo, state, dest, logger)
	}
	return filepath.WalkDir(rootPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			logger.Warn("skipping symlinked secret file", "key", secretPathKey(rootPath, path))
			return nil
		}
		if !d.Type().IsRegular() {
			logger.Warn("skipping non-regular secret file", "key", secretPathKey(rootPath, path), "mode", d.Type())
			return nil
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			return infoErr
		}
		relPath, relErr := filepath.Rel(rootPath, path)
		if relErr != nil {
			return fmt.Errorf("resolve relative secret path: %w", relErr)
		}
		key := filepath.ToSlash(relPath)
		return expandSecretFile(ctx, path, key, info, state, dest, logger)
	})
}

type secretFileExpansionState struct {
	filesSeen  int
	totalBytes int
}

func expandSecretFile(
	ctx context.Context,
	path string,
	key string,
	info fs.FileInfo,
	state *secretFileExpansionState,
	dest map[string]string,
	logger *slog.Logger,
) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if state == nil {
		state = &secretFileExpansionState{}
	}
	if state.filesSeen >= defaultSecretExpansionMaxFiles {
		return fmt.Errorf("%w: secret file count exceeds max %d", errInvalidSecretDescriptor, defaultSecretExpansionMaxFiles)
	}
	if info != nil && info.Size() > defaultSecretExpansionMaxFileBytes {
		return fmt.Errorf(
			"%w: secret file %s exceeds max size %d",
			errInvalidSecretDescriptor,
			key,
			defaultSecretExpansionMaxFileBytes,
		)
	}
	content, readErr := os.ReadFile(path)
	if readErr != nil {
		logger.Warn("failed to load secret file content", "key", key, "error", sanitizeSecretLogError(readErr))
		return readErr
	}
	if len(content) > defaultSecretExpansionMaxFileBytes {
		return fmt.Errorf(
			"%w: secret file %s exceeds max size %d",
			errInvalidSecretDescriptor,
			key,
			defaultSecretExpansionMaxFileBytes,
		)
	}
	if state.totalBytes+len(content) > defaultSecretExpansionMaxTotalSize {
		return fmt.Errorf(
			"%w: secret file expansion exceeds max total size %d",
			errInvalidSecretDescriptor,
			defaultSecretExpansionMaxTotalSize,
		)
	}
	state.filesSeen++
	state.totalBytes += len(content)
	storeExpandedSecret(dest, key, strings.TrimRight(string(content), "\n"), false, logger, secretSourceLabel("file", key))
	return nil
}

func expandSecretsFromEnv(
	ctx context.Context,
	descriptor string,
	dest map[string]string,
	logger *slog.Logger,
) error {
	prefix := strings.TrimPrefix(descriptor, "env:")
	if prefix == "" {
		return fmt.Errorf("%w: env secret prefix must not be empty", errInvalidSecretDescriptor)
	}
	for _, envVar := range os.Environ() {
		if err := ctx.Err(); err != nil {
			return err
		}
		parts := strings.SplitN(envVar, "=", 2)
		if len(parts) != 2 {
			continue
		}
		name, value := parts[0], parts[1]
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		key := strings.TrimPrefix(name, prefix)
		if key == "" {
			logger.Warn("secret env prefix matched empty key", "prefix", prefix, "envVar", name)
			continue
		}
		storeExpandedSecret(dest, key, value, false, logger, secretSourceLabel("env", key))
	}
	return nil
}

// Get returns a specific secret by its key.
func (s *Secrets) Get(key string) (string, bool) {
	if s == nil {
		return "", false
	}
	val, ok := s.rawSecrets[key]
	return val, ok
}

// GetAll returns a copy of the secret keys. The values are redacted
// to prevent accidental logging of sensitive data.
func (s *Secrets) GetAll() map[string]string {
	if s == nil || len(s.rawSecrets) == 0 {
		return map[string]string{}
	}
	redacted := make(map[string]string, len(s.rawSecrets))
	for k := range s.rawSecrets {
		redacted[k] = "[REDACTED]"
	}
	return redacted
}

// Names returns the available secret keys in sorted order without exposing values.
func (s *Secrets) Names() []string {
	if s == nil || len(s.rawSecrets) == 0 {
		return []string{}
	}
	names := make([]string, 0, len(s.rawSecrets))
	for name := range s.rawSecrets {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Select returns a copy of the requested plaintext secrets only.
// Missing keys are ignored so callers can safely request optional values.
func (s *Secrets) Select(keys ...string) map[string]string {
	if s == nil || len(s.rawSecrets) == 0 || len(keys) == 0 {
		return map[string]string{}
	}
	selected := make(map[string]string, len(keys))
	for _, key := range keys {
		if value, ok := s.rawSecrets[key]; ok {
			selected[key] = value
		}
	}
	return selected
}

// Format implements fmt.Formatter to prevent accidental logging of secrets.
// It ensures that printing the Secrets struct (e.g., with %+v) does not leak values.
func (s *Secrets) Format(f fmt.State, verb rune) {
	_, _ = f.Write([]byte("[redacted secrets]"))
}

// LogValue implements slog.LogValuer to prevent structured logs from serializing
// plaintext secret values or keys.
func (s *Secrets) LogValue() slog.Value {
	return slog.StringValue("[redacted secrets]")
}

// ExecutionContext provides metadata and utilities for a single execution of an Engram.
// It serves as a dependency injection container for services provided by the SDK
// runtime, such as logging, tracing, and information about the current Story.
// This context is passed to the `Process` method of a `BatchEngram`.
type ExecutionContext struct {
	logger     *slog.Logger
	tracer     trace.Tracer
	storyInfo  StoryInfo
	celContext map[string]any
}

// NewExecutionContext is a constructor used internally by the SDK runtime.
func NewExecutionContext(logger *slog.Logger, tracer trace.Tracer, storyInfo StoryInfo) *ExecutionContext {
	return NewExecutionContextWithCELContext(logger, tracer, storyInfo, nil)
}

// NewExecutionContextWithCELContext is a constructor that also attaches CEL context data.
func NewExecutionContextWithCELContext(
	logger *slog.Logger,
	tracer trace.Tracer,
	storyInfo StoryInfo,
	celContext map[string]any,
) *ExecutionContext {
	return &ExecutionContext{
		logger:     logger,
		tracer:     tracer,
		storyInfo:  storyInfo,
		celContext: cloneConfigMap(celContext),
	}
}

// StoryInfo contains metadata about the currently executing Story and Step.
type StoryInfo struct {
	StoryName        string `json:"storyName"`
	StoryRunID       string `json:"storyRunID"`
	StepName         string `json:"stepName"`
	StepRunID        string `json:"stepRunID"`
	StepRunNamespace string `json:"stepRunNamespace"`
}

// StoryInfo returns metadata about the currently executing Story and Step.
//
// This includes the Story name, StoryRun ID, Step name, and StepRun ID,
// which are useful for logging, tracing, and correlation.
func (e *ExecutionContext) StoryInfo() StoryInfo {
	return e.storyInfo
}

// Logger returns the slog.Logger configured for this execution context.
//
// This logger should be used for all engram logging to ensure consistent
// formatting and integration with the SDK's observability stack.
func (e *ExecutionContext) Logger() *slog.Logger {
	return e.logger
}

// Tracer returns the OpenTelemetry tracer for this execution context.
//
// Use this tracer to create spans for internal operations, enabling
// distributed tracing across the execution pipeline.
func (e *ExecutionContext) Tracer() trace.Tracer {
	return e.tracer
}

// CELContext returns a defensive copy of the CEL context map provided by the
// controller (inputs + steps) so callers cannot mutate SDK-owned runtime state.
func (e *ExecutionContext) CELContext() map[string]any {
	return cloneConfigMap(e.celContext)
}

// Result is the universal return type for a BatchEngram's Process method.
// It encapsulates the output data. The SDK uses this structure to determine
// the output of the step.
type Result struct {
	// Data is the output of the Engram. This can be any serializable type.
	// The SDK will automatically handle JSON marshaling and, if configured,
	// transparently offload the data to external storage if it exceeds the
	// size threshold.
	Data any
}

// NewResultFrom wraps the provided data in a Result. It keeps examples and callers
// working with a single helper so future metadata can be attached centrally.
func NewResultFrom(data any) *Result {
	return &Result{Data: data}
}

// Engram is the foundational interface for all executable components in bobrapet.
// It establishes a common initialization contract.
//
// The generic parameter `C` represents a developer-defined struct for static
// configuration. The SDK runtime will automatically unmarshal the `with` block
// from the Engram or Impulse resource's YAML definition into this struct and pass
// it to the `Init` method.
type Engram[C any] interface {
	// Init is called once at the start of an Engram's lifecycle. It is the ideal
	// place for setting up connections, validating configuration, or performing
	// any other one-time setup.
	Init(ctx context.Context, config C, secrets *Secrets) error
}

// BatchEngram is the interface for components that run as a single, finite task,
// typically executed as a Kubernetes Job. This is the most common type of Engram,
// used for data processing, API calls, and other script-like operations.
//
// The generic parameters `C` (configuration) and `I` (inputs) allow for
// complete type safety from the YAML definition to your Go code.
type BatchEngram[C any, I any] interface {
	Engram[C]
	// Process contains the core business logic of the Engram. It is called exactly
	// once per execution. The SDK provides the typed inputs `I`, and expects a
	// `Result` in return, which it uses to report the output and status of the
	// StepRun. An error should be returned for any failures.
	Process(ctx context.Context, execCtx *ExecutionContext, inputs I) (*Result, error)
}

// Impulse is the interface for long-running components that act as triggers for
// Stories. They typically listen for external events (e.g., via webhooks,
// message queues) and use the provided Kubernetes client to create new StoryRuns.
// An Impulse usually runs as a Kubernetes Deployment.
type Impulse[C any] interface {
	Engram[C]
	// Run is the main entry point for the Impulse's long-running process. The SDK
	// calls this method after `Init`, providing a pre-configured Kubernetes client
	// for interacting with bobrapet resources (like creating StoryRuns). The method
	// should block until the Impulse's work is complete or the context is canceled.
	Run(ctx context.Context, client *k8s.Client) error
}

// StreamMessage represents a single message in a bidirectional stream with metadata.
// Metadata enables tracing and correlation across streaming pipeline steps.
type StreamMessage struct {
	// Kind declares the semantic intent of the packet (e.g., "data", "heartbeat").
	Kind string
	// MessageID is an optional caller-defined identifier that assists with deduplication.
	MessageID string
	// Timestamp captures when the packet was produced. Zero-value timestamps are omitted.
	Timestamp time.Time
	// Metadata contains tracing information (StoryRunID, StepName, etc.) from DataPacket.
	// This should be propagated through the streaming pipeline to maintain observability.
	Metadata map[string]string
	// Payload is the JSON-encoded data to be processed. Prefer Binary for new code paths.
	Payload []byte
	// Audio carries PCM audio frames when present.
	Audio *AudioFrame
	// Video carries encoded or raw video frames when present.
	Video *VideoFrame
	// Binary carries arbitrary non-audio/video frames when present.
	Binary *BinaryFrame
	// Inputs contains the evaluated step 'with:' configuration (CEL-resolved per packet).
	// This is analogous to BUBU_TRIGGER_DATA in batch mode - dynamic configuration that can
	// reference outputs from previous steps. The Hub evaluates this before forwarding.
	// Empty if the step has no 'with:' block or evaluation failed.
	Inputs []byte
	// Transports mirrors the Story's declared transports, allowing engrams to decide whether
	// to keep payloads on the hot path (e.g., LiveKit) or fall back to storage without
	// rereading pod environment.
	Transports []TransportDescriptor
	// Envelope carries optional stream sequencing metadata from the transport layer.
	// When set, it can be used for ordering and replay-aware processing.
	Envelope *transportpb.StreamEnvelope
}

// InboundMessage wraps a transport-delivered StreamMessage and lets the SDK runtime
// track when user processing is complete for delivery policies that require it.
// Best-effort traffic ignores completion signals.
type InboundMessage struct {
	StreamMessage
	receipt *streamMessageReceipt
}

type streamMessageReceipt struct {
	once        sync.Once
	onProcessed func()
}

// NewInboundMessage wraps a StreamMessage for inbound streaming delivery.
func NewInboundMessage(msg StreamMessage) InboundMessage {
	return InboundMessage{StreamMessage: msg}
}

// BindProcessingReceipt attaches an SDK-managed completion hook to an inbound
// message. Runtime code uses this to defer transport acknowledgements until user
// processing explicitly completes. External callers normally do not need it.
func BindProcessingReceipt(msg InboundMessage, onProcessed func()) InboundMessage {
	if onProcessed == nil {
		return msg
	}
	msg.receipt = &streamMessageReceipt{onProcessed: onProcessed}
	return msg
}

// Done notifies the runtime that processing of this inbound message completed
// successfully. Messages without an attached processing receipt ignore this call,
// so best-effort traffic does not require special handling.
func (m InboundMessage) Done() {
	if m.receipt == nil {
		return
	}
	m.receipt.once.Do(func() {
		if m.receipt.onProcessed != nil {
			m.receipt.onProcessed()
		}
	})
}

// Validate reports whether the message uses a transport shape the SDK can encode
// without dropping or reinterpreting data.
func (m StreamMessage) Validate() error { //nolint:gocyclo
	kind := strings.TrimSpace(m.Kind)
	if kind != m.Kind {
		return fmt.Errorf("%w: stream message kind must not have surrounding whitespace", ErrInvalidStreamMessage)
	}
	if strings.TrimSpace(m.MessageID) != m.MessageID {
		return fmt.Errorf("%w: stream message message_id must not have surrounding whitespace", ErrInvalidStreamMessage)
	}
	for key := range m.Metadata {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("%w: stream message metadata keys must not be empty", ErrInvalidStreamMessage)
		}
		if strings.TrimSpace(key) != key {
			return fmt.Errorf("%w: stream message metadata key %q must not have surrounding whitespace", ErrInvalidStreamMessage, key) //nolint:lll
		}
	}
	if err := validateStreamKind(kind); err != nil {
		return err
	}
	if err := validateReservedStreamKindUsage(m, kind); err != nil {
		return err
	}
	frameKinds := make([]string, 0, 3)
	if m.Audio != nil {
		frameKinds = append(frameKinds, "audio")
	}
	if m.Video != nil {
		frameKinds = append(frameKinds, "video")
	}
	if m.Binary != nil {
		frameKinds = append(frameKinds, "binary")
	}
	if len(frameKinds) > 1 {
		return fmt.Errorf("%w: multiple frame payloads set (%s)", ErrInvalidStreamMessage, strings.Join(frameKinds, ", "))
	}
	if err := validateAudioFrame(m.Audio); err != nil {
		return err
	}
	if err := validateVideoFrame(m.Video); err != nil {
		return err
	}
	if err := validateBinaryFrame(m.Binary, m); err != nil {
		return err
	}
	if m.Binary != nil && len(m.Payload) > 0 && !bytes.Equal(m.Payload, m.Binary.Payload) {
		return fmt.Errorf("%w: payload and binary payload must match when both are set", ErrInvalidStreamMessage)
	}
	return nil
}

func validateStreamKind(kind string) error {
	for _, r := range kind {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '.', r == '-', r == '_':
		default:
			return fmt.Errorf("%w: stream message kind %q contains unsupported characters", ErrInvalidStreamMessage, kind)
		}
	}
	return nil
}

func validateReservedStreamKindUsage(msg StreamMessage, kind string) error {
	switch strings.ToLower(kind) {
	case StreamMessageKindError:
		if len(msg.Payload) == 0 {
			return fmt.Errorf("%w: error messages require payload", ErrInvalidStreamMessage)
		}
		if msg.Audio != nil || msg.Video != nil || msg.Binary != nil {
			return fmt.Errorf("%w: error messages must not carry media frames", ErrInvalidStreamMessage)
		}
	case StreamMessageKindHeartbeat, StreamMessageKindNoop:
		if carriesReservedKindBody(msg) {
			return fmt.Errorf("%w: %s messages must not carry payload, metadata, or frames", ErrInvalidStreamMessage, strings.ToLower(kind)) //nolint:lll
		}
	}
	return nil
}

func carriesReservedKindBody(msg StreamMessage) bool {
	return len(msg.Payload) > 0 ||
		len(msg.Inputs) > 0 ||
		len(msg.Transports) > 0 ||
		len(msg.Metadata) > 0 ||
		msg.Audio != nil ||
		msg.Video != nil ||
		msg.Binary != nil ||
		msg.Envelope != nil ||
		strings.TrimSpace(msg.MessageID) != "" ||
		!msg.Timestamp.IsZero()
}

func carriesStructuredEnvelopeFields(msg StreamMessage) bool {
	return strings.TrimSpace(msg.Kind) != "" ||
		strings.TrimSpace(msg.MessageID) != "" ||
		!msg.Timestamp.IsZero() ||
		len(msg.Metadata) > 0 ||
		len(msg.Payload) > 0 ||
		len(msg.Inputs) > 0 ||
		len(msg.Transports) > 0 ||
		msg.Envelope != nil
}

func validateAudioFrame(audio *AudioFrame) error {
	if audio == nil {
		return nil
	}
	if strings.TrimSpace(audio.Codec) != audio.Codec {
		return fmt.Errorf("%w: audio frame codec must not have surrounding whitespace", ErrInvalidStreamMessage)
	}
	if len(audio.PCM) == 0 {
		return fmt.Errorf("%w: audio frame missing pcm payload", ErrInvalidStreamMessage)
	}
	if audio.SampleRateHz <= 0 {
		return fmt.Errorf("%w: audio frame sample rate must be positive", ErrInvalidStreamMessage)
	}
	if audio.Channels <= 0 {
		return fmt.Errorf("%w: audio frame channels must be positive", ErrInvalidStreamMessage)
	}
	if audio.Timestamp < 0 {
		return fmt.Errorf("%w: audio frame timestamp must not be negative", ErrInvalidStreamMessage)
	}
	return nil
}

func validateVideoFrame(video *VideoFrame) error {
	if video == nil {
		return nil
	}
	if strings.TrimSpace(video.Codec) != video.Codec {
		return fmt.Errorf("%w: video frame codec must not have surrounding whitespace", ErrInvalidStreamMessage)
	}
	if len(video.Payload) == 0 {
		return fmt.Errorf("%w: video frame missing payload", ErrInvalidStreamMessage)
	}
	if !video.Raw && strings.TrimSpace(video.Codec) == "" {
		return fmt.Errorf("%w: encoded video frame requires codec", ErrInvalidStreamMessage)
	}
	if video.Raw && (video.Width == 0 || video.Height == 0) {
		return fmt.Errorf("%w: raw video frame requires width and height", ErrInvalidStreamMessage)
	}
	if video.Timestamp < 0 {
		return fmt.Errorf("%w: video frame timestamp must not be negative", ErrInvalidStreamMessage)
	}
	return nil
}

func validateBinaryFrame(binary *BinaryFrame, msg StreamMessage) error {
	if binary == nil {
		return nil
	}
	if len(binary.Payload) == 0 {
		return fmt.Errorf("%w: binary frame missing payload", ErrInvalidStreamMessage)
	}
	if binary.Timestamp < 0 {
		return fmt.Errorf("%w: binary frame timestamp must not be negative", ErrInvalidStreamMessage)
	}
	mimeType := strings.TrimSpace(binary.MimeType)
	if mimeType != binary.MimeType {
		return fmt.Errorf("%w: binary frame mime type must not have surrounding whitespace", ErrInvalidStreamMessage)
	}
	if mimeType != "" {
		parsedMediaType, _, err := mime.ParseMediaType(mimeType)
		if err != nil {
			return fmt.Errorf("%w: binary frame mime type %q is invalid", ErrInvalidStreamMessage, mimeType)
		}
		mimeType = strings.ToLower(parsedMediaType)
	}
	if mimeType == envelope.MIMEType && !carriesStructuredEnvelopeFields(msg) {
		return fmt.Errorf(
			"%w: binary frame MIME type %q is reserved for envelope payloads",
			ErrInvalidStreamMessage,
			envelope.MIMEType,
		)
	}
	if mimeType == envelope.MIMEType && !bytes.Equal(binary.Payload, msg.Payload) {
		return fmt.Errorf(
			"%w: reserved envelope MIME type requires binary payload to mirror the structured payload",
			ErrInvalidStreamMessage,
		)
	}
	return nil
}

// StreamingEngram is the interface for components that handle real-time,
// continuous data streams. They are typically used for tasks like transformations,
// filtering, or routing of data between other streaming systems. A StreamingEngram
// usually runs as a Kubernetes Deployment and communicates over gRPC.
type StreamingEngram[C any] interface {
	Engram[C]
	// Stream is the core method for handling bidirectional data flow with metadata.
	// The SDK provides inbound messages plus an outbound StreamMessage channel.
	// Metadata should be propagated to enable tracing across the streaming pipeline.
	// The method should process messages from `in`, call Done on messages it handled
	// successfully (or intentionally dropped), and write results to `out` until the
	// input channel is closed or the context is canceled.
	Stream(ctx context.Context, in <-chan InboundMessage, out chan<- StreamMessage) error
}
