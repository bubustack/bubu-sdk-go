package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/bubu-sdk-go/k8s"
	sdkenv "github.com/bubustack/bubu-sdk-go/pkg/env"
	"github.com/bubustack/core/contracts"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
)

// ErrLogsUnavailable indicates that logs cannot be published (for example, when
// not running inside a StepRun or when storage is disabled). Callers may treat
// this as a soft failure.
var ErrLogsUnavailable = errors.New("log publishing unavailable: not running inside a StepRun or storage disabled")

const (
	defaultLogContentType = "text/plain; charset=utf-8"
	logPatchTimeout       = 5 * time.Second
	defaultLogCaptureSize = 1024 * 1024 // 1 MiB
	defaultLogFileMaxSize = 5 * 1024 * 1024
	logAllowedRootsEnv    = "BUBU_LOG_ALLOWED_ROOTS"
	logFileMaxBytesEnv    = "BUBU_LOG_FILE_MAX_BYTES"
)

var (
	logPublisherMu     sync.Mutex
	logPublisherInst   *logPublisher
	logPublisherErr    error // only set for permanent errors (missing env)
	logClientFactory   = func() (logPatcher, error) { return k8s.SharedClient() }
	logSegmentSanitize = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)
)

type logPatcher interface {
	PatchStepRunStatus(ctx context.Context, stepRunName string, status runsv1alpha1.StepRunStatus) error
}

type logPublisher struct {
	client    logPatcher
	stepRunID string
	namespace string
	storyRun  string
	storyName string
	stepName  string
	engram    string
}

type logCapture struct {
	buf *cappedBuffer
}

func (c *logCapture) Drain() []byte {
	if c == nil || c.buf == nil {
		return nil
	}
	return c.buf.Drain()
}

type cappedBuffer struct {
	mu   sync.Mutex
	max  int
	data []byte
}

func newCappedBuffer(max int) *cappedBuffer {
	if max <= 0 {
		max = defaultLogCaptureSize
	}
	return &cappedBuffer{max: max}
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if b == nil || len(p) == 0 {
		return len(p), nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.max <= 0 {
		return len(p), nil
	}
	if len(p) >= b.max {
		b.data = append(b.data[:0], p[len(p)-b.max:]...)
		return len(p), nil
	}
	if len(b.data)+len(p) > b.max {
		overflow := len(b.data) + len(p) - b.max
		b.data = append(b.data[overflow:], p...)
		return len(p), nil
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *cappedBuffer) Drain() []byte {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.data) == 0 {
		return nil
	}
	out := make([]byte, len(b.data))
	copy(out, b.data)
	b.data = b.data[:0]
	return out
}

func newDefaultLoggerWithCapture() (*slog.Logger, *logCapture) {
	opts := &slog.HandlerOptions{}
	if isDebugEnabled() {
		opts.Level = slog.LevelDebug
	}
	buf := newCappedBuffer(defaultLogCaptureSize)
	writer := io.MultiWriter(os.Stdout, buf)
	logger := slog.New(slog.NewJSONHandler(writer, opts))
	return logger, &logCapture{buf: buf}
}

func publishCapturedLogs(ctx context.Context) {
	capture := logCaptureFromContext(ctx)
	if capture == nil {
		return
	}
	payload := capture.Drain()
	if len(payload) == 0 {
		return
	}
	if err := PublishLogsWithContentType(ctx, payload, "application/x-ndjson"); err != nil && !errors.Is(err, ErrLogsUnavailable) { //nolint:lll
		LoggerFromContext(ctx).Warn("Failed to publish captured logs", "error", err)
	}
}

// PublishLogs uploads log bytes to storage (when enabled) and patches StepRun.status.logs
// with a storage reference. If storage is disabled, it returns ErrLogsUnavailable.
func PublishLogs(ctx context.Context, payload []byte) error {
	return PublishLogsWithContentType(ctx, payload, "")
}

// PublishLogsWithContentType is like PublishLogs but lets callers set a content type.
func PublishLogsWithContentType(ctx context.Context, payload []byte, contentType string) error {
	if len(payload) == 0 {
		return nil
	}
	publisher, err := getLogPublisher()
	if err != nil {
		return err
	}
	sm, err := storage.SharedManager(ctx)
	if err != nil {
		return fmt.Errorf("failed to create storage manager: %w", err)
	}
	if sm == nil || sm.GetStore() == nil {
		return ErrLogsUnavailable
	}
	objectPath := buildLogStoragePath(publisher, time.Now().UTC())
	if objectPath == "" {
		return fmt.Errorf("failed to build log storage path")
	}
	ctype := strings.TrimSpace(contentType)
	if ctype == "" {
		ctype = defaultLogContentType
	}
	if err := sm.WriteBlob(ctx, objectPath, ctype, payload); err != nil {
		return err
	}

	ref := map[string]any{
		storage.StorageRefKey:         objectPath,
		storage.StorageContentTypeKey: ctype,
	}
	if schema, version := logSchemaMetadata(publisher); schema != "" {
		ref[storage.StorageSchemaKey] = schema
		if version != "" {
			ref[storage.StorageSchemaVersionKey] = version
		}
	}

	raw, err := json.Marshal(ref)
	if err != nil {
		return fmt.Errorf("failed to marshal log reference: %w", err)
	}
	patch := runsv1alpha1.StepRunStatus{
		Logs: &k8sruntime.RawExtension{Raw: raw},
	}
	ctx, cancel := context.WithTimeout(ctx, logPatchTimeout)
	defer cancel()
	return publisher.client.PatchStepRunStatus(ctx, publisher.stepRunID, patch)
}

// PublishLogFile reads the provided file and publishes its contents as logs.
func PublishLogFile(ctx context.Context, path string, contentType string) error { //nolint:revive
	data, err := readAllowedLogFile(path)
	if err != nil {
		return err
	}
	return PublishLogsWithContentType(ctx, data, contentType)
}

func readAllowedLogFile(path string) ([]byte, error) { //nolint:revive
	resolvedPath, err := resolveAllowedLogFilePath(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(resolvedPath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat log file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("log file path must reference a regular file")
	}
	maxBytes := resolveLogFileMaxBytes()
	if maxBytes > 0 && info.Size() > int64(maxBytes) {
		return nil, fmt.Errorf("log file exceeds max bytes (%d > %d)", info.Size(), maxBytes)
	}
	data, err := os.ReadFile(resolvedPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read log file: %w", err)
	}
	return data, nil
}

func resolveAllowedLogFilePath(path string) (string, error) { //nolint:revive
	cleanPath := strings.TrimSpace(path)
	if cleanPath == "" {
		return "", fmt.Errorf("log file path is required")
	}
	absPath, err := filepath.Abs(cleanPath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve log file path: %w", err)
	}
	resolvedPath, err := filepath.EvalSymlinks(absPath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve log file path: %w", err)
	}
	for _, root := range resolveAllowedLogRoots() {
		if pathWithinRoot(resolvedPath, root) {
			return resolvedPath, nil
		}
	}
	return "", fmt.Errorf("log file path %q is outside allowed roots", cleanPath)
}

func resolveAllowedLogRoots() []string {
	rawRoots := strings.TrimSpace(os.Getenv(logAllowedRootsEnv))
	var candidates []string
	if rawRoots == "" {
		candidates = defaultLogAllowedRoots()
	} else {
		candidates = strings.Split(rawRoots, string(os.PathListSeparator))
	}

	roots := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		root := canonicalizeLogRoot(candidate)
		if root == "" {
			continue
		}
		if _, ok := seen[root]; ok {
			continue
		}
		seen[root] = struct{}{}
		roots = append(roots, root)
	}
	return roots
}

func defaultLogAllowedRoots() []string {
	roots := []string{"/tmp", "/var/tmp", "/var/log", os.TempDir()}
	if wd, err := os.Getwd(); err == nil && strings.TrimSpace(wd) != "" {
		roots = append(roots, wd)
	}
	return roots
}

func canonicalizeLogRoot(path string) string { //nolint:revive
	cleanPath := strings.TrimSpace(path)
	if cleanPath == "" {
		return ""
	}
	absPath, err := filepath.Abs(cleanPath)
	if err != nil {
		return ""
	}
	resolvedPath, err := filepath.EvalSymlinks(absPath)
	if err == nil {
		return filepath.Clean(resolvedPath)
	}
	return filepath.Clean(absPath)
}

func pathWithinRoot(path string, root string) bool { //nolint:revive
	if strings.TrimSpace(path) == "" || strings.TrimSpace(root) == "" {
		return false
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
}

func resolveLogFileMaxBytes() int {
	return sdkenv.GetInt(logFileMaxBytesEnv, defaultLogFileMaxSize)
}

func getLogPublisher() (*logPublisher, error) {
	logPublisherMu.Lock()
	defer logPublisherMu.Unlock()
	if logPublisherInst != nil {
		return logPublisherInst, nil
	}
	if logPublisherErr != nil {
		// Permanent error (e.g. missing env) — don't retry.
		return nil, logPublisherErr
	}
	stepRunID := strings.TrimSpace(os.Getenv(contracts.StepRunNameEnv))
	if stepRunID == "" {
		logPublisherErr = ErrLogsUnavailable
		return nil, logPublisherErr
	}
	client, err := logClientFactory()
	if err != nil {
		// Transient error — don't cache, allow retry on next call.
		return nil, fmt.Errorf("failed to initialize log client: %w", err)
	}
	logPublisherInst = &logPublisher{
		client:    client,
		stepRunID: stepRunID,
		namespace: k8s.ResolvePodNamespace(),
		storyRun:  strings.TrimSpace(os.Getenv(contracts.StoryRunIDEnv)),
		storyName: strings.TrimSpace(os.Getenv(contracts.StoryNameEnv)),
		stepName:  strings.TrimSpace(os.Getenv(contracts.StepNameEnv)),
		engram:    strings.TrimSpace(os.Getenv(contracts.EngramNameEnv)),
	}
	return logPublisherInst, nil
}

func buildLogStoragePath(p *logPublisher, ts time.Time) string {
	if p == nil || strings.TrimSpace(p.stepRunID) == "" {
		return ""
	}
	segments := []string{"logs"}
	if v := sanitizeLogSegment(p.storyRun); v != "" {
		segments = append(segments, v)
	}
	if v := sanitizeLogSegment(p.stepName); v != "" {
		segments = append(segments, v)
	}
	if v := sanitizeLogSegment(p.stepRunID); v != "" {
		segments = append(segments, v)
	}
	filename := fmt.Sprintf("%d.log", ts.UnixNano())
	segments = append(segments, filename)
	key := path.Join(segments...)
	return storage.NamespacedKey(strings.TrimSpace(p.namespace), key)
}

func sanitizeLogSegment(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	cleaned := logSegmentSanitize.ReplaceAllString(trimmed, "-")
	cleaned = strings.Trim(cleaned, "-_.")
	return cleaned
}

func logSchemaMetadata(p *logPublisher) (string, string) {
	if p == nil {
		return "", ""
	}
	namespace := strings.TrimSpace(p.namespace)
	storyName := strings.TrimSpace(p.storyName)
	stepName := strings.TrimSpace(p.stepName)
	engramName := strings.TrimSpace(p.engram)

	var schema string
	switch {
	case namespace != "" && engramName != "":
		schema = fmt.Sprintf("bubu://engram/%s/%s/logs", namespace, engramName)
	case namespace != "" && storyName != "" && stepName != "":
		schema = fmt.Sprintf("bubu://story/%s/%s/steps/%s/logs", namespace, storyName, stepName)
	case storyName != "" && stepName != "":
		schema = fmt.Sprintf("bubu://story/%s/steps/%s/logs", storyName, stepName)
	}

	version := strings.TrimSpace(os.Getenv(contracts.EngramVersionEnv))
	if version == "" {
		version = strings.TrimSpace(os.Getenv(contracts.StoryVersionEnv))
	}
	return schema, version
}
