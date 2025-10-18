package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/bubustack/bubu-sdk-go/pkg/metrics"
)

const (
	// DefaultMaxInlineSize is the threshold in bytes above which data is offloaded to storage.
	//
	// Aligns with operator's DefaultMaxInlineSize (bobrapet/internal/config/controller_config.go:246).
	// Override via BUBU_MAX_INLINE_SIZE. Keeping this low (1 KiB) prevents apiserver/etcd overload
	// while allowing small outputs to remain inline for fast access.
	DefaultMaxInlineSize = 1 * 1024 // 1 KiB

	// DefaultMaxRecursionDepth is the maximum nesting depth for hydration/dehydration.
	//
	// Prevents stack overflow on deeply nested data structures. Override via BUBU_MAX_RECURSION_DEPTH.
	DefaultMaxRecursionDepth = 10

	// DefaultStorageTimeout provides ample time for large file uploads (e.g., 100MB at 1MB/s = 100s + overhead).
	//
	// Operators should tune this based on expected output sizes and S3 latency:
	//   timeout >= (max_output_mb / upload_bandwidth_mbps) * 1.5 + baseline_latency_sec
	// Override via BUBU_STORAGE_TIMEOUT.
	DefaultStorageTimeout = 300 * time.Second // 5min for storage ops

	storageRefKey = "$bubuStorageRef"
)

// getMaxInlineSize returns the max inline size from env or default
func getMaxInlineSize() (int, error) {
	if v := os.Getenv("BUBU_MAX_INLINE_SIZE"); v != "" {
		val, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("invalid BUBU_MAX_INLINE_SIZE '%s': must be a positive integer", v)
		}
		if val <= 0 {
			return 0, fmt.Errorf("invalid BUBU_MAX_INLINE_SIZE '%s': must be > 0", v)
		}
		return val, nil
	}
	return DefaultMaxInlineSize, nil
}

// getMaxRecursionDepth returns the max recursion depth from env or default
func getMaxRecursionDepth() int {
	if v := os.Getenv("BUBU_MAX_RECURSION_DEPTH"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			return i
		}
	}
	return DefaultMaxRecursionDepth
}

// getStorageTimeout returns the timeout for storage operations from env or default
func getStorageTimeout() time.Duration {
	if v := os.Getenv("BUBU_STORAGE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return DefaultStorageTimeout
}

// StoredObject is a wrapper for data offloaded to storage, providing metadata
// about the content type to ensure correct hydration.
type StoredObject struct {
	// ContentType indicates whether the stored data is "json" or "raw" text.
	ContentType string `json:"contentType"`
	// Data holds the actual content. For JSON, it's a RawMessage; for raw, it's a string.
	Data json.RawMessage `json:"data"`
}

// StorageManager handles the transparent offloading of large inputs and outputs
// to a configured storage backend.
type StorageManager struct {
	store         Store
	maxInlineSize int
	outputPrefix  string
	inputPrefix   string
}

// Context helpers for metrics attribution
type stepRunIDKey struct{}

// WithStepRunID attaches a StepRunID to the context for hydration metrics attribution.
func WithStepRunID(ctx context.Context, stepRunID string) context.Context {
	if stepRunID == "" {
		return ctx
	}
	return context.WithValue(ctx, stepRunIDKey{}, stepRunID)
}

func stepRunIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(stepRunIDKey{}).(string); ok {
		return v
	}
	return ""
}

// GetStore returns the underlying Store implementation used by this manager.
// This is primarily useful for testing and debugging.
func (sm *StorageManager) GetStore() Store {
	return sm.store
}

// NewManager creates a new StorageManager, automatically configuring the
// storage backend based on environment variables.
func NewManager(ctx context.Context) (*StorageManager, error) {
	maxSize, err := getMaxInlineSize()
	if err != nil {
		return nil, err
	}

	outputPrefix := "outputs"
	if prefix := os.Getenv("BUBU_STORAGE_OUTPUT_PREFIX"); prefix != "" {
		outputPrefix = prefix
	}
	inputPrefix := "inputs"
	if prefix := os.Getenv("BUBU_STORAGE_INPUT_PREFIX"); prefix != "" {
		inputPrefix = prefix
	}

	var store Store
	provider := os.Getenv("BUBU_STORAGE_PROVIDER")

	switch provider {
	case "s3":
		s3Store, err := NewS3Store(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize S3 storage: %w", err)
		}
		store = s3Store
	case "file":
		basePath := os.Getenv("BUBU_STORAGE_PATH")
		if basePath == "" {
			return nil, fmt.Errorf("BUBU_STORAGE_PROVIDER is set to 'file' but BUBU_STORAGE_PATH is not set")
		}
		fileStore, err := NewFileStore(basePath)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize file storage: %w", err)
		}
		store = fileStore
	case "", "none":
		// Disabled or not set
		store = nil
	default:
		return nil, fmt.Errorf("unknown BUBU_STORAGE_PROVIDER '%s' (expected 's3', 'file', or 'none')", provider)
	}

	return &StorageManager{
		store:         store,
		maxInlineSize: maxSize,
		outputPrefix:  outputPrefix,
		inputPrefix:   inputPrefix,
	}, nil
}

// Hydrate recursively scans a data structure for storage references and replaces
// them with the actual content from the storage backend.
func (sm *StorageManager) Hydrate(ctx context.Context, data any) (any, error) {
	if sm.store == nil {
		// When storage is disabled, pass through inputs unchanged unless they contain
		// a storage reference which we cannot hydrate without a backend.
		if containsStorageRef(data, 0) {
			return nil, fmt.Errorf(
				"found storage reference in inputs but storage is disabled " +
					"(set BUBU_STORAGE_PROVIDER=s3 or file)",
			)
		}
		// This maintains backward compatibility with existing tests and behavior.
		return data, nil
	}

	// Enforce timeout on storage operations to prevent indefinite hangs
	timeout := getStorageTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result, err := sm.hydrateValue(ctx, data, 0)
	if err == nil && result != nil {
		// Record hydration size for observability
		size := estimateSize(result)
		metrics.RecordHydrationSize(ctx, int64(size), stepRunIDFromContext(ctx))
	}
	return result, err
}

// DehydrateInputs dehydrates StoryRun inputs.
func (sm *StorageManager) DehydrateInputs(ctx context.Context, data any, storyRunID string) (any, error) {
	if sm.store == nil {
		if data != nil {
			size := estimateSize(data)
			if size > sm.maxInlineSize {
				return nil, fmt.Errorf(
					"output size %d bytes exceeds inline limit %d and storage is disabled; "+
						"either enable storage (BUBU_STORAGE_PROVIDER=s3|file), "+
						"increase limit (BUBU_MAX_INLINE_SIZE=%d or higher), "+
						"or reduce output size",
					size, sm.maxInlineSize, size,
				)
			}
		}
		return data, nil
	}

	timeout := getStorageTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result, err := sm.dehydrateValue(ctx, data, storyRunID, "", 0, sm.inputPrefix)
	if err == nil && data != nil {
		size := estimateSize(data)
		metrics.RecordDehydrationSize(ctx, int64(size), storyRunID)
	}
	return result, err
}

// Dehydrate recursively checks the size of a data structure. If it exceeds the inline
// size limit, it saves the data to the storage backend and replaces it with a
// storage reference.
func (sm *StorageManager) Dehydrate(ctx context.Context, data any, stepRunID string) (any, error) {
	if sm.store == nil {
		// When storage is disabled, prevent dangerously large payloads from being inlined
		// into the StepRun status, which could exceed apiserver/etcd limits.
		if data != nil {
			size := estimateSize(data)
			if size > sm.maxInlineSize {
				return nil, fmt.Errorf("output size %d bytes exceeds inline limit %d and storage is disabled; "+
					"either enable storage (BUBU_STORAGE_PROVIDER=s3|file), "+
					"increase limit (BUBU_MAX_INLINE_SIZE=%d or higher), "+
					"or reduce output size",
					size, sm.maxInlineSize, size,
				)
			}
		}
		// Otherwise, pass through outputs unchanged. Responsibility for size management
		// lies with cluster policy in this mode.
		return data, nil
	}

	// Enforce timeout on storage operations to prevent indefinite hangs
	timeout := getStorageTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result, err := sm.dehydrateValue(ctx, data, stepRunID, "", 0, sm.outputPrefix)
	if err == nil && data != nil {
		// Record dehydration size for observability
		size := estimateSize(data)
		metrics.RecordDehydrationSize(ctx, int64(size), stepRunID)
	}
	return result, err
}

// validateStorageRef checks if a storage reference path is safe to use.
// It rejects:
// - Absolute paths (e.g., /etc/passwd, C:\secrets\)
// - Path traversal attempts (e.g., ../, ..\, or embedded .. components)
// This provides defense-in-depth against path traversal attacks from malicious
// or compromised upstream engrams, even if storage backend should enforce isolation.
func validateStorageRef(refPath string) error {
	if refPath == "" {
		return fmt.Errorf("storage reference path is empty")
	}

	// Reject absolute paths
	if filepath.IsAbs(refPath) {
		return fmt.Errorf("invalid storage reference: absolute paths not allowed (got '%s')", refPath)
	}

	// Check for path traversal using platform-independent approach
	// filepath.Clean normalizes the path, then we verify it's still relative and doesn't escape
	cleanPath := filepath.Clean(refPath)

	// After cleaning, check if path tries to escape (starts with ..)
	if len(cleanPath) >= 2 && cleanPath[0] == '.' && cleanPath[1] == '.' {
		return fmt.Errorf(
			"invalid storage reference: path traversal detected (got '%s', normalized to '%s')",
			refPath, cleanPath,
		)
	}

	// Also check for ../ or ..\ anywhere in the original path to catch obfuscation attempts
	// (e.g., "foo/../bar" would normalize to "bar" but indicates traversal intent)
	if strings.Contains(refPath, ".."+string(filepath.Separator)) ||
		strings.Contains(refPath, ".."+string('/')) || // Unix separator
		(runtime.GOOS == "windows" && strings.Contains(refPath, ".."+string('\\'))) { // Windows separator
		return fmt.Errorf("invalid storage reference: path traversal detected (got '%s')", refPath)
	}

	return nil
}

func (sm *StorageManager) hydrateValue(ctx context.Context, value any, depth int) (any, error) {
	maxDepth := getMaxRecursionDepth()
	if depth > maxDepth {
		return nil, fmt.Errorf("maximum recursion depth (%d) exceeded during hydration", maxDepth)
	}
	switch v := value.(type) {
	case map[string]any:
		return sm.hydrateMap(ctx, v, depth)
	case []any:
		return sm.hydrateSlice(ctx, v, depth)
	default:
		return value, nil
	}
}

func (sm *StorageManager) hydrateMap(ctx context.Context, m map[string]any, depth int) (any, error) {
	if refPath, ok := singleStorageRef(m); ok {
		return sm.hydrateFromStorageRef(ctx, refPath)
	}
	hydratedMap := make(map[string]any)
	for k, val := range m {
		hv, err := sm.hydrateValue(ctx, val, depth+1)
		if err != nil {
			return nil, err
		}
		hydratedMap[k] = hv
	}
	return hydratedMap, nil
}

func (sm *StorageManager) hydrateSlice(ctx context.Context, s []any, depth int) (any, error) {
	hydrated := make([]any, len(s))
	for i, item := range s {
		hv, err := sm.hydrateValue(ctx, item, depth+1)
		if err != nil {
			return nil, err
		}
		hydrated[i] = hv
	}
	return hydrated, nil
}

func singleStorageRef(m map[string]any) (string, bool) {
	if ref, exists := m[storageRefKey]; exists && len(m) == 1 {
		if refPath, ok := ref.(string); ok {
			return refPath, true
		}
	}
	return "", false
}

func (sm *StorageManager) hydrateFromStorageRef(ctx context.Context, refPath string) (any, error) {
	if err := validateStorageRef(refPath); err != nil {
		return nil, fmt.Errorf("failed to read offloaded data from '%s': %w", refPath, err)
	}
	var buf bytes.Buffer
	if err := sm.store.Read(ctx, refPath, &buf); err != nil {
		return nil, fmt.Errorf("failed to read offloaded data from '%s': %w", refPath, err)
	}
	var storedObj StoredObject
	if err := json.Unmarshal(buf.Bytes(), &storedObj); err != nil {
		return nil, fmt.Errorf(
			"failed to unmarshal stored object from '%s', content may be malformed: %w",
			refPath, err,
		)
	}
	return hydrateStoredObject(storedObj, refPath)
}

func hydrateStoredObject(obj StoredObject, refPath string) (any, error) {
	switch obj.ContentType {
	case "json":
		var loaded any
		if err := json.Unmarshal(obj.Data, &loaded); err != nil {
			return nil, fmt.Errorf("failed to unmarshal hydrated JSON data from '%s': %w", refPath, err)
		}
		return loaded, nil
	case "raw":
		var raw string
		if err := json.Unmarshal(obj.Data, &raw); err != nil {
			return nil, fmt.Errorf("failed to unmarshal hydrated raw content from '%s': %w", refPath, err)
		}
		return raw, nil
	default:
		return nil, fmt.Errorf("unknown content type '%s' in stored object '%s'", obj.ContentType, refPath)
	}
}

func (sm *StorageManager) dehydrateValue(
	ctx context.Context,
	value any,
	id, keyPrefix string,
	depth int,
	pathPrefix string,
) (any, error) {
	maxDepth := getMaxRecursionDepth()
	if depth > maxDepth {
		return nil, fmt.Errorf("maximum recursion depth (%d) exceeded during dehydration", maxDepth)
	}
	switch v := value.(type) {
	case map[string]any:
		return sm.dehydrateMap(ctx, v, id, keyPrefix, depth, pathPrefix)
	case []any:
		return sm.dehydrateSlice(ctx, v, id, keyPrefix, depth, pathPrefix)
	case string:
		return sm.dehydrateString(ctx, v, id, keyPrefix, pathPrefix)
	default:
		return sm.dehydrateOther(ctx, v, id, keyPrefix, pathPrefix)
	}
}

func (sm *StorageManager) dehydrateMap(
	ctx context.Context,
	m map[string]any,
	id, keyPrefix string,
	depth int,
	pathPrefix string,
) (any, error) {
	dehydratedMap := make(map[string]any)
	for k, val := range m {
		newPrefix := k
		if keyPrefix != "" {
			newPrefix = fmt.Sprintf("%s.%s", keyPrefix, k)
		}
		dv, err := sm.dehydrateValue(ctx, val, id, newPrefix, depth+1, pathPrefix)
		if err != nil {
			return nil, err
		}
		dehydratedMap[k] = dv
	}
	return dehydratedMap, nil
}

func (sm *StorageManager) dehydrateSlice(
	ctx context.Context,
	s []any,
	id, keyPrefix string,
	depth int,
	pathPrefix string,
) (any, error) {
	dehydrated := make([]any, len(s))
	hadOffload := false
	for i, item := range s {
		idx := strconv.Itoa(i)
		if keyPrefix != "" {
			idx = fmt.Sprintf("%s.%s", keyPrefix, idx)
		}
		dv, err := sm.dehydrateValue(ctx, item, id, idx, depth+1, pathPrefix)
		if err != nil {
			return nil, err
		}
		if m, ok := dv.(map[string]any); ok && len(m) == 1 {
			if _, ok := m[storageRefKey]; ok {
				hadOffload = true
			}
		}
		dehydrated[i] = dv
	}
	if !hadOffload {
		encodedSlice, err := json.Marshal(s)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal slice for size check: %w", err)
		}
		if len(encodedSlice) > sm.maxInlineSize {
			return sm.offloadValue(ctx, encodedSlice, "json", id, keyPrefix, pathPrefix)
		}
	}
	return dehydrated, nil
}

func (sm *StorageManager) dehydrateString(
	ctx context.Context,
	v string,
	id, keyPrefix, pathPrefix string,
) (any, error) {
	if len(v) <= sm.maxInlineSize {
		return v, nil
	}
	marshaledString, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal raw string for storage: %w", err)
	}
	return sm.offloadValue(ctx, marshaledString, "raw", id, keyPrefix, pathPrefix)
}

func (sm *StorageManager) dehydrateOther(
	ctx context.Context,
	v any,
	id, keyPrefix, pathPrefix string,
) (any, error) {
	encoded, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal value for size check: %w", err)
	}
	if len(encoded) <= sm.maxInlineSize {
		return v, nil
	}
	return sm.offloadValue(ctx, encoded, "json", id, keyPrefix, pathPrefix)
}

// offloadValue handles the process of wrapping a value in a StoredObject,
// writing it to the storage backend, and returning a storage reference.
// Uses io.Pipe to stream JSON encoding directly to storage without intermediate buffering.
func (sm *StorageManager) offloadValue(
	ctx context.Context,
	rawData []byte,
	contentType,
	id,
	keyPrefix,
	pathPrefix string,
) (any, error) {
	storedObj := StoredObject{
		ContentType: contentType,
		Data:        json.RawMessage(rawData),
	}

	fileName := fmt.Sprintf("%s.json", keyPrefix)
	if keyPrefix == "" {
		fileName = "output.json"
	}
	filePath := filepath.Join(pathPrefix, id, fileName)

	// Validate constructed path before writing (defense-in-depth; matches hydration path)
	if err := validateStorageRef(filePath); err != nil {
		return nil, fmt.Errorf("invalid storage path for offload: %w", err)
	}

	// Stream JSON encoding to storage via pipe to avoid buffering in memory
	pr, pw := io.Pipe()
	encErrCh := make(chan error, 1)
	wrErrCh := make(chan error, 1)

	// Encoder goroutine
	go func() {
		defer func() { _ = pw.Close() }()
		encoder := json.NewEncoder(pw)
		encErrCh <- encoder.Encode(storedObj)
	}()

	// Writer goroutine
	go func() {
		wrErrCh <- sm.store.Write(ctx, filePath, pr)
	}()

	// Wait for both to complete; ensure the pipe writer is closed on error to avoid deadlocks
	var encErr, wrErr error
	for i := 0; i < 2; i++ {
		select {
		case encErr = <-encErrCh:
			if encErr != nil {
				_ = pw.CloseWithError(encErr)
			}
		case wrErr = <-wrErrCh:
			if wrErr != nil {
				_ = pw.CloseWithError(wrErr)
			}
		}
	}

	if encErr != nil {
		return nil, fmt.Errorf("failed to encode stored object: %w", encErr)
	}
	if wrErr != nil {
		return nil, fmt.Errorf("failed to write offloaded value for key '%s': %w", keyPrefix, wrErr)
	}

	// Return the storage reference in place of the large value.
	return map[string]any{storageRefKey: filePath}, nil
}

// estimateSize estimates the JSON size of a value for metrics.
// This is a rough estimate and may not be exact.
func estimateSize(v any) int {
	b, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	return len(b)
}

// containsStorageRef walks the provided value looking for a map that is exactly a
// single key equal to the storageRefKey. It protects against deep recursion.
func containsStorageRef(value any, depth int) bool {
	maxDepth := getMaxRecursionDepth()
	if depth > maxDepth {
		return false
	}
	switch t := value.(type) {
	case map[string]any:
		if ref, ok := t[storageRefKey]; ok && len(t) == 1 {
			_, isString := ref.(string)
			return isString
		}
		for _, v := range t {
			if containsStorageRef(v, depth+1) {
				return true
			}
		}
		return false
	case []any:
		for _, item := range t {
			if containsStorageRef(item, depth+1) {
				return true
			}
		}
		return false
	default:
		return false
	}
}
