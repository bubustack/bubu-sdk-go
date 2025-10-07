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
	}, nil
}

// Hydrate recursively scans a data structure for storage references and replaces
// them with the actual content from the storage backend.
func (sm *StorageManager) Hydrate(ctx context.Context, data any) (any, error) {
	if sm.store == nil {
		// When storage is disabled, pass through inputs unchanged unless they contain
		// a storage reference which we cannot hydrate without a backend.
		if containsStorageRef(data, 0) {
			return nil, fmt.Errorf("found storage reference in inputs but storage is disabled (set BUBU_STORAGE_PROVIDER=s3 or file)")
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
				return nil, fmt.Errorf("output size %d bytes exceeds inline limit %d and storage is disabled; either enable storage (BUBU_STORAGE_PROVIDER=s3|file), increase limit (BUBU_MAX_INLINE_SIZE=%d or higher), or reduce output size", size, sm.maxInlineSize, size)
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

	result, err := sm.dehydrateValue(ctx, data, stepRunID, "", 0)
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
		return fmt.Errorf("invalid storage reference: path traversal detected (got '%s', normalized to '%s')", refPath, cleanPath)
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
		// Check if it's a storage reference
		if ref, exists := v[storageRefKey]; exists && len(v) == 1 {
			refPath, ok := ref.(string)
			if !ok {
				return nil, fmt.Errorf("invalid storage reference: '%v' is not a string", ref)
			}
			// Validate the reference path before reading from storage
			if err := validateStorageRef(refPath); err != nil {
				return nil, fmt.Errorf("failed to read offloaded data from '%s': %w", refPath, err)
			}

			var buf bytes.Buffer
			if err := sm.store.Read(ctx, refPath, &buf); err != nil {
				return nil, fmt.Errorf("failed to read offloaded data from '%s': %w", refPath, err)
			}

			// Unmarshal into the StoredObject wrapper.
			var storedObj StoredObject
			if err := json.Unmarshal(buf.Bytes(), &storedObj); err != nil {
				return nil, fmt.Errorf("failed to unmarshal stored object from '%s', content may be malformed: %w", refPath, err)
			}

			// Handle based on ContentType.
			switch storedObj.ContentType {
			case "json":
				var loadedValue any
				if err := json.Unmarshal(storedObj.Data, &loadedValue); err != nil {
					return nil, fmt.Errorf("failed to unmarshal hydrated JSON data from '%s': %w", refPath, err)
				}
				return loadedValue, nil
			case "raw":
				// For raw content, we need to unquote the JSON string.
				var rawContent string
				if err := json.Unmarshal(storedObj.Data, &rawContent); err != nil {
					return nil, fmt.Errorf("failed to unmarshal hydrated raw content from '%s': %w", refPath, err)
				}
				return rawContent, nil
			default:
				return nil, fmt.Errorf("unknown content type '%s' in stored object '%s'", storedObj.ContentType, refPath)
			}
		}
		// Otherwise, recurse through the map
		hydratedMap := make(map[string]any)
		for k, val := range v {
			hydratedVal, err := sm.hydrateValue(ctx, val, depth+1)
			if err != nil {
				return nil, err
			}
			hydratedMap[k] = hydratedVal
		}
		return hydratedMap, nil
	case []any:
		hydratedSlice := make([]any, len(v))
		for i, item := range v {
			hydratedItem, err := sm.hydrateValue(ctx, item, depth+1)
			if err != nil {
				return nil, err
			}
			hydratedSlice[i] = hydratedItem
		}
		return hydratedSlice, nil
	default:
		return value, nil
	}
}

func (sm *StorageManager) dehydrateValue(ctx context.Context, value any, stepRunID, keyPrefix string, depth int) (any, error) {
	maxDepth := getMaxRecursionDepth()
	if depth > maxDepth {
		return nil, fmt.Errorf("maximum recursion depth (%d) exceeded during dehydration", maxDepth)
	}

	// 1. Recurse into complex types first.
	switch v := value.(type) {
	case map[string]any:
		dehydratedMap := make(map[string]any)
		for k, val := range v {
			// Create a unique path component for the nested field.
			newPrefix := k
			if keyPrefix != "" {
				newPrefix = fmt.Sprintf("%s.%s", keyPrefix, k)
			}
			dehydratedVal, err := sm.dehydrateValue(ctx, val, stepRunID, newPrefix, depth+1)
			if err != nil {
				return nil, err
			}
			dehydratedMap[k] = dehydratedVal
		}
		return dehydratedMap, nil
	case []any:
		// First pass: dehydrate elements individually
		dehydratedSlice := make([]any, len(v))
		hadIndividualOffload := false
		for i, item := range v {
			newPrefix := strconv.Itoa(i)
			if keyPrefix != "" {
				newPrefix = fmt.Sprintf("%s.%s", keyPrefix, newPrefix)
			}
			dehydratedItem, err := sm.dehydrateValue(ctx, item, stepRunID, newPrefix, depth+1)
			if err != nil {
				return nil, err
			}
			// Detect if this element was offloaded to a storage ref
			if m, ok := dehydratedItem.(map[string]any); ok && len(m) == 1 {
				if _, ok := m[storageRefKey]; ok {
					hadIndividualOffload = true
				}
			}
			dehydratedSlice[i] = dehydratedItem
		}
		// If no individual elements were offloaded but the entire slice is large,
		// offload the whole slice as a single object.
		if !hadIndividualOffload {
			bytes, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal slice for size check: %w", err)
			}
			if len(bytes) > sm.maxInlineSize {
				return sm.offloadValue(ctx, bytes, "json", stepRunID, keyPrefix)
			}
		}
		return dehydratedSlice, nil
	case string:
		// For strings, we also check size and potentially store them externally.
		if len(v) > sm.maxInlineSize {
			// The data for a raw string needs to be a JSON-encoded string.
			marshaledString, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal raw string for storage: %w", err)
			}
			return sm.offloadValue(ctx, marshaledString, "raw", stepRunID, keyPrefix)
		}
		return v, nil
	default:
		// For any other type, we marshal it to see its size.
		bytes, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal value for size check: %w", err)
		}
		if len(bytes) <= sm.maxInlineSize {
			return value, nil // Value is small enough, return as-is.
		}
		return sm.offloadValue(ctx, bytes, "json", stepRunID, keyPrefix)
	}
}

// offloadValue handles the process of wrapping a value in a StoredObject,
// writing it to the storage backend, and returning a storage reference.
// Uses io.Pipe to stream JSON encoding directly to storage without intermediate buffering.
func (sm *StorageManager) offloadValue(ctx context.Context, rawData []byte, contentType, stepRunID, keyPrefix string) (any, error) {
	storedObj := StoredObject{
		ContentType: contentType,
		Data:        json.RawMessage(rawData),
	}

	fileName := fmt.Sprintf("%s.json", keyPrefix)
	if keyPrefix == "" {
		fileName = "output.json"
	}
	filePath := filepath.Join(sm.outputPrefix, stepRunID, fileName)

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
		defer pw.Close()
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
