package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

const (
	DefaultMaxInlineSize = 32 * 1024 // 32 KiB
	storageRefKey        = "$bubuStorageRef"
)

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
}

// GetStore returns the underlying storage implementation.
func (sm *StorageManager) GetStore() Store {
	return sm.store
}

// NewManager creates a new StorageManager, automatically configuring the
// storage backend based on environment variables.
func NewManager(ctx context.Context) (*StorageManager, error) {
	maxSize := DefaultMaxInlineSize
	if maxSizeStr := os.Getenv("BUBU_MAX_INLINE_SIZE"); maxSizeStr != "" {
		if val, err := strconv.Atoi(maxSizeStr); err == nil && val > 0 {
			fmt.Printf("BUBU SDK DIAGNOSTIC: Overriding max inline size from environment: %d bytes\n", val)
			maxSize = val
		}
	}

	var store Store
	provider := os.Getenv("BUBU_STORAGE_PROVIDER")
	fmt.Printf("BUBU SDK DIAGNOSTIC: Initializing storage manager with provider: '%s'\n", provider)

	switch provider {
	case "s3":
		s3Store, err := NewS3Store(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize S3 storage: %w", err)
		}
		store = s3Store
	case "file":
		basePath := os.Getenv("BUBU_STORAGE_PATH")
		if basePath != "" {
			fileStore, err := NewFileStore(basePath)
			if err != nil {
				return nil, fmt.Errorf("failed to initialize file storage: %w", err)
			}
			store = fileStore
		}
	default:
		// No provider or an unknown provider means storage is disabled.
		fmt.Println("BUBU SDK DIAGNOSTIC: No storage provider configured, offloading is disabled.")
		store = nil
	}

	return &StorageManager{
		store:         store,
		maxInlineSize: maxSize,
	}, nil
}

// Hydrate recursively scans a data structure for storage references and replaces
// them with the actual content from the storage backend.
func (sm *StorageManager) Hydrate(ctx context.Context, data interface{}) (interface{}, error) {
	if sm.store == nil {
		return data, nil
	}
	return sm.hydrateValue(ctx, data)
}

// Dehydrate recursively checks the size of a data structure. If it exceeds the inline
// size limit, it saves the data to the storage backend and replaces it with a
// storage reference.
func (sm *StorageManager) Dehydrate(ctx context.Context, data interface{}, stepRunID string) (interface{}, error) {
	if sm.store == nil {
		return data, nil
	}
	return sm.dehydrateValue(ctx, data, stepRunID, "")
}

func (sm *StorageManager) hydrateValue(ctx context.Context, value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case map[string]interface{}:
		// Check if it's a storage reference
		if ref, exists := v[storageRefKey]; exists && len(v) == 1 {
			refPath, ok := ref.(string)
			if !ok {
				return nil, fmt.Errorf("invalid storage reference: '%v' is not a string", ref)
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
				var loadedValue interface{}
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
		hydratedMap := make(map[string]interface{})
		for k, val := range v {
			hydratedVal, err := sm.hydrateValue(ctx, val)
			if err != nil {
				return nil, err
			}
			hydratedMap[k] = hydratedVal
		}
		return hydratedMap, nil
	case []interface{}:
		hydratedSlice := make([]interface{}, len(v))
		for i, item := range v {
			hydratedItem, err := sm.hydrateValue(ctx, item)
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

func (sm *StorageManager) dehydrateValue(ctx context.Context, value interface{}, stepRunID, keyPrefix string) (interface{}, error) {
	// 1. Recurse into complex types first.
	switch v := value.(type) {
	case map[string]interface{}:
		dehydratedMap := make(map[string]interface{})
		for k, val := range v {
			// Create a unique path component for the nested field.
			newPrefix := k
			if keyPrefix != "" {
				newPrefix = fmt.Sprintf("%s.%s", keyPrefix, k)
			}
			dehydratedVal, err := sm.dehydrateValue(ctx, val, stepRunID, newPrefix)
			if err != nil {
				return nil, err
			}
			dehydratedMap[k] = dehydratedVal
		}
		return dehydratedMap, nil
	case []interface{}:
		// For now, we treat slices as atomic units. If a slice is too large, the whole thing is offloaded.
		// A future enhancement could be to selectively dehydrate large elements within a slice.
	}

	// 2. For primitive types (or slices treated as a whole), check the size.
	var rawData []byte
	var contentType string

	// Determine the content type. If it's a simple string, treat it as raw.
	// Otherwise, marshal it as JSON. This is key for handling non-JSON text correctly.
	if str, ok := value.(string); ok {
		contentType = "raw"
		// The data for a raw string needs to be a JSON-encoded string.
		rawData = []byte(fmt.Sprintf(`"%s"`, str))
	} else {
		contentType = "json"
		b, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal value for size check: %w", err)
		}
		rawData = b
	}

	if len(rawData) <= sm.maxInlineSize {
		return value, nil // Value is small enough, return as-is.
	}

	// 3. If the value is too large, wrap it in our StoredObject and offload it.
	storedObj := StoredObject{
		ContentType: contentType,
		Data:        json.RawMessage(rawData),
	}

	finalPayload, err := json.Marshal(storedObj)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal stored object wrapper: %w", err)
	}

	filePath := filepath.Join("outputs", stepRunID, fmt.Sprintf("%s.json", keyPrefix))
	if err := sm.store.Write(ctx, filePath, bytes.NewReader(finalPayload)); err != nil {
		return nil, fmt.Errorf("failed to write offloaded value for key '%s': %w", keyPrefix, err)
	}

	// Return the storage reference in place of the large value.
	return map[string]interface{}{storageRefKey: filePath}, nil
}
