package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestStorageManager_Hydrate(t *testing.T) {
	ctx := context.Background()
	mock := newMockStore()
	sm := &StorageManager{
		store:         mock,
		maxInlineSize: 100,
	}

	// Setup: Store some data in the mock
	storedData := StoredObject{
		ContentType: "json",
		Data:        json.RawMessage(`{"nested":"value"}`),
	}
	dataBytes, _ := json.Marshal(storedData)
	mock.data["test/path.json"] = dataBytes

	tests := []struct {
		name    string
		input   any
		want    any
		wantErr bool
	}{
		{
			name: "hydrate storage reference",
			input: map[string]any{
				"$bubuStorageRef": "test/path.json",
			},
			want: map[string]any{
				"nested": "value",
			},
			wantErr: false,
		},
		{
			name: "pass through regular map",
			input: map[string]any{
				"key": "value",
			},
			want: map[string]any{
				"key": "value",
			},
			wantErr: false,
		},
		{
			name: "hydrate nested structure",
			input: map[string]any{
				"outer": map[string]any{
					"$bubuStorageRef": "test/path.json",
				},
			},
			want: map[string]any{
				"outer": map[string]any{
					"nested": "value",
				},
			},
			wantErr: false,
		},
		{
			name:    "simple string",
			input:   "just a string",
			want:    "just a string",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sm.Hydrate(ctx, tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Hydrate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Hydrate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStorageManager_Hydrate_RawContent(t *testing.T) {
	ctx := context.Background()
	mock := newMockStore()
	sm := &StorageManager{
		store:         mock,
		maxInlineSize: 100,
	}

	// Store raw string content
	rawStored := StoredObject{
		ContentType: "raw",
		Data:        json.RawMessage(`"This is raw text content"`),
	}
	dataBytes, _ := json.Marshal(rawStored)
	mock.data["raw/content.json"] = dataBytes

	input := map[string]any{
		"$bubuStorageRef": "raw/content.json",
	}

	got, err := sm.Hydrate(ctx, input)
	if err != nil {
		t.Fatalf("Hydrate() error = %v", err)
	}

	expected := "This is raw text content"
	if got != expected {
		t.Errorf("Hydrate() = %v, want %v", got, expected)
	}
}

func TestStorageManager_Hydrate_NoStore(t *testing.T) {
	ctx := context.Background()
	sm := &StorageManager{
		store:         nil,
		maxInlineSize: 100,
	}

	t.Run("error on storage reference", func(t *testing.T) {
		input := map[string]any{
			"$bubuStorageRef": "test/path.json",
		}

		// Should error when encountering storage ref with no backend
		_, err := sm.Hydrate(ctx, input)
		if err == nil {
			t.Fatal("Hydrate() should error when storage reference found but storage is disabled")
		}
		if !strings.Contains(err.Error(), "storage reference") {
			t.Errorf("Expected error about storage reference, got: %v", err)
		}
	})

	t.Run("pass through normal data", func(t *testing.T) {
		input := map[string]any{
			"normalField": "value",
			"nested": map[string]any{
				"data": 123,
			},
		}

		// Should pass through unchanged when no storage refs present
		got, err := sm.Hydrate(ctx, input)
		if err != nil {
			t.Fatalf("Hydrate() unexpected error = %v", err)
		}

		if !reflect.DeepEqual(got, input) {
			t.Errorf("Hydrate() should pass through unchanged when no storage refs")
		}
	})
}

func TestStorageManager_Dehydrate(t *testing.T) {
	ctx := context.Background()
	mock := newMockStore()
	sm := &StorageManager{
		store:         mock,
		maxInlineSize: 50, // Small size to trigger offloading
	}

	tests := []struct {
		name        string
		input       any
		stepRunID   string
		wantOffload bool
		wantWritten int
	}{
		{
			name:        "small string stays inline",
			input:       "small",
			stepRunID:   "run-1",
			wantOffload: false,
			wantWritten: 0,
		},
		{
			name:        "large string gets offloaded",
			input:       "This is a very long string that definitely exceeds the inline size limit and should be offloaded",
			stepRunID:   "run-2",
			wantOffload: true,
			wantWritten: 1,
		},
		{
			name: "small map stays inline",
			input: map[string]any{
				"key": "value",
			},
			stepRunID:   "run-3",
			wantOffload: false,
			wantWritten: 0,
		},
		{
			name: "large array gets offloaded",
			input: []any{
				"element1", "element2", "element3", "element4", "element5",
				"element6", "element7", "element8", "element9", "element10",
			},
			stepRunID:   "run-4",
			wantOffload: true,
			wantWritten: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock.writes = 0 // Reset counter
			got, err := sm.Dehydrate(ctx, tt.input, tt.stepRunID)
			if err != nil {
				t.Errorf("Dehydrate() error = %v", err)
				return
			}

			if tt.wantOffload {
				// Check if result is a storage reference
				if refMap, ok := got.(map[string]any); ok {
					if _, hasRef := refMap["$bubuStorageRef"]; !hasRef {
						t.Error("Expected storage reference in result")
					}
				} else {
					t.Error("Expected map with storage reference")
				}
			}

			if mock.writes != tt.wantWritten {
				t.Errorf("Expected %d writes, got %d", tt.wantWritten, mock.writes)
			}
		})
	}
}

func TestStorageManager_Dehydrate_NoStore(t *testing.T) {
	ctx := context.Background()

	t.Run("error on size exceeding limit", func(t *testing.T) {
		sm := &StorageManager{
			store:         nil,
			maxInlineSize: 10,
		}

		input := "This is a very long string that exceeds the limit"

		// Should error when data exceeds limit and storage is disabled
		_, err := sm.Dehydrate(ctx, input, "run-1")
		if err == nil {
			t.Fatal("Dehydrate() should error when size exceeds limit and storage is disabled")
		}
		if !strings.Contains(err.Error(), "storage is disabled") {
			t.Errorf("Expected error about storage disabled, got: %v", err)
		}
	})

	t.Run("pass through small data", func(t *testing.T) {
		sm := &StorageManager{
			store:         nil,
			maxInlineSize: 100,
		}

		input := "small string"

		// Should pass through when data is under the limit
		got, err := sm.Dehydrate(ctx, input, "run-1")
		if err != nil {
			t.Fatalf("Dehydrate() unexpected error = %v", err)
		}

		if !reflect.DeepEqual(got, input) {
			t.Errorf("Dehydrate() should pass through small data unchanged")
		}
	})
}

func TestStorageManager_Dehydrate_NestedMap(t *testing.T) {
	ctx := context.Background()
	mock := newMockStore()
	sm := &StorageManager{
		store:         mock,
		maxInlineSize: 20,
	}

	input := map[string]any{
		"small": "ok",
		"large": "This is a very long string that will be offloaded to storage",
	}

	got, err := sm.Dehydrate(ctx, input, "run-nested")
	if err != nil {
		t.Fatalf("Dehydrate() error = %v", err)
	}

	resultMap, ok := got.(map[string]any)
	if !ok {
		t.Fatal("Expected result to be a map")
	}

	// Small value should stay inline
	if resultMap["small"] != "ok" {
		t.Error("Small value should stay inline")
	}

	// Large value should be a reference
	if largeRef, ok := resultMap["large"].(map[string]any); ok {
		if _, hasRef := largeRef["$bubuStorageRef"]; !hasRef {
			t.Error("Large value should be offloaded")
		}
	} else {
		t.Error("Large value should be a storage reference")
	}
}

func TestStorageManager_RoundTrip(t *testing.T) {
	ctx := context.Background()
	mock := newMockStore()
	sm := &StorageManager{
		store:         mock,
		maxInlineSize: 30,
	}

	original := map[string]any{
		"data": "This is a string that is definitely longer than the inline size limit",
	}

	// Dehydrate
	dehydrated, err := sm.Dehydrate(ctx, original, "roundtrip-test")
	if err != nil {
		t.Fatalf("Dehydrate() error = %v", err)
	}

	// Hydrate
	hydrated, err := sm.Hydrate(ctx, dehydrated)
	if err != nil {
		t.Fatalf("Hydrate() error = %v", err)
	}

	// Should match original
	if !reflect.DeepEqual(hydrated, original) {
		t.Errorf("Round trip failed: got %v, want %v", hydrated, original)
	}
}

func TestStorageManager_GetStore(t *testing.T) {
	mock := newMockStore()
	sm := &StorageManager{
		store: mock,
	}

	if sm.GetStore() != mock {
		t.Error("GetStore() should return the underlying store")
	}
}

func TestStoredObject(t *testing.T) {
	// Test JSON marshaling/unmarshaling of StoredObject
	obj := StoredObject{
		ContentType: "json",
		Data:        json.RawMessage(`{"key":"value"}`),
	}

	marshaled, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("Marshal error = %v", err)
	}

	var unmarshaled StoredObject
	if err := json.Unmarshal(marshaled, &unmarshaled); err != nil {
		t.Fatalf("Unmarshal error = %v", err)
	}

	if unmarshaled.ContentType != obj.ContentType {
		t.Errorf("ContentType = %v, want %v", unmarshaled.ContentType, obj.ContentType)
	}

	if !bytes.Equal(unmarshaled.Data, obj.Data) {
		t.Errorf("Data = %v, want %v", unmarshaled.Data, obj.Data)
	}
}

// Tests for NewManager
func TestNewManager_FileStore(t *testing.T) {
	tmpDir := t.TempDir()

	os.Setenv("BUBU_STORAGE_PROVIDER", "file")
	os.Setenv("BUBU_STORAGE_PATH", tmpDir)
	defer os.Unsetenv("BUBU_STORAGE_PROVIDER")
	defer os.Unsetenv("BUBU_STORAGE_PATH")

	ctx := context.Background()
	manager, err := NewManager(ctx)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	if manager == nil {
		t.Fatal("NewManager() returned nil manager")
	}

	if manager.store == nil {
		t.Error("NewManager() did not initialize file store")
	}

	if manager.maxInlineSize != DefaultMaxInlineSize {
		t.Errorf("NewManager() maxInlineSize = %d, want %d", manager.maxInlineSize, DefaultMaxInlineSize)
	}
}

func TestNewManager_NoProvider(t *testing.T) {
	os.Unsetenv("BUBU_STORAGE_PROVIDER")
	os.Unsetenv("BUBU_STORAGE_PATH")

	ctx := context.Background()
	manager, err := NewManager(ctx)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	if manager.store != nil {
		t.Error("NewManager() should have nil store when no provider is set")
	}
}

func TestNewManager_FileProviderMissingPath(t *testing.T) {
	ctx := context.Background()

	// Set file provider but don't set path
	os.Setenv("BUBU_STORAGE_PROVIDER", "file")
	os.Unsetenv("BUBU_STORAGE_PATH")
	defer os.Unsetenv("BUBU_STORAGE_PROVIDER")

	_, err := NewManager(ctx)
	if err == nil {
		t.Fatal("NewManager() should error when BUBU_STORAGE_PROVIDER=file but BUBU_STORAGE_PATH is not set")
	}

	expectedMsg := "BUBU_STORAGE_PROVIDER is set to 'file' but BUBU_STORAGE_PATH is not set"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message %q, got: %v", expectedMsg, err)
	}
}

func TestNewManager_CustomMaxInlineSize(t *testing.T) {
	os.Setenv("BUBU_MAX_INLINE_SIZE", "1024")
	defer os.Unsetenv("BUBU_MAX_INLINE_SIZE")

	ctx := context.Background()
	manager, err := NewManager(ctx)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	if manager.maxInlineSize != 1024 {
		t.Errorf("NewManager() maxInlineSize = %d, want 1024", manager.maxInlineSize)
	}
}

func TestNewManager_InvalidMaxInlineSize(t *testing.T) {
	os.Setenv("BUBU_MAX_INLINE_SIZE", "not-a-number")
	defer os.Unsetenv("BUBU_MAX_INLINE_SIZE")

	ctx := context.Background()
	manager, err := NewManager(ctx)
	if err == nil {
		t.Fatalf("NewManager() expected error for invalid BUBU_MAX_INLINE_SIZE, got nil")
	}
	if manager != nil {
		t.Errorf("NewManager() expected nil manager on error, got %v", manager)
	}
	expectedMsg := "invalid BUBU_MAX_INLINE_SIZE 'not-a-number': must be a positive integer"
	if err.Error() != expectedMsg {
		t.Errorf("NewManager() error = %v, want %v", err.Error(), expectedMsg)
	}
}

func TestNewManager_FileStore_InvalidPath(t *testing.T) {
	os.Setenv("BUBU_STORAGE_PROVIDER", "file")
	os.Setenv("BUBU_STORAGE_PATH", "/this/path/does/not/exist")
	defer os.Unsetenv("BUBU_STORAGE_PROVIDER")
	defer os.Unsetenv("BUBU_STORAGE_PATH")

	ctx := context.Background()
	_, err := NewManager(ctx)
	if err == nil {
		t.Error("NewManager() should return error for invalid path")
	}
}

func TestNewManager_S3Store_NoCredentials(t *testing.T) {
	// Unset any AWS variables to ensure a clean slate
	os.Unsetenv("AWS_ACCESS_KEY_ID")
	os.Unsetenv("AWS_SECRET_ACCESS_KEY")
	os.Unsetenv("AWS_SESSION_TOKEN")
	os.Unsetenv("AWS_REGION")

	os.Setenv("BUBU_STORAGE_PROVIDER", "s3")
	defer os.Unsetenv("BUBU_STORAGE_PROVIDER")

	ctx := context.Background()
	_, err := NewManager(ctx)
	if err == nil {
		t.Error("NewManager() should return an error when S3 provider is selected but no credentials or region are configured")
	}
}

func TestStorageManager_Dehydrate_SliceElements(t *testing.T) {
	ctx := context.Background()
	mock := newMockStore()
	sm := &StorageManager{
		store:         mock,
		maxInlineSize: 20,
		outputPrefix:  "outputs",
	}

	longString := "this string is definitely longer than twenty characters"
	input := []any{
		"short",
		longString,
		map[string]any{"key": "value"}, // small map
		map[string]any{"key": longString},
	}

	dehydrated, err := sm.Dehydrate(ctx, input, "slice-run-id")
	if err != nil {
		t.Fatalf("Dehydrate() error = %v", err)
	}

	dehydratedSlice, ok := dehydrated.([]any)
	if !ok {
		t.Fatalf("Expected result to be a slice, got %T", dehydrated)
	}

	if len(dehydratedSlice) != 4 {
		t.Fatalf("Expected slice of length 4, got %d", len(dehydratedSlice))
	}

	// 1. Check the small string
	if dehydratedSlice[0] != "short" {
		t.Errorf("Expected first element to be 'short', got %v", dehydratedSlice[0])
	}

	// 2. Check the long string
	ref1, ok := dehydratedSlice[1].(map[string]any)
	if !ok || ref1["$bubuStorageRef"] == nil {
		t.Errorf("Expected second element to be a storage ref, got %v", dehydratedSlice[1])
	}

	// 3. Check the small map
	if !reflect.DeepEqual(dehydratedSlice[2], map[string]any{"key": "value"}) {
		t.Errorf("Expected third element to be the original small map, got %v", dehydratedSlice[2])
	}

	// 4. Check the map containing a large string
	mapWithRef, ok := dehydratedSlice[3].(map[string]any)
	if !ok {
		t.Fatalf("Expected fourth element to be a map, got %T", dehydratedSlice[3])
	}
	nestedRef, ok := mapWithRef["key"].(map[string]any)
	if !ok || nestedRef["$bubuStorageRef"] == nil {
		t.Errorf("Expected nested value in map to be a storage ref, got %v", mapWithRef["key"])
	}

	// We expect two writes: one for the standalone long string and one for the long string inside the map
	if mock.writes != 2 {
		t.Errorf("Expected 2 writes to the store, got %d", mock.writes)
	}
}
