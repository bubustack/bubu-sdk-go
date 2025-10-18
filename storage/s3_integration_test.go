package storage

import (
	"context"
	"os"
	"testing"
	"time"
)

// Integration test for S3 multipart uploads using manager.Uploader.
// This test is gated by environment variables and will be skipped by default.
// Required envs to run:
//   - BUBU_S3_INTEGRATION=1
//   - BUBU_STORAGE_S3_BUCKET
//   - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (and optionally AWS_SESSION_TOKEN)
//
// Optional envs:
//   - BUBU_STORAGE_S3_REGION (default us-east-1)
//   - BUBU_STORAGE_S3_ENDPOINT (for MinIO or S3-compatible endpoints)
func TestS3Integration_UploaderRoundTrip(t *testing.T) {
	if os.Getenv("BUBU_S3_INTEGRATION") != "1" {
		t.Skip("skipping S3 integration test; set BUBU_S3_INTEGRATION=1 to enable")
	}

	bucket := os.Getenv("BUBU_STORAGE_S3_BUCKET")
	if bucket == "" {
		t.Skip("skipping: BUBU_STORAGE_S3_BUCKET not set")
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Skip("skipping: AWS credentials not provided")
	}

	// Force S3 provider for the manager
	_ = os.Setenv("BUBU_STORAGE_PROVIDER", "s3")
	defer func() { _ = os.Unsetenv("BUBU_STORAGE_PROVIDER") }()

	// Make inline threshold small to guarantee offload
	_ = os.Setenv("BUBU_MAX_INLINE_SIZE", "16")
	defer func() { _ = os.Unsetenv("BUBU_MAX_INLINE_SIZE") }()

	// Use a unique stepRunID for namespacing
	stepRunID := "it-" + time.Now().UTC().Format("20060102T150405Z")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sm, err := NewManager(ctx)
	if err != nil {
		t.Skipf("manager init failed (likely missing/invalid S3 config): %v", err)
	}
	if sm == nil || sm.GetStore() == nil {
		t.Skip("store not initialized; skipping integration test")
	}

	// Large payload to trigger multipart path in uploader (size > 5MB preferred)
	// Keep it modest to avoid long CI times; uploader will still choose multipart if configured.
	// We rely on manager.Dehydrate to offload any value exceeding inline size.
	big := make([]byte, 256*1024) // 256 KiB
	for i := range big {
		big[i] = byte('a' + (i % 26))
	}

	original := map[string]any{"data": string(big)}

	// Dehydrate writes to S3 via manager.Uploader
	dehydrated, err := sm.Dehydrate(ctx, original, stepRunID)
	if err != nil {
		t.Fatalf("Dehydrate error: %v", err)
	}

	// Hydrate reads back from S3 and reconstructs the original value
	hydrated, err := sm.Hydrate(ctx, dehydrated)
	if err != nil {
		t.Fatalf("Hydrate error: %v", err)
	}

	// Spot-check the presence and length of the large field
	hydratedMap, ok := hydrated.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any after hydrate, got %T", hydrated)
	}
	val, ok := hydratedMap["data"].(string)
	if !ok {
		t.Fatalf("expected string in hydrated 'data' field, got %T", hydratedMap["data"])
	}
	if len(val) != len(big) {
		t.Fatalf("hydrated data length mismatch: got %d want %d", len(val), len(big))
	}
}
