package storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Store implements the Store interface for an S3-compatible object store.
// It supports standard AWS S3 and S3-compatible services like MinIO.
// Uses manager.Uploader for automatic multipart uploads on large files.
type S3Store struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

// getS3RetryConfig returns retry configuration from env or defaults
func getS3RetryConfig() (maxAttempts int, maxBackoff time.Duration) {
	maxAttempts = 3 // Default
	if v := os.Getenv("BUBU_STORAGE_S3_MAX_RETRIES"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			maxAttempts = i
		}
	}

	maxBackoff = 10 * time.Second // Default
	if v := os.Getenv("BUBU_STORAGE_S3_MAX_BACKOFF"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			maxBackoff = d
		}
	}

	return maxAttempts, maxBackoff
}

// getS3UsePathStyle determines whether to use path-style addressing for S3.
// - Path-style: https://s3.amazonaws.com/bucket/key (required for MinIO, Ceph)
// - Virtual-hosted-style: https://bucket.s3.amazonaws.com/key (AWS S3 standard, required for TLS)
//
// Logic:
// 1. If BUBU_STORAGE_S3_USE_PATH_STYLE is set, use that value
// 2. If BUBU_STORAGE_S3_ENDPOINT is set (custom endpoint → MinIO/Ceph), default to true
// 3. Otherwise (AWS S3), default to false (virtual-hosted)
func getS3UsePathStyle() bool {
	if v := os.Getenv("BUBU_STORAGE_S3_USE_PATH_STYLE"); v != "" {
		return v == "true"
	}

	// Auto-detect: custom endpoints (MinIO, Ceph) typically require path-style
	endpoint := os.Getenv("BUBU_STORAGE_S3_ENDPOINT")
	if endpoint != "" {
		return true
	}

	// Default to virtual-hosted-style for AWS S3 (TLS compatibility)
	return false
}

// NewS3Store creates a new S3Store.
// It automatically configures the S3 client from environment variables.
func NewS3Store(ctx context.Context) (*S3Store, error) {
	bucket := os.Getenv("BUBU_STORAGE_S3_BUCKET")
	if bucket == "" {
		return nil, fmt.Errorf("s3 storage is enabled but BUBU_STORAGE_S3_BUCKET is not set")
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Configure retry strategy with exponential backoff
	maxAttempts, maxBackoff := getS3RetryConfig()
	cfg.Retryer = func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = maxAttempts
			o.MaxBackoff = maxBackoff
		})
	}

	// Create a custom HTTP client with configurable timeout
	// Default 5min accommodates large file uploads (e.g., 100MB model weights)
	timeout := 5 * time.Minute
	if v := os.Getenv("BUBU_STORAGE_S3_HTTP_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			timeout = d
		}
	}
	httpClient := &http.Client{
		Timeout: timeout,
	}

	// Prioritize our custom region env var, but default to a safe value for S3-compatibles.
	if region := os.Getenv("BUBU_STORAGE_S3_REGION"); region != "" {
		cfg.Region = region
	} else if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}

	endpoint := os.Getenv("BUBU_STORAGE_S3_ENDPOINT")
	if endpoint != "" {
		cfg.BaseEndpoint = aws.String(endpoint)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// Use path-style for MinIO/Ceph, virtual-hosted for AWS S3 (TLS)
		o.UsePathStyle = getS3UsePathStyle()
		o.HTTPClient = httpClient
	})

	// Create uploader with automatic multipart for large files
	// Default part size: 5MB, concurrency: 5
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		// Part size for multipart uploads (default 5MB is good)
		if partSize := os.Getenv("BUBU_STORAGE_S3_PART_SIZE"); partSize != "" {
			if size, err := strconv.ParseInt(partSize, 10, 64); err == nil && size > 0 {
				u.PartSize = size
			}
		}
		// Concurrency for parallel uploads (default 5)
		if concurrency := os.Getenv("BUBU_STORAGE_S3_CONCURRENCY"); concurrency != "" {
			if n, err := strconv.Atoi(concurrency); err == nil && n > 0 {
				u.Concurrency = n
			}
		}
	})

	return &S3Store{
		client:   client,
		uploader: uploader,
		bucket:   bucket,
	}, nil
}

// Write uploads data to S3 from a reader.
// Uses manager.Uploader which automatically handles multipart uploads for large files.
// For files >5MB, it will automatically split into parts and upload in parallel.
func (s *S3Store) Write(ctx context.Context, path string, reader io.Reader) error {
	input := &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &path,
		Body:   reader,
	}

	// Server-side encryption controls (default to AES-256 if unset)
	if sse := os.Getenv("BUBU_STORAGE_S3_SSE"); sse == "kms" {
		algo := s3types.ServerSideEncryptionAwsKms
		input.ServerSideEncryption = algo
		if kmsKey := os.Getenv("BUBU_STORAGE_S3_SSE_KMS_KEY_ID"); kmsKey != "" {
			input.SSEKMSKeyId = &kmsKey
		}
	} else {
		// Default and explicit "s3"
		algo := s3types.ServerSideEncryptionAes256
		input.ServerSideEncryption = algo
	}

	// Use uploader which automatically handles:
	// - Small files: single PutObject
	// - Large files: automatic multipart upload with parallel parts
	// - No need to know file size upfront!
	_, err := s.uploader.Upload(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to upload object '%s' to s3 bucket '%s': %w", path, s.bucket, err)
	}
	return nil
}

// Read downloads data from S3 and writes it to a writer.
func (s *S3Store) Read(ctx context.Context, path string, writer io.Writer) error {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &path,
	})
	if err != nil {
		return fmt.Errorf("failed to get object '%s' from s3 bucket '%s': %w", path, s.bucket, err)
	}
	defer func() { _ = out.Body.Close() }()

	_, err = io.Copy(writer, out.Body)
	return err
}
