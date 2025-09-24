package storage

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Store implements the Store interface for an S3-compatible object store.
type S3Store struct {
	client *s3.Client
	bucket string
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

	// Prioritize our custom region env var, but default to a safe value for S3-compatibles.
	if region := os.Getenv("BUBU_STORAGE_S3_REGION"); region != "" {
		cfg.Region = region
	} else if cfg.Region == "" {
		fmt.Println("BUBU SDK DIAGNOSTIC: No S3 region configured. Defaulting to 'us-east-1'.")
		cfg.Region = "us-east-1"
	}

	endpoint := os.Getenv("BUBU_STORAGE_S3_ENDPOINT")
	if endpoint != "" {
		cfg.BaseEndpoint = aws.String(endpoint)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// Use path-style addressing for MinIO compatibility
		o.UsePathStyle = true
	})

	return &S3Store{
		client: client,
		bucket: bucket,
	}, nil
}

// Write uploads data to S3 from a reader.
func (s *S3Store) Write(ctx context.Context, path string, reader io.Reader) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &path,
		Body:   reader,
	})
	if err != nil {
		return fmt.Errorf("failed to put object to s3: %w", err)
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
		return fmt.Errorf("failed to get object from s3: %w", err)
	}
	defer out.Body.Close()

	_, err = io.Copy(writer, out.Body)
	return err
}
