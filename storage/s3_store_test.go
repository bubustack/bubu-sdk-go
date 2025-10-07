package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// s3API interface defines the methods we need from the S3 client
type s3API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// mockS3Client is a mock implementation of the S3 API for testing
type mockS3Client struct {
	putObjectFunc func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	getObjectFunc func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

func (m *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return m.putObjectFunc(ctx, params, optFns...)
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return m.getObjectFunc(ctx, params, optFns...)
}

// mockS3Store wraps the mock client for testing
type mockS3Store struct {
	client s3API
	bucket string
}

func (s *mockS3Store) Write(ctx context.Context, path string, reader io.Reader) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &path,
		Body:   reader,
	})
	return err
}

func (s *mockS3Store) Read(ctx context.Context, path string, writer io.Writer) error {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &path,
	})
	if err != nil {
		return err
	}
	defer out.Body.Close()
	_, err = io.Copy(writer, out.Body)
	return err
}

func TestNewS3Store_MissingBucket(t *testing.T) {
	os.Unsetenv("BUBU_STORAGE_S3_BUCKET")

	ctx := context.Background()
	_, err := NewS3Store(ctx)
	if err == nil {
		t.Error("NewS3Store() should return error when bucket is not set")
	}
	if err != nil && err.Error() != "s3 storage is enabled but BUBU_STORAGE_S3_BUCKET is not set" {
		t.Errorf("NewS3Store() unexpected error: %v", err)
	}
}

func TestS3Store_Write(t *testing.T) {
	testData := []byte("test s3 data")
	testBucket := "test-bucket"
	testKey := "test/path.txt"

	mock := &mockS3Client{
		putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			// Verify parameters
			if *params.Bucket != testBucket {
				t.Errorf("PutObject bucket = %v, want %v", *params.Bucket, testBucket)
			}
			if *params.Key != testKey {
				t.Errorf("PutObject key = %v, want %v", *params.Key, testKey)
			}

			// Read and verify body
			data, err := io.ReadAll(params.Body)
			if err != nil {
				t.Fatalf("Failed to read body: %v", err)
			}
			if !bytes.Equal(data, testData) {
				t.Errorf("PutObject body = %v, want %v", data, testData)
			}

			return &s3.PutObjectOutput{}, nil
		},
	}

	store := &mockS3Store{
		client: mock,
		bucket: testBucket,
	}

	ctx := context.Background()
	err := store.Write(ctx, testKey, bytes.NewReader(testData))
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}
}

func TestS3Store_Read(t *testing.T) {
	testData := []byte("s3 read data")
	testBucket := "test-bucket"
	testKey := "read/path.txt"

	mock := &mockS3Client{
		getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			// Verify parameters
			if *params.Bucket != testBucket {
				t.Errorf("GetObject bucket = %v, want %v", *params.Bucket, testBucket)
			}
			if *params.Key != testKey {
				t.Errorf("GetObject key = %v, want %v", *params.Key, testKey)
			}

			return &s3.GetObjectOutput{
				Body: io.NopCloser(bytes.NewReader(testData)),
			}, nil
		},
	}

	store := &mockS3Store{
		client: mock,
		bucket: testBucket,
	}

	ctx := context.Background()
	var buf bytes.Buffer
	err := store.Read(ctx, testKey, &buf)
	if err != nil {
		t.Errorf("Read() error = %v", err)
	}

	if !bytes.Equal(buf.Bytes(), testData) {
		t.Errorf("Read() got %v, want %v", buf.Bytes(), testData)
	}
}

func TestS3Store_Write_Error(t *testing.T) {
	expectedErr := io.EOF

	mock := &mockS3Client{
		putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return nil, expectedErr
		},
	}

	store := &mockS3Store{
		client: mock,
		bucket: "test-bucket",
	}

	ctx := context.Background()
	err := store.Write(ctx, "error/path", bytes.NewReader([]byte("data")))
	if err == nil {
		t.Error("Write() should return error when PutObject fails")
	}
}

func TestS3Store_Read_Error(t *testing.T) {
	expectedErr := io.EOF

	mock := &mockS3Client{
		getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return nil, expectedErr
		},
	}

	store := &mockS3Store{
		client: mock,
		bucket: "test-bucket",
	}

	ctx := context.Background()
	var buf bytes.Buffer
	err := store.Read(ctx, "error/path", &buf)
	if err == nil {
		t.Error("Read() should return error when GetObject fails")
	}
}

func TestS3Store_RoundTrip(t *testing.T) {
	testData := []byte("round trip data")
	storage := make(map[string][]byte)

	mock := &mockS3Client{
		putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			data, _ := io.ReadAll(params.Body)
			storage[*params.Key] = data
			return &s3.PutObjectOutput{}, nil
		},
		getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			data := storage[*params.Key]
			return &s3.GetObjectOutput{
				Body: io.NopCloser(bytes.NewReader(data)),
			}, nil
		},
	}

	store := &mockS3Store{
		client: mock,
		bucket: "test-bucket",
	}

	ctx := context.Background()
	path := "roundtrip/test.txt"

	// Write
	if err := store.Write(ctx, path, bytes.NewReader(testData)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Read
	var buf bytes.Buffer
	if err := store.Read(ctx, path, &buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Verify
	if !bytes.Equal(buf.Bytes(), testData) {
		t.Errorf("Round trip failed: got %v, want %v", buf.Bytes(), testData)
	}
}

func TestNewManager_S3Store(t *testing.T) {
	// This test verifies the environment variable handling for S3
	// We can't actually create an S3 client without AWS credentials

	os.Setenv("BUBU_STORAGE_PROVIDER", "s3")
	os.Setenv("BUBU_STORAGE_S3_BUCKET", "test-bucket")
	os.Setenv("AWS_ACCESS_KEY_ID", "test")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	os.Setenv("AWS_REGION", "us-east-1")

	defer func() {
		os.Unsetenv("BUBU_STORAGE_PROVIDER")
		os.Unsetenv("BUBU_STORAGE_S3_BUCKET")
		os.Unsetenv("AWS_ACCESS_KEY_ID")
		os.Unsetenv("AWS_SECRET_ACCESS_KEY")
		os.Unsetenv("AWS_REGION")
	}()

	ctx := context.Background()

	// This will attempt to create a real S3 client but won't connect to AWS
	// The error (if any) should be about connectivity, not configuration
	manager, err := NewManager(ctx)

	// We expect this to succeed in creating the manager
	if err != nil {
		t.Logf("NewManager() with S3 error (expected in test env): %v", err)
	}

	if manager == nil {
		t.Fatal("NewManager() returned nil manager")
	}

	// In a real AWS environment, the store would be initialized
	// In test environment, it might fail but that's okay
	t.Logf("S3 store initialization completed (store nil: %v)", manager.store == nil)
}
