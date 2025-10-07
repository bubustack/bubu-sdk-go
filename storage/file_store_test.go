package storage

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestNewFileStore(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir := t.TempDir()

	tests := []struct {
		name     string
		basePath string
		wantErr  bool
	}{
		{
			name:     "valid base path",
			basePath: tmpDir,
			wantErr:  false,
		},
		{
			name:     "empty base path",
			basePath: "",
			wantErr:  true,
		},
		{
			name:     "non-existent path",
			basePath: filepath.Join(tmpDir, "does-not-exist"),
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewFileStore(tt.basePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewFileStore() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && got == nil {
				t.Error("NewFileStore() returned nil without error")
			}
			if err == nil && got.basePath != tt.basePath {
				t.Errorf("NewFileStore() basePath = %v, want %v", got.basePath, tt.basePath)
			}
		})
	}
}

func TestFileStore_Write(t *testing.T) {
	tmpDir := t.TempDir()
	fs, err := NewFileStore(tmpDir)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}

	ctx := context.Background()
	testData := []byte("test content")
	testPath := "test/file.txt"

	err = fs.Write(ctx, testPath, bytes.NewReader(testData))
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}

	// Verify the file was created
	fullPath := filepath.Join(tmpDir, testPath)
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		t.Error("Write() did not create file")
	}

	// Verify the content
	content, err := os.ReadFile(fullPath)
	if err != nil {
		t.Fatalf("Failed to read written file: %v", err)
	}
	if !bytes.Equal(content, testData) {
		t.Errorf("Write() wrote %v, want %v", content, testData)
	}
}

func TestFileStore_Write_CreatesDirectories(t *testing.T) {
	tmpDir := t.TempDir()
	fs, err := NewFileStore(tmpDir)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}

	ctx := context.Background()
	testData := []byte("nested content")
	testPath := "deep/nested/path/file.txt"

	err = fs.Write(ctx, testPath, bytes.NewReader(testData))
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}

	// Verify the nested directories were created
	fullPath := filepath.Join(tmpDir, testPath)
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		t.Error("Write() did not create nested directories")
	}
}

func TestFileStore_Read(t *testing.T) {
	tmpDir := t.TempDir()
	fs, err := NewFileStore(tmpDir)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}

	ctx := context.Background()
	testData := []byte("test content for reading")
	testPath := "read/test.txt"

	// First write a file
	if err := fs.Write(ctx, testPath, bytes.NewReader(testData)); err != nil {
		t.Fatalf("Setup Write() error = %v", err)
	}

	// Now read it back
	var buf bytes.Buffer
	err = fs.Read(ctx, testPath, &buf)
	if err != nil {
		t.Errorf("Read() error = %v", err)
	}

	if !bytes.Equal(buf.Bytes(), testData) {
		t.Errorf("Read() got %v, want %v", buf.Bytes(), testData)
	}
}

func TestFileStore_Read_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	fs, err := NewFileStore(tmpDir)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}

	ctx := context.Background()
	var buf bytes.Buffer

	err = fs.Read(ctx, "does-not-exist.txt", &buf)
	if err == nil {
		t.Error("Read() should return error for non-existent file")
	}
}

func TestFileStore_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	fs, err := NewFileStore(tmpDir)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}

	ctx := context.Background()
	testCases := []struct {
		name string
		data []byte
		path string
	}{
		{
			name: "simple text",
			data: []byte("hello world"),
			path: "simple.txt",
		},
		{
			name: "binary data",
			data: []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD},
			path: "binary.dat",
		},
		{
			name: "large content",
			data: bytes.Repeat([]byte("a"), 10000),
			path: "large.txt",
		},
		{
			name: "empty file",
			data: []byte{},
			path: "empty.txt",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Write
			if err := fs.Write(ctx, tc.path, bytes.NewReader(tc.data)); err != nil {
				t.Fatalf("Write() error = %v", err)
			}

			// Read
			var buf bytes.Buffer
			if err := fs.Read(ctx, tc.path, &buf); err != nil {
				t.Fatalf("Read() error = %v", err)
			}

			// Verify
			if !bytes.Equal(buf.Bytes(), tc.data) {
				t.Errorf("Round trip failed: got %d bytes, want %d bytes", len(buf.Bytes()), len(tc.data))
			}
		})
	}
}
