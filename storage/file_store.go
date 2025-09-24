package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// FileStore implements the Store interface for a local filesystem.
type FileStore struct {
	basePath string
}

// NewFileStore creates a new FileStore.
// It requires a base path where all files will be stored.
func NewFileStore(basePath string) (*FileStore, error) {
	if basePath == "" {
		return nil, fmt.Errorf("file store base path cannot be empty")
	}
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("storage path '%s' provided but does not exist", basePath)
	}
	return &FileStore{basePath: basePath}, nil
}

// Write saves data from a reader to a file at the given path relative to the basePath.
func (fs *FileStore) Write(_ context.Context, path string, reader io.Reader) error {
	fullPath := filepath.Join(fs.basePath, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory for file store: %w", err)
	}
	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create file for writing: %w", err)
	}
	defer file.Close()
	_, err = io.Copy(file, reader)
	return err
}

// Read retrieves data from a file and writes it to a writer.
func (fs *FileStore) Read(_ context.Context, path string, writer io.Writer) error {
	fullPath := filepath.Join(fs.basePath, path)
	file, err := os.Open(fullPath)
	if err != nil {
		return fmt.Errorf("failed to open file for reading: %w", err)
	}
	defer file.Close()
	_, err = io.Copy(writer, file)
	return err
}
