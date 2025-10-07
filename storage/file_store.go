package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

// contextReader wraps an io.Reader and returns ctx.Err() when the context is canceled.
type contextReader struct {
	ctx context.Context
	r   io.Reader
}

// Read implements io.Reader with context cancellation support.
func (cr *contextReader) Read(p []byte) (int, error) {
	select {
	case <-cr.ctx.Done():
		return 0, cr.ctx.Err()
	default:
		return cr.r.Read(p)
	}
}

// contextWriter wraps an io.Writer and returns ctx.Err() when the context is canceled.
type contextWriter struct {
	ctx context.Context
	w   io.Writer
}

// Write implements io.Writer with context cancellation support.
func (cw *contextWriter) Write(p []byte) (int, error) {
	select {
	case <-cw.ctx.Done():
		return 0, cw.ctx.Err()
	default:
		return cw.w.Write(p)
	}
}

// Write saves data from a reader to a file at the given path relative to the basePath.
func (fs *FileStore) Write(ctx context.Context, path string, reader io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Validate that joined path is within basePath to prevent path traversal
	fullPath := filepath.Join(fs.basePath, path)
	relPath, err := filepath.Rel(fs.basePath, fullPath)
	if err != nil || strings.HasPrefix(relPath, ".."+string(filepath.Separator)) || filepath.IsAbs(relPath) {
		return fmt.Errorf("invalid storage path: path traversal detected (requested '%s', resolved to '%s')", path, fullPath)
	}
	// Additional safety: ensure fullPath is still within basePath after Clean
	if !strings.HasPrefix(filepath.Clean(fullPath), filepath.Clean(fs.basePath)) {
		return fmt.Errorf("invalid storage path: resolved path '%s' escapes base path '%s'", fullPath, fs.basePath)
	}
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory '%s' for file store: %w", filepath.Dir(fullPath), err)
	}

	// Write to a temporary file first to ensure atomicity.
	tempFile, err := os.CreateTemp(filepath.Dir(fullPath), ".tmp-"+filepath.Base(path)+"-")
	if err != nil {
		return fmt.Errorf("failed to create temporary file for writing: %w", err)
	}
	defer func() {
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name()) // Clean up temp file on exit.
	}()

	// Wrap reader to be context-aware so cancellation interrupts long writes
	cr := &contextReader{ctx: ctx, r: reader}
	if _, err = io.Copy(tempFile, cr); err != nil {
		return fmt.Errorf("failed to write to temporary file '%s': %w", tempFile.Name(), err)
	}

	// Close the file before renaming.
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file '%s': %w", tempFile.Name(), err)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	// Atomically rename the temporary file to the final destination.
	if err := os.Rename(tempFile.Name(), fullPath); err != nil {
		return fmt.Errorf("failed to atomically move temporary file to '%s': %w", fullPath, err)
	}

	return nil
}

// Read retrieves data from a file and writes it to a writer.
func (fs *FileStore) Read(ctx context.Context, path string, writer io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Validate that joined path is within basePath to prevent path traversal
	fullPath := filepath.Join(fs.basePath, path)
	relPath, err := filepath.Rel(fs.basePath, fullPath)
	if err != nil || strings.HasPrefix(relPath, ".."+string(filepath.Separator)) || filepath.IsAbs(relPath) {
		return fmt.Errorf("invalid storage path: path traversal detected (requested '%s', resolved to '%s')", path, fullPath)
	}
	// Additional safety: ensure fullPath is still within basePath after Clean
	if !strings.HasPrefix(filepath.Clean(fullPath), filepath.Clean(fs.basePath)) {
		return fmt.Errorf("invalid storage path: resolved path '%s' escapes base path '%s'", fullPath, fs.basePath)
	}

	file, err := os.Open(fullPath)
	if err != nil {
		return fmt.Errorf("failed to open file '%s' for reading: %w", fullPath, err)
	}
	defer file.Close()
	// Wrap writer to be context-aware so cancellation interrupts long reads
	cw := &contextWriter{ctx: ctx, w: writer}
	_, err = io.Copy(cw, file)
	return err
}
