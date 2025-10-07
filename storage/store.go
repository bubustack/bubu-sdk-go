package storage

import (
	"context"
	"io"
)

// Store is the interface for a generic storage backend.
// It provides a streaming Read/Write interface.
type Store interface {
	// Write saves the data from the reader to the storage backend at the specified path.
	Write(ctx context.Context, path string, reader io.Reader) error

	// Read retrieves the data from the storage backend at the specified path and writes it to the writer.
	Read(ctx context.Context, path string, writer io.Writer) error
}
